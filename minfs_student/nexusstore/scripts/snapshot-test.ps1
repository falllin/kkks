param(
    [switch]$Build,
    [switch]$Nacos,
    [switch]$Local,
    [ValidateRange(8, 64)][int]$OverwriteCount = 24
)
. (Join-Path $PSScriptRoot 'common.ps1')
if ($Nacos -and $Local) { throw 'Choose either -Nacos or -Local.' }

function Get-SnapshotShardState([string]$NodeId, [string]$Shard) {
    $cluster = Get-NexusCluster $NodeId
    $states = @($cluster.shards | Where-Object { [string]$_.groupId -eq $Shard })
    if ($states.Count -ne 1) { throw "Expected one local state for $NodeId/$Shard." }
    return $states[0]
}

function Get-SnapshotNodeBase([string]$NodeId) {
    return "http://127.0.0.1:$((Get-NexusNode $NodeId).HttpPort)"
}

function Assert-SnapshotFile([string]$NodeId, [string]$Key, [string]$ExpectedHash) {
    $response = Invoke-NexusHttp -Uri (Get-NexusFileUri (Get-SnapshotNodeBase $NodeId) $Key)
    Assert-NexusSuccess $response "Read through $NodeId"
    if ((Get-NexusHash $response.Bytes) -ne $ExpectedHash) {
        throw "Content hash mismatch through $NodeId for $Key."
    }
}

function Wait-SnapshotInstalled([string]$NodeId, [string]$Shard, [long]$MinimumIndex, [int]$TimeoutSeconds = 60) {
    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastState = $null
    do {
        try {
            $lastState = Get-SnapshotShardState $NodeId $Shard
            if ([long]$lastState.snapshotIndex -ge $MinimumIndex -and
                [long]$lastState.lastApplied -ge $MinimumIndex) { return $lastState }
        } catch { }
        Start-Sleep -Milliseconds 300
    } while ([datetime]::UtcNow -lt $deadline)
    $observed = if ($null -eq $lastState) { 'status unavailable' } else {
        "snapshotIndex=$($lastState.snapshotIndex), lastApplied=$($lastState.lastApplied)"
    }
    throw "$NodeId did not install $Shard snapshot $MinimumIndex within $TimeoutSeconds seconds ($observed)."
}

Write-Host 'This test stops only verified demo processes, keeps persistent data, and restores all three nodes in finally.'
Write-Host 'With -Build, stop the cluster first so its running JAR is not overwritten.'
& (Join-Path $PSScriptRoot 'start-cluster.ps1') -Build:$Build -Nacos:$Nacos -Local:$Local
$key = '/demo/snapshot-' + [guid]::NewGuid().ToString('N') + '.bin'
$restored = $false
try {
    $base = Get-SnapshotNodeBase 'node1'
    $route = Invoke-NexusHttp -Uri "$base/api/route?key=$([Uri]::EscapeDataString($key))"
    Assert-NexusSuccess $route 'Find snapshot demo shard'
    $shard = [string]($route.Text | ConvertFrom-Json).shard
    $initialBytes = [Text.Encoding]::UTF8.GetBytes("$key initial value")
    Assert-NexusSuccess (Invoke-NexusHttp -Method PUT -Uri (Get-NexusFileUri $base $key) -Body $initialBytes) 'Write initial value'

    $leader = Wait-NexusLeader -Shard $shard
    $initialState = Get-SnapshotShardState $leader $shard
    # A local status read does not append a Raft GET entry.
    Wait-NexusApplied -Shard $shard -MinimumIndex ([long]$initialState.commitIndex)
    $follower = @($script:NexusNodes | Where-Object { $_.Id -ne $leader } | ForEach-Object { $_.Id })[0]
    $followerBefore = Get-SnapshotShardState $follower $shard
    if ($followerBefore.role -eq 'LEADER') { throw 'Leadership changed while selecting the lagging follower; retry the demo.' }
    $followerAppliedBefore = [long]$followerBefore.lastApplied
    Stop-NexusNode $follower
    $survivors = @($script:NexusNodes | Where-Object { $_.Id -ne $follower } | ForEach-Object { $_.Id })
    Write-Host "1. PASS initial value committed in $shard; stopped follower $follower at lastApplied=$followerAppliedBefore."

    $leader = Wait-NexusLeader -Shard $shard -NodeIds $survivors
    $leaderBase = Get-SnapshotNodeBase $leader
    # Existing data may already be close to the automatic snapshot threshold.
    # Start measurement at a fresh boundary, below the default 128-entry threshold.
    $baselineCompact = Invoke-NexusHttp -Method POST -Uri "$leaderBase/api/admin/compact" -TimeoutSeconds 60
    Assert-NexusSuccess $baselineCompact 'Compact before measuring snapshot progress'
    $baseline = Get-SnapshotShardState $leader $shard
    $baselineSnapshotIndex = [long]$baseline.snapshotIndex
    Write-Host "   Measurement baseline: snapshotIndex=$baselineSnapshotIndex, retained entries=$($baseline.retainedLogEntries)."
    $finalBytes = $initialBytes
    for ($version = 1; $version -le $OverwriteCount; $version++) {
        # Same key, different small payloads: the snapshot keeps only the current value.
        $finalBytes = [Text.Encoding]::UTF8.GetBytes("$key version=$version " + ('snapshot-demo-' * 64))
        $response = Invoke-NexusHttp -Method PUT -Uri (Get-NexusFileUri $leaderBase $key) -Body $finalBytes
        Assert-NexusSuccess $response "Overwrite $version/$OverwriteCount with one replica offline"
    }
    $expectedHash = Get-NexusHash $finalBytes
    $leader = Wait-NexusLeader -Shard $shard -NodeIds $survivors
    $leaderBase = Get-SnapshotNodeBase $leader
    $beforeCompact = Get-SnapshotShardState $leader $shard
    $compact = Invoke-NexusHttp -Method POST -Uri "$leaderBase/api/admin/compact" -TimeoutSeconds 60
    Assert-NexusSuccess $compact 'Compact local shard logs'
    $compactResult = $compact.Text | ConvertFrom-Json
    if ([string]$compactResult.nodeId -ne $leader) { throw 'Compaction response must describe the addressed local node.' }
    # Check compaction before any file GET, because strong GET appends another entry.
    $afterCompact = Get-SnapshotShardState $leader $shard
    $snapshotIndex = [long]$afterCompact.snapshotIndex
    $retainedBefore = [long]$beforeCompact.retainedLogEntries
    $retainedAfter = [long]$afterCompact.retainedLogEntries
    if ($snapshotIndex -le $baselineSnapshotIndex -or $snapshotIndex -le $followerAppliedBefore) {
        throw "Snapshot did not advance beyond both the measurement baseline and the offline follower (index=$snapshotIndex)."
    }
    if ($snapshotIndex -lt [long]$beforeCompact.commitIndex) {
        throw "Snapshot $snapshotIndex did not include all acknowledged writes (commit=$($beforeCompact.commitIndex))."
    }
    # A lower configured threshold can compact automatically during the writes.
    # Count progress by absolute indices, so a final no-op manual compact is valid.
    $appendedEntries = [long]$afterCompact.lastLogIndex - [long]$baseline.lastLogIndex
    $measuredEntries = [long]$afterCompact.lastLogIndex - $baselineSnapshotIndex
    $compactedEntries = $snapshotIndex - $baselineSnapshotIndex
    $automaticallyCompacted = [long]$beforeCompact.snapshotIndex - $baselineSnapshotIndex
    if ($appendedEntries -lt $OverwriteCount -or $compactedEntries -lt $OverwriteCount -or
        $retainedAfter -gt [Math]::Floor($measuredEntries / 4) -or
        ($retainedAfter + $compactedEntries) -ne $measuredEntries) {
        throw "Compaction did not substantially trim the measured log: appended=$appendedEntries, compacted=$compactedEntries, retained=$retainedAfter."
    }
    if ([long]$afterCompact.snapshotBytes -le 0) { throw 'Compaction produced no persisted snapshot bytes.' }
    Write-Host "2. PASS $OverwriteCount majority writes; $leader snapshotIndex $baselineSnapshotIndex -> $snapshotIndex; $compactedEntries entries compacted, $retainedAfter retained."
    Write-Host "   Entries compacted automatically during writes=$automaticallyCompacted; final manual compact retained entries $retainedBefore -> $retainedAfter."
    Write-Host "   Local logBytes $($beforeCompact.logBytes) -> $($afterCompact.logBytes); snapshotBytes=$($afterCompact.snapshotBytes)."

    & (Join-Path $PSScriptRoot 'start-cluster.ps1') -Nacos:$Nacos -Local:$Local
    $installed = Wait-SnapshotInstalled -NodeId $follower -Shard $shard -MinimumIndex $snapshotIndex
    Wait-NexusApplied -Shard $shard -MinimumIndex ([long]$afterCompact.commitIndex)
    Write-Host "3. PASS $follower installed snapshot locally: snapshotIndex=$($installed.snapshotIndex), lastApplied=$($installed.lastApplied)."
    foreach ($node in $script:NexusNodes) { Assert-SnapshotFile $node.Id $key $expectedHash }
    $currentLeader = Wait-NexusLeader -Shard $shard
    $readState = Get-SnapshotShardState $currentLeader $shard
    Wait-NexusApplied -Shard $shard -MinimumIndex ([long]$readState.commitIndex)
    Write-Host '4. PASS final SHA-256 matches through every node; local lastApplied values caught up after reads.'

    # Keep the restored follower running. The remaining two replicas need each other
    # for both the new leader election and the strong-read majority after this stop.
    Stop-NexusNode $leader
    $afterLeaderStop = @($script:NexusNodes | Where-Object { $_.Id -ne $leader } | ForEach-Object { $_.Id })
    $newLeader = Wait-NexusLeader -Shard $shard -NodeIds $afterLeaderStop
    foreach ($nodeId in $afterLeaderStop) { Assert-SnapshotFile $nodeId $key $expectedHash }
    Write-Host "5. PASS stopped snapshot sender $leader; elected $newLeader, and recovered replica $follower participates in the available majority."

    $required = Get-SnapshotShardState $newLeader $shard
    & (Join-Path $PSScriptRoot 'start-cluster.ps1') -Nacos:$Nacos -Local:$Local
    Wait-NexusApplied -Shard $shard -MinimumIndex ([long]$required.commitIndex)
    foreach ($node in $script:NexusNodes) { Assert-SnapshotFile $node.Id $key $expectedHash }
    $restored = $true
    Write-Host '6. PASS all three nodes restored and the latest file remains readable.'
}
finally {
    if (-not $restored) {
        try { & (Join-Path $PSScriptRoot 'start-cluster.ps1') -Nacos:$Nacos -Local:$Local }
        catch { Write-Warning "Automatic restore failed. Inspect logs and run start-cluster.ps1: $_" }
    }
    try {
        $cleanup = Invoke-NexusHttp -Method DELETE -Uri (Get-NexusFileUri (Get-SnapshotNodeBase 'node1') $key)
        if (($cleanup.Status -lt 200 -or $cleanup.Status -ge 300) -and $cleanup.Status -ne 404) {
            Write-Warning "Temporary demo key remains: $key (HTTP $($cleanup.Status))."
        }
    } catch { Write-Warning "Could not clean up temporary demo key ${key}: $_" }
}
Write-Host 'Snapshot demo passed. Three nodes remain running. Use stop-cluster.ps1 to stop them.'
