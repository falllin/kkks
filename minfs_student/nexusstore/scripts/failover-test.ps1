param([switch]$Build, [switch]$Nacos)
. (Join-Path $PSScriptRoot 'common.ps1')

function Find-KeyInShard([string]$Prefix, [string]$Shard, [string]$BaseUrl) {
    for ($attempt = 0; $attempt -lt 1000; $attempt++) {
        $candidate = "$Prefix-$attempt.bin"
        $route = Invoke-NexusHttp -Uri "$BaseUrl/api/route?key=$([Uri]::EscapeDataString($candidate))"
        Assert-NexusSuccess $route 'Find route'
        if ([string]($route.Text | ConvertFrom-Json).shard -eq $Shard) { return $candidate }
    }
    throw "Unable to generate a key mapped to $Shard."
}

function Assert-FileHash([string]$BaseUrl, [string]$Key, [string]$ExpectedHash) {
    $response = Invoke-NexusHttp -Uri (Get-NexusFileUri $BaseUrl $Key)
    Assert-NexusSuccess $response "Read $Key"
    if ((Get-NexusHash $response.Bytes) -ne $ExpectedHash) { throw "Content mismatch: $Key" }
}

Write-Host 'This test stops only verified demo processes and restores all three nodes in finally.'
& (Join-Path $PSScriptRoot 'start-cluster.ps1') -Build:$Build -Nacos:$Nacos
$prefix = '/demo/failover-' + [guid]::NewGuid().ToString('N')
$keyBefore = "$prefix-before.bin"
$keyAfter = $null
$keyMinority = $null
$bytesBefore = [byte[]]::new(1024)
$bytesAfter = [byte[]]::new(2048)
for ($i = 0; $i -lt $bytesBefore.Length; $i++) { $bytesBefore[$i] = [byte]($i % 256) }
for ($i = 0; $i -lt $bytesAfter.Length; $i++) { $bytesAfter[$i] = [byte](255 - ($i % 256)) }
$hashBefore = Get-NexusHash $bytesBefore
$hashAfter = Get-NexusHash $bytesAfter
$restored = $false
try {
    $base = 'http://127.0.0.1:8081'
    $route = Invoke-NexusHttp -Uri "$base/api/route?key=$([Uri]::EscapeDataString($keyBefore))"
    Assert-NexusSuccess $route 'Find route'
    $shard = [string]($route.Text | ConvertFrom-Json).shard
    $keyAfter = Find-KeyInShard "$prefix-after" $shard $base
    $keyMinority = Find-KeyInShard "$prefix-minority" $shard $base
    Assert-NexusSuccess (Invoke-NexusHttp -Method PUT -Uri (Get-NexusFileUri $base $keyBefore) -Body $bytesBefore) 'Write before failure'
    $oldLeader = Wait-NexusLeader -Shard $shard
    Write-Host "1. Committed initial file in $shard. Stop leader $oldLeader."
    Stop-NexusNode $oldLeader
    $survivors = @($script:NexusNodes | Where-Object { $_.Id -ne $oldLeader } | ForEach-Object { $_.Id })
    $newLeader = Wait-NexusLeader -Shard $shard -NodeIds $survivors
    $liveBase = "http://127.0.0.1:$((Get-NexusNode $newLeader).HttpPort)"
    Assert-FileHash $liveBase $keyBefore $hashBefore
    Assert-NexusSuccess (Invoke-NexusHttp -Method PUT -Uri (Get-NexusFileUri $liveBase $keyAfter) -Body $bytesAfter) 'Write after leader failure'
    Assert-FileHash $liveBase $keyAfter $hashAfter
    $leaderStatus = Get-NexusCluster $newLeader
    $committed = @($leaderStatus.shards | Where-Object { [string]$_.groupId -eq $shard })[0]
    $requiredIndex = [long]$committed.commitIndex
    Write-Host "2. PASS new leader $newLeader; old data survives and new writes succeed (commit $requiredIndex)."

    # Remove the new leader as well, leaving a single replica: no majority exists.
    Stop-NexusNode $newLeader
    $minorityNode = @($survivors | Where-Object { $_ -ne $newLeader })[0]
    $minorityBase = "http://127.0.0.1:$((Get-NexusNode $minorityNode).HttpPort)"
    $failedWrite = Invoke-NexusHttp -Method PUT -Uri (Get-NexusFileUri $minorityBase $keyMinority) -Body $bytesBefore
    if ($failedWrite.Status -ne 503) { throw "Without a majority, write must return HTTP 503; got $($failedWrite.Status)." }
    $failedRead = Invoke-NexusHttp -Uri (Get-NexusFileUri $minorityBase $keyBefore)
    if ($failedRead.Status -ne 503) { throw "Without a majority, strong read must return HTTP 503; got $($failedRead.Status)." }
    Write-Host "3. PASS only $minorityNode remains: write=503 and strong read=503."

    & (Join-Path $PSScriptRoot 'start-cluster.ps1') -Nacos:$Nacos
    Wait-NexusApplied -Shard $shard -MinimumIndex $requiredIndex
    foreach ($node in $script:NexusNodes) {
        $nodeBase = "http://127.0.0.1:$($node.HttpPort)"
        Assert-FileHash $nodeBase $keyBefore $hashBefore
        Assert-FileHash $nodeBase $keyAfter $hashAfter
    }
    $restored = $true
    Write-Host '4. PASS restarted replicas caught up; acknowledged files are readable through every node.'
}
finally {
    if (-not $restored) {
        try { & (Join-Path $PSScriptRoot 'start-cluster.ps1') -Nacos:$Nacos }
        catch { Write-Warning "Automatic restore failed. Run start-cluster.ps1 after inspecting logs: $_" }
    }
    foreach ($key in @($keyBefore, $keyAfter, $keyMinority)) {
        if ([string]::IsNullOrEmpty($key)) { continue }
        try {
            $cleanup = Invoke-NexusHttp -Method DELETE -Uri (Get-NexusFileUri 'http://127.0.0.1:8081' $key)
            if (($cleanup.Status -lt 200 -or $cleanup.Status -ge 300) -and $cleanup.Status -ne 404) {
                Write-Warning "Temporary demo key remains: $key (HTTP $($cleanup.Status))."
            }
        } catch { Write-Warning "Could not clean up temporary demo key ${key}: $_" }
    }
}
Write-Host 'Failover demo passed. Three nodes remain running. Use stop-cluster.ps1 to stop them.'
