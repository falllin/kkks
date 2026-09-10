param([switch]$Build, [switch]$Nacos, [switch]$Local)
. (Join-Path $PSScriptRoot 'common.ps1')
if ($Nacos -and $Local) { throw 'Choose either -Nacos or -Local.' }

New-Item -ItemType Directory -Path $script:NexusRuntime -Force | Out-Null
if ($Build) {
    $running = @($script:NexusNodes | Where-Object { $null -ne (Get-NexusOwnedProcess $_.Id) })
    if ($running.Count -gt 0) { throw 'Stop this demo before -Build so the running JAR is not overwritten.' }
    & mvn -f (Join-Path $script:NexusDemoRoot 'pom.xml') package
    if ($LASTEXITCODE -ne 0) { throw "Maven build failed ($LASTEXITCODE)." }
}
if (-not (Test-Path -LiteralPath $script:NexusJar)) {
    throw "JAR not found: $($script:NexusJar). Run this script with -Build first."
}
# Validate all missing nodes before launching any process.
foreach ($node in $script:NexusNodes) {
    if ($null -eq (Get-NexusOwnedProcess $node.Id)) { Assert-NexusPortsFree $node }
    elseif ($Nacos -or $Local) {
        $record = Get-NexusRecord $node.Id
        if (($Nacos -and -not $record.nacos) -or ($Local -and $record.nacos)) {
            throw 'Stop the cluster before switching the Nacos/local profile.'
        }
    }
}
$started = [Collections.Generic.List[string]]::new()
try {
    foreach ($node in $script:NexusNodes) {
        if (Start-NexusNode -NodeId $node.Id -UseNacos $Nacos.IsPresent -UseSavedProfile (-not $Local.IsPresent)) { $started.Add($node.Id) }
    }
    Wait-NexusHealthy
    foreach ($shard in @('s0', 's1', 's2', 's3')) {
        $leader = Wait-NexusLeader -Shard $shard
        Write-Host "$shard leader: $leader"
    }
    Write-Host 'Cluster ready: http://127.0.0.1:8081/api/cluster (also ports 8082, 8083).'
    Write-Host "Logs/PID records: $($script:NexusRuntime)"
}
catch {
    foreach ($nodeId in $started) {
        try { Stop-NexusNode $nodeId } catch { Write-Warning "Rollback failed for ${nodeId}: $_" }
    }
    throw
}
