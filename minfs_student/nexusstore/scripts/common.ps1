Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:NexusDemoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$script:NexusRuntime = Join-Path $script:NexusDemoRoot '.runtime'
$script:NexusJar = Join-Path $script:NexusDemoRoot 'nexus-server\target\nexus-server-1.0.0.jar'
$script:NexusNodes = @(
    [pscustomobject]@{ Id = 'node1'; HttpPort = 8081; RpcPort = 9091 },
    [pscustomobject]@{ Id = 'node2'; HttpPort = 8082; RpcPort = 9092 },
    [pscustomobject]@{ Id = 'node3'; HttpPort = 8083; RpcPort = 9093 }
)
Add-Type -AssemblyName System.Net.Http

function Get-NexusNode([string]$NodeId) {
    $node = @($script:NexusNodes | Where-Object { $_.Id -eq $NodeId })
    if ($node.Count -ne 1) { throw "Unknown demo node: $NodeId" }
    return $node[0]
}

function Get-NexusRecord([string]$NodeId) {
    $path = Join-Path $script:NexusRuntime "$NodeId.json"
    if (-not (Test-Path -LiteralPath $path)) { return $null }
    try { return Get-Content -LiteralPath $path -Raw | ConvertFrom-Json }
    catch { throw "Invalid process record $path. Inspect it before restarting. $_" }
}

function Get-NexusOwnedProcess([string]$NodeId) {
    $record = Get-NexusRecord $NodeId
    if ($null -eq $record) { return $null }
    if ($record.root -ne $script:NexusDemoRoot -or $record.nodeId -ne $NodeId) {
        throw "Process record does not belong to this checkout: $NodeId"
    }
    $process = Get-Process -Id ([int]$record.pid) -ErrorAction SilentlyContinue
    if ($null -eq $process) { return $null }
    $cim = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)" -ErrorAction SilentlyContinue
    $ownerPattern = '(^|\s|\")' + [regex]::Escape("-Dnexus.demo.owner=$($script:NexusDemoRoot)") + '(\"|\s|$)'
    $nodePattern = '(^|\s|\")' + [regex]::Escape("--nexus.node-id=$NodeId") + '(\"|\s|$)'
    # PS7 ConvertFrom-Json may already produce DateTime; a cast preserves its timezone Kind.
    $recordStart = ([datetime]$record.startedUtc).ToUniversalTime()
    $sameStart = [Math]::Abs(($process.StartTime.ToUniversalTime() - $recordStart).TotalSeconds) -lt 2
    if ($null -eq $cim -or $cim.Name -notmatch '^java(w)?\.exe$' -or
        $cim.CommandLine -notmatch $ownerPattern -or $cim.CommandLine -notmatch $nodePattern -or
        -not $cim.CommandLine.Contains($script:NexusJar) -or -not $sameStart) {
        Write-Warning "PID $($record.pid) is no longer our $NodeId process; it will not be touched."
        return $null
    }
    return $process
}

function Stop-NexusNode([string]$NodeId) {
    $process = Get-NexusOwnedProcess $NodeId
    if ($null -eq $process) { Write-Host "$NodeId is already stopped."; return }
    $ownedId = $process.Id
    Stop-Process -Id $ownedId -Force -ErrorAction Stop
    if (-not $process.WaitForExit(15000)) { throw "Timed out stopping $NodeId (PID $ownedId)." }
    # Keep the record to preserve the optional Nacos profile on restart.
    Write-Host "Stopped $NodeId (PID $ownedId)."
}

function Assert-NexusPortsFree($Node) {
    $listeners = [Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    foreach ($port in @($Node.HttpPort, $Node.RpcPort)) {
        if (@($listeners | Where-Object { $_.Port -eq $port }).Count -gt 0) {
            throw "Port $port is already listening, but there is no verified demo PID for $($Node.Id). Stop its owner yourself or use different application ports."
        }
    }
}

function Start-NexusNode([string]$NodeId, [bool]$UseNacos = $false, [bool]$UseSavedProfile = $true) {
    $existing = Get-NexusOwnedProcess $NodeId
    if ($null -ne $existing) { Write-Host "$NodeId is already running (PID $($existing.Id))."; return $false }
    $node = Get-NexusNode $NodeId
    Assert-NexusPortsFree $node
    $record = Get-NexusRecord $NodeId
    if ($UseSavedProfile -and $null -ne $record -and $record.nacos) { $UseNacos = $true }
    $java = (Get-Command java -ErrorAction Stop).Source
    $dataDir = Join-Path $script:NexusDemoRoot "data\$NodeId"
    $arguments = @(
        '-Xms64m', '-Xmx256m', '-Dfile.encoding=UTF-8', "-Dnexus.demo.owner=$($script:NexusDemoRoot)",
        '-jar', $script:NexusJar, "--server.port=$($node.HttpPort)", "--nexus.node-id=$NodeId",
        "--nexus.rpc-port=$($node.RpcPort)", "--nexus.data-dir=$dataDir"
    )
    if ($UseNacos) { $arguments += '--spring.profiles.active=nacos' }
    # Start-Process joins ArgumentList; quoting each argument preserves paths with spaces.
    $argumentLine = ($arguments | ForEach-Object { '"' + $_ + '"' }) -join ' '
    $process = Start-Process -FilePath $java -ArgumentList $argumentLine -WorkingDirectory $script:NexusDemoRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $script:NexusRuntime "$NodeId.out.log") `
        -RedirectStandardError (Join-Path $script:NexusRuntime "$NodeId.err.log")
    $saved = [ordered]@{
        nodeId = $NodeId; pid = $process.Id; root = $script:NexusDemoRoot
        startedUtc = $process.StartTime.ToUniversalTime().ToString('o'); nacos = $UseNacos
    }
    $saved | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $script:NexusRuntime "$NodeId.json") -Encoding UTF8
    Write-Host "Started $NodeId (PID $($process.Id), HTTP $($node.HttpPort), RPC $($node.RpcPort))."
    return $true
}

function Invoke-NexusHttp {
    param([string]$Method = 'GET', [string]$Uri, [byte[]]$Body, [int]$TimeoutSeconds = 20)
    $handler = [Net.Http.HttpClientHandler]::new()
    $handler.UseProxy = $false
    $client = [Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromSeconds($TimeoutSeconds)
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method), $Uri)
    $response = $null
    try {
        if ($PSBoundParameters.ContainsKey('Body')) {
            $request.Content = [Net.Http.ByteArrayContent]::new($Body)
            $request.Content.Headers.ContentType = [Net.Http.Headers.MediaTypeHeaderValue]::new('application/octet-stream')
        }
        $response = $client.SendAsync($request).GetAwaiter().GetResult()
        $bytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
        return [pscustomobject]@{ Status = [int]$response.StatusCode; Bytes = $bytes; Text = [Text.Encoding]::UTF8.GetString($bytes) }
    }
    finally {
        if ($null -ne $response) { $response.Dispose() }
        $request.Dispose()
        $client.Dispose()
    }
}

function Assert-NexusSuccess($Response, [string]$Action) {
    if ($Response.Status -lt 200 -or $Response.Status -ge 300) {
        throw "$Action failed: HTTP $($Response.Status) $($Response.Text)"
    }
}

function Get-NexusFileUri([string]$BaseUrl, [string]$Key) {
    return "$($BaseUrl.TrimEnd('/'))/api/files?key=$([Uri]::EscapeDataString($Key))"
}

function Get-NexusHash([byte[]]$Bytes) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Wait-NexusHealthy([int]$TimeoutSeconds = 120) {
    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastChecks = [ordered]@{}
    do {
        $ready = 0
        foreach ($node in $script:NexusNodes) {
            if ($null -eq (Get-NexusOwnedProcess $node.Id)) { throw "$($node.Id) exited. Inspect .runtime/$($node.Id).err.log and .out.log." }
            try {
                $response = Invoke-NexusHttp -Uri "http://127.0.0.1:$($node.HttpPort)/actuator/health" -TimeoutSeconds 2
                $detail = $response.Text
                if ($detail.Length -gt 500) { $detail = $detail.Substring(0, 500) + '...' }
                $lastChecks[$node.Id] = "HTTP $($response.Status) $detail"
                if ($response.Status -eq 200) { $ready++ }
            } catch { $lastChecks[$node.Id] = $_.Exception.Message }
        }
        if ($ready -eq 3) { return }
        Start-Sleep -Milliseconds 500
    } while ([datetime]::UtcNow -lt $deadline)
    $details = ($lastChecks.GetEnumerator() | ForEach-Object { "$($_.Key): $($_.Value)" }) -join '; '
    throw "Cluster health did not become ready. Last checks: $details. Inspect .runtime/*.log."
}

function Get-NexusCluster([string]$NodeId) {
    $node = Get-NexusNode $NodeId
    $response = Invoke-NexusHttp -Uri "http://127.0.0.1:$($node.HttpPort)/api/cluster" -TimeoutSeconds 3
    Assert-NexusSuccess $response 'Cluster status'
    return $response.Text | ConvertFrom-Json
}

function Wait-NexusLeader([string]$Shard, [string[]]$NodeIds = @('node1', 'node2', 'node3'), [int]$TimeoutSeconds = 30) {
    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        foreach ($nodeId in $NodeIds) {
            try {
                $status = Get-NexusCluster $nodeId
                foreach ($state in $status.shards) {
                    if ([string]$state.groupId -eq $Shard -and $state.role -eq 'LEADER') { return $nodeId }
                }
            } catch { }
        }
        Start-Sleep -Milliseconds 300
    } while ([datetime]::UtcNow -lt $deadline)
    throw "No leader appeared for $Shard within $TimeoutSeconds seconds."
}

function Wait-NexusApplied([string]$Shard, [long]$MinimumIndex, [int]$TimeoutSeconds = 40) {
    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $caughtUp = 0
        foreach ($node in $script:NexusNodes) {
            try {
                $status = Get-NexusCluster $node.Id
                $state = @($status.shards | Where-Object { [string]$_.groupId -eq $Shard })
                if ($state.Count -eq 1 -and [long]$state[0].lastApplied -ge $MinimumIndex) { $caughtUp++ }
            } catch { }
        }
        if ($caughtUp -eq 3) { return }
        Start-Sleep -Milliseconds 300
    } while ([datetime]::UtcNow -lt $deadline)
    throw "Not all nodes applied $Shard index $MinimumIndex after recovery."
}
