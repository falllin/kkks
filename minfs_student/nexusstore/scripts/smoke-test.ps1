param([string]$BaseUrl = 'http://127.0.0.1:8081', [string]$ReadBaseUrl = 'http://127.0.0.1:8082')
. (Join-Path $PSScriptRoot 'common.ps1')

$key = '/demo/smoke-' + [guid]::NewGuid().ToString('N') + '.bin'
$uri = Get-NexusFileUri $BaseUrl $key
$bytes = [byte[]]::new(4096)
for ($i = 0; $i -lt $bytes.Length; $i++) { $bytes[$i] = [byte]($i % 256) }
$expectedHash = Get-NexusHash $bytes
$created = $false
try {
    $put = Invoke-NexusHttp -Method PUT -Uri $uri -Body $bytes
    Assert-NexusSuccess $put 'Upload binary file'
    $created = $true
    $get = Invoke-NexusHttp -Uri (Get-NexusFileUri $ReadBaseUrl $key)
    Assert-NexusSuccess $get 'Download binary file'
    $actualHash = Get-NexusHash $get.Bytes
    if ($actualHash -ne $expectedHash) { throw "SHA-256 mismatch: expected $expectedHash, got $actualHash" }
    Write-Host "PASS binary upload/download ($($bytes.Length) bytes), SHA-256: $actualHash"
    $delete = Invoke-NexusHttp -Method DELETE -Uri $uri
    Assert-NexusSuccess $delete 'Delete file'
    $created = $false
    $missing = Invoke-NexusHttp -Uri $uri
    if ($missing.Status -ne 404) { throw "Expected HTTP 404 after deletion, got $($missing.Status): $($missing.Text)" }
    Write-Host 'PASS delete followed by HTTP 404.'
}
finally {
    if ($created) {
        try { $null = Invoke-NexusHttp -Method DELETE -Uri $uri }
        catch { Write-Warning "Could not clean up temporary demo key ${key}: $_" }
    }
}
