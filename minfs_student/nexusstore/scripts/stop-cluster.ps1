. (Join-Path $PSScriptRoot 'common.ps1')
$failures = [Collections.Generic.List[string]]::new()
foreach ($node in $script:NexusNodes) {
    try { Stop-NexusNode $node.Id }
    catch { $failures.Add("$($node.Id): $_") }
}
if ($failures.Count -gt 0) { throw ($failures -join [Environment]::NewLine) }
Write-Host 'Demo processes stopped. Persistent data and logs were kept.'
