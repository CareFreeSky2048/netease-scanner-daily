$ErrorActionPreference = 'Stop'
$runningScanners = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match '^(python|pythonw|py)(\.exe)?$' -and
    $_.CommandLine -match '(netease[^\s"'']*scanner[^\s"'']*\.py|netease_server[\\/]server\.py)'
})
if ($runningScanners.Count -gt 0) {
    $runningScanners | Select-Object ProcessId, Name | ConvertTo-Json
    Write-Error 'Scanner process detected. Stop it before preparing a snapshot.'
    exit 1
}
Write-Output 'No local scanner process detected.'
