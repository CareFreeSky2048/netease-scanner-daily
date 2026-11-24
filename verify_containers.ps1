param([switch]$KeepRunning, [switch]$Tunnel, [switch]$LowResource, [switch]$UseLocalSource)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$testNetwork = 'netease-scanner-smoke'
$testNames = @('netease-smoke-caddy', 'netease-smoke-scanner', 'netease-smoke-api')
$existingNames = @(docker ps -a --format '{{.Names}}')
if ($LASTEXITCODE -ne 0) { throw 'Cannot access Docker' }
foreach ($testName in $testNames) {
    if ($existingNames -contains $testName) { throw "Test container already exists: $testName" }
}
$smokeRoot = Join-Path $PSScriptRoot 'smoke'
$smokeData = Join-Path $smokeRoot 'data'
$smokeConfig = Join-Path $smokeRoot 'config'
$smokeSecrets = Join-Path $smokeRoot 'secrets'
New-Item -ItemType Directory -Path $smokeData,$smokeConfig,$smokeSecrets -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'config.example.json') -Destination (Join-Path $smokeConfig 'config.json')
foreach ($recordName in @('VA_Tracks_Output.csv','VA_Album_Date_Cache.json','VA_Scan_Metrics.json','VA_Scan_Tuning_Report.txt')) {
    Copy-Item -LiteralPath (Join-Path (Split-Path $PSScriptRoot -Parent) $recordName) -Destination $smokeData
}
Copy-Item -LiteralPath (Join-Path (Split-Path $PSScriptRoot -Parent) 'VA_Artist_Caches') -Destination $smokeData -Recurse -Force
function Assert-DockerSuccess { if ($LASTEXITCODE -ne 0) { throw "Docker command failed: $LASTEXITCODE" } }
$cleanup = -not $KeepRunning
try {
    $apiLimits = @()
    $scannerLimits = @()
    $caddyLimits = @()
    $sourceMounts = @()
    if ($UseLocalSource) {
        $sourceMounts = @('-v',"${PSScriptRoot}/server.py:/app/server.py:ro",'-v',"${PSScriptRoot}/templates:/app/templates:ro")
    }
    if ($LowResource) {
        $apiLimits = @('--cpus','0.25','--memory','160m','--memory-swap','320m','-e','NODE_OPTIONS=--max-old-space-size=96')
        $scannerLimits = @('--cpus','0.60','--memory','448m','--memory-swap','1536m','-e','NETEASE_MAX_THREADS=8')
        $caddyLimits = @('--cpus','0.50','--memory','64m','--memory-swap','64m')
    }
    docker network create $testNetwork | Out-Null
    Assert-DockerSuccess
    docker run -d --name netease-smoke-api --network $testNetwork --network-alias netease-api @apiLimits netease-scanner-netease-api | Out-Null
    Assert-DockerSuccess
    docker run -d --name netease-smoke-scanner --network $testNetwork --network-alias scanner -p 127.0.0.1:15000:5000 @scannerLimits @sourceMounts `
        -v "${smokeData}:/data" -v "${smokeConfig}:/config:ro" -v "${smokeSecrets}:/run/secrets:ro" netease-scanner-scanner | Out-Null
    Assert-DockerSuccess
    $hashResult = docker run --rm caddy:2.10.2-alpine caddy hash-password --plaintext temporary-smoke-password
    Assert-DockerSuccess
    $hashText = ($hashResult | Select-Object -Last 1).Trim()
    $envFile = Join-Path $smokeRoot 'caddy.env'
    [System.IO.File]::WriteAllText($envFile, "SCANNER_DOMAIN=:8080`nWEB_USER=tester`nWEB_PASSWORD_HASH=$hashText`n", [System.Text.UTF8Encoding]::new($false))
    $caddyPath = Join-Path $PSScriptRoot $(if ($Tunnel) { 'Caddyfile.tunnel' } else { 'Caddyfile' })
    docker run -d --name netease-smoke-caddy --network $testNetwork -p 127.0.0.1:18080:8080 --env-file $envFile @caddyLimits `
        -v "${caddyPath}:/etc/caddy/Caddyfile:ro" caddy:2.10.2-alpine | Out-Null
    Assert-DockerSuccess
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $health = Invoke-RestMethod -Uri 'http://127.0.0.1:15000/health' -TimeoutSec 2
            if ($health.status -eq 'ok') { $ready = $true; break }
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $ready) { throw 'Scanner health check failed' }
    docker exec netease-smoke-api node -e "(async()=>{for(let i=0;i<30;i++){try{if((await fetch('http://127.0.0.1:3000/',{signal:AbortSignal.timeout(1500)})).ok)return}catch{}await new Promise(r=>setTimeout(r,1000))}process.exit(1)})()"
    Assert-DockerSuccess
    try {
        Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:18080/api/status' -TimeoutSec 5 | Out-Null
        throw 'Unauthenticated access was allowed'
    } catch {
        if (-not $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 401) { throw }
    }
    $basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('tester:temporary-smoke-password'))
    $headers = @{Authorization="Basic $basic"}
    $status = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/api/status' -Headers $headers -TimeoutSec 10
    if ($status.schedule -ne '00:03' -or $status.schedule_enabled -or $status.cookie_configured) { throw 'Unexpected schedule/credential state' }
    if ($LowResource) {
        if ($status.thread_limit -ne 8 -or $status.defaults.threads -ne 5) { throw 'Small-host thread limit missing' }
        $actualLimits = docker inspect --format '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.NanoCpus}}' netease-smoke-scanner
        Assert-DockerSuccess
        if ($actualLimits.Trim() -ne '469762048 1610612736 600000000') { throw 'Small-host memory/swap/CPU limits missing' }
    }
    $records = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/api/legacy?limit=1' -Headers $headers -TimeoutSec 5
    if ($records.total -lt 1 -or $records.results.Count -ne 1) { throw 'Migrated CSV was not readable' }
    $snapshot = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/api/pre-migration?limit=1' -Headers $headers -TimeoutSec 5
    if ($snapshot.total -ne $records.total) { throw 'Migration-time snapshot was not readable' }
    $today = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/api/today' -Headers $headers -TimeoutSec 5
    if ($today.timezone -ne 'Asia/Shanghai' -or $today.total -ne 0 -or $today.runs.Count -ne 0) { throw 'Unexpected today results in isolated smoke data' }
    $page = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:18080/' -Headers $headers -TimeoutSec 5
    if ($page.StatusCode -ne 200 -or $page.Content -notmatch '/api/stream') { throw 'Frontend page check failed' }
    if ($page.Content -notmatch 'id="syncRecords"') { throw 'Record return button missing' }
    $syncScript = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:18080/static/records.js' -Headers $headers -TimeoutSec 5
    if ($syncScript.StatusCode -ne 200 -or $syncScript.Content -notmatch 'validateManifest') { throw 'Record return script missing' }
    $exportHeaders = @{Authorization="Basic $basic"; 'X-Scanner-Request'='1'}
    $export = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/api/records/export' -Headers $exportHeaders -Method Post -ContentType 'application/json' -Body '{}' -TimeoutSec 30
    if ($export.files.Count -lt 4 -or $export.files.path -contains 'cookie.txt') { throw 'Unexpected record export' }
    $exportZip = Join-Path $smokeRoot 'records-export.zip'
    Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:18080/api/records/$($export.export_id)/zip" -Headers $headers -OutFile $exportZip -TimeoutSec 30
    Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/records/$($export.export_id)/release" -Headers $exportHeaders -Method Post -ContentType 'application/json' -Body '{}' -TimeoutSec 5 | Out-Null
    $recordCount = $records.total
    Write-Output "Containers verified: API, scanner, login protection, frontend, $recordCount legacy rows; schedule 00:03 disabled pending VPS validation."
    if ($KeepRunning) { Write-Output 'Preview: http://127.0.0.1:15000 (local-only scanner; production authentication checked on port 18080).' }
} catch {
    $cleanup = $true
    $failure = $_
    $ErrorActionPreference = 'Continue'
    foreach ($testName in $testNames) { docker logs --tail=20 $testName 2>$null }
    $ErrorActionPreference = 'Stop'
    throw $failure
} finally {
    if ($cleanup) {
        foreach ($testName in $testNames) { docker rm -f $testName 2>$null | Out-Null }
        docker network rm $testNetwork 2>$null | Out-Null
    }
}
