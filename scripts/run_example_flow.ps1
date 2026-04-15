Set-Location $PSScriptRoot\..

$container = 'conductarr'
$spin = [char[]]('|', '/', '-', '\')
$i = 0
while ($true) {
    $status = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    if ($LASTEXITCODE -eq 0 -and $status -eq 'healthy') {
        Write-Host "`r$container is healthy.      "
        break
    }
    Write-Host -NoNewline "`rWaiting for $container $($spin[$i % 4]) "
    $i++
    Start-Sleep 2
}

# Mock service keys (hardcoded — see tests/mocks)
$env:SABNZBD_URL     = if ($env:SABNZBD_URL)    { $env:SABNZBD_URL }    else { 'http://localhost:8080' }
$env:SABNZBD_API_KEY = 'sabnzbd-test-key'
$env:RADARR_URL      = if ($env:RADARR_URL)     { $env:RADARR_URL }     else { 'http://localhost:7878' }
$env:RADARR_API_KEY  = 'radarr-test-key'
$env:SONARR_URL      = if ($env:SONARR_URL)     { $env:SONARR_URL }     else { 'http://localhost:8989' }
$env:SONARR_API_KEY  = 'sonarr-test-key'

uv run python scripts/example_flow.py

Write-Host "`n--- Conductarr container logs (last 30 lines) ---"
docker logs --tail 30 conductarr
