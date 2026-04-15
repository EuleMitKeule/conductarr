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

# Resolve API keys from dev service config files
$env:SABNZBD_URL    = if ($env:SABNZBD_URL)    { $env:SABNZBD_URL }    else { 'http://localhost:8080' }
$env:SABNZBD_API_KEY = uv run python scripts/get_api_key.py dev/sabnzbd/sabnzbd.ini sabnzbd
$env:RADARR_URL     = if ($env:RADARR_URL)     { $env:RADARR_URL }     else { 'http://localhost:7878' }
$env:RADARR_API_KEY  = uv run python scripts/get_api_key.py dev/radarr/config.xml arr
$env:SONARR_URL     = if ($env:SONARR_URL)     { $env:SONARR_URL }     else { 'http://localhost:8989' }
$env:SONARR_API_KEY  = uv run python scripts/get_api_key.py dev/sonarr/config.xml arr

uv run python scripts/example_flow.py
