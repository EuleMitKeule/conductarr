# Stop and remove the SABnzbd container, wipe its state, then restart it.
# Preserves sabnzbd.ini (API key) and sabnzbd.ini.bak.

Set-Location $PSScriptRoot\..

docker compose -f docker-compose.dev.yml rm -sf sabnzbd conductarr

$dirs = @(
    '.\dev\sabnzbd\admin',
    '.\dev\sabnzbd\Downloads\incomplete',
    '.\dev\sabnzbd\Downloads\complete'
)
foreach ($dir in $dirs) {
    if (Test-Path $dir) {
        Get-ChildItem $dir | Remove-Item -Recurse -Force
        Write-Host "Cleared: $dir"
    }
}
