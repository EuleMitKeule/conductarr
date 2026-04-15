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

uv run python scripts/example_flow.py
