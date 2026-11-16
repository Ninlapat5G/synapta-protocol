# Starts, stops and checks Synapta Lab without a window.
#   Lab-Control.ps1 start [-Port 7878] [-NoTailscale]
#   Lab-Control.ps1 stop          (saves the topology to lab-saved\ first)
#   Lab-Control.ps1 status
# Run it from the folder that holds synapta-lab.exe. Exit code 0 = did what was asked.
param(
    [Parameter(Mandatory)][ValidateSet('start', 'stop', 'status')][string]$Action,
    [int]$Port = 7878,
    [switch]$NoTailscale
)

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$exe = Join-Path $here 'synapta-lab.exe'
$pidFile = Join-Path $here 'lab.pid'
$portFile = Join-Path $here 'lab.port'

# The lab of this folder if it is running. Checking the exe path keeps a recycled pid
# (or another folder's lab) from being mistaken for ours or killed by mistake.
function Get-RunningLab {
    if (-not (Test-Path $pidFile)) { return $null }
    $id = [int](Get-Content $pidFile -Raw)
    $process = Get-Process -Id $id -ErrorAction SilentlyContinue
    if ($process -and $process.Path -eq $exe) { return $process }
    return $null
}

function Get-LabPort {
    if (Test-Path $portFile) { return [int](Get-Content $portFile -Raw) }
    return $Port
}

switch ($Action) {
    'start' {
        if (Get-RunningLab) { Write-Output "already running on port $(Get-LabPort)"; exit 0 }
        $arguments = @('--port', $Port)
        if ($NoTailscale) { $arguments += '--no-tailscale' }
        $process = Start-Process -FilePath $exe -ArgumentList $arguments -WorkingDirectory $here `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $here 'lab.log') -RedirectStandardError (Join-Path $here 'lab.err')
        # Failing to bind the port ends the process within a moment: say so instead of "started".
        Start-Sleep -Seconds 2
        if ($process.HasExited) {
            Write-Error "synapta-lab stopped right away (port $Port busy?). See lab.err"
            exit 1
        }
        Set-Content $pidFile $process.Id
        Set-Content $portFile $Port
        Write-Output "started on http://127.0.0.1:$Port/ (pid $($process.Id))"
    }
    'stop' {
        $process = Get-RunningLab
        if (-not $process) { Write-Output 'not running'; exit 0 }
        # Nodes and trace live only in memory. Keep the topology so a stop is never a loss.
        try {
            $topology = Invoke-RestMethod "http://127.0.0.1:$(Get-LabPort)/api/topology" -TimeoutSec 5
            if ($topology.nodes.Count -gt 0) {
                $saved = Join-Path $here 'lab-saved'
                New-Item -ItemType Directory -Force $saved | Out-Null
                $file = Join-Path $saved ("topology-{0}.json" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
                # No BOM: Set-Content -Encoding utf8 adds one and the lab's JSON parser rejects it on load.
                [IO.File]::WriteAllText($file, ($topology | ConvertTo-Json -Depth 20), (New-Object Text.UTF8Encoding $false))
                Write-Output "topology saved to $file"
            }
        } catch {
            Write-Output "could not save the topology first ($($_.Exception.Message)); stopping anyway"
        }
        Stop-Process -Id $process.Id -Force
        Remove-Item $pidFile, $portFile -ErrorAction SilentlyContinue
        Write-Output 'stopped'
    }
    'status' {
        if (Get-RunningLab) { Write-Output "running on port $(Get-LabPort)"; exit 0 }
        Write-Output 'not running'
        exit 3
    }
}
