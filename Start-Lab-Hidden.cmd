@echo off
rem Starts Synapta Lab with no window. Extra arguments go to Lab-Control.ps1 (-Port 7990, -NoTailscale).
rem Stop it with Stop-Lab.cmd.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0Lab-Control.ps1" start %*
