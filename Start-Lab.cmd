@echo off
cd /d "%~dp0"
echo Open http://127.0.0.1:7878/ or this PC Tailscale IP on your phone.
echo Save your topology before closing this window.
"%~dp0synapta-lab.exe" %*
if errorlevel 1 pause
