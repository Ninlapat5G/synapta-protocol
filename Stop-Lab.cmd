@echo off
rem Stops the Synapta Lab started by Start-Lab-Hidden.cmd, saving its topology to lab-saved\ first.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0Lab-Control.ps1" stop
