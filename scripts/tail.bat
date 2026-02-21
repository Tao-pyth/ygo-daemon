@echo off
setlocal
pushd "%~dp0.."
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\tail.ps1" %*
set RC=%ERRORLEVEL%
popd
exit /b %RC%
