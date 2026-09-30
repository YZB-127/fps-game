@echo off
setlocal
cd /d "%~dp0"

echo ================================================
echo   FPS LAN server / 3D FPS multiplayer server
echo   (Chinese guide: see README-lianji.md)
echo ================================================
echo.

where node >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Node.js not found. Install it first: https://nodejs.org
  echo.
  pause
  exit /b 1
)

echo [NOTE] Keep this window open while playing.
echo        To stop the server: press Ctrl+C here, or close this window.
echo.

set PORT_IN=
set /p PORT_IN=Port [press Enter for 8000]:
if "%PORT_IN%"=="" set PORT_IN=8000

echo.
echo [START] port %PORT_IN%
echo.
node server.js %PORT_IN%

echo.
echo [STOPPED] server exited. If the port was busy, run this file again with another port.
pause
