Write-Host "Starting SmartDash Backend..." -ForegroundColor Cyan
Set-Location "$PSScriptRoot\backend"
.\venv\Scripts\uvicorn main:app --reload --host 0.0.0.0 --port 8000
