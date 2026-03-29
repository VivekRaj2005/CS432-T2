if (Test-Path .\logs.log) { Remove-Item .\logs.log }
& ./.venv/Scripts/Activate.ps1
python.exe .\main.py