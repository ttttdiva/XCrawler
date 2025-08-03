@echo off
wsl.exe -d Debian -e bash -c "cd ~/projects/48_EventMonitor && source venv/bin/activate && python main.py"