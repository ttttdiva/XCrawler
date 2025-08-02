@echo off
wsl.exe -d Debian -e bash -c "cd ~/XCrawler && source venv/bin/activate && python main.py"