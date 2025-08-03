@echo off
chcp 65001 > nul
echo EventMonitor セットアップスクリプト
echo =================================

REM Python確認
echo.
echo Python環境を確認中...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo エラー: Pythonがインストールされていません
    echo Python 3.8以上をインストールしてください
    pause
    exit /b 1
)

python --version
echo Pythonが見つかりました

REM 仮想環境の作成
echo.
echo 仮想環境を作成中...
if not exist "venv" (
    python -m venv venv
    echo 仮想環境を作成しました
) else (
    echo 仮想環境は既に存在します
)

REM 仮想環境の有効化
echo.
echo 仮想環境を有効化中...
call venv\Scripts\activate.bat

REM 依存関係のインストール
echo.
echo 依存関係をインストール中...
python -m pip install --upgrade pip
pip install -r requirements.txt

REM 設定ファイルのコピー
echo.
echo 設定ファイルを準備中...

if not exist ".env" (
    copy .env.example .env
    echo .env ファイルを作成しました
    echo 重要: .env ファイルを編集して認証情報を設定してください
) else (
    echo .env ファイルは既に存在します
)

if not exist "config.yaml" (
    copy config.yaml.example config.yaml
    echo config.yaml ファイルを作成しました
    echo 重要: config.yaml を編集して監視対象アカウントを設定してください
) else (
    echo config.yaml ファイルは既に存在します
)

REM ディレクトリ作成
echo.
echo 必要なディレクトリを作成中...
if not exist "data" mkdir data
if not exist "logs" mkdir logs

REM MySQL確認
echo.
echo MySQLの設定確認
echo 以下のコマンドでデータベースを作成してください：
echo mysql -u root -p
echo CREATE DATABASE eventmonitor CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
echo CREATE USER 'eventmonitor'@'localhost' IDENTIFIED BY 'your_password_here';
echo GRANT ALL PRIVILEGES ON eventmonitor.* TO 'eventmonitor'@'localhost';
echo FLUSH PRIVILEGES;

echo.
echo セットアップ手順:
echo 1. .env ファイルを編集して以下を設定:
echo    - Twitter認証情報 (auth_token, ct0)
echo    - OpenAI/Gemini APIキー
echo    - MySQL接続情報
echo    - Discord Webhook URL
echo.
echo 2. config.yaml を編集して監視対象アカウントを設定
echo.
echo 3. MySQLデータベースを作成
echo.
echo 4. 以下のコマンドで実行:
echo    venv\Scripts\activate
echo    python main.py
echo.
echo セットアップが完了しました！
pause