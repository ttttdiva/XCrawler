#!/bin/bash

echo "EventMonitor セットアップスクリプト"
echo "================================="

# 色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Python確認
echo -e "\n${YELLOW}Python環境を確認中...${NC}"
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
elif command -v python &> /dev/null; then
    PYTHON_CMD=python
else
    echo -e "${RED}エラー: Pythonがインストールされていません${NC}"
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}✓ Python $PYTHON_VERSION が見つかりました${NC}"

# 仮想環境の作成
echo -e "\n${YELLOW}仮想環境を作成中...${NC}"
if [ ! -f "venv/bin/activate" ]; then
    # 不完全なvenvディレクトリがあれば削除
    if [ -d "venv" ]; then
        echo -e "${YELLOW}不完全な仮想環境を削除中...${NC}"
        rm -rf venv
    fi
    
    $PYTHON_CMD -m venv venv 2>/dev/null
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}python3-venvが見つかりません。インストール中...${NC}"
        sudo apt update && sudo apt install -y python3-venv
        if [ $? -ne 0 ]; then
            echo -e "${RED}エラー: python3-venvのインストールに失敗しました${NC}"
            exit 1
        fi
        $PYTHON_CMD -m venv venv
        if [ $? -ne 0 ]; then
            echo -e "${RED}エラー: 仮想環境の作成に失敗しました${NC}"
            exit 1
        fi
    fi
    echo -e "${GREEN}✓ 仮想環境を作成しました${NC}"
else
    echo -e "${GREEN}✓ 仮想環境は既に存在します${NC}"
fi

# 仮想環境の有効化
echo -e "\n${YELLOW}仮想環境を有効化中...${NC}"
source venv/bin/activate

# pipのアップグレード
echo -e "\n${YELLOW}pipをアップグレード中...${NC}"
pip install --upgrade pip --quiet

# 依存関係のインストール
echo -e "\n${YELLOW}依存関係をインストール中...${NC}"
pip install -r requirements.txt

# SQLite関連の設定
echo -e "\n${YELLOW}SQLiteの設定を確認中...${NC}"
# Python標準のsqlite3モジュールが使用可能
echo -e "${GREEN}✓ SQLiteは標準ライブラリに含まれています${NC}"

# WSL環境チェック（pysqlite3が必要な場合）
if grep -qi microsoft /proc/version 2>/dev/null; then
    echo -e "${YELLOW}WSL環境を検出しました。pysqlite3-binaryをインストール中...${NC}"
    pip install pysqlite3-binary
    echo -e "${GREEN}✓ WSL用SQLite互換ライブラリをインストールしました${NC}"
fi

# 設定ファイルの確認
echo -e "\n${YELLOW}設定ファイルを確認中...${NC}"

# .env ファイル
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠ .env ファイルが見つかりません${NC}"
    echo -e "  以下の手順で作成してください:"
    echo -e "  ${GREEN}cp .env.example .env${NC}"
    echo -e "  ${GREEN}nano .env${NC}  # または好きなエディタで編集"
    echo ""
    echo -e "  最低限必要な設定:"
    echo -e "  • TWITTER_ACCOUNT_1_TOKEN"
    echo -e "  • TWITTER_ACCOUNT_1_CT0"
else
    echo -e "${GREEN}✓ .env ファイルが見つかりました${NC}"
fi

# config.yaml ファイル（リポジトリに含まれているはず）
if [ ! -f "config.yaml" ]; then
    echo -e "${RED}⚠ config.yaml が見つかりません${NC}"
    echo -e "  リポジトリから取得してください"
else
    echo -e "${GREEN}✓ config.yaml が見つかりました${NC}"
fi

# monitored_accounts.csv ファイル
if [ ! -f "monitored_accounts.csv" ]; then
    cat > monitored_accounts.csv << 'EOF'
username,display_name,event_detection_enabled,account_type
example_user1,サンプルユーザー1,1,
example_user2,サンプルユーザー2,1,
log_only_user,ログ専用アカウント,0,log
EOF
    echo -e "${GREEN}✓ monitored_accounts.csv テンプレートを作成しました${NC}"
    echo -e "${YELLOW}⚠ 重要: monitored_accounts.csv を編集して監視対象アカウントを設定してください${NC}"
else
    echo -e "${GREEN}✓ monitored_accounts.csv ファイルは既に存在します${NC}"
fi

echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}セットアップが完了しました！${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo -e "\n${YELLOW}次の手順:${NC}"
echo ""
echo "1. ${YELLOW}Twitter/X認証情報の取得:${NC}"
echo "   - ブラウザでTwitter/Xを開く"
echo "   - F12キーで開発者ツールを開く"
echo "   - Application → Cookies → x.com から取得:"
echo "     • auth_token → TWITTER_ACCOUNT_1_TOKEN"
echo "     • ct0 → TWITTER_ACCOUNT_1_CT0"
echo ""
echo "2. ${YELLOW}設定ファイルの編集:${NC}"
echo "   - .env: 認証情報を設定"
echo "   - monitored_accounts.csv: 監視対象アカウントを設定"
echo "   - config.yaml: 必要に応じて詳細設定を調整"
echo ""
echo "3. ${YELLOW}実行方法:${NC}"
echo -e "   ${GREEN}source venv/bin/activate${NC}"
echo -e "   ${GREEN}python main.py${NC}           # 単発実行"
echo -e "   ${GREEN}python main.py --daemon${NC}  # 継続実行"
echo ""
echo "   ※ 初回実行時にSQLiteデータベース（data/eventmonitor.db）が自動作成されます"
echo ""
echo "4. ${YELLOW}テスト:${NC}"
echo -e "   ${GREEN}python tests/test_config.py${NC}  # 設定確認"
echo -e "   ${GREEN}python tests/test_basic.py${NC}   # 基本動作テスト"
echo ""
echo "5. ${YELLOW}オプション機能:${NC}"
echo "   • クローラーモード: config.yaml で event_detection.enabled: false"
echo "   • Hydrus連携: .env で HYDRUS_API_KEY を設定"
echo "   • HFバックアップ: .env で HUGGINGFACE_API_KEY を設定"
echo ""
echo -e "${GREEN}詳細は README.md を参照してください。${NC}"