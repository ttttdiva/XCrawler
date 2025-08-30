#!/usr/bin/env python3
"""
HuggingFace上のmonitoring/ディレクトリ内のファイルを正しい構造に移動するスクリプト
monitoring/{username}/images/* -> images/{username}/*
monitoring/{username}/videos/* -> videos/{username}/*
"""

import sys
import os
from pathlib import Path
from huggingface_hub import HfApi, list_repo_files, CommitOperationCopy, CommitOperationDelete, create_commit
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HFStructureFixer")

def main():
    # HuggingFace API設定
    api_key = os.getenv('HUGGINGFACE_API_KEY')
    if not api_key:
        logger.error("HUGGINGFACE_API_KEY not found")
        sys.exit(1)
    
    api = HfApi(token=api_key)
    repo_name = "Sageen/EventMonitor_1"
    
    try:
        # リポジトリ内のファイル一覧を取得
        logger.info(f"Fetching file list from {repo_name}...")
        files = list_repo_files(repo_id=repo_name, repo_type="dataset", token=api_key)
        
        # monitoring/で始まるファイルを抽出
        monitoring_files = [f for f in files if f.startswith("monitoring/")]
        logger.info(f"Found {len(monitoring_files)} files in monitoring/ directory")
        
        if not monitoring_files:
            logger.info("No files to move")
            return
        
        # ファイルの移動計画を作成
        move_plan = []
        for old_path in monitoring_files:
            # monitoring/{username}/images/* -> images/{username}/*
            # monitoring/{username}/videos/* -> videos/{username}/*
            parts = old_path.split('/')
            if len(parts) >= 3:
                username = parts[1]
                media_type = parts[2]  # images or videos
                
                if media_type in ['images', 'videos']:
                    # 新しいパスを構築
                    new_path_parts = [media_type, username] + parts[3:]
                    new_path = '/'.join(new_path_parts)
                    move_plan.append((old_path, new_path))
        
        logger.info(f"Move plan created: {len(move_plan)} files to move")
        
        # 移動計画を表示
        print("\n=== Move Plan ===")
        for old, new in move_plan[:5]:  # 最初の5件を表示
            print(f"  {old} -> {new}")
        if len(move_plan) > 5:
            print(f"  ... and {len(move_plan) - 5} more files")
        
        # 確認
        response = input("\nProceed with moving files? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted by user")
            return
        
        # 25,000ファイルずつバッチ処理
        BATCH_SIZE = 12500  # 操作が2倍になるため（copy + delete）、12,500ファイル = 25,000操作
        total_success = 0
        total_failed = 0
        
        for batch_idx in range(0, len(move_plan), BATCH_SIZE):
            batch = move_plan[batch_idx:batch_idx + BATCH_SIZE]
            logger.info(f"Processing batch {batch_idx // BATCH_SIZE + 1}/{(len(move_plan) + BATCH_SIZE - 1) // BATCH_SIZE}: {len(batch)} files")
            
            # CommitOperationを使って一括で移動
            operations = []
            
            for old_path, new_path in batch:
                # コピー操作を追加
                operations.append(
                    CommitOperationCopy(
                        src_path_in_repo=old_path,
                        path_in_repo=new_path
                    )
                )
                # 削除操作を追加
                operations.append(
                    CommitOperationDelete(
                        path_in_repo=old_path
                    )
                )
            
            logger.info(f"Creating commit with {len(operations)} operations...")
            
            try:
                # 一括コミット（コピーと削除を同時に実行）
                create_commit(
                    repo_id=repo_name,
                    repo_type="dataset",
                    operations=operations,
                    commit_message=f"Move files from monitoring/ to correct structure (batch {batch_idx // BATCH_SIZE + 1})",
                    token=api_key
                )
                
                logger.info(f"Successfully moved {len(batch)} files in batch {batch_idx // BATCH_SIZE + 1}!")
                total_success += len(batch)
                
            except Exception as e:
                logger.error(f"Failed to commit batch {batch_idx // BATCH_SIZE + 1}: {e}")
                total_failed += len(batch)
        
        success_count = total_success
        failed_count = total_failed
        
        # 結果表示
        logger.info(f"\n=== Results ===")
        logger.info(f"Success: {success_count} files")
        logger.info(f"Failed: {failed_count} files")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()