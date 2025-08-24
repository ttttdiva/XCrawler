import os
import json
import logging
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class RcloneConfig:
    """Configuration for Rclone encryption"""
    remote_name: Optional[str] = None  # 省略時は自動検出
    config_path: Optional[str] = None
    temp_dir: Path = Path(".rclone_temp")


class RcloneClient:
    """Client for handling rclone encryption operations"""
    
    def __init__(self, config: RcloneConfig):
        self.config = config
        self.logger = logging.getLogger('hf_backup.rclone')
        
        # Create temp directory if it doesn't exist
        self.temp_dir = Path(config.temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        
        # Verify rclone is available
        if not self._check_rclone():
            raise RuntimeError("rclone not found. Please install rclone first.")
        
        # 自動検出が必要な場合
        if not self.config.remote_name:
            self.config.remote_name = self._auto_detect_crypt_remote()
            if not self.config.remote_name:
                raise RuntimeError("No crypt remote found in rclone config")
            self.logger.info(f"Auto-detected crypt remote: {self.config.remote_name}")
        
        # Verify remote exists
        if not self._verify_remote():
            raise RuntimeError(f"Remote '{self.config.remote_name}' not found in rclone config")
    
    def _check_rclone(self) -> bool:
        """Check if rclone is installed and available"""
        try:
            cmd = ["rclone", "version"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def _verify_remote(self) -> bool:
        """Verify that the configured remote exists"""
        remotes = self.list_remotes()
        return self.config.remote_name in remotes
    
    def _auto_detect_crypt_remote(self) -> Optional[str]:
        """暗号化リモートを自動検出"""
        try:
            # rclone config showで設定を取得
            cmd = ["rclone", "config", "show"]
            if self.config.config_path:
                cmd.extend(["--config", self.config.config_path])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return None
            
            # type = cryptのリモートを探す
            crypt_remotes = []
            current_remote = None
            for line in result.stdout.split('\n'):
                if line.startswith('[') and line.endswith(']'):
                    current_remote = line[1:-1]
                elif line.strip().startswith('type = crypt'):
                    if current_remote:
                        crypt_remotes.append(current_remote)
            
            # 最初のcryptリモートを返す
            if crypt_remotes:
                return crypt_remotes[0]
            
            return None
        except Exception as e:
            self.logger.error(f"Failed to auto-detect crypt remote: {e}")
            return None
    
    def _run_rclone_command(self, args: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
        """Run an rclone command with proper config"""
        cmd = ["rclone"] + args
        if self.config.config_path:
            config_path = Path(self.config.config_path).resolve()
            cmd.extend(["--config", str(config_path)])
        
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=cwd or str(self.temp_dir)
        )
    
    def cleanup(self):
        """Clean up any temporary files"""
        encrypted_dir = Path.cwd() / "eventmonitor_encrypted_files"
        if encrypted_dir.exists():
            import shutil
            try:
                shutil.rmtree(encrypted_dir)
            except Exception as e:
                self.logger.warning(f"Failed to clean up {encrypted_dir}: {e}")
    
    def encrypt_file(self, file_path: Path, encrypted_path: Path) -> Optional[Path]:
        """Encrypt a single file using rclone"""
        try:
            # Create parent directory if it doesn't exist
            encrypted_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get list of existing files before encryption
            # The encrypted files go to eventmonitor_encrypted_files in current directory
            encrypted_dir = Path.cwd() / "eventmonitor_encrypted_files"
            existing_files = set()
            if encrypted_dir.exists():
                existing_files = {f for f in encrypted_dir.rglob('*') if f.is_file()}
            
            # Build rclone command
            cmd = ["rclone", "copyto"]
            if self.config.config_path:
                # Use absolute path for config file
                config_path = Path(self.config.config_path).resolve()
                cmd.extend(["--config", str(config_path)])
            
            # For rclone crypt, we need to preserve the relative path structure
            # but let rclone handle the encryption of directory names
            rel_path = encrypted_path.relative_to(self.temp_dir)
            cmd.extend([
                str(file_path),
                f"{self.config.remote_name}:{rel_path}"
            ])
            
            # Run rclone encryption 
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                self.logger.error(f"rclone encryption failed for {file_path}: {result.stderr}")
                self.logger.error(f"Command was: {' '.join(cmd)}")
                return None
            
            # Debug output
            self.logger.debug(f"rclone output: {result.stdout}")
            self.logger.debug(f"Working directory: {Path.cwd()}")
            
            # Find the newly created encrypted file
            time.sleep(0.5)  # Increased delay to ensure file is written
            
            self.logger.debug(f"Looking for encrypted files in: {encrypted_dir}")
            self.logger.debug(f"Directory exists: {encrypted_dir.exists()}")
            
            if encrypted_dir.exists():
                # Get all files after encryption
                new_files = {f for f in encrypted_dir.rglob('*') if f.is_file()}
                # Find the difference - the newly created file
                created_files = new_files - existing_files
                
                if created_files:
                    # Should be exactly one new file
                    actual_path = created_files.pop()
                    return actual_path
            
            self.logger.error(f"Could not find encrypted file for {file_path}")
            return None
            
        except Exception as e:
            self.logger.error(f"Exception during encryption of {file_path}: {e}")
            return None
    
    def decrypt_file(self, encrypted_path: Path, decrypted_path: Path) -> bool:
        """Decrypt a single file using rclone"""
        try:
            # Create parent directory if it doesn't exist
            decrypted_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build rclone command
            cmd = ["rclone", "copyto"]
            if self.config.config_path:
                cmd.extend(["--config", self.config.config_path])
            
            cmd.extend([
                f"{self.config.remote_name}:{encrypted_path.name}",
                str(decrypted_path)
            ])
            
            # Run rclone decryption
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(encrypted_path.parent)
            )
            
            if result.returncode != 0:
                self.logger.error(f"rclone decryption failed for {encrypted_path}: {result.stderr}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Exception during decryption of {encrypted_path}: {e}")
            return False
    
    def encrypt_files_batch(self, file_paths: List[Path], base_dir: Path, batch_size: int = 500) -> Dict[Path, Path]:
        """Encrypt multiple files and return mapping of original to encrypted paths
        
        Args:
            file_paths: List of files to encrypt
            base_dir: Base directory for relative path calculation
            batch_size: Maximum number of files to process at once (default: 500)
        """
        encrypted_files = {}
        
        if not file_paths:
            return encrypted_files
        
        # Process files in chunks to avoid memory/CPU overload
        total_files = len(file_paths)
        chunks = [file_paths[i:i + batch_size] for i in range(0, total_files, batch_size)]
        
        self.logger.info(f"Processing {total_files} files in {len(chunks)} batches of up to {batch_size} files each")
        
        # Clean up any existing encrypted files directory only at the start
        encrypted_dir = Path.cwd() / "eventmonitor_encrypted_files"
        if encrypted_dir.exists():
            import shutil
            try:
                self.logger.info(f"Cleaning up existing encrypted directory: {encrypted_dir}")
                shutil.rmtree(encrypted_dir)
            except Exception as e:
                self.logger.warning(f"Failed to clean up existing encrypted dir: {e}")
        
        for chunk_idx, chunk in enumerate(chunks):
            self.logger.info(f"Processing batch {chunk_idx + 1}/{len(chunks)} ({len(chunk)} files)")
            
            # Create a temporary directory for source files
            import tempfile
            with tempfile.TemporaryDirectory() as temp_source_dir:
                temp_source_path = Path(temp_source_dir)
                
                # Copy files to temporary directory maintaining structure
                file_mapping = {}  # Maps temp path to original path
                for file_path in chunk:
                    try:
                        # Calculate relative path from base directory
                        rel_path = file_path.relative_to(base_dir)
                        
                        # Create destination in temp directory
                        temp_dest = temp_source_path / rel_path
                        temp_dest.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Copy file
                        import shutil
                        shutil.copy2(file_path, temp_dest)
                        file_mapping[temp_dest] = file_path
                        
                    except Exception as e:
                        self.logger.error(f"Error copying {file_path}: {e}")
                
                if not file_mapping:
                    self.logger.warning(f"No files copied for encryption in batch {chunk_idx + 1}")
                    continue
                
                # Use rclone sync to encrypt entire directory at once
                cmd = ["rclone", "sync"]
                if self.config.config_path:
                    config_path = Path(self.config.config_path).resolve()
                    cmd.extend(["--config", str(config_path)])
                
                # Use batch-specific subdirectory to avoid conflicts
                batch_subdir = f"batch_{chunk_idx}"
                cmd.extend([
                    str(temp_source_path),
                    f"{self.config.remote_name}:{batch_subdir}",
                    "--create-empty-src-dirs",
                    "--transfers", "4",  # Limit concurrent transfers
                    "--checkers", "4"    # Limit concurrent checkers
                ])
                
                self.logger.info(f"Encrypting batch {chunk_idx + 1} ({len(file_mapping)} files)...")
                
                # Run rclone sync
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    self.logger.error(f"rclone batch encryption failed for batch {chunk_idx + 1}: {result.stderr}")
                    continue
                
                # Wait for encrypted files to be created
                time.sleep(1)
                
                # Find all encrypted files
                if encrypted_dir.exists():
                    # Look for files in the batch-specific subdirectory
                    batch_dir = encrypted_dir / batch_subdir
                    if batch_dir.exists():
                        batch_encrypted_files = list(batch_dir.rglob('*'))
                        batch_encrypted_files = [f for f in batch_encrypted_files if f.is_file()]
                    else:
                        # Fallback: look in the entire encrypted directory
                        batch_encrypted_files = list(encrypted_dir.rglob('*'))
                        batch_encrypted_files = [f for f in batch_encrypted_files if f.is_file()]
                    
                    self.logger.debug(f"Found {len(batch_encrypted_files)} encrypted files in batch {chunk_idx + 1}")
                    
                    # Simple mapping: assume same order and count
                    if len(batch_encrypted_files) == len(file_mapping):
                        # Sort both lists to ensure consistent mapping
                        sorted_temp_paths = sorted(file_mapping.keys())
                        sorted_encrypted = sorted(batch_encrypted_files)
                        
                        self.logger.info(f"Mapping {len(sorted_temp_paths)} files to {len(sorted_encrypted)} encrypted files")
                        
                        for temp_path, enc_path in zip(sorted_temp_paths, sorted_encrypted):
                            original_path = file_mapping[temp_path]
                            encrypted_files[original_path] = enc_path
                            self.logger.debug(f"Mapped: {original_path.name} -> {enc_path.name}")
                            self.logger.debug(f"  Original: {original_path}")
                            self.logger.debug(f"  Encrypted: {enc_path}")
                    else:
                        self.logger.warning(f"Batch {chunk_idx + 1}: File count mismatch - expected {len(file_mapping)}, got {len(batch_encrypted_files)}")
                        # Fallback to more complex mapping
                        for temp_path, original_path in file_mapping.items():
                            rel_path = temp_path.relative_to(temp_source_path)
                            
                            # Search for corresponding encrypted file
                            for encrypted_file in batch_encrypted_files:
                                if encrypted_file not in encrypted_files.values():
                                    encrypted_files[original_path] = encrypted_file
                                    self.logger.debug(f"Mapped (fallback): {original_path.name} -> {encrypted_file.name}")
                                    break
                    
                    self.logger.info(f"Batch {chunk_idx + 1} complete: encrypted {len([k for k, v in file_mapping.items() if file_mapping[k] in encrypted_files])} files")
                else:
                    self.logger.error(f"Encrypted directory not found after batch {chunk_idx + 1} encryption")
            
            # Small delay between batches to avoid overload
            if chunk_idx < len(chunks) - 1:
                self.logger.info(f"Waiting 2 seconds before next batch...")
                time.sleep(2)
        
        self.logger.info(f"Total encrypted: {len(encrypted_files)} files")
        return encrypted_files
    
    def cleanup_temp_files(self, encrypted_files: Dict[Path, Path]):
        """Clean up temporary encrypted files"""
        for encrypted_path in encrypted_files.values():
            try:
                if encrypted_path.exists():
                    encrypted_path.unlink()
            except Exception as e:
                self.logger.warning(f"Failed to clean up {encrypted_path}: {e}")
    
    def list_remotes(self) -> List[str]:
        """List all configured rclone remotes"""
        try:
            cmd = ["rclone", "listremotes"]
            if self.config.config_path:
                cmd.extend(["--config", self.config.config_path])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output (each line is a remote with trailing colon)
            remotes = [line.rstrip(':') for line in result.stdout.strip().split('\n') if line]
            return remotes
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to list rclone remotes: {e}")
            return []
    
    def get_remote_info(self, remote_name: str) -> Optional[Dict]:
        """Get information about a specific remote"""
        try:
            cmd = ["rclone", "config", "dump"]
            if self.config.config_path:
                cmd.extend(["--config", self.config.config_path])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            config_data = json.loads(result.stdout)
            return config_data.get(remote_name)
            
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            self.logger.error(f"Failed to get remote info for {remote_name}: {e}")
            return None
    
    def cleanup_temp_dir(self):
        """Clean up the entire temp directory"""
        import shutil
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temp directory: {self.temp_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up temp directory {self.temp_dir}: {e}")
    
    def __del__(self):
        """Destructor to ensure temp directory is cleaned up"""
        try:
            self.cleanup_temp_dir()
        except Exception:
            pass