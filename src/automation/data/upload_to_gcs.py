#!/usr/bin/env python3
"""
GCSアップロードスクリプト

ローカルのダウンロードデータをGCSにアップロードする。
- 重複チェック（MD5ハッシュ比較）
- 差分アップロード
- リトライ処理
- Cloud Run環境対応

Issue #6: ローカル→GCS自動アップロードスクリプトの実装
Issue #54: GCSアップロード処理のCloud Run対応
"""

import base64
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from google.api_core import retry
from google.auth import default as auth_default
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """アップロード結果を格納するデータクラス"""

    total_files: int
    uploaded_files: int
    skipped_files: int
    failed_files: int
    uploaded_bytes: int


class GCSUploader:
    """GCSアップロードクラス"""

    # リトライ設定
    DEFAULT_RETRY = retry.Retry(
        initial=1.0,
        maximum=60.0,
        multiplier=2.0,
        deadline=300.0,
    )

    # サポートするファイル拡張子
    SUPPORTED_EXTENSIONS = {".csv", ".txt", ".lzh"}

    def __init__(
        self,
        project_id: str,
        bucket_name: str,
        local_base_dir: Optional[Path] = None,
        credentials_path: Optional[str] = None,
    ):
        """
        初期化

        Args:
            project_id: GCPプロジェクトID
            bucket_name: GCSバケット名
            local_base_dir: ローカルの基準ディレクトリ（デフォルト: downloaded_files）
            credentials_path: サービスアカウントキーファイルのパス（オプション）
        """
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = self._create_client(project_id, credentials_path)
        self.bucket = self.client.bucket(bucket_name)

        if local_base_dir is None:
            self.local_base_dir = (
                Path(__file__).parent.parent.parent.parent / "downloaded_files"
            )
        else:
            self.local_base_dir = local_base_dir

    def _create_client(
        self, project_id: str, credentials_path: Optional[str]
    ) -> storage.Client:
        """
        GCS Clientを作成

        認証の優先順位:
        1. credentials_path引数で指定されたサービスアカウントキー
        2. GOOGLE_APPLICATION_CREDENTIALS環境変数
        3. Application Default Credentials (Cloud Run/gcloud auth)

        Cloud Run環境では、サービスアカウントが自動的に
        アタッチされるため、追加の認証設定は不要。

        Args:
            project_id: GCPプロジェクトID
            credentials_path: サービスアカウントキーファイルのパス

        Returns:
            GCS Client

        Raises:
            RuntimeError: 認証に失敗した場合
        """
        # 1. 引数で指定されたサービスアカウントキーを使用
        if credentials_path:
            if not os.path.exists(credentials_path):
                raise RuntimeError(
                    f"サービスアカウントキーファイルが見つかりません: {credentials_path}"
                )
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            logger.info("サービスアカウントキーファイルで認証しました")
            return storage.Client(project=project_id, credentials=credentials)

        # 2. 環境変数で指定されたサービスアカウントキーを使用
        env_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if env_credentials_path and os.path.exists(env_credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                env_credentials_path
            )
            logger.info("GOOGLE_APPLICATION_CREDENTIALS で認証しました")
            return storage.Client(project=project_id, credentials=credentials)

        # 3. Application Default Credentials を使用（Cloud Run環境向け）
        try:
            credentials, detected_project = auth_default()
            logger.info(
                f"Application Default Credentials で認証しました "
                f"(検出されたプロジェクト: {detected_project})"
            )
            return storage.Client(project=project_id, credentials=credentials)
        except DefaultCredentialsError as e:
            error_msg = (
                "GCP認証情報が見つかりません。\n"
                "以下のいずれかの方法で認証を設定してください:\n\n"
                "方法1: gcloud CLIでログイン（開発環境向け）\n"
                "  $ gcloud auth application-default login\n\n"
                "方法2: サービスアカウントキーを環境変数で指定\n"
                "  $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json\n\n"
                "方法3: Cloud Run環境では、サービスアカウントを設定\n"
                "  --service-account オプションでデプロイ時に指定"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _calculate_md5(self, file_path: Path) -> str:
        """
        ファイルのMD5ハッシュを計算

        Args:
            file_path: ファイルパス

        Returns:
            MD5ハッシュ値（16進数文字列）
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_gcs_blob_md5(self, blob_name: str) -> Optional[str]:
        """
        GCS上のファイルのMD5ハッシュを取得

        Args:
            blob_name: GCS上のファイル名

        Returns:
            MD5ハッシュ値（存在しない場合はNone）
        """
        blob = self.bucket.blob(blob_name)
        try:
            blob.reload()
            if blob.md5_hash:
                # Base64エンコードされたMD5を16進数に変換
                md5_bytes = base64.b64decode(blob.md5_hash)
                return md5_bytes.hex()
            return None
        except NotFound:
            return None

    def _should_upload(self, local_path: Path, blob_name: str) -> bool:
        """
        アップロードが必要かどうかを判定

        Args:
            local_path: ローカルファイルパス
            blob_name: GCS上のファイル名

        Returns:
            アップロードが必要な場合True
        """
        local_md5 = self._calculate_md5(local_path)
        gcs_md5 = self._get_gcs_blob_md5(blob_name)

        if gcs_md5 is None:
            return True

        return local_md5 != gcs_md5

    def _upload_file_with_retry(
        self,
        local_path: Path,
        blob_name: str,
        max_retries: int = 3,
    ) -> bool:
        """
        リトライ付きでファイルをアップロード

        Args:
            local_path: ローカルファイルパス
            blob_name: GCS上のファイル名
            max_retries: 最大リトライ回数

        Returns:
            アップロード成功の場合True
        """
        blob = self.bucket.blob(blob_name)

        for attempt in range(max_retries):
            try:
                blob.upload_from_filename(
                    str(local_path),
                    retry=self.DEFAULT_RETRY,
                )
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.warning(
                        f"リトライ {attempt + 1}/{max_retries}: "
                        f"{wait_time}秒後に再試行... ({blob_name})"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"アップロード失敗 {blob_name}: {e}")
                    return False

        return False

    def upload_directory(
        self,
        data_type: str,
        force: bool = False,
        dry_run: bool = False,
    ) -> UploadResult:
        """
        特定のデータタイプのディレクトリをアップロード

        Args:
            data_type: データタイプ（Baa, Kyf, Sec など）
            force: 強制アップロード（重複チェックをスキップ）
            dry_run: ドライラン（実際にはアップロードしない）

        Returns:
            アップロード結果
        """
        local_dir = self.local_base_dir / data_type

        if not local_dir.exists():
            logger.warning(f"ディレクトリが見つかりません: {local_dir}")
            return UploadResult(
                total_files=0,
                uploaded_files=0,
                skipped_files=0,
                failed_files=0,
                uploaded_bytes=0,
            )

        # 対象ファイルを収集
        files = [
            f
            for f in local_dir.iterdir()
            if f.is_file() and f.suffix.lower() in self.SUPPORTED_EXTENSIONS
        ]

        total_files = len(files)
        uploaded_files = 0
        skipped_files = 0
        failed_files = 0
        uploaded_bytes = 0

        logger.info(f"データタイプ: {data_type}, 対象ファイル数: {total_files}")

        for i, file_path in enumerate(sorted(files), 1):
            blob_name = f"{data_type}/{file_path.name}"

            # 重複チェック
            if not force and not self._should_upload(file_path, blob_name):
                skipped_files += 1
                if i % 100 == 0:
                    logger.debug(
                        f"進捗: {i}/{total_files} "
                        f"(スキップ: {skipped_files})"
                    )
                continue

            if dry_run:
                logger.info(f"[DRY RUN] アップロード: {file_path.name}")
                uploaded_files += 1
                uploaded_bytes += file_path.stat().st_size
                continue

            # アップロード
            if self._upload_file_with_retry(file_path, blob_name):
                uploaded_files += 1
                uploaded_bytes += file_path.stat().st_size
                if i % 100 == 0 or i == total_files:
                    logger.info(
                        f"進捗: {i}/{total_files} "
                        f"(アップロード: {uploaded_files})"
                    )
            else:
                failed_files += 1
                logger.error(f"失敗: {file_path.name}")

        return UploadResult(
            total_files=total_files,
            uploaded_files=uploaded_files,
            skipped_files=skipped_files,
            failed_files=failed_files,
            uploaded_bytes=uploaded_bytes,
        )

    def upload_all(
        self,
        force: bool = False,
        dry_run: bool = False,
    ) -> UploadResult:
        """
        すべてのデータタイプをアップロード

        Args:
            force: 強制アップロード
            dry_run: ドライラン

        Returns:
            アップロード結果の合計
        """
        total_result = UploadResult(
            total_files=0,
            uploaded_files=0,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=0,
        )

        # ダウンロードディレクトリ内のサブディレクトリを取得
        if not self.local_base_dir.exists():
            logger.warning(f"ベースディレクトリが見つかりません: {self.local_base_dir}")
            return total_result

        data_types = [
            d.name
            for d in self.local_base_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

        logger.info(f"検出されたデータタイプ: {', '.join(sorted(data_types))}")

        for data_type in sorted(data_types):
            result = self.upload_directory(data_type, force=force, dry_run=dry_run)
            total_result.total_files += result.total_files
            total_result.uploaded_files += result.uploaded_files
            total_result.skipped_files += result.skipped_files
            total_result.failed_files += result.failed_files
            total_result.uploaded_bytes += result.uploaded_bytes

        return total_result

    def upload_file(
        self,
        local_path: Path,
        blob_name: str,
        force: bool = False,
    ) -> bool:
        """
        単一ファイルをアップロード

        Args:
            local_path: ローカルファイルパス
            blob_name: GCS上のファイル名
            force: 強制アップロード

        Returns:
            アップロード成功の場合True
        """
        if not local_path.exists():
            logger.error(f"ファイルが見つかりません: {local_path}")
            return False

        if not force and not self._should_upload(local_path, blob_name):
            logger.info(f"スキップ（既存・同一）: {blob_name}")
            return True

        return self._upload_file_with_retry(local_path, blob_name)

    def verify_bucket_exists(self) -> bool:
        """
        バケットが存在するか確認

        Returns:
            バケットが存在する場合True
        """
        try:
            self.client.get_bucket(self.bucket_name)
            return True
        except NotFound:
            return False


def format_bytes(size: int) -> str:
    """バイト数を人間が読める形式に変換"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def create_uploader_from_env(
    local_base_dir: Optional[Path] = None,
) -> Optional[GCSUploader]:
    """
    環境変数からGCSUploaderを作成

    環境変数:
        GCP_PROJECT_ID: GCPプロジェクトID
        GCS_BUCKET_RAW: バケット名のサフィックス（デフォルト: keiba-raw-data）
        GOOGLE_APPLICATION_CREDENTIALS: サービスアカウントキーファイルのパス（オプション）

    Args:
        local_base_dir: ローカルの基準ディレクトリ

    Returns:
        GCSUploaderインスタンス（設定が不足している場合はNone）
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP_PROJECT_ID環境変数が設定されていません")
        return None

    bucket_suffix = os.environ.get("GCS_BUCKET_RAW", "keiba-raw-data")
    bucket_name = f"{project_id}-{bucket_suffix}"

    logger.info(f"GCPプロジェクトID: {project_id}")
    logger.info(f"GCSバケット名: {bucket_name}")

    try:
        return GCSUploader(
            project_id=project_id,
            bucket_name=bucket_name,
            local_base_dir=local_base_dir,
        )
    except RuntimeError as e:
        logger.error(f"GCSUploaderの作成に失敗: {e}")
        return None


def main():
    """メイン処理"""
    import argparse
    import sys

    from dotenv import load_dotenv

    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="GCSアップロードスクリプト")
    parser.add_argument(
        "--data-type",
        type=str,
        default=None,
        help="アップロードするデータタイプ（例: Baa, Kyf）。指定しない場合はすべて。",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="強制アップロード（重複チェックをスキップ）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="ドライラン（実際にはアップロードしない）",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="サービスアカウントキーファイルのパス",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        default=None,
        help="ローカルの基準ディレクトリ",
    )
    args = parser.parse_args()

    # .envファイルを読み込み
    load_dotenv()

    # 環境変数を取得
    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_suffix = os.getenv("GCS_BUCKET_RAW", "keiba-raw-data")

    if not project_id:
        logger.error("GCP_PROJECT_ID環境変数が設定されていません。")
        sys.exit(1)

    # バケット名を構築
    bucket_name = f"{project_id}-{bucket_suffix}"

    logger.info(f"GCPプロジェクトID: {project_id}")
    logger.info(f"GCSバケット名: {bucket_name}")

    # ローカルディレクトリの設定
    local_base_dir = Path(args.local_dir) if args.local_dir else None

    # アップローダーを作成
    try:
        uploader = GCSUploader(
            project_id,
            bucket_name,
            local_base_dir=local_base_dir,
            credentials_path=args.credentials,
        )
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)

    # バケットの存在確認
    if not uploader.verify_bucket_exists():
        logger.error(f"バケット {bucket_name} が見つかりません。")
        logger.error("scripts/setup_gcp.sh を実行してバケットを作成してください。")
        sys.exit(1)

    # アップロード実行
    start_time = time.time()

    if args.data_type:
        result = uploader.upload_directory(
            args.data_type,
            force=args.force,
            dry_run=args.dry_run,
        )
    else:
        result = uploader.upload_all(
            force=args.force,
            dry_run=args.dry_run,
        )

    elapsed_time = time.time() - start_time

    # 結果を表示
    logger.info("=" * 60)
    logger.info("アップロード結果")
    logger.info("=" * 60)
    logger.info(f"総ファイル数: {result.total_files}")
    logger.info(f"アップロード: {result.uploaded_files}")
    logger.info(f"スキップ: {result.skipped_files}")
    logger.info(f"失敗: {result.failed_files}")
    logger.info(f"アップロードサイズ: {format_bytes(result.uploaded_bytes)}")
    logger.info(f"処理時間: {elapsed_time:.2f}秒")

    if result.failed_files > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
