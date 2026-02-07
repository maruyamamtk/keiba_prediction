#!/usr/bin/env python3
"""
データパイプライン統合モジュール

JRDBダウンロード → GCSアップロード → BigQueryロード の
一連のフローを統合管理する。

Cloud Run環境での実行を想定し、各ステップのエラーハンドリングと
リトライ処理を含む。

Issue #66: データパイプライン統合とCloud Run対応
"""

import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.data.jrdb_downloader import (
    DownloadResult,
    JRDBDownloader,
    create_downloader_from_env,
)
from src.data.upload_to_gcs import GCSUploader, UploadResult, create_uploader_from_env

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """パイプライン実行結果を格納するデータクラス"""

    success: bool
    download_result: Optional[DownloadResult] = None
    upload_result: Optional[UploadResult] = None
    error_message: Optional[str] = None
    output_dir: Optional[Path] = None


class DataPipeline:
    """データパイプライン統合クラス"""

    def __init__(
        self,
        downloader: Optional[JRDBDownloader] = None,
        uploader: Optional[GCSUploader] = None,
        use_temp_dir: bool = True,
    ):
        """
        初期化

        Args:
            downloader: JRDBDownloaderインスタンス（省略時は環境変数から作成）
            uploader: GCSUploaderインスタンス（省略時は環境変数から作成）
            use_temp_dir: 一時ディレクトリを使用するか（Cloud Run環境向け）
        """
        self.use_temp_dir = use_temp_dir
        self.downloader = downloader or create_downloader_from_env()
        self.uploader = uploader or create_uploader_from_env()

        if self.downloader is None:
            raise RuntimeError("JRDBDownloaderの初期化に失敗しました")
        if self.uploader is None:
            raise RuntimeError("GCSUploaderの初期化に失敗しました")

    def run_download_and_upload(
        self,
        start_date: str,
        datatype: Optional[str] = None,
    ) -> PipelineResult:
        """
        ダウンロード→GCSアップロードを実行

        Args:
            start_date: 開始日付（yymmdd形式）
            datatype: データタイプ（省略時はすべて）

        Returns:
            パイプライン実行結果
        """
        logger.info(f"データパイプライン開始: start_date={start_date}, datatype={datatype}")

        try:
            # Step 1: JRDBダウンロード
            logger.info("Step 1: JRDBダウンロード開始")
            if datatype:
                download_result = self.downloader.download_from_date(
                    datatype.upper(), start_date
                )
                total_downloaded = download_result.downloaded_files
            else:
                download_results = self.downloader.download_all_from_date(start_date)
                total_downloaded = sum(r.downloaded_files for r in download_results.values())
                # 合計結果を作成
                download_result = DownloadResult(
                    total_files=sum(r.total_files for r in download_results.values()),
                    downloaded_files=total_downloaded,
                    skipped_files=sum(r.skipped_files for r in download_results.values()),
                    failed_files=sum(r.failed_files for r in download_results.values()),
                )

            logger.info(f"Step 1完了: {total_downloaded}ファイルをダウンロード")

            # ダウンロードファイルが0件の場合は早期リターン
            if total_downloaded == 0:
                logger.info("ダウンロードファイルが0件のため、アップロードをスキップします")
                return PipelineResult(
                    success=True,
                    download_result=download_result,
                    upload_result=UploadResult(
                        total_files=0,
                        uploaded_files=0,
                        skipped_files=0,
                        failed_files=0,
                        uploaded_bytes=0,
                    ),
                    output_dir=self.downloader.get_output_dir(),
                )

            # Step 2: GCSアップロード
            logger.info("Step 2: GCSアップロード開始")

            # アップローダーのローカルディレクトリをダウンロード先に設定
            self.uploader.local_base_dir = self.downloader.get_output_dir()

            if datatype:
                upload_result = self.uploader.upload_directory(datatype)
            else:
                upload_result = self.uploader.upload_all()

            logger.info(
                f"Step 2完了: {upload_result.uploaded_files}ファイルをGCSにアップロード"
            )

            # 一時ディレクトリの場合はクリーンアップ
            if self.use_temp_dir:
                self.downloader.cleanup()
                logger.info("一時ディレクトリをクリーンアップしました")

            # 成功判定: アップロード失敗が0件
            success = upload_result.failed_files == 0

            return PipelineResult(
                success=success,
                download_result=download_result,
                upload_result=upload_result,
                output_dir=self.downloader.get_output_dir(),
            )

        except Exception as e:
            logger.error(f"パイプライン実行中にエラーが発生しました: {e}", exc_info=True)
            # エラー時も一時ディレクトリをクリーンアップ
            if self.use_temp_dir:
                try:
                    self.downloader.cleanup()
                except Exception:
                    pass

            return PipelineResult(
                success=False,
                error_message=str(e),
            )


def create_pipeline_from_env(use_temp_dir: bool = True) -> Optional[DataPipeline]:
    """
    環境変数からDataPipelineを作成

    Args:
        use_temp_dir: 一時ディレクトリを使用するか

    Returns:
        DataPipelineインスタンス（設定が不足している場合はNone）
    """
    try:
        downloader = create_downloader_from_env()
        uploader = create_uploader_from_env()

        if downloader is None or uploader is None:
            return None

        return DataPipeline(
            downloader=downloader,
            uploader=uploader,
            use_temp_dir=use_temp_dir,
        )
    except RuntimeError as e:
        logger.error(f"パイプラインの作成に失敗: {e}")
        return None


def main():
    """メイン処理（CLIからの実行用）"""
    import argparse
    import sys

    from dotenv import load_dotenv

    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="データパイプライン実行")
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="開始日付（yymmdd形式、例: 240101）",
    )
    parser.add_argument(
        "--datatype",
        type=str,
        default=None,
        help="データタイプ（例: KAA）。指定しない場合はすべて",
    )
    parser.add_argument(
        "--no-temp-dir",
        action="store_true",
        help="一時ディレクトリを使用しない（既存のdownloaded_filesを使用）",
    )
    args = parser.parse_args()

    # .envを読み込み
    load_dotenv()

    # パイプライン作成
    pipeline = create_pipeline_from_env(use_temp_dir=not args.no_temp_dir)
    if pipeline is None:
        logger.error("パイプラインの作成に失敗しました")
        sys.exit(1)

    # パイプライン実行
    result = pipeline.run_download_and_upload(
        start_date=args.start_date,
        datatype=args.datatype,
    )

    # 結果表示
    logger.info("=" * 60)
    logger.info("データパイプライン実行結果")
    logger.info("=" * 60)

    if result.success:
        logger.info("ステータス: 成功")
        if result.download_result:
            logger.info(f"ダウンロード: {result.download_result.downloaded_files}ファイル")
            logger.info(f"  スキップ: {result.download_result.skipped_files}ファイル")
            logger.info(f"  失敗: {result.download_result.failed_files}ファイル")
        if result.upload_result:
            logger.info(f"アップロード: {result.upload_result.uploaded_files}ファイル")
            logger.info(f"  スキップ: {result.upload_result.skipped_files}ファイル")
            logger.info(f"  失敗: {result.upload_result.failed_files}ファイル")
    else:
        logger.error("ステータス: 失敗")
        if result.error_message:
            logger.error(f"エラー: {result.error_message}")

    logger.info("=" * 60)

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
