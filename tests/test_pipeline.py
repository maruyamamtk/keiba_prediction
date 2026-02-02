#!/usr/bin/env python3
"""
src.data.pipeline のテスト

データパイプライン統合モジュールのテスト。
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data.jrdb_downloader import DownloadResult
from src.data.pipeline import DataPipeline, PipelineResult, create_pipeline_from_env
from src.data.upload_to_gcs import UploadResult


class TestDataPipeline:
    """DataPipelineクラスのテスト"""

    def test_init_with_instances(self):
        """インスタンスを渡して初期化"""
        downloader = MagicMock()
        uploader = MagicMock()

        pipeline = DataPipeline(downloader=downloader, uploader=uploader)

        assert pipeline.downloader is downloader
        assert pipeline.uploader is uploader

    def test_init_without_instances_raises_error(self):
        """インスタンスなしで初期化するとエラー"""
        with patch("src.data.pipeline.create_downloader_from_env", return_value=None):
            with patch("src.data.pipeline.create_uploader_from_env", return_value=None):
                with pytest.raises(RuntimeError, match="JRDBDownloaderの初期化に失敗"):
                    DataPipeline()

    def test_run_download_and_upload_success(self):
        """ダウンロード→アップロードが成功"""
        # モック作成
        downloader = MagicMock()
        uploader = MagicMock()

        # ダウンロード結果
        download_result = DownloadResult(
            total_files=10,
            downloaded_files=5,
            skipped_files=3,
            failed_files=2,
        )
        downloader.download_all_from_date.return_value = {
            "BAA": download_result,
        }
        downloader.get_output_dir.return_value = Path("/tmp/test")

        # アップロード結果
        upload_result = UploadResult(
            total_files=5,
            uploaded_files=5,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=1024,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=False)
        result = pipeline.run_download_and_upload(start_date="240101")

        # 検証
        assert result.success is True
        assert result.download_result.downloaded_files == 5
        assert result.upload_result.uploaded_files == 5
        downloader.download_all_from_date.assert_called_once_with("240101")
        uploader.upload_all.assert_called_once()

    def test_run_download_and_upload_with_datatype(self):
        """特定のデータタイプでダウンロード→アップロード"""
        downloader = MagicMock()
        uploader = MagicMock()

        download_result = DownloadResult(
            total_files=5,
            downloaded_files=3,
            skipped_files=2,
            failed_files=0,
        )
        downloader.download_from_date.return_value = download_result
        downloader.get_output_dir.return_value = Path("/tmp/test")

        upload_result = UploadResult(
            total_files=3,
            uploaded_files=3,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=512,
        )
        uploader.upload_directory.return_value = upload_result

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=False)
        result = pipeline.run_download_and_upload(start_date="240101", datatype="BAA")

        # 検証
        assert result.success is True
        downloader.download_from_date.assert_called_once_with("BAA", "240101")
        uploader.upload_directory.assert_called_once_with("BAA")

    def test_run_download_and_upload_zero_downloads(self):
        """ダウンロードが0件の場合、アップロードをスキップ"""
        downloader = MagicMock()
        uploader = MagicMock()

        download_result = DownloadResult(
            total_files=0,
            downloaded_files=0,
            skipped_files=0,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {
            "BAA": download_result,
        }
        downloader.get_output_dir.return_value = Path("/tmp/test")

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=False)
        result = pipeline.run_download_and_upload(start_date="240101")

        # アップロードが呼ばれていないことを確認
        uploader.upload_all.assert_not_called()
        assert result.success is True
        assert result.upload_result.uploaded_files == 0

    def test_run_download_and_upload_with_temp_dir_cleanup(self):
        """一時ディレクトリを使用する場合、完了後にクリーンアップ"""
        downloader = MagicMock()
        uploader = MagicMock()

        download_result = DownloadResult(
            total_files=5,
            downloaded_files=3,
            skipped_files=2,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        downloader.get_output_dir.return_value = Path("/tmp/test")

        upload_result = UploadResult(
            total_files=3,
            uploaded_files=3,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=512,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=True)
        result = pipeline.run_download_and_upload(start_date="240101")

        # クリーンアップが呼ばれたことを確認
        downloader.cleanup.assert_called_once()
        assert result.success is True

    def test_run_download_and_upload_error_handling(self):
        """エラーが発生した場合、エラー結果を返す"""
        downloader = MagicMock()
        uploader = MagicMock()

        downloader.download_all_from_date.side_effect = Exception("ダウンロード失敗")

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=True)
        result = pipeline.run_download_and_upload(start_date="240101")

        # エラー結果を確認
        assert result.success is False
        assert "ダウンロード失敗" in result.error_message
        # エラー時もクリーンアップが呼ばれることを確認
        downloader.cleanup.assert_called_once()

    def test_run_download_and_upload_sets_uploader_local_base_dir(self):
        """アップローダーのローカルディレクトリがダウンロード先に設定される"""
        downloader = MagicMock()
        uploader = MagicMock()

        download_result = DownloadResult(
            total_files=5,
            downloaded_files=3,
            skipped_files=2,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        output_dir = Path("/tmp/test_output")
        downloader.get_output_dir.return_value = output_dir

        upload_result = UploadResult(
            total_files=3,
            uploaded_files=3,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=512,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = DataPipeline(downloader=downloader, uploader=uploader, use_temp_dir=False)
        pipeline.run_download_and_upload(start_date="240101")

        # アップローダーのlocal_base_dirが設定されたことを確認
        assert uploader.local_base_dir == output_dir


class TestCreatePipelineFromEnv:
    """create_pipeline_from_env関数のテスト"""

    def test_create_pipeline_from_env_success(self):
        """環境変数から正常にパイプラインを作成"""
        downloader = MagicMock()
        uploader = MagicMock()

        with patch("src.data.pipeline.create_downloader_from_env", return_value=downloader):
            with patch("src.data.pipeline.create_uploader_from_env", return_value=uploader):
                pipeline = create_pipeline_from_env()

                assert pipeline is not None
                assert pipeline.downloader is downloader
                assert pipeline.uploader is uploader

    def test_create_pipeline_from_env_downloader_none(self):
        """ダウンローダーの作成に失敗した場合"""
        uploader = MagicMock()

        with patch("src.data.pipeline.create_downloader_from_env", return_value=None):
            with patch("src.data.pipeline.create_uploader_from_env", return_value=uploader):
                pipeline = create_pipeline_from_env()

                assert pipeline is None

    def test_create_pipeline_from_env_uploader_none(self):
        """アップローダーの作成に失敗した場合"""
        downloader = MagicMock()

        with patch("src.data.pipeline.create_downloader_from_env", return_value=downloader):
            with patch("src.data.pipeline.create_uploader_from_env", return_value=None):
                pipeline = create_pipeline_from_env()

                assert pipeline is None


class TestPipelineResult:
    """PipelineResultデータクラスのテスト"""

    def test_pipeline_result_success(self):
        """成功結果の作成"""
        download_result = DownloadResult(
            total_files=10,
            downloaded_files=5,
            skipped_files=3,
            failed_files=2,
        )
        upload_result = UploadResult(
            total_files=5,
            uploaded_files=5,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=1024,
        )

        result = PipelineResult(
            success=True,
            download_result=download_result,
            upload_result=upload_result,
            output_dir=Path("/tmp/test"),
        )

        assert result.success is True
        assert result.download_result == download_result
        assert result.upload_result == upload_result
        assert result.error_message is None

    def test_pipeline_result_error(self):
        """エラー結果の作成"""
        result = PipelineResult(
            success=False,
            error_message="テストエラー",
        )

        assert result.success is False
        assert result.error_message == "テストエラー"
        assert result.download_result is None
        assert result.upload_result is None
