#!/usr/bin/env python3
"""
src.pipeline.daily_pipeline と src.api.app のテスト

日次パイプラインとHTTP APIのテスト。
"""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.data.jrdb_downloader import DownloadResult
from src.data.load_to_bq import BatchLoadResult, LoadResult
from src.data.upload_to_gcs import UploadResult
from src.pipeline.daily_pipeline import (
    DailyPipeline,
    PipelineResult,
    StepResult,
    create_pipeline_from_env,
)


class TestDailyPipeline:
    """DailyPipelineクラスのテスト"""

    def test_init_with_instances(self):
        """インスタンスを渡して初期化"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()

        pipeline = DailyPipeline(
            downloader=downloader,
            uploader=uploader,
            bq_loader=bq_loader,
        )

        assert pipeline._downloader is downloader
        assert pipeline._uploader is uploader
        assert pipeline._bq_loader is bq_loader

    def test_parse_target_date_none(self):
        """target_dateがNoneの場合は当日を返す"""
        result = DailyPipeline.parse_target_date(None)
        assert result == date.today()

    def test_parse_target_date_valid(self):
        """正しい日付形式"""
        result = DailyPipeline.parse_target_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_parse_target_date_invalid(self):
        """不正な日付形式"""
        with pytest.raises(ValueError, match="日付形式が不正"):
            DailyPipeline.parse_target_date("20240115")

    def test_date_to_yymmdd(self):
        """dateオブジェクトをyymmdd形式に変換"""
        d = date(2024, 1, 15)
        result = DailyPipeline.date_to_yymmdd(d)
        assert result == "240115"


class TestDailyPipelineStepDownload:
    """DailyPipeline._step_downloadのテスト"""

    def test_step_download_success(self):
        """ダウンロード成功"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=10,
            downloaded_files=5,
            skipped_files=3,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}

        pipeline = DailyPipeline(downloader=downloader)
        result = pipeline._step_download(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["downloaded"] == 5
        assert result.details["skipped"] == 3
        assert result.details["failed"] == 0
        downloader.download_all_from_date.assert_called_once_with("240115")

    def test_step_download_partial(self):
        """一部ダウンロード失敗"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=10,
            downloaded_files=5,
            skipped_files=3,
            failed_files=2,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}

        pipeline = DailyPipeline(downloader=downloader)
        result = pipeline._step_download(date(2024, 1, 15))

        assert result.status == "partial"
        assert result.details["failed"] == 2

    def test_step_download_error(self):
        """ダウンロードエラー"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("接続エラー")

        pipeline = DailyPipeline(downloader=downloader)
        result = pipeline._step_download(date(2024, 1, 15))

        assert result.status == "failed"
        assert "接続エラー" in result.error_message


class TestDailyPipelineStepUpload:
    """DailyPipeline._step_uploadのテスト"""

    def test_step_upload_success(self):
        """アップロード成功"""
        downloader = MagicMock()
        uploader = MagicMock()

        downloader.get_output_dir.return_value = Path("/tmp/test")
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=5,
            uploaded_files=5,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=1024,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = DailyPipeline(downloader=downloader, uploader=uploader)
        result = pipeline._step_upload()

        assert result.status == "success"
        assert result.details["uploaded"] == 5
        # ローカルディレクトリが元に戻されていることを確認
        assert uploader.local_base_dir == Path("/original")

    def test_step_upload_partial(self):
        """一部アップロード失敗"""
        downloader = MagicMock()
        uploader = MagicMock()

        downloader.get_output_dir.return_value = Path("/tmp/test")
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=5,
            uploaded_files=3,
            skipped_files=0,
            failed_files=2,
            uploaded_bytes=512,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = DailyPipeline(downloader=downloader, uploader=uploader)
        result = pipeline._step_upload()

        assert result.status == "partial"
        assert result.details["failed"] == 2


class TestDailyPipelineStepLoadToBq:
    """DailyPipeline._step_load_to_bqのテスト"""

    def test_step_load_to_bq_success(self):
        """BigQueryロード成功"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240115.csv",
            "Kyf/KYF240115.csv",
            "Baa/BAA240114.csv",  # 対象外
        ]
        batch_result = BatchLoadResult(
            total_files=2,
            success_count=2,
            skipped_count=0,
            failed_count=0,
            total_records=100,
            results=[],
            failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["files"] == 2
        assert result.details["records"] == 100
        # 正しいファイルのみがロードされていることを確認
        call_args = bq_loader.load_files_batch.call_args
        assert "Baa/BAA240115.csv" in call_args[0][0]
        assert "Kyf/KYF240115.csv" in call_args[0][0]
        assert "Baa/BAA240114.csv" not in call_args[0][0]

    def test_step_load_to_bq_no_files(self):
        """ロード対象ファイルなし"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = ["Baa/BAA240114.csv"]

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["files"] == 0
        bq_loader.load_files_batch.assert_not_called()

    def test_step_load_to_bq_partial(self):
        """一部ロード失敗"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = ["Baa/BAA240115.csv"]
        batch_result = BatchLoadResult(
            total_files=2,
            success_count=1,
            skipped_count=0,
            failed_count=1,
            total_records=50,
            results=[],
            failed_files=["Kyf/KYF240115.csv"],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 15))

        assert result.status == "partial"
        assert result.details["failed"] == 1


class TestDailyPipelineRun:
    """DailyPipeline.runのテスト"""

    def test_run_success(self):
        """パイプライン全体が成功"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()

        # ダウンロード
        download_result = DownloadResult(
            total_files=10, downloaded_files=5, skipped_files=3, failed_files=0
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        downloader.get_output_dir.return_value = Path("/tmp/test")

        # アップロード
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=5,
            uploaded_files=5,
            skipped_files=0,
            failed_files=0,
            uploaded_bytes=1024,
        )
        uploader.upload_all.return_value = upload_result

        # BigQueryロード
        bq_loader.list_csv_files.return_value = ["Baa/BAA240115.csv"]
        batch_result = BatchLoadResult(
            total_files=1,
            success_count=1,
            skipped_count=0,
            failed_count=0,
            total_records=100,
            results=[],
            failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = DailyPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader
        )
        result = pipeline.run("2024-01-15")

        assert result.status == "success"
        assert result.target_date == "2024-01-15"
        assert result.files_downloaded == 5
        assert result.files_uploaded == 5
        assert result.files_loaded == 1
        assert result.records_loaded == 100
        assert len(result.steps) == 3
        downloader.cleanup.assert_called_once()

    def test_run_download_failure(self):
        """ダウンロード失敗時にパイプラインが停止"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("ダウンロードエラー")

        pipeline = DailyPipeline(downloader=downloader)
        result = pipeline.run("2024-01-15")

        assert result.status == "failed"
        assert "ダウンロード失敗" in result.error_message
        assert len(result.steps) == 1

    def test_run_cleanup_on_error(self):
        """エラー時もクリーンアップが実行される"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("エラー")

        pipeline = DailyPipeline(downloader=downloader)
        pipeline.run("2024-01-15")

        downloader.cleanup.assert_called_once()


class TestPipelineResultToDict:
    """PipelineResult.to_dictのテスト"""

    def test_to_dict(self):
        """辞書形式への変換"""
        result = PipelineResult(
            status="success",
            target_date="2024-01-15",
            files_downloaded=5,
            files_uploaded=5,
            files_loaded=3,
            records_loaded=100,
            duration_seconds=10.5,
            steps=[
                StepResult(
                    step_name="download",
                    status="success",
                    duration_seconds=3.0,
                    details={"downloaded": 5},
                )
            ],
        )

        d = result.to_dict()

        assert d["status"] == "success"
        assert d["target_date"] == "2024-01-15"
        assert d["files_loaded"] == 3
        assert d["duration_seconds"] == 10.5
        assert len(d["steps"]) == 1
        assert d["steps"][0]["step_name"] == "download"


class TestCreatePipelineFromEnv:
    """create_pipeline_from_env関数のテスト"""

    def test_create_pipeline_from_env_success(self):
        """環境変数から正常にパイプラインを作成"""
        pipeline = create_pipeline_from_env()
        assert pipeline is not None
        assert isinstance(pipeline, DailyPipeline)


# FastAPI テスト
class TestDailyPipelineAPI:
    """FastAPI エンドポイントのテスト"""

    @pytest.fixture
    def client(self):
        """テストクライアント"""
        from fastapi.testclient import TestClient

        from src.api.app import app

        return TestClient(app)

    def test_root_endpoint(self, client):
        """ルートエンドポイント"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_health_endpoint(self, client):
        """ヘルスチェックエンドポイント"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_load_daily_validation_error(self, client):
        """日付形式が不正な場合"""
        response = client.post(
            "/api/v1/load/daily",
            json={"target_date": "invalid-date"},
        )
        assert response.status_code == 422

    @patch("src.api.app.get_pipeline")
    def test_load_daily_success(self, mock_get_pipeline, client):
        """日次ロード成功"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = PipelineResult(
            status="success",
            target_date="2024-01-15",
            files_downloaded=5,
            files_uploaded=5,
            files_loaded=3,
            records_loaded=100,
            duration_seconds=10.5,
        )
        mock_get_pipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/load/daily",
            json={"target_date": "2024-01-15"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["target_date"] == "2024-01-15"
        assert data["files_loaded"] == 3
        assert data["records_loaded"] == 100

    @patch("src.api.app.get_pipeline")
    def test_load_daily_default_date(self, mock_get_pipeline, client):
        """日付省略時は当日"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = PipelineResult(
            status="success",
            target_date=date.today().strftime("%Y-%m-%d"),
            files_loaded=0,
            records_loaded=0,
        )
        mock_get_pipeline.return_value = mock_pipeline

        response = client.post("/api/v1/load/daily", json={})

        assert response.status_code == 200
        mock_pipeline.run.assert_called_once_with(None)

    @patch("src.api.app.get_pipeline")
    def test_load_daily_failed(self, mock_get_pipeline, client):
        """日次ロード失敗"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = PipelineResult(
            status="failed",
            target_date="2024-01-15",
            error_message="ダウンロードエラー",
        )
        mock_get_pipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/load/daily",
            json={"target_date": "2024-01-15"},
        )

        assert response.status_code == 500
        data = response.json()
        assert data["status"] == "failed"
        assert data["error_message"] == "ダウンロードエラー"

    def test_load_daily_async_accepted(self, client):
        """非同期ロードが受付される"""
        response = client.post(
            "/api/v1/load/daily/async",
            json={"target_date": "2024-01-15"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert data["target_date"] == "2024-01-15"
