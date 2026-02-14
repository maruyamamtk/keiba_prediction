#!/usr/bin/env python3
"""
src.automation.pipeline.full_load_pipeline と src.automation.api.app の全件ロードテスト

過去分全件ロードパイプラインとHTTP APIのテスト。
"""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.automation.data.jrdb_downloader import DownloadResult
from src.automation.data.load_to_bq import BatchLoadResult
from src.automation.data.upload_to_gcs import UploadResult
from src.automation.pipeline.full_load_pipeline import (
    FullLoadPipeline,
    FullLoadResult,
    FullLoadStepResult,
)


class TestFullLoadPipeline:
    """FullLoadPipelineクラスのテスト"""

    def test_init_with_instances(self):
        """インスタンスを渡して初期化"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()

        pipeline = FullLoadPipeline(
            downloader=downloader,
            uploader=uploader,
            bq_loader=bq_loader,
        )

        assert pipeline._downloader is downloader
        assert pipeline._uploader is uploader
        assert pipeline._bq_loader is bq_loader

    def test_parse_date_none(self):
        """Noneの場合はNoneを返す"""
        result = FullLoadPipeline.parse_date(None)
        assert result is None

    def test_parse_date_valid(self):
        """正しい日付形式"""
        result = FullLoadPipeline.parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_parse_date_invalid(self):
        """不正な日付形式"""
        with pytest.raises(ValueError, match="日付形式が不正"):
            FullLoadPipeline.parse_date("20240115")

    def test_date_to_yymmdd(self):
        """yymmdd変換"""
        d = date(2024, 1, 15)
        assert FullLoadPipeline.date_to_yymmdd(d) == "240115"


class TestFilterFilesByDate:
    """_filter_files_by_dateのテスト"""

    def test_filter_with_start_and_end(self):
        """開始日・終了日の両方でフィルタ"""
        files = [
            "Baa/BAA230101.csv",
            "Baa/BAA240115.csv",
            "Baa/BAA240601.csv",
            "Baa/BAA250101.csv",
        ]
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        result = FullLoadPipeline._filter_files_by_date(files, start, end)

        assert "Baa/BAA240115.csv" in result
        assert "Baa/BAA240601.csv" in result
        assert "Baa/BAA230101.csv" not in result
        assert "Baa/BAA250101.csv" not in result

    def test_filter_with_start_only(self):
        """開始日のみでフィルタ"""
        files = [
            "Baa/BAA230101.csv",
            "Baa/BAA240115.csv",
        ]
        start = date(2024, 1, 1)

        result = FullLoadPipeline._filter_files_by_date(files, start, None)

        assert "Baa/BAA240115.csv" in result
        assert "Baa/BAA230101.csv" not in result

    def test_filter_with_end_only(self):
        """終了日のみでフィルタ"""
        files = [
            "Baa/BAA240115.csv",
            "Baa/BAA250101.csv",
        ]
        end = date(2024, 12, 31)

        result = FullLoadPipeline._filter_files_by_date(files, None, end)

        assert "Baa/BAA240115.csv" in result
        assert "Baa/BAA250101.csv" not in result

    def test_filter_excludes_1990s(self):
        """90年代のファイルを除外"""
        files = [
            "Baa/BAA990101.csv",  # 1999年 → 除外
            "Baa/BAA240101.csv",
        ]

        result = FullLoadPipeline._filter_files_by_date(
            files, None, None
        )

        assert "Baa/BAA990101.csv" not in result
        assert "Baa/BAA240101.csv" in result

    def test_filter_no_date_match(self):
        """yymmddパターンに合わないファイルは除外"""
        files = [
            "Baa/readme.csv",
            "Baa/BAA240115.csv",
        ]

        result = FullLoadPipeline._filter_files_by_date(files, None, None)

        assert "Baa/readme.csv" not in result
        assert "Baa/BAA240115.csv" in result


class TestFullLoadStepDownload:
    """_step_downloadのテスト"""

    def test_download_success(self):
        """ダウンロード成功"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=100, downloaded_files=80, skipped_files=20, failed_files=0
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}

        pipeline = FullLoadPipeline(downloader=downloader)
        result = pipeline._step_download(date(2020, 1, 1), None)

        assert result.status == "success"
        assert result.details["downloaded"] == 80
        downloader.download_all_from_date.assert_called_once_with("200101")

    def test_download_default_start_date(self):
        """start_dateがNoneの場合デフォルト"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=0, downloaded_files=0, skipped_files=0, failed_files=0
        )
        downloader.download_all_from_date.return_value = {}

        pipeline = FullLoadPipeline(downloader=downloader)
        pipeline._step_download(None, None)

        downloader.download_all_from_date.assert_called_once_with("200101")

    def test_download_error(self):
        """ダウンロードエラー"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("接続エラー")

        pipeline = FullLoadPipeline(downloader=downloader)
        result = pipeline._step_download(date(2020, 1, 1), None)

        assert result.status == "failed"
        assert "接続エラー" in result.error_message


class TestFullLoadStepUpload:
    """_step_uploadのテスト"""

    def test_upload_success(self):
        """アップロード成功"""
        downloader = MagicMock()
        uploader = MagicMock()

        downloader.get_output_dir.return_value = Path("/tmp/test")
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=50, uploaded_files=50, skipped_files=0,
            failed_files=0, uploaded_bytes=10240,
        )
        uploader.upload_all.return_value = upload_result

        pipeline = FullLoadPipeline(downloader=downloader, uploader=uploader)
        result = pipeline._step_upload()

        assert result.status == "success"
        assert result.details["uploaded"] == 50
        assert uploader.local_base_dir == Path("/original")


class TestFullLoadStepLoadToBq:
    """_step_load_to_bqのテスト"""

    def test_load_all_files(self):
        """全ファイルロード"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240101.csv",
            "Baa/BAA240115.csv",
        ]
        batch_result = BatchLoadResult(
            total_files=2, success_count=2, skipped_count=0,
            failed_count=0, total_records=200, results=[], failed_files=[],
            duration_seconds=10.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = FullLoadPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(None, None)

        assert result.status == "success"
        assert result.details["files"] == 2
        assert result.details["records"] == 200

    def test_load_with_date_filter(self):
        """日付フィルタ付きロード"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA230101.csv",
            "Baa/BAA240115.csv",
        ]
        batch_result = BatchLoadResult(
            total_files=1, success_count=1, skipped_count=0,
            failed_count=0, total_records=100, results=[], failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = FullLoadPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 1), date(2024, 12, 31))

        # 240115のみがフィルタされてロードされる
        call_args = bq_loader.load_files_batch.call_args
        assert len(call_args[0][0]) == 1
        assert "Baa/BAA240115.csv" in call_args[0][0]

    def test_load_no_files(self):
        """ロード対象なし"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = []

        pipeline = FullLoadPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(None, None)

        assert result.status == "success"
        assert result.details["files"] == 0
        bq_loader.load_files_batch.assert_not_called()


class TestFullLoadRun:
    """FullLoadPipeline.runのテスト"""

    def _create_pipeline_mocks(self):
        """テスト用モック一式を作成"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()

        download_result = DownloadResult(
            total_files=100, downloaded_files=80,
            skipped_files=20, failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        downloader.get_output_dir.return_value = Path("/tmp/test")

        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=80, uploaded_files=80, skipped_files=0,
            failed_files=0, uploaded_bytes=10240,
        )
        uploader.upload_all.return_value = upload_result

        bq_loader.list_csv_files.return_value = ["Baa/BAA240115.csv"]
        batch_result = BatchLoadResult(
            total_files=1, success_count=1, skipped_count=0,
            failed_count=0, total_records=100, results=[], failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        return downloader, uploader, bq_loader

    def test_run_success(self):
        """全体成功"""
        downloader, uploader, bq_loader = self._create_pipeline_mocks()
        feature_pipeline = MagicMock()
        feature_pipeline.run.return_value = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "deleted_rows": 100,
            "inserted_rows": 500,
            "elapsed_time": 30.0,
        }

        pipeline = FullLoadPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader,
            feature_pipeline=feature_pipeline,
        )
        result = pipeline.run("2024-01-01", "2024-12-31")

        assert result.status == "success"
        assert result.start_date == "2024-01-01"
        assert result.end_date == "2024-12-31"
        assert result.files_downloaded == 80
        assert result.files_uploaded == 80
        assert result.files_loaded == 1
        assert result.records_loaded == 100
        assert result.features_inserted == 500
        assert len(result.steps) == 4
        assert result.job_id  # 生成されている
        downloader.cleanup.assert_called_once()
        feature_pipeline.run.assert_called_once_with(
            start_date="2024-01-01", end_date="2024-12-31"
        )

    def test_run_without_dates(self):
        """日付指定なし（全期間）"""
        downloader, uploader, bq_loader = self._create_pipeline_mocks()
        feature_pipeline = MagicMock()
        feature_pipeline.run.return_value = {
            "start_date": "2016-01-01",
            "end_date": date.today().strftime("%Y-%m-%d"),
            "deleted_rows": 0,
            "inserted_rows": 1000,
            "elapsed_time": 60.0,
        }

        pipeline = FullLoadPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader,
            feature_pipeline=feature_pipeline,
        )
        result = pipeline.run()

        assert result.status == "success"
        assert result.start_date == "全期間"
        assert result.end_date == "全期間"

    def test_run_download_failure(self):
        """ダウンロード失敗でパイプライン停止"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("エラー")

        pipeline = FullLoadPipeline(downloader=downloader)
        result = pipeline.run("2024-01-01")

        assert result.status == "failed"
        assert "ダウンロード失敗" in result.error_message
        assert len(result.steps) == 1

    def test_run_cleanup_on_error(self):
        """エラー時もクリーンアップ実行"""
        downloader = MagicMock()
        downloader.download_all_from_date.side_effect = Exception("エラー")

        pipeline = FullLoadPipeline(downloader=downloader)
        pipeline.run("2024-01-01")

        downloader.cleanup.assert_called_once()


class TestFullLoadStepGenerateFeatures:
    """FullLoadPipeline._step_generate_featuresのテスト"""

    def test_step_generate_features_success(self):
        """特徴量生成成功"""
        feature_pipeline = MagicMock()
        feature_pipeline.run.return_value = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "deleted_rows": 100,
            "inserted_rows": 500,
            "elapsed_time": 30.0,
        }

        pipeline = FullLoadPipeline(feature_pipeline=feature_pipeline)
        result = pipeline._step_generate_features(
            date(2024, 1, 1), date(2024, 12, 31)
        )

        assert result.status == "success"
        assert result.details["inserted_rows"] == 500
        assert result.details["deleted_rows"] == 100
        feature_pipeline.run.assert_called_once_with(
            start_date="2024-01-01", end_date="2024-12-31"
        )

    def test_step_generate_features_default_dates(self):
        """日付未指定時のデフォルト値"""
        feature_pipeline = MagicMock()
        feature_pipeline.run.return_value = {
            "start_date": "2016-01-01",
            "end_date": date.today().strftime("%Y-%m-%d"),
            "deleted_rows": 0,
            "inserted_rows": 1000,
            "elapsed_time": 60.0,
        }

        pipeline = FullLoadPipeline(feature_pipeline=feature_pipeline)
        result = pipeline._step_generate_features(None, None)

        assert result.status == "success"
        call_args = feature_pipeline.run.call_args
        assert call_args[1]["start_date"] == "2016-01-01"
        assert call_args[1]["end_date"] == date.today().strftime("%Y-%m-%d")

    def test_step_generate_features_error(self):
        """特徴量生成エラー"""
        feature_pipeline = MagicMock()
        feature_pipeline.run.side_effect = Exception("BigQueryエラー")

        pipeline = FullLoadPipeline(feature_pipeline=feature_pipeline)
        result = pipeline._step_generate_features(
            date(2024, 1, 1), date(2024, 12, 31)
        )

        assert result.status == "failed"
        assert "BigQueryエラー" in result.error_message

    def test_run_feature_failure_results_in_partial(self):
        """特徴量生成失敗時はpartialステータス"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()
        feature_pipeline = MagicMock()

        # 3ステップ成功
        download_result = DownloadResult(
            total_files=100, downloaded_files=80,
            skipped_files=20, failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        downloader.get_output_dir.return_value = Path("/tmp/test")
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=80, uploaded_files=80, skipped_files=0,
            failed_files=0, uploaded_bytes=10240,
        )
        uploader.upload_all.return_value = upload_result
        bq_loader.list_csv_files.return_value = ["Baa/BAA240115.csv"]
        batch_result = BatchLoadResult(
            total_files=1, success_count=1, skipped_count=0,
            failed_count=0, total_records=100, results=[], failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        # 特徴量生成失敗
        feature_pipeline.run.side_effect = Exception("SQL実行エラー")

        pipeline = FullLoadPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader,
            feature_pipeline=feature_pipeline,
        )
        result = pipeline.run("2024-01-01", "2024-12-31")

        assert result.status == "partial"
        assert "特徴量生成失敗" in result.error_message
        assert result.files_loaded == 1
        assert result.records_loaded == 100
        assert len(result.steps) == 4


class TestFullLoadResultToDict:
    """FullLoadResult.to_dictのテスト"""

    def test_to_dict(self):
        """辞書変換"""
        result = FullLoadResult(
            status="success",
            job_id="abc12345",
            start_date="2024-01-01",
            end_date="2024-12-31",
            files_downloaded=80,
            files_uploaded=80,
            files_loaded=50,
            records_loaded=5000,
            features_inserted=500,
            duration_seconds=120.5,
            steps=[
                FullLoadStepResult(
                    step_name="download",
                    status="success",
                    duration_seconds=60.0,
                    details={"downloaded": 80},
                )
            ],
        )

        d = result.to_dict()

        assert d["status"] == "success"
        assert d["job_id"] == "abc12345"
        assert d["start_date"] == "2024-01-01"
        assert d["end_date"] == "2024-12-31"
        assert d["files_loaded"] == 50
        assert d["features_inserted"] == 500
        assert d["duration_seconds"] == 120.5
        assert len(d["steps"]) == 1


# FastAPI テスト
class TestFullLoadAPI:
    """FastAPI 全件ロードエンドポイントのテスト"""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from src.automation.api.app import app
        return TestClient(app)

    def test_root_version_updated(self, client):
        """ルートエンドポイントのバージョン確認"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["version"] == "1.1.0"

    def test_load_full_accepted(self, client):
        """全件ロードが受付される"""
        response = client.post(
            "/api/v1/load/full",
            json={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["job_id"]
        assert data["start_date"] == "2024-01-01"
        assert data["end_date"] == "2024-12-31"

    def test_load_full_no_dates(self, client):
        """日付省略で全期間"""
        response = client.post("/api/v1/load/full", json={})

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert data["start_date"] == "全期間"
        assert data["end_date"] == "全期間"

    def test_load_full_invalid_date(self, client):
        """不正な日付形式"""
        response = client.post(
            "/api/v1/load/full",
            json={"start_date": "invalid"},
        )
        assert response.status_code == 422

    @patch("src.automation.api.app.FullLoadPipeline")
    def test_load_full_sync_success(self, MockPipeline, client):
        """同期全件ロード成功"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = FullLoadResult(
            status="success",
            job_id="test123",
            start_date="2024-01-01",
            end_date="2024-12-31",
            files_downloaded=80,
            files_uploaded=80,
            files_loaded=50,
            records_loaded=5000,
            duration_seconds=120.5,
        )
        MockPipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/load/full/sync",
            json={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["files_loaded"] == 50
        assert data["records_loaded"] == 5000

    @patch("src.automation.api.app.FullLoadPipeline")
    def test_load_full_sync_failed(self, MockPipeline, client):
        """同期全件ロード失敗"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = FullLoadResult(
            status="failed",
            job_id="test123",
            start_date="2024-01-01",
            end_date="2024-12-31",
            error_message="ダウンロードエラー",
        )
        MockPipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/load/full/sync",
            json={"start_date": "2024-01-01"},
        )

        assert response.status_code == 500
        data = response.json()
        assert data["status"] == "failed"
