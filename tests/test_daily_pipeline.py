#!/usr/bin/env python3
"""
src.automation.pipeline.daily_pipeline と src.automation.api.app のテスト

日次パイプラインとHTTP APIのテスト。
"""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.automation.data.jrdb_downloader import DownloadResult
from src.automation.data.load_to_bq import BatchLoadResult, LoadResult
from src.automation.data.upload_to_gcs import UploadResult
from src.automation.pipeline.daily_pipeline import (
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
        # 7日前 (2024-01-15 - 7日 = 2024-01-08) が start_date, 翌日 (240116) が end_date として渡されること
        downloader.download_all_from_date.assert_called_once_with("240108", "240116")

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

    def test_step_download_uses_lookback(self):
        """ルックバック7日が適用されること: target_date - 7日 が download_all_from_date に渡される"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=0,
            downloaded_files=0,
            skipped_files=0,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}

        pipeline = DailyPipeline(downloader=downloader)
        pipeline._step_download(date(2024, 1, 15))  # 240115

        # start_date=240108 (7日前), end_date=240116 (翌日) が渡されること
        downloader.download_all_from_date.assert_called_once_with("240108", "240116")

    def test_step_download_lookback_crosses_month_boundary(self):
        """月をまたぐルックバックが正しく計算される: 2024-03-05 - 7日 = 2024-02-27"""
        downloader = MagicMock()
        download_result = DownloadResult(
            total_files=0,
            downloaded_files=0,
            skipped_files=0,
            failed_files=0,
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}

        pipeline = DailyPipeline(downloader=downloader)
        pipeline._step_download(date(2024, 3, 5))  # 240305

        # start_date=240227 (7日前), end_date=240306 (翌日) が渡されること
        downloader.download_all_from_date.assert_called_once_with("240227", "240306")


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
        """BigQueryロード成功: サポート対象データタイプのファイルがすべてロードされる"""
        bq_loader = MagicMock()
        # list_csv_filesはdata_typesフィルタにより対象ファイルのみ返す
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240115.csv",
            "Kyf/KYF240115.csv",
            "Baa/BAA240114.csv",
        ]
        batch_result = BatchLoadResult(
            total_files=3,
            success_count=2,
            skipped_count=1,
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
        # data_typesフィルタと within_days=7 が渡されていることを確認
        call_args = bq_loader.list_csv_files.call_args
        assert "data_types" in call_args[1]
        assert "BAA" in call_args[1]["data_types"]
        assert call_args[1].get("within_days") == 7
        # skip_loadedが有効で全ファイルがバッチに渡されることを確認
        batch_call_args = bq_loader.load_files_batch.call_args
        assert len(batch_call_args[0][0]) == 3
        assert batch_call_args[1]["skip_loaded"] is True

    def test_step_load_to_bq_no_files(self):
        """ロード対象ファイルなし（サポート対象データタイプのファイルがGCSにない）"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = []

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["files"] == 0
        bq_loader.load_files_batch.assert_not_called()

    def test_step_load_to_bq_loads_all_dates(self):
        """パイプライン実行日と異なる日付のファイルもロードされる"""
        bq_loader = MagicMock()
        # 実行日は1/15だが、1/13（土）と1/14（日）のファイルもGCSに存在
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240113.csv",
            "Baa/BAA240114.csv",
            "Kyf/KYF240113.csv",
        ]
        batch_result = BatchLoadResult(
            total_files=3,
            success_count=3,
            skipped_count=0,
            failed_count=0,
            total_records=150,
            results=[],
            failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = DailyPipeline(bq_loader=bq_loader)
        # 1/15（月）に実行しても、1/13, 1/14のファイルがロードされる
        result = pipeline._step_load_to_bq(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["files"] == 3
        assert result.details["records"] == 150
        # 全3ファイルがバッチに渡される
        batch_call_args = bq_loader.load_files_batch.call_args
        assert len(batch_call_args[0][0]) == 3

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

    def test_step_load_to_bq_sunday_forces_reload_today_files(self):
        """日曜日は当日yymmddのファイルのみ skip_loaded=False で強制再ロードされる（解決策B）"""
        bq_loader = MagicMock()
        # 2024-01-14（日曜）に実行: 当日ファイル240114 と前日ファイル240113 が存在
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240114.csv",  # 当日
            "Baa/BAA240113.csv",  # 前日
            "Kyf/KYF240113.csv",  # 前日
        ]
        # 当日バッチと前日バッチの結果を個別に返す
        today_batch_result = BatchLoadResult(
            total_files=1,
            success_count=1,
            skipped_count=0,
            failed_count=0,
            total_records=36,
            results=[],
            failed_files=[],
            duration_seconds=3.0,
        )
        other_batch_result = BatchLoadResult(
            total_files=2,
            success_count=0,
            skipped_count=2,
            failed_count=0,
            total_records=0,
            results=[],
            failed_files=[],
            duration_seconds=1.0,
        )
        bq_loader.load_files_batch.side_effect = [today_batch_result, other_batch_result]

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 14))  # 日曜日

        assert result.status == "success"
        # 当日 1 件成功 + 前日 2 件スキップ
        assert result.details["files"] == 1
        assert result.details["records"] == 36
        assert result.details["skipped"] == 2

        # load_files_batch が 2 回呼ばれていること
        assert bq_loader.load_files_batch.call_count == 2

        # 1回目（当日ファイル）は skip_loaded=False で呼ばれること
        first_call = bq_loader.load_files_batch.call_args_list[0]
        assert first_call[0][0] == ["Baa/BAA240114.csv"]
        assert first_call[1]["skip_loaded"] is False

        # 2回目（前日ファイル）は skip_loaded=True で呼ばれること
        second_call = bq_loader.load_files_batch.call_args_list[1]
        assert set(second_call[0][0]) == {"Baa/BAA240113.csv", "Kyf/KYF240113.csv"}
        assert second_call[1]["skip_loaded"] is True

    def test_step_load_to_bq_sunday_no_today_files(self):
        """日曜日に当日ファイルがない場合は前日ファイルのみ通常ロード"""
        bq_loader = MagicMock()
        # 当日ファイルなし、前日ファイルのみ
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240113.csv",
        ]
        other_batch_result = BatchLoadResult(
            total_files=1,
            success_count=0,
            skipped_count=1,
            failed_count=0,
            total_records=0,
            results=[],
            failed_files=[],
            duration_seconds=1.0,
        )
        bq_loader.load_files_batch.return_value = other_batch_result

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 14))  # 日曜日

        assert result.status == "success"
        # 当日ファイルなし → 当日バッチ呼ばれない、前日バッチのみ
        # load_files_batch は other_files 分のみ呼ばれる
        assert bq_loader.load_files_batch.call_count == 1
        call_args = bq_loader.load_files_batch.call_args
        assert call_args[1]["skip_loaded"] is True

    def test_step_load_to_bq_weekday_uses_skip_loaded(self):
        """平日（月〜土）は通常のGCSタイムスタンプ比較ロジックが使われる"""
        bq_loader = MagicMock()
        bq_loader.list_csv_files.return_value = [
            "Baa/BAA240115.csv",
            "Baa/BAA240114.csv",
        ]
        batch_result = BatchLoadResult(
            total_files=2,
            success_count=1,
            skipped_count=1,
            failed_count=0,
            total_records=18,
            results=[],
            failed_files=[],
            duration_seconds=2.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        pipeline = DailyPipeline(bq_loader=bq_loader)
        result = pipeline._step_load_to_bq(date(2024, 1, 15))  # 月曜日

        assert result.status == "success"
        # load_files_batch は 1 回のみ呼ばれ skip_loaded=True
        assert bq_loader.load_files_batch.call_count == 1
        call_args = bq_loader.load_files_batch.call_args
        assert call_args[0][0] == ["Baa/BAA240115.csv", "Baa/BAA240114.csv"]
        assert call_args[1]["skip_loaded"] is True


class TestDailyPipelineRun:
    """DailyPipeline.runのテスト"""

    def test_run_success(self):
        """パイプライン全体が成功"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()
        feature_pipeline = MagicMock()

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

        # 特徴量生成
        feature_pipeline.run.return_value = {
            "start_date": "2024-01-15",
            "end_date": "2024-01-15",
            "deleted_rows": 10,
            "inserted_rows": 50,
            "elapsed_time": 3.0,
        }

        pipeline = DailyPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader,
            feature_pipeline=feature_pipeline,
        )
        result = pipeline.run("2024-01-15")

        assert result.status == "success"
        assert result.target_date == "2024-01-15"
        assert result.files_downloaded == 5
        assert result.files_uploaded == 5
        assert result.files_loaded == 1
        assert result.records_loaded == 100
        assert result.features_inserted == 50
        assert len(result.steps) == 4
        downloader.cleanup.assert_called_once()
        feature_pipeline.run.assert_called_once_with(
            start_date="2024-01-15", end_date="2024-01-16"
        )

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


class TestDailyPipelineStepGenerateFeatures:
    """DailyPipeline._step_generate_featuresのテスト"""

    def test_step_generate_features_success(self):
        """特徴量生成成功"""
        feature_pipeline = MagicMock()
        feature_pipeline.run.return_value = {
            "start_date": "2024-01-15",
            "end_date": "2024-01-15",
            "deleted_rows": 10,
            "inserted_rows": 50,
            "elapsed_time": 3.0,
        }

        pipeline = DailyPipeline(feature_pipeline=feature_pipeline)
        result = pipeline._step_generate_features(date(2024, 1, 15))

        assert result.status == "success"
        assert result.details["inserted_rows"] == 50
        assert result.details["deleted_rows"] == 10
        feature_pipeline.run.assert_called_once_with(
            start_date="2024-01-15", end_date="2024-01-16"
        )

    def test_step_generate_features_error(self):
        """特徴量生成エラー"""
        feature_pipeline = MagicMock()
        feature_pipeline.run.side_effect = Exception("BigQueryエラー")

        pipeline = DailyPipeline(feature_pipeline=feature_pipeline)
        result = pipeline._step_generate_features(date(2024, 1, 15))

        assert result.status == "failed"
        assert "BigQueryエラー" in result.error_message

    def test_run_feature_failure_results_in_partial(self):
        """特徴量生成失敗時はpartialステータス"""
        downloader = MagicMock()
        uploader = MagicMock()
        bq_loader = MagicMock()
        feature_pipeline = MagicMock()

        # ダウンロード成功
        download_result = DownloadResult(
            total_files=10, downloaded_files=5, skipped_files=3, failed_files=0
        )
        downloader.download_all_from_date.return_value = {"BAA": download_result}
        downloader.get_output_dir.return_value = Path("/tmp/test")

        # アップロード成功
        uploader.local_base_dir = Path("/original")
        upload_result = UploadResult(
            total_files=5, uploaded_files=5, skipped_files=0,
            failed_files=0, uploaded_bytes=1024,
        )
        uploader.upload_all.return_value = upload_result

        # BigQueryロード成功
        bq_loader.list_csv_files.return_value = ["Baa/BAA240115.csv"]
        batch_result = BatchLoadResult(
            total_files=1, success_count=1, skipped_count=0,
            failed_count=0, total_records=100, results=[], failed_files=[],
            duration_seconds=5.0,
        )
        bq_loader.load_files_batch.return_value = batch_result

        # 特徴量生成失敗
        feature_pipeline.run.side_effect = Exception("SQL実行エラー")

        pipeline = DailyPipeline(
            downloader=downloader, uploader=uploader, bq_loader=bq_loader,
            feature_pipeline=feature_pipeline,
        )
        result = pipeline.run("2024-01-15")

        assert result.status == "partial"
        assert "特徴量生成失敗" in result.error_message
        assert result.files_loaded == 1
        assert result.records_loaded == 100
        assert len(result.steps) == 4


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
            features_inserted=50,
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
        assert d["features_inserted"] == 50
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

        from src.automation.api.app import app

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

    @patch("src.automation.api.app.get_pipeline")
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

    @patch("src.automation.api.app.get_pipeline")
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

    @patch("src.automation.api.app.get_pipeline")
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


class TestFeatureGenerateAPI:
    """特徴量生成APIエンドポイントのテスト"""

    @pytest.fixture
    def client(self):
        """テストクライアント"""
        from fastapi.testclient import TestClient

        from src.automation.api.app import app

        return TestClient(app)

    def test_generate_features_validation_error(self, client):
        """日付形式が不正な場合"""
        response = client.post(
            "/api/v1/features/generate",
            json={"start_date": "invalid", "end_date": "2024-12-31"},
        )
        assert response.status_code == 422

    def test_generate_features_missing_fields(self, client):
        """必須フィールド欠落"""
        response = client.post(
            "/api/v1/features/generate",
            json={},
        )
        assert response.status_code == 422

    @patch("src.ml.features.feature_pipeline.FeaturePipeline")
    @patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"})
    def test_generate_features_success(self, MockPipeline, client):
        """特徴量生成成功"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "deleted_rows": 100,
            "inserted_rows": 500,
            "elapsed_time": 30.5,
        }
        MockPipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/features/generate",
            json={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["inserted_rows"] == 500
        assert data["deleted_rows"] == 100
        assert data["elapsed_time"] == 30.5

    @patch("src.ml.features.feature_pipeline.FeaturePipeline")
    @patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"})
    def test_generate_features_error(self, MockPipeline, client):
        """特徴量生成失敗"""
        mock_pipeline = MagicMock()
        mock_pipeline.run.side_effect = Exception("BigQuery接続エラー")
        MockPipeline.return_value = mock_pipeline

        response = client.post(
            "/api/v1/features/generate",
            json={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        )

        assert response.status_code == 500

    def test_generate_features_async_accepted(self, client):
        """非同期特徴量生成が受付される"""
        response = client.post(
            "/api/v1/features/generate/async",
            json={"start_date": "2024-01-01", "end_date": "2024-12-31"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert data["start_date"] == "2024-01-01"
        assert data["end_date"] == "2024-12-31"
