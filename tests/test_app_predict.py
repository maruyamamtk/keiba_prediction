"""
app.py の予測エンドポイント補助関数のテスト (Issue #117)

_get_latest_model_from_gcs と _run_predict のモデルパス委譲のテスト。
"""

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.automation.api.app import (
    _get_latest_model_from_gcs,
    _run_predict,
)


def _sample_result_df() -> pd.DataFrame:
    return pd.DataFrame({
        "race_id": ["race_1"],
        "race_date": [datetime.date(2026, 6, 28)],
        "venue_code": ["05"],
        "course_type": ["turf"],
        "horse_id": ["h1"],
    })


def _make_blob(name: str, updated: datetime.datetime) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.updated = updated
    return blob


class TestGetLatestModelFromGCS:
    """_get_latest_model_from_gcsのテスト"""

    def test_returns_latest_txt_blob(self):
        """複数モデルがある場合、最新の.txtファイルのURIが返ること"""
        blobs = [
            _make_blob("models/20260101/lgbm_ranker.txt", datetime.datetime(2026, 1, 1)),
            _make_blob("models/20260201/lgbm_ranker.txt", datetime.datetime(2026, 2, 1)),
            _make_blob("models/20260201/config.yaml", datetime.datetime(2026, 2, 2)),  # .txt以外は無視
        ]

        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.bucket.return_value.list_blobs.return_value = blobs

            gcs_uri = _get_latest_model_from_gcs(project_id="test-project")

        # 最新の .txt が返ること（2026-02-01 が最新）
        assert gcs_uri == "gs://test-project-keiba-models/models/20260201/lgbm_ranker.txt"

    def test_raises_when_no_model_found(self):
        """モデルファイルが存在しない場合、FileNotFoundErrorが発生すること"""
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            # .txt ファイルが1件もない
            mock_client.bucket.return_value.list_blobs.return_value = [
                _make_blob("models/readme.md", datetime.datetime(2026, 1, 1)),
            ]

            with pytest.raises(FileNotFoundError, match="モデルファイルが見つかりません"):
                _get_latest_model_from_gcs(project_id="test-project")

    def test_ignores_non_txt_files(self):
        """`.txt` 以外のファイルは無視されること"""
        blobs = [
            _make_blob("models/config.yaml", datetime.datetime(2026, 3, 1)),
            _make_blob("models/model.pkl", datetime.datetime(2026, 3, 2)),
            _make_blob("models/lgbm.txt", datetime.datetime(2026, 2, 1)),
        ]

        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.bucket.return_value.list_blobs.return_value = blobs

            gcs_uri = _get_latest_model_from_gcs(project_id="my-project")

        # .txt のみが対象（lgbm.txt が唯一の.txt）
        assert "lgbm.txt" in gcs_uri


class TestRunPredictModelPathDelegation:
    """_run_predict がモデルパスを predict_pipeline へ委譲することのテスト

    校正器 meta.json のダウンロードは predict_pipeline 側（_download_model_from_gcs）
    に一元化されているため、_run_predict は gs:// URI を先にローカル解決せず
    そのまま渡さなければならない（校正バイパス不具合の回帰防止）。
    """

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=pd.DataFrame())
    def test_gcs_uri_passed_through_unchanged(self, mock_pipeline, _mock_config):
        """gs:// URI はローカル解決されず、そのまま predict_pipeline に渡ること"""
        gcs_uri = "gs://my-project-keiba-models/lgbm_ranker_multi/20260627/lgbm_ranker_multi_20260627.txt"

        _run_predict(
            model_path=gcs_uri,
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        assert mock_pipeline.call_args.kwargs["model_path"] == gcs_uri

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=pd.DataFrame())
    @patch("src.automation.api.app._get_latest_model_from_gcs")
    def test_none_resolves_to_latest_gcs_uri(self, mock_latest, mock_pipeline, _mock_config):
        """model_path=None のとき最新モデルの gs:// URI を解決してそのまま渡すこと"""
        latest_uri = "gs://my-project-keiba-models/lgbm_ranker_multi/20260627/lgbm_ranker_multi_20260627.txt"
        mock_latest.return_value = latest_uri

        _run_predict(
            model_path=None,
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        mock_latest.assert_called_once_with("my-project")
        assert mock_pipeline.call_args.kwargs["model_path"] == latest_uri


class TestRunPredictTrackConditionFreshness:
    """_run_predict の馬場状態予報(KAA)鮮度チェック統合のテスト（Issue #437）"""

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=_sample_result_df())
    @patch("src.models.predict.check_track_condition_freshness")
    @patch("src.utils.line_notify.push_messages")
    def test_notifies_line_when_stale(
        self, mock_push, mock_check, mock_pipeline, _mock_config, monkeypatch
    ):
        """欠損率が閾値を超えた場合、LINE通知が送られること"""
        mock_check.return_value = (0.5, ["race_1"])
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "dummy-token")
        monkeypatch.setenv("LINE_USER_ID", "dummy-user")

        _run_predict(
            model_path="gs://bucket/model.txt",
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        mock_push.assert_called_once()

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=_sample_result_df())
    @patch("src.models.predict.check_track_condition_freshness")
    @patch("src.utils.line_notify.push_messages")
    def test_no_notification_when_fresh(
        self, mock_push, mock_check, mock_pipeline, _mock_config, monkeypatch
    ):
        """欠損率が閾値以下の場合、LINE通知は送られないこと"""
        mock_check.return_value = (0.0, [])
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "dummy-token")
        monkeypatch.setenv("LINE_USER_ID", "dummy-user")

        _run_predict(
            model_path="gs://bucket/model.txt",
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        mock_push.assert_not_called()

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=_sample_result_df())
    @patch("src.models.predict.check_track_condition_freshness")
    @patch("src.utils.line_notify.push_messages")
    def test_no_notification_without_env_vars(
        self, mock_push, mock_check, mock_pipeline, _mock_config, monkeypatch
    ):
        """LINE環境変数が未設定の場合は通知をスキップし、予測処理は継続すること"""
        mock_check.return_value = (0.9, ["race_1"])
        monkeypatch.delenv("LINE_CHANNEL_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("LINE_USER_ID", raising=False)

        result = _run_predict(
            model_path="gs://bucket/model.txt",
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        mock_push.assert_not_called()
        assert result["num_races"] == 1

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=_sample_result_df())
    @patch("src.models.predict.check_track_condition_freshness")
    @patch("src.utils.line_notify.push_messages", side_effect=Exception("LINE API error"))
    def test_line_failure_does_not_raise(
        self, mock_push, mock_check, mock_pipeline, _mock_config, monkeypatch
    ):
        """LINE通知が失敗しても例外が伝播せず予測処理が継続すること"""
        mock_check.return_value = (0.9, ["race_1"])
        monkeypatch.setenv("LINE_CHANNEL_ACCESS_TOKEN", "dummy-token")
        monkeypatch.setenv("LINE_USER_ID", "dummy-user")

        result = _run_predict(
            model_path="gs://bucket/model.txt",
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        assert result["num_races"] == 1

    @patch("src.models.train.load_config", return_value={})
    @patch("src.models.predict.predict_pipeline", return_value=_sample_result_df())
    @patch(
        "src.models.predict.check_track_condition_freshness",
        side_effect=Exception("BigQuery一時障害"),
    )
    @patch("src.utils.line_notify.push_messages")
    def test_freshness_check_failure_does_not_abort_prediction(
        self, mock_push, mock_check, mock_pipeline, _mock_config
    ):
        """鮮度チェック自体が例外を送出しても予測結果は正常に返ること（保存処理を巻き込まない）"""
        result = _run_predict(
            model_path="gs://bucket/model.txt",
            target_dates=[datetime.date(2026, 6, 28)],
            save_to_bq=False,
            project_id="my-project",
        )

        assert result["num_races"] == 1
        mock_push.assert_not_called()
