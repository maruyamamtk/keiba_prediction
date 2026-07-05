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
