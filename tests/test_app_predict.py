"""
app.py の予測エンドポイント補助関数のテスト (Issue #117)

_get_latest_model_from_gcs と _resolve_model_path のテスト。
"""

import datetime
import os
from unittest.mock import MagicMock, patch

import pytest

from src.automation.api.app import (
    _get_latest_model_from_gcs,
    _resolve_model_path,
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


class TestResolveModelPath:
    """_resolve_model_pathのテスト"""

    def test_local_path_returned_as_is(self):
        """ローカルパスはそのまま返されること"""
        local_path, tmpdir = _resolve_model_path("/tmp/model.txt", "test-project")
        assert local_path == "/tmp/model.txt"
        assert tmpdir is None

    def test_gcs_uri_is_downloaded(self):
        """GCS URIの場合、ローカルにダウンロードされること"""
        with patch("google.cloud.storage.Client") as mock_client_cls:
            mock_blob = mock_client_cls.return_value.bucket.return_value.blob.return_value

            def fake_download(path):
                open(path, "w").write("dummy")

            mock_blob.download_to_filename.side_effect = fake_download

            local_path, tmpdir = _resolve_model_path(
                "gs://my-project-keiba-models/models/20260201/lgbm_ranker.txt",
                "my-project",
            )

        assert local_path.endswith("lgbm_ranker.txt")
        assert tmpdir is not None
        assert os.path.exists(local_path)

        # クリーンアップ
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_none_triggers_auto_fetch(self):
        """model_path=Noneの場合、_get_latest_model_from_gcsが呼ばれること"""
        with patch("src.automation.api.app._get_latest_model_from_gcs") as mock_get_latest:
            mock_get_latest.return_value = "/tmp/auto_fetched_model.txt"

            local_path, tmpdir = _resolve_model_path(None, "test-project")

        mock_get_latest.assert_called_once_with("test-project")
        assert local_path == "/tmp/auto_fetched_model.txt"
        assert tmpdir is None

    def test_invalid_gcs_uri_raises_value_error(self):
        """バケット名だけでパスがないGCS URIはValueErrorが発生すること"""
        with pytest.raises(ValueError, match="不正なGCS URI"):
            _resolve_model_path("gs://bucket-only", "test-project")
