"""
モデル再学習APIエンドポイントのテスト (Issue #221)

POST /api/v1/model/retrain      - 同期再学習
POST /api/v1/model/retrain/async - 非同期再学習（即時レスポンス）
"""

import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.automation.api.app import app

client = TestClient(app)

# train_pipeline() が返すモック結果
MOCK_TRAIN_RESULT = {
    "execution_date": "2026-04-07",
    "model_path": "/tmp/lgbm_ranker_20260407.txt",
    "gcs_uri": "gs://test-project-keiba-models/lgbm_ranker_multi/20260407/lgbm_ranker_multi_20260407.txt",
    "metrics": {
        "ndcg@3": 0.72,
        "recall@3": 0.81,
        "auc": 0.73,
        "num_races": 1200,
    },
    "tuning": {
        "best_value": 0.73,
        "n_trials": 50,
        "best_trial_number": 32,
        "best_params": {"num_leaves": 63, "learning_rate": 0.05},
    },
    "best_iteration": 300,
    "train_rows": 400000,
    "valid_rows": 50000,
    "predict_rows": 0,
    "num_features": 257,
    "top_features": [],
}


class TestRetrainEndpoint:
    """POST /api/v1/model/retrain のテスト"""

    def test_success_with_default_params(self):
        """デフォルトパラメータで再学習が成功する"""
        with (
            patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
            patch(
                "src.automation.api.app._run_retrain",
                return_value=MOCK_TRAIN_RESULT,
            ) as mock_retrain,
        ):
            response = client.post("/api/v1/model/retrain", json={})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["gcs_uri"] == MOCK_TRAIN_RESULT["gcs_uri"]
        assert body["metrics"]["ndcg@3"] == pytest.approx(0.72)
        assert body["tuning"]["n_trials"] == 50

        # _run_retrain が正しい引数で呼ばれていること
        call_kwargs = mock_retrain.call_args.kwargs
        assert call_kwargs["project_id"] == "test-project"
        assert call_kwargs["n_trials"] is None
        assert call_kwargs["tune_timeout"] is None

    def test_success_with_explicit_params(self):
        """execution_date / n_trials / tune_timeout を明示指定して再学習が成功する"""
        payload = {
            "execution_date": "2026-04-07",
            "n_trials": 30,
            "tune_timeout": 1800,
        }
        with (
            patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
            patch(
                "src.automation.api.app._run_retrain",
                return_value=MOCK_TRAIN_RESULT,
            ) as mock_retrain,
        ):
            response = client.post("/api/v1/model/retrain", json=payload)

        assert response.status_code == 200
        call_kwargs = mock_retrain.call_args.kwargs
        assert call_kwargs["execution_date"] == datetime.date(2026, 4, 7)
        assert call_kwargs["n_trials"] == 30
        assert call_kwargs["tune_timeout"] == 1800

    def test_returns_500_when_project_id_missing(self):
        """GCP_PROJECT_ID が未設定のとき 500 を返す"""
        with patch.dict("os.environ", {"GCP_PROJECT_ID": ""}):
            response = client.post("/api/v1/model/retrain", json={})

        assert response.status_code == 500
        assert "GCP_PROJECT_ID" in response.json()["detail"]

    def test_returns_422_for_invalid_date(self):
        """不正な日付形式を指定したとき 422 を返す"""
        with patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}):
            response = client.post(
                "/api/v1/model/retrain",
                json={"execution_date": "not-a-date"},
            )

        assert response.status_code == 422

    def test_returns_500_on_train_pipeline_error(self):
        """train_pipeline が例外を送出したとき 500 を返す"""
        with (
            patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
            patch(
                "src.automation.api.app._run_retrain",
                side_effect=RuntimeError("BQ接続エラー"),
            ),
        ):
            response = client.post("/api/v1/model/retrain", json={})

        assert response.status_code == 500
        assert "BQ接続エラー" in response.json()["detail"]


class TestRetrainAsyncEndpoint:
    """POST /api/v1/model/retrain/async のテスト"""

    def test_returns_accepted_immediately(self):
        """非同期エンドポイントは即時 accepted を返す"""
        with (
            patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
            patch("src.automation.api.app._run_retrain", return_value=MOCK_TRAIN_RESULT),
        ):
            response = client.post("/api/v1/model/retrain/async", json={})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "accepted"
        assert "message" in body
        assert "execution_date" in body

    def test_returns_500_when_project_id_missing(self):
        """GCP_PROJECT_ID が未設定のとき 500 を返す"""
        with patch.dict("os.environ", {"GCP_PROJECT_ID": ""}):
            response = client.post("/api/v1/model/retrain/async", json={})

        assert response.status_code == 500

    def test_accepts_explicit_execution_date(self):
        """execution_date を明示指定したとき受付レスポンスにその日付が含まれる"""
        with (
            patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
            patch("src.automation.api.app._run_retrain", return_value=MOCK_TRAIN_RESULT),
        ):
            response = client.post(
                "/api/v1/model/retrain/async",
                json={"execution_date": "2026-04-07"},
            )

        assert response.status_code == 200
        assert response.json()["execution_date"] == "2026-04-07"


class TestRunRetrain:
    """_run_retrain() ヘルパー関数のテスト"""

    def test_calls_train_pipeline_with_tune_true(self):
        """train_pipeline が tune=True で呼び出されること"""
        mock_config = {"data": {}, "model": {}, "gcs": {}}
        with (
            patch("src.models.train.load_config", return_value=mock_config),
            patch(
                "src.models.train.train_pipeline",
                return_value=MOCK_TRAIN_RESULT,
            ) as mock_pipeline,
        ):
            from src.automation.api.app import _run_retrain

            result = _run_retrain(
                project_id="test-project",
                execution_date=datetime.date(2026, 4, 7),
                n_trials=10,
                tune_timeout=600,
            )

        mock_pipeline.assert_called_once()
        call_kwargs = mock_pipeline.call_args.kwargs
        assert call_kwargs["tune"] is True
        assert call_kwargs["n_trials"] == 10
        assert call_kwargs["tune_timeout"] == 600
        assert result == MOCK_TRAIN_RESULT
