"""
generate_evaluation_report.py のテスト
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# サンプルデータ生成ユーティリティ
# ---------------------------------------------------------------------------

def _make_sample_df(n_races: int = 20, horses_per_race: int = 8) -> pd.DataFrame:
    """テスト用サンプルDataFrameを生成する"""
    np.random.seed(42)
    rows = []
    for race_idx in range(n_races):
        race_id = f"race_{race_idx:04d}"
        finish_positions = np.random.permutation(horses_per_race) + 1
        for horse_idx in range(horses_per_race):
            rows.append({
                "race_id": race_id,
                "horse_id": f"horse_{horse_idx:03d}",
                "race_date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=race_idx),
                "finish_position": int(finish_positions[horse_idx]),
                "horse_number": horse_idx + 1,
                "feature_a": np.random.randn(),
                "feature_b": np.random.randn(),
                "feature_c": np.random.randint(0, 5),
                "course_type": np.random.choice(["芝", "ダート"]),
                "track_condition": np.random.choice(["良", "稍重", "重"]),
                "venue_code": "01",
                "jockey_id": f"jockey_{np.random.randint(0, 10):02d}",
                "trainer_id": f"trainer_{np.random.randint(0, 5):02d}",
                "target_place": 3,
                "created_at": pd.Timestamp("2024-01-01"),
                "race_number": race_idx % 12 + 1,
            })
    return pd.DataFrame(rows)


EXCLUDE_COLS = [
    "race_id", "horse_id", "race_date", "target_place",
    "finish_position", "venue_code", "jockey_id", "trainer_id",
    "created_at", "race_number",
]
CAT_COLS = ["course_type", "track_condition"]


# ---------------------------------------------------------------------------
# compute_metrics のテスト
# ---------------------------------------------------------------------------

class TestComputeMetrics:
    """compute_metrics 関数のテスト"""

    def _make_mock_booster(self, n_features: int = 3):
        """モックBoosterを生成する"""
        mock = MagicMock()
        # predictは特徴量の第1列をスコアとして返す（決定論的）
        mock.predict.side_effect = lambda X: X.iloc[:, 0].values.astype(float)
        return mock

    def test_returns_required_keys(self):
        """必要なキーが返ること"""
        from scripts.generate_evaluation_report import compute_metrics

        df = _make_sample_df(n_races=5, horses_per_race=6)
        booster = self._make_mock_booster()
        result = compute_metrics(booster, df, EXCLUDE_COLS, CAT_COLS)

        assert "ndcg@3" in result
        assert "recall@3" in result
        assert "auc" in result
        assert "num_races" in result
        assert "num_rows" in result

    def test_metric_ranges(self):
        """各指標が正しい範囲に収まること"""
        from scripts.generate_evaluation_report import compute_metrics

        df = _make_sample_df(n_races=10, horses_per_race=8)
        booster = self._make_mock_booster()
        result = compute_metrics(booster, df, EXCLUDE_COLS, CAT_COLS)

        assert 0.0 <= result["ndcg@3"] <= 1.0
        assert 0.0 <= result["recall@3"] <= 1.0
        assert 0.0 <= result["auc"] <= 1.0
        assert result["num_races"] == 10
        assert result["num_rows"] == 80

    def test_num_races_correct(self):
        """レース数が正しく計算されること"""
        from scripts.generate_evaluation_report import compute_metrics

        df = _make_sample_df(n_races=15, horses_per_race=6)
        booster = self._make_mock_booster()
        result = compute_metrics(booster, df, EXCLUDE_COLS, CAT_COLS)

        assert result["num_races"] == 15

    def test_categorical_columns_converted(self):
        """カテゴリカルカラムが適切に変換されること"""
        from scripts.generate_evaluation_report import compute_metrics

        df = _make_sample_df(n_races=5, horses_per_race=6)
        # object型の確認
        assert df["course_type"].dtype == object

        booster = MagicMock()
        # predict呼び出し時のXを検証
        captured_X = []

        def capture_predict(X):
            captured_X.append(X.copy())
            return np.zeros(len(X))

        booster.predict.side_effect = capture_predict
        compute_metrics(booster, df, EXCLUDE_COLS, CAT_COLS)

        assert len(captured_X) == 1
        X = captured_X[0]
        # カテゴリカル列はcategory型になっているはず
        for col in CAT_COLS:
            if col in X.columns:
                assert X[col].dtype.name == "category", f"{col} should be category type"


# ---------------------------------------------------------------------------
# plot_feature_importance のテスト
# ---------------------------------------------------------------------------

class TestPlotFeatureImportance:
    """plot_feature_importance 関数のテスト"""

    def _make_mock_booster(self, n_features: int = 10):
        mock = MagicMock()
        mock.feature_importance.return_value = np.random.rand(n_features) * 1000
        mock.feature_name.return_value = [f"feature_{i}" for i in range(n_features)]
        return mock

    def test_creates_file(self, tmp_path):
        """グラフファイルが生成されること"""
        from scripts.generate_evaluation_report import plot_feature_importance

        booster = self._make_mock_booster(n_features=25)
        output_path = str(tmp_path / "importance.png")
        result_path = plot_feature_importance(booster, top_n=20, output_path=output_path)

        assert result_path == output_path
        assert Path(output_path).exists()

    def test_top_n_respected(self, tmp_path):
        """top_n件のみ表示されること（エラーなく実行できること）"""
        from scripts.generate_evaluation_report import plot_feature_importance

        booster = self._make_mock_booster(n_features=30)
        output_path = str(tmp_path / "importance_top5.png")
        result_path = plot_feature_importance(booster, top_n=5, output_path=output_path)

        assert Path(result_path).exists()

    def test_fewer_features_than_top_n(self, tmp_path):
        """特徴量数がtop_nより少い場合もエラーなく動作すること"""
        from scripts.generate_evaluation_report import plot_feature_importance

        booster = self._make_mock_booster(n_features=5)
        output_path = str(tmp_path / "importance_small.png")
        result_path = plot_feature_importance(booster, top_n=20, output_path=output_path)

        assert Path(result_path).exists()


# ---------------------------------------------------------------------------
# load_model_local のテスト
# ---------------------------------------------------------------------------

class TestLoadModelLocal:
    """load_model_local 関数のテスト"""

    def test_file_not_found(self, tmp_path):
        """存在しないファイルでFileNotFoundErrorが発生すること"""
        from scripts.generate_evaluation_report import load_model_local

        with pytest.raises(FileNotFoundError):
            load_model_local(str(tmp_path / "nonexistent.txt"))

    def test_loads_meta_if_exists(self, tmp_path):
        """メタデータJSONが存在する場合に読み込まれること"""
        from scripts.generate_evaluation_report import load_model_local

        # ダミーモデルファイルを作成（実際のLGBMモデルではないため、lgb.Boosterは失敗する）
        # ここではlgb.Boosterのモックを使ってテストする
        model_file = tmp_path / "model.txt"
        model_file.write_text("dummy")
        meta_file = tmp_path / "model.meta.json"
        meta_data = {"feature_names": ["a", "b"], "best_iteration": 100}
        meta_file.write_text(json.dumps(meta_data))

        with patch("scripts.generate_evaluation_report.lgb.Booster") as mock_booster_cls:
            mock_booster = MagicMock()
            mock_booster_cls.return_value = mock_booster

            booster, meta = load_model_local(str(model_file))

            assert meta["feature_names"] == ["a", "b"]
            assert meta["best_iteration"] == 100


# ---------------------------------------------------------------------------
# build_report のテスト
# ---------------------------------------------------------------------------

class TestBuildReport:
    """build_report 関数のテスト"""

    def _make_mock_booster(self):
        mock = MagicMock()
        mock.feature_name.return_value = [f"feature_{i}" for i in range(5)]
        mock.feature_importance.return_value = np.array([100.0, 80.0, 60.0, 40.0, 20.0])
        return mock

    def test_returns_string(self):
        """Markdownレポートが文字列として返ること"""
        from scripts.generate_evaluation_report import build_report

        booster = self._make_mock_booster()
        metrics = {"ndcg@3": 0.65, "recall@3": 0.75, "auc": 0.72, "num_races": 100, "num_rows": 1000}

        report = build_report(
            booster=booster,
            meta={"params": {"objective": "lambdarank"}, "best_iteration": 500},
            train_metrics=metrics,
            valid_metrics=metrics,
            test_metrics=None,
            feature_importance_path="",
            monthly_metrics_path="",
            train_period=("2020-01-01", "2023-12-31"),
            valid_period=("2024-01-01", "2024-06-30"),
            test_period=None,
            model_path="src/models/model.txt",
            generation_date="2026-02-28 00:00:00",
        )

        assert isinstance(report, str)
        assert "# モデル評価レポート" in report

    def test_contains_required_sections(self):
        """必要なセクションが含まれること"""
        from scripts.generate_evaluation_report import build_report

        booster = self._make_mock_booster()
        metrics = {"ndcg@3": 0.65, "recall@3": 0.75, "auc": 0.72, "num_races": 100, "num_rows": 1000}

        report = build_report(
            booster=booster,
            meta={},
            train_metrics=metrics,
            valid_metrics=metrics,
            test_metrics=None,
            feature_importance_path="",
            monthly_metrics_path="",
            train_period=("2020-01-01", "2023-12-31"),
            valid_period=("2024-01-01", "2024-06-30"),
            test_period=None,
            model_path="model.txt",
            generation_date="2026-02-28",
        )

        assert "モデル概要" in report
        assert "時系列分割設定" in report
        assert "評価指標" in report
        assert "特徴量重要度" in report
        assert "目標指標との比較" in report

    def test_goal_achievement_indicators(self):
        """目標達成・未達成の表示が正しいこと"""
        from scripts.generate_evaluation_report import build_report

        booster = self._make_mock_booster()
        # 目標達成
        metrics_pass = {"ndcg@3": 0.75, "recall@3": 0.85, "auc": 0.80, "num_races": 100, "num_rows": 1000}
        report_pass = build_report(
            booster=booster, meta={},
            train_metrics=metrics_pass, valid_metrics=metrics_pass, test_metrics=None,
            feature_importance_path="", monthly_metrics_path="",
            train_period=("2020-01-01", "2023-12-31"), valid_period=("2024-01-01", "2024-06-30"),
            test_period=None, model_path="", generation_date="2026-02-28",
        )
        assert "✓" in report_pass

        # 目標未達成
        metrics_fail = {"ndcg@3": 0.50, "recall@3": 0.60, "auc": 0.55, "num_races": 100, "num_rows": 1000}
        report_fail = build_report(
            booster=booster, meta={},
            train_metrics=metrics_fail, valid_metrics=metrics_fail, test_metrics=None,
            feature_importance_path="", monthly_metrics_path="",
            train_period=("2020-01-01", "2023-12-31"), valid_period=("2024-01-01", "2024-06-30"),
            test_period=None, model_path="", generation_date="2026-02-28",
        )
        assert "✗" in report_fail

    def test_with_test_metrics(self):
        """テスト期間の指標も含まれること"""
        from scripts.generate_evaluation_report import build_report

        booster = self._make_mock_booster()
        metrics = {"ndcg@3": 0.65, "recall@3": 0.75, "auc": 0.72, "num_races": 100, "num_rows": 1000}

        report = build_report(
            booster=booster, meta={},
            train_metrics=metrics, valid_metrics=metrics, test_metrics=metrics,
            feature_importance_path="", monthly_metrics_path="",
            train_period=("2020-01-01", "2022-12-31"),
            valid_period=("2023-01-01", "2023-06-30"),
            test_period=("2023-07-01", "2023-12-31"),
            model_path="", generation_date="2026-02-28",
        )

        assert "テスト (Test)" in report
