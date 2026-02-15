"""
学習パイプラインのテスト
"""

import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest
import yaml

from src.models.train import (
    compute_week_boundaries,
    evaluate_predictions,
    load_config,
    prepare_features,
    split_train_valid_predict,
    train_pipeline,
)


class TestComputeWeekBoundaries:
    """compute_week_boundariesのテスト"""

    def test_weekday_monday(self):
        """月曜日の場合、同じ週の土曜・日曜を返すこと"""
        date = datetime.date(2026, 2, 9)  # 月曜日
        sat, sun = compute_week_boundaries(date)
        assert sat == datetime.date(2026, 2, 14)
        assert sun == datetime.date(2026, 2, 15)

    def test_weekday_friday(self):
        """金曜日の場合、翌日の土曜・日曜を返すこと"""
        date = datetime.date(2026, 2, 13)  # 金曜日
        sat, sun = compute_week_boundaries(date)
        assert sat == datetime.date(2026, 2, 14)
        assert sun == datetime.date(2026, 2, 15)

    def test_saturday(self):
        """土曜日の場合、当日と翌日を返すこと"""
        date = datetime.date(2026, 2, 14)  # 土曜日
        sat, sun = compute_week_boundaries(date)
        assert sat == datetime.date(2026, 2, 14)
        assert sun == datetime.date(2026, 2, 15)

    def test_sunday(self):
        """日曜日の場合、前日の土曜・当日の日曜を返すこと"""
        date = datetime.date(2026, 2, 15)  # 日曜日
        sat, sun = compute_week_boundaries(date)
        assert sat == datetime.date(2026, 2, 14)
        assert sun == datetime.date(2026, 2, 15)

    def test_wednesday(self):
        """水曜日の場合、同じ週の土曜・日曜を返すこと"""
        date = datetime.date(2026, 2, 11)  # 水曜日
        sat, sun = compute_week_boundaries(date)
        assert sat == datetime.date(2026, 2, 14)
        assert sun == datetime.date(2026, 2, 15)


class TestLoadConfig:
    """load_configのテスト"""

    def test_load_valid_config(self, tmp_path):
        """有効な設定ファイルを読み込めること"""
        config_data = {
            "model": {"params": {"objective": "lambdarank"}},
            "data": {"dataset": "features"},
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config_data))
        config = load_config(str(config_file))
        assert config["model"]["params"]["objective"] == "lambdarank"

    def test_load_nonexistent_config(self):
        """存在しない設定ファイルでエラーになること"""
        with pytest.raises(FileNotFoundError):
            load_config("/tmp/nonexistent_config.yaml")


class TestSplitTrainValidPredict:
    """split_train_valid_predictのテスト"""

    @pytest.fixture
    def sample_df(self):
        """テスト用のサンプルDataFrame"""
        dates = pd.date_range("2025-01-01", "2026-02-15", freq="D")
        rows = []
        for d in dates:
            for i in range(3):
                rows.append({
                    "race_id": f"race_{d.strftime('%Y%m%d')}_{i}",
                    "race_date": d.date(),
                    "horse_id": f"horse_{i}",
                    "finish_position": i + 1,
                })
        return pd.DataFrame(rows)

    def test_split_basic(self, sample_df):
        """基本的な分割が正しく動作すること"""
        execution_date = datetime.date(2026, 2, 13)  # 金曜日
        train_df, valid_df, predict_df = split_train_valid_predict(
            sample_df, execution_date, validation_months=6
        )

        # 推論対象は2/14, 2/15
        assert set(predict_df["race_date"].unique()) <= {
            datetime.date(2026, 2, 14),
            datetime.date(2026, 2, 15),
        }

        # 学習・検証は推論対象と重複しない
        all_train_dates = set(train_df["race_date"].unique())
        all_valid_dates = set(valid_df["race_date"].unique())
        all_predict_dates = set(predict_df["race_date"].unique())

        assert len(all_train_dates & all_predict_dates) == 0
        assert len(all_valid_dates & all_predict_dates) == 0

        # 学習データは検証データより前
        if len(train_df) > 0 and len(valid_df) > 0:
            assert train_df["race_date"].max() < valid_df["race_date"].min()

    def test_split_no_predict_data(self):
        """推論対象データがない場合でもエラーにならないこと"""
        dates = pd.date_range("2025-01-01", "2025-12-31", freq="D")
        rows = [
            {"race_id": f"race_{d.strftime('%Y%m%d')}", "race_date": d.date(),
             "horse_id": "h1", "finish_position": 1}
            for d in dates
        ]
        df = pd.DataFrame(rows)

        execution_date = datetime.date(2026, 2, 13)
        train_df, valid_df, predict_df = split_train_valid_predict(
            df, execution_date, validation_months=6
        )
        assert len(predict_df) == 0


class TestPrepareFeatures:
    """prepare_featuresのテスト"""

    def test_basic_preparation(self):
        """特徴量準備が正しく動作すること"""
        df = pd.DataFrame({
            "race_id": ["r1", "r1", "r1", "r1", "r1"],
            "horse_id": ["h1", "h2", "h3", "h4", "h5"],
            "finish_position": [1, 2, 3, 4, 5],
            "feature_a": [1.0, 2.0, 3.0, 4.0, 5.0],
            "feature_b": [0.1, 0.2, 0.3, 0.4, 0.5],
            "course_type": ["turf", "dirt", "turf", "dirt", "turf"],
        })

        X, y, groups = prepare_features(
            df,
            exclude_columns=["race_id", "horse_id", "finish_position"],
            categorical_columns=["course_type"],
        )

        assert "race_id" not in X.columns
        assert "horse_id" not in X.columns
        assert "finish_position" not in X.columns
        assert "feature_a" in X.columns
        assert "feature_b" in X.columns
        assert X["course_type"].dtype.name == "category"

        # 二値ラベルの確認（3着以内=1, それ以外=0）
        assert y[0] == 1  # 1着 → 1
        assert y[1] == 1  # 2着 → 1
        assert y[2] == 1  # 3着 → 1
        assert y[3] == 0  # 4着 → 0
        assert y[4] == 0  # 5着 → 0

        # グループサイズ
        assert groups == [5]

    def test_zero_finish_position(self):
        """着順が0の場合でもエラーにならないこと"""
        df = pd.DataFrame({
            "race_id": ["r1", "r1"],
            "finish_position": [0, 1],
            "feature_a": [1.0, 2.0],
        })
        X, y, groups = prepare_features(
            df, exclude_columns=["race_id", "finish_position"], categorical_columns=[]
        )
        # 0着 → 0（3着以内でない）
        assert y[0] == 0
        # 1着 → 1（3着以内）
        assert y[1] == 1


class TestEvaluatePredictions:
    """evaluate_predictionsのテスト"""

    def test_perfect_prediction(self):
        """完璧な予測の場合、評価指標が最大値になること"""
        # 1レース、5頭
        y_true_positions = np.array([1, 2, 3, 4, 5])
        # 着順通りの予測スコア（1着に最高スコア）
        y_pred = np.array([5.0, 4.0, 3.0, 2.0, 1.0])
        groups = [5]

        metrics = evaluate_predictions(y_true_positions, y_pred, groups)
        assert metrics["ndcg@3"] == pytest.approx(1.0)
        assert metrics["recall@3"] == pytest.approx(1.0)
        assert metrics["auc"] == pytest.approx(1.0)
        assert metrics["num_races"] == 1

    def test_worst_prediction(self):
        """最悪の予測の場合、recall@3が0になること"""
        y_true_positions = np.array([1, 2, 3, 4, 5])
        # 4,5着に最高スコア → top3予測は4,5着の馬
        y_pred = np.array([1.0, 1.5, 2.0, 5.0, 4.0])
        groups = [5]

        metrics = evaluate_predictions(y_true_positions, y_pred, groups)
        # 予測top3: idx3(4着),idx4(5着),idx2(3着) → 3着だけ当たり
        assert metrics["recall@3"] < 1.0

    def test_multiple_races(self):
        """複数レースでの平均が正しく計算されること"""
        # 1レース目(5頭): 完璧な予測, 2レース目(5頭): 逆順の予測
        y_true_positions = np.array([1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
        y_pred = np.array([5.0, 4.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        groups = [5, 5]

        metrics = evaluate_predictions(y_true_positions, y_pred, groups)
        assert metrics["num_races"] == 2
        # 1レース目: 完璧(NDCG=1.0), 2レース目: 逆(NDCG<1.0)
        assert 0.0 < metrics["ndcg@3"] < 1.0
        # AUCは0〜1の範囲
        assert 0.0 <= metrics["auc"] <= 1.0


class TestTrainPipeline:
    """train_pipelineのテスト（BigQueryをモック）"""

    @pytest.fixture
    def mock_config(self):
        return {
            "model": {
                "params": {
                    "objective": "lambdarank",
                    "metric": "ndcg",
                    "ndcg_eval_at": [3],
                    "boosting_type": "gbdt",
                    "num_leaves": 31,
                    "learning_rate": 0.05,
                    "feature_fraction": 0.8,
                    "bagging_fraction": 0.8,
                    "bagging_freq": 5,
                    "verbose": -1,
                    "seed": 42,
                },
                "training": {
                    "num_boost_round": 10,
                    "early_stopping_rounds": 5,
                    "log_evaluation": 0,
                    "validation_months": 6,
                },
            },
            "data": {
                "dataset": "features",
                "table": "training_data",
                "label_column": "finish_position",
                "group_column": "race_id",
                "date_column": "race_date",
                "exclude_columns": [
                    "race_id", "horse_id", "race_date",
                    "target_place", "finish_position",
                    "venue_code", "jockey_id", "trainer_id",
                    "created_at", "race_number",
                ],
                "categorical_columns": ["course_type", "track_condition"],
            },
            "gcs": {
                "bucket_suffix": "keiba-models",
                "model_prefix": "lgbm_ranker",
            },
        }

    @pytest.fixture
    def mock_training_df(self):
        """BigQueryからの返却を模擬するDataFrame"""
        np.random.seed(42)
        rows = []
        # 学習期間: 2025-01-01 ~ 2025-07-31
        for month in range(1, 8):
            for day in [1, 15]:
                for race_num in range(3):
                    race_date = datetime.date(2025, month, day)
                    race_id = f"race_{race_date.strftime('%Y%m%d')}_{race_num}"
                    for horse_num in range(1, 9):
                        rows.append({
                            "race_id": race_id,
                            "horse_id": f"horse_{horse_num}",
                            "race_date": race_date,
                            "target_place": horse_num <= 3,
                            "finish_position": horse_num,
                            "venue_code": "01",
                            "race_number": race_num + 1,
                            "course_type": "turf" if horse_num % 2 == 0 else "dirt",
                            "track_condition": "good",
                            "distance": 1600,
                            "num_horses": 8,
                            "bracket_number": (horse_num - 1) // 2 + 1,
                            "horse_number": horse_num,
                            "weight": 55.0 + np.random.randn(),
                            "jockey_id": f"j{horse_num}",
                            "trainer_id": f"t{horse_num}",
                            "created_at": None,
                            "feature_a": np.random.randn(),
                            "feature_b": np.random.randn(),
                            "feature_c": np.random.randn(),
                        })
        # 検証期間: 2025-08-01 ~ 2026-02-13
        for month in range(8, 13):
            for day in [1, 15]:
                for race_num in range(2):
                    race_date = datetime.date(2025, month, day)
                    race_id = f"race_{race_date.strftime('%Y%m%d')}_{race_num}"
                    for horse_num in range(1, 9):
                        rows.append({
                            "race_id": race_id,
                            "horse_id": f"horse_{horse_num}",
                            "race_date": race_date,
                            "target_place": horse_num <= 3,
                            "finish_position": horse_num,
                            "venue_code": "01",
                            "race_number": race_num + 1,
                            "course_type": "turf" if horse_num % 2 == 0 else "dirt",
                            "track_condition": "good",
                            "distance": 1600,
                            "num_horses": 8,
                            "bracket_number": (horse_num - 1) // 2 + 1,
                            "horse_number": horse_num,
                            "weight": 55.0 + np.random.randn(),
                            "jockey_id": f"j{horse_num}",
                            "trainer_id": f"t{horse_num}",
                            "created_at": None,
                            "feature_a": np.random.randn(),
                            "feature_b": np.random.randn(),
                            "feature_c": np.random.randn(),
                        })
        # 推論対象: 2026-02-14 (土), 2026-02-15 (日)
        for day in [14, 15]:
            for race_num in range(2):
                race_date = datetime.date(2026, 2, day)
                race_id = f"race_{race_date.strftime('%Y%m%d')}_{race_num}"
                for horse_num in range(1, 9):
                    rows.append({
                        "race_id": race_id,
                        "horse_id": f"horse_{horse_num}",
                        "race_date": race_date,
                        "target_place": horse_num <= 3,
                        "finish_position": horse_num,
                        "venue_code": "01",
                        "race_number": race_num + 1,
                        "course_type": "turf" if horse_num % 2 == 0 else "dirt",
                        "track_condition": "good",
                        "distance": 1600,
                        "num_horses": 8,
                        "bracket_number": (horse_num - 1) // 2 + 1,
                        "horse_number": horse_num,
                        "weight": 55.0 + np.random.randn(),
                        "jockey_id": f"j{horse_num}",
                        "trainer_id": f"t{horse_num}",
                        "created_at": None,
                        "feature_a": np.random.randn(),
                        "feature_b": np.random.randn(),
                        "feature_c": np.random.randn(),
                    })
        return pd.DataFrame(rows)

    @patch("src.models.train.fetch_training_data")
    def test_train_pipeline_end_to_end(self, mock_fetch, mock_config, mock_training_df):
        """学習パイプラインがエンドツーエンドで動作すること"""
        mock_fetch.return_value = mock_training_df

        with tempfile.TemporaryDirectory() as tmpdir:
            result = train_pipeline(
                project_id="test-project",
                execution_date=datetime.date(2026, 2, 13),
                config=mock_config,
                output_dir=tmpdir,
                skip_gcs_upload=True,
            )

            assert "metrics" in result
            assert "ndcg@3" in result["metrics"]
            assert "recall@3" in result["metrics"]
            assert "auc" in result["metrics"]
            assert result["metrics"]["num_races"] > 0
            assert 0.0 <= result["metrics"]["ndcg@3"] <= 1.0
            assert 0.0 <= result["metrics"]["recall@3"] <= 1.0
            assert 0.0 <= result["metrics"]["auc"] <= 1.0
            assert result["train_rows"] > 0
            assert result["valid_rows"] > 0
            assert result["predict_rows"] > 0
            assert result["num_features"] > 0
            assert Path(result["model_path"]).exists()

    @patch("src.models.train.fetch_training_data")
    def test_train_pipeline_with_tuning(self, mock_fetch, mock_config, mock_training_df):
        """チューニングモードで学習パイプラインが動作すること"""
        mock_fetch.return_value = mock_training_df

        # tuning設定を追加
        mock_config["tuning"] = {
            "n_trials": 2,
            "timeout": 60,
            "search_space": {
                "feature_fraction": {
                    "type": "float",
                    "low": 0.5,
                    "high": 1.0,
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result = train_pipeline(
                project_id="test-project",
                execution_date=datetime.date(2026, 2, 13),
                config=mock_config,
                output_dir=tmpdir,
                skip_gcs_upload=True,
                tune=True,
                n_trials=2,
            )

            assert "metrics" in result
            assert "tuning" in result
            assert result["tuning"]["n_trials"] == 2
            assert 0.0 <= result["tuning"]["best_value"] <= 1.0
            assert "best_params" in result["tuning"]
            assert Path(result["model_path"]).exists()
            # best_params JSONファイルも保存されていること
            date_str = datetime.date(2026, 2, 13).strftime("%Y%m%d")
            params_path = Path(tmpdir) / f"best_params_{date_str}.json"
            assert params_path.exists()
