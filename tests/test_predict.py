"""
推論パイプラインのテスト
"""

import datetime
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from src.models.lgbm_ranker_multi import JRA_PRIZE_WEIGHTS, LGBMRankerMulti, LGBMRankerMultiConfig
from src.models.predict import (
    fetch_prediction_data,
    format_predictions,
    fetch_race_results,
    normalize_win_place_prob,
    predict_pipeline,
    save_predictions_to_bq,
    save_predictions_to_gcs,
)


class TestPredictPipeline:
    """predict_pipelineのテスト"""

    @pytest.fixture
    def trained_model_path(self, tmp_path):
        """学習済みLGBMRankerMultiモデルを一時ファイルに作成"""
        np.random.seed(42)
        n_races = 10
        n_horses = 8
        n_total = n_races * n_horses

        X_train = pd.DataFrame({
            "distance": np.random.choice([1200, 1600, 2000], n_total),
            "num_horses": [n_horses] * n_total,
            "bracket_number": np.tile(np.arange(1, n_horses + 1), n_races),
            "horse_number": np.tile(np.arange(1, n_horses + 1), n_races),
            "weight": 55.0 + np.random.randn(n_total),
            "course_type": np.random.choice(["turf", "dirt"], n_total),
            "track_condition": np.random.choice(["good", "soft"], n_total),
            "feature_a": np.random.randn(n_total),
            "feature_b": np.random.randn(n_total),
            "feature_c": np.random.randn(n_total),
        })
        for col in ["course_type", "track_condition"]:
            X_train[col] = X_train[col].astype("category")

        positions = np.tile(np.arange(1, n_horses + 1), n_races)
        y_train = np.array([JRA_PRIZE_WEIGHTS.get(int(p), 0) for p in positions])
        groups_train = [n_horses] * n_races

        X_valid = X_train.copy()
        y_valid = y_train.copy()
        groups_valid = groups_train.copy()

        config = LGBMRankerMultiConfig(num_boost_round=10, early_stopping_rounds=5, log_evaluation=0)
        ranker = LGBMRankerMulti(config=config)
        ranker.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid,
                     categorical_feature=["course_type", "track_condition"])

        model_path = str(tmp_path / "model.txt")
        ranker.save(model_path)
        return model_path

    @pytest.fixture
    def mock_predict_df(self):
        """推論対象のモックデータ"""
        np.random.seed(42)
        rows = []
        for day in [14, 15]:
            for race_num in range(2):
                race_date = datetime.date(2026, 2, day)
                race_id = f"race_{race_date.strftime('%Y%m%d')}_{race_num}"
                for horse_num in range(1, 9):
                    rows.append({
                        "race_id": race_id,
                        "horse_id": f"horse_{horse_num}",
                        "horse_name": f"テスト馬{horse_num}",
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

    @patch("src.models.predict.fetch_race_results")
    @patch("src.models.predict.fetch_prediction_data")
    def test_predict_pipeline_basic(self, mock_fetch, mock_race_results, trained_model_path, mock_predict_df):
        """推論パイプラインが正常に動作すること"""
        mock_fetch.return_value = mock_predict_df
        mock_race_results.return_value = pd.DataFrame(columns=["race_id", "horse_id", "finish_position"])

        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "date_column": "race_date",
                "exclude_columns": [
                    "race_id", "horse_id", "race_date",
                    "target_place", "finish_position",
                    "venue_code", "jockey_id", "trainer_id",
                    "created_at", "race_number",
                ],
                "categorical_columns": ["course_type", "track_condition"],
            },
        }

        result_df = predict_pipeline(
            project_id="test-project",
            execution_date=datetime.date(2026, 2, 13),
            config=config,
            model_path=trained_model_path,
        )

        assert len(result_df) > 0
        assert "pred_score" in result_df.columns
        assert "pred_rank" in result_df.columns
        assert "race_id" in result_df.columns
        assert "horse_number" in result_df.columns

        # 各レース内でpred_rankが1からnum_horsesまで振られていること
        for race_id, group in result_df.groupby("race_id"):
            ranks = sorted(group["pred_rank"].tolist())
            assert ranks[0] == 1

    @patch("src.models.predict.fetch_race_results")
    @patch("src.models.predict.fetch_prediction_data")
    def test_predict_pipeline_with_target_dates(
        self, mock_fetch, mock_race_results, trained_model_path, mock_predict_df
    ):
        """target_datesを指定した場合、その日付でfetch_prediction_dataが呼ばれること"""
        mock_fetch.return_value = mock_predict_df
        mock_race_results.return_value = pd.DataFrame(columns=["race_id", "horse_id", "finish_position"])

        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "date_column": "race_date",
                "exclude_columns": [
                    "race_id", "horse_id", "race_date",
                    "target_place", "finish_position",
                    "venue_code", "jockey_id", "trainer_id",
                    "created_at", "race_number",
                ],
                "categorical_columns": ["course_type", "track_condition"],
            },
        }

        target_dates = [datetime.date(2026, 1, 10), datetime.date(2026, 1, 12)]

        result_df = predict_pipeline(
            project_id="test-project",
            execution_date=datetime.date(2026, 2, 13),
            config=config,
            model_path=trained_model_path,
            target_dates=target_dates,
        )

        # fetch_prediction_dataに指定した日付が渡されること
        call_args = mock_fetch.call_args
        assert call_args.kwargs["target_dates"] == target_dates

        assert len(result_df) > 0

    @patch("src.models.predict.fetch_race_results")
    @patch("src.models.predict.fetch_prediction_data")
    def test_predict_pipeline_default_uses_week_boundaries(
        self, mock_fetch, mock_race_results, trained_model_path, mock_predict_df
    ):
        """target_dates未指定時、execution_dateの週の土日が使われること"""
        mock_fetch.return_value = mock_predict_df
        mock_race_results.return_value = pd.DataFrame(columns=["race_id", "horse_id", "finish_position"])

        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "date_column": "race_date",
                "exclude_columns": [
                    "race_id", "horse_id", "race_date",
                    "target_place", "finish_position",
                    "venue_code", "jockey_id", "trainer_id",
                    "created_at", "race_number",
                ],
                "categorical_columns": ["course_type", "track_condition"],
            },
        }

        # 2026-02-13（金曜日）→ 同じ週の土曜2/14, 日曜2/15
        execution_date = datetime.date(2026, 2, 13)
        result_df = predict_pipeline(
            project_id="test-project",
            execution_date=execution_date,
            config=config,
            model_path=trained_model_path,
        )

        call_args = mock_fetch.call_args
        passed_dates = call_args.kwargs["target_dates"]
        assert datetime.date(2026, 2, 14) in passed_dates
        assert datetime.date(2026, 2, 15) in passed_dates

    @patch("src.models.predict.fetch_race_results")
    @patch("src.models.predict.fetch_prediction_data")
    def test_predict_pipeline_empty_data(self, mock_fetch, mock_race_results, trained_model_path):
        """推論対象データがない場合、空のDataFrameを返すこと"""
        mock_fetch.return_value = pd.DataFrame()
        mock_race_results.return_value = pd.DataFrame(columns=["race_id", "horse_id", "finish_position"])

        config = {
            "data": {
                "dataset": "features",
                "table": "training_data",
                "date_column": "race_date",
                "exclude_columns": ["race_id", "horse_id", "race_date",
                                    "target_place", "finish_position",
                                    "venue_code", "jockey_id", "trainer_id",
                                    "created_at", "race_number"],
                "categorical_columns": [],
            },
        }

        result_df = predict_pipeline(
            project_id="test-project",
            execution_date=datetime.date(2026, 2, 13),
            config=config,
            model_path=trained_model_path,
        )

        assert len(result_df) == 0


class TestFormatPredictions:
    """format_predictionsのテスト"""

    def test_format_empty(self):
        """空DataFrameの場合、メッセージが返ること"""
        result = format_predictions(pd.DataFrame())
        assert "推論対象データがありません" in result

    def test_format_with_data(self):
        """データがある場合、整形された文字列が返ること"""
        df = pd.DataFrame({
            "race_id": ["r1", "r1", "r1"],
            "race_date": [datetime.date(2026, 2, 14)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_name": ["テスト馬1", "テスト馬2", "テスト馬3"],
            "horse_number": [1, 2, 3],
            "venue_code": ["01", "01", "01"],
            "race_number": [1, 1, 1],
            "pred_score": [0.8, 0.5, 0.3],
            "win_place_prob": [0.7, 0.5, 0.3],
            "pred_rank": [1, 2, 3],
            "finish_position": [2, 1, 3],
        })

        result = format_predictions(df)
        assert "札幌" in result  # venue_code "01" → "札幌" に変換される
        assert "1R" in result
        assert "0.8000" in result

    def test_format_position_zero_shows_dash(self):
        """finish_position=0の場合、'-'が表示されること（取消/除外/中止の馬）"""
        # classify_race_pattern は最低3頭必要なのでダミー馬を追加
        df = pd.DataFrame({
            "race_id": ["r1", "r1", "r1"],
            "race_date": [datetime.date(2026, 2, 14)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_name": ["テスト馬1", "テスト馬2", "ダミー馬3"],
            "horse_number": [1, 2, 3],
            "venue_code": ["05", "05", "05"],
            "race_number": [1, 1, 1],
            "pred_score": [0.8, 0.5, 0.2],
            "win_place_prob": [0.7, 0.5, 0.2],
            "pred_rank": [1, 2, 3],
            "finish_position": [1, 0, 3],  # 2頭目は取消（finish_position=0）
        })

        result = format_predictions(df)
        lines = result.split("\n")
        # 着順カラムの値を取得（各行末尾が着順）
        horse_lines = [l for l in lines if "テスト馬" in l]
        assert len(horse_lines) == 2
        # 1着は "1" が表示される
        assert horse_lines[0].strip().endswith("1")
        # 取消馬は "-" が表示される
        assert horse_lines[1].strip().endswith("-")

    def test_format_position_none_shows_dash(self):
        """finish_positionがNaNの場合（未来レース）、'-'が表示されること"""
        # classify_race_pattern は最低3頭必要なのでダミー馬を追加
        df = pd.DataFrame({
            "race_id": ["r1", "r1", "r1"],
            "race_date": [datetime.date(2026, 2, 14)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_name": ["テスト馬1", "ダミー馬2", "ダミー馬3"],
            "horse_number": [1, 2, 3],
            "venue_code": ["05", "05", "05"],
            "race_number": [1, 1, 1],
            "pred_score": [0.8, 0.5, 0.2],
            "win_place_prob": [0.7, 0.5, 0.2],
            "pred_rank": [1, 2, 3],
            "finish_position": [float("nan"), float("nan"), float("nan")],  # 未来レースはNaN
        })

        result = format_predictions(df)
        horse_lines = [l for l in result.split("\n") if "テスト馬" in l]
        assert len(horse_lines) == 1
        assert horse_lines[0].strip().endswith("-")


class TestSavePredictionsToBQ:
    """save_predictions_to_bqのテスト"""

    @pytest.fixture
    def sample_result_df(self):
        """予測結果のサンプルDataFrame"""
        return pd.DataFrame({
            "race_id": ["race_20260214_01", "race_20260214_01", "race_20260214_01"],
            "race_date": [datetime.date(2026, 2, 14)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_number": [1, 2, 3],
            "horse_name": ["テスト馬1", "テスト馬2", "テスト馬3"],
            "venue_code": ["05", "05", "05"],
            "race_number": [1, 1, 1],
            "win_place_prob": [0.7, 0.5, 0.3],
            "pred_score": [0.8, 0.5, 0.2],
            "pred_rank": [1, 2, 3],
        })

    @pytest.fixture
    def mock_bq_client(self):
        """BigQueryクライアントのモック"""
        mock_job = type("Job", (), {"result": lambda self: None})()
        mock_query_job = type("QueryJob", (), {"result": lambda self: None})()

        with patch("src.models.predict.bigquery.Client") as mock_client_cls:
            with patch("src.models.predict.bigquery.Table") as mock_table_cls:
                with patch("src.models.predict.bigquery.LoadJobConfig"):
                    mock_client = mock_client_cls.return_value
                    mock_client.create_table.return_value = mock_table_cls.return_value
                    mock_client.load_table_from_dataframe.return_value = mock_job
                    mock_client.query.return_value = mock_query_job
                    mock_client.delete_table.return_value = None
                    yield mock_client

    def test_save_predictions_success(self, sample_result_df, mock_bq_client):
        """正常にBigQuery保存が呼び出されること"""
        saved_rows = save_predictions_to_bq(
            result_df=sample_result_df,
            project_id="test-project",
        )

        assert saved_rows == len(sample_result_df)
        mock_bq_client.create_table.assert_called_once()
        mock_bq_client.load_table_from_dataframe.assert_called_once()
        mock_bq_client.query.assert_called_once()
        # MERGE文が正しいキーを含んでいること
        merge_query = mock_bq_client.query.call_args[0][0]
        assert "race_id" in merge_query
        assert "horse_id" in merge_query
        assert "MERGE" in merge_query

    def test_save_predictions_empty_df(self):
        """空DataFrameは0行を返すこと（BigQuery呼び出しなし）"""
        with patch("src.models.predict.bigquery.Client") as mock_client_cls:
            saved_rows = save_predictions_to_bq(
                result_df=pd.DataFrame(),
                project_id="test-project",
            )
        assert saved_rows == 0
        mock_client_cls.assert_not_called()

    def test_save_predictions_missing_required_columns(self):
        """必須カラムが不足している場合はValueErrorが発生すること"""
        df = pd.DataFrame({"race_id": ["r1"], "horse_id": ["h1"]})
        # win_place_prob と pred_score が欠けている
        with pytest.raises(ValueError, match="必須カラムが不足しています"):
            save_predictions_to_bq(result_df=df, project_id="test-project")

    def test_save_predictions_with_place_odds(self, sample_result_df, mock_bq_client):
        """place_oddsカラムがある場合も保存DataFrame内にplace_oddsが含まれること"""
        df_with_odds = sample_result_df.copy()
        df_with_odds["place_odds"] = [3.2, 5.1, 8.4]

        saved_rows = save_predictions_to_bq(
            result_df=df_with_odds,
            project_id="test-project",
        )

        assert saved_rows == len(df_with_odds)
        # load_table_from_dataframeに渡されたDataFrameにplace_oddsが含まれること
        call_args = mock_bq_client.load_table_from_dataframe.call_args
        saved_df = call_args[0][0]
        assert "place_odds" in saved_df.columns

    def test_save_predictions_temp_table_cleanup_on_error(self, sample_result_df):
        """エラー発生時でも一時テーブルが削除されること"""
        with patch("src.models.predict.bigquery.Client") as mock_client_cls:
            with patch("src.models.predict.bigquery.Table"):
                with patch("src.models.predict.bigquery.LoadJobConfig"):
                    mock_client = mock_client_cls.return_value
                    mock_client.create_table.return_value = mock_client_cls.return_value
                    # load_table_from_dataframe でエラーを発生させる
                    mock_client.load_table_from_dataframe.side_effect = Exception("BQ Error")
                    mock_client.delete_table.return_value = None

                    with pytest.raises(Exception, match="BQ Error"):
                        save_predictions_to_bq(
                            result_df=sample_result_df,
                            project_id="test-project",
                        )

        # delete_tableが呼ばれたことを確認（finallyブロック）
        mock_client.delete_table.assert_called_once()


class TestSavePredictionsToGCS:
    """save_predictions_to_gcsのテスト"""

    @pytest.fixture
    def sample_result_df(self):
        """予測結果のサンプルDataFrame"""
        return pd.DataFrame({
            "race_id": ["race_20260301_01", "race_20260301_01", "race_20260301_01"],
            "race_date": [datetime.date(2026, 3, 1)] * 3,
            "horse_id": ["h1", "h2", "h3"],
            "horse_number": [1, 2, 3],
            "horse_name": ["テスト馬1", "テスト馬2", "テスト馬3"],
            "venue_code": ["05", "05", "05"],
            "race_number": [1, 1, 1],
            "win_place_prob": [0.7, 0.5, 0.3],
            "pred_score": [0.8, 0.5, 0.2],
            "pred_rank": [1, 2, 3],
        })

    def test_save_to_gcs_success(self, sample_result_df):
        """正常にGCS保存が呼び出されること"""
        with patch("src.models.predict.storage.Client") as mock_client_cls:
            mock_blob = mock_client_cls.return_value.bucket.return_value.blob.return_value
            mock_blob.upload_from_string.return_value = None

            gcs_uri = save_predictions_to_gcs(
                result_df=sample_result_df,
                project_id="test-project",
                race_date=datetime.date(2026, 3, 1),
            )

        assert gcs_uri == "gs://test-project-keiba-predictions/2026-03-01/predictions.csv"
        mock_blob.upload_from_string.assert_called_once()
        # CSVとして保存されていること
        call_args = mock_blob.upload_from_string.call_args
        assert call_args.kwargs.get("content_type") == "text/csv" or call_args[1].get("content_type") == "text/csv"

    def test_save_to_gcs_empty_df_raises(self):
        """空DataFrameはValueErrorが発生すること"""
        with pytest.raises(ValueError, match="保存するデータがありません"):
            save_predictions_to_gcs(
                result_df=pd.DataFrame(),
                project_id="test-project",
            )

    def test_save_to_gcs_auto_date(self, sample_result_df):
        """race_date未指定時はDataFrameの最小日付が使われること"""
        with patch("src.models.predict.storage.Client") as mock_client_cls:
            mock_blob = mock_client_cls.return_value.bucket.return_value.blob.return_value
            mock_blob.upload_from_string.return_value = None

            gcs_uri = save_predictions_to_gcs(
                result_df=sample_result_df,
                project_id="test-project",
            )

        # race_dateの最小値 2026-03-01 がパスに使われること
        assert "2026-03-01" in gcs_uri

    def test_save_to_gcs_correct_bucket_path(self, sample_result_df):
        """バケット名が正しく生成されること"""
        with patch("src.models.predict.storage.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_blob = mock_client.bucket.return_value.blob.return_value
            mock_blob.upload_from_string.return_value = None

            save_predictions_to_gcs(
                result_df=sample_result_df,
                project_id="my-project",
                race_date=datetime.date(2026, 3, 1),
            )

        # バケット名が {project_id}-keiba-predictions であること
        mock_client.bucket.assert_called_with("my-project-keiba-predictions")
        # blobパスが {date}/predictions.csv であること
        mock_client.bucket.return_value.blob.assert_called_with("2026-03-01/predictions.csv")


class TestFetchPredictionData:
    """fetch_prediction_data が force_sql=True 時に feature_query_raw.sql を直接実行することを検証 (Issue #288)"""

    def _make_mock_df(self, dates: list[datetime.date]) -> pd.DataFrame:
        rows = []
        for d in dates:
            rows.append({
                "race_id": f"race_{d.strftime('%Y%m%d')}_01",
                "horse_id": "h1",
                "horse_name": "テスト馬1",
                "race_date": d,
                "horse_number": 1,
            })
        return pd.DataFrame(rows)

    @patch("src.models.predict.bigquery.Client")
    @patch("src.models.predict.FeaturePipeline")
    def test_uses_feature_pipeline_generate_query(self, mock_pipeline_cls, mock_bq_cls):
        """force_sql=True 時に FeaturePipeline.generate_query() が呼ばれてSQLが直接実行されること"""
        target_dates = [datetime.date(2026, 5, 17)]
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.generate_query.return_value = "SELECT 1"

        mock_bq = mock_bq_cls.return_value
        mock_df = self._make_mock_df(target_dates)
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = mock_df

        result = fetch_prediction_data(project_id="test-project", target_dates=target_dates, force_sql=True)

        mock_pipeline_cls.assert_called_once_with("test-project")
        mock_pipeline.generate_query.assert_called_once_with("2023-05-18", "2026-05-17")
        mock_bq.query.assert_called_once_with("SELECT 1")
        assert len(result) == 1

    @patch("src.models.predict.bigquery.Client")
    @patch("src.models.predict.FeaturePipeline")
    def test_does_not_query_training_data_table(self, mock_pipeline_cls, mock_bq_cls):
        """force_sql=True 時は training_data を直接参照するクエリが実行されないこと"""
        target_dates = [datetime.date(2026, 5, 17)]
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.generate_query.return_value = "SELECT * FROM raw.horse_results"

        mock_bq = mock_bq_cls.return_value
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = self._make_mock_df(target_dates)

        fetch_prediction_data(project_id="test-project", target_dates=target_dates, force_sql=True)

        executed_sql = mock_bq.query.call_args[0][0]
        assert "training_data" not in executed_sql

    @patch("src.models.predict.bigquery.Client")
    @patch("src.models.predict.FeaturePipeline")
    def test_multi_date_uses_min_max_as_range(self, mock_pipeline_cls, mock_bq_cls):
        """複数日指定時は min/max を start_date/end_date として渡すこと"""
        target_dates = [datetime.date(2026, 5, 17), datetime.date(2026, 5, 18)]
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.generate_query.return_value = "SELECT 1"

        mock_bq = mock_bq_cls.return_value
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = self._make_mock_df(target_dates)

        fetch_prediction_data(project_id="test-project", target_dates=target_dates, force_sql=True)

        mock_pipeline.generate_query.assert_called_once_with("2023-05-18", "2026-05-18")

    @patch("src.models.predict.bigquery.Client")
    @patch("src.models.predict.FeaturePipeline")
    def test_filters_by_target_dates(self, mock_pipeline_cls, mock_bq_cls):
        """SQLの結果が target_dates に含まれない日付を除外すること"""
        target_dates = [datetime.date(2026, 5, 17)]
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.generate_query.return_value = "SELECT 1"

        mock_bq = mock_bq_cls.return_value
        # 2日分を返すが target_dates は1日のみ
        extra_dates = [datetime.date(2026, 5, 17), datetime.date(2026, 5, 16)]
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = self._make_mock_df(extra_dates)

        result = fetch_prediction_data(project_id="test-project", target_dates=target_dates, force_sql=True)

        # target_dates にある 2026-05-17 の行のみ残ること
        assert len(result) == 1
        assert all(r == datetime.date(2026, 5, 17) for r in result["race_date"])

    @patch("src.models.predict.bigquery.Client")
    @patch("src.models.predict.FeaturePipeline")
    def test_returns_empty_df_when_no_rows(self, mock_pipeline_cls, mock_bq_cls):
        """SQLが0行を返す場合、空DataFrameを返すこと"""
        target_dates = [datetime.date(2026, 5, 17)]
        mock_pipeline = mock_pipeline_cls.return_value
        mock_pipeline.generate_query.return_value = "SELECT 1"

        mock_bq = mock_bq_cls.return_value
        mock_bq.query.return_value.result.return_value.to_dataframe.return_value = pd.DataFrame()

        result = fetch_prediction_data(project_id="test-project", target_dates=target_dates, force_sql=True)

        assert len(result) == 0


class TestNormalizeWinPlaceProb:
    """normalize_win_place_prob のテスト"""

    def _make_df(self, race_scores: dict[str, list]) -> pd.DataFrame:
        rows = []
        for race_id, scores in race_scores.items():
            for i, s in enumerate(scores):
                rows.append({"race_id": race_id, "pred_score": float(s), "horse_number": i + 1})
        return pd.DataFrame(rows)

    def test_probabilities_sum_to_n_places_per_race(self):
        """各レース内でwin_place_probの合計がmin(3, 出走頭数)になること"""
        # 4頭レース → 合計 3.0
        df4 = self._make_df({"R1": [2.0, 1.0, 0.5, 0.1]})
        result4 = normalize_win_place_prob(df4)
        assert abs(result4["win_place_prob"].sum() - 3.0) < 1e-6

        # 2頭レース → 合計 min(3,2) = 2.0
        df2 = self._make_df({"R2": [1.0, 0.0]})
        result2 = normalize_win_place_prob(df2)
        assert abs(result2["win_place_prob"].sum() - 2.0) < 1e-6

    def test_each_prob_at_most_one(self):
        """各馬のwin_place_probが1.0を超えないこと"""
        df = self._make_df({"R1": [5.0, 1.0, 0.5, 0.1, 0.0]})
        result = normalize_win_place_prob(df)
        assert (result["win_place_prob"] <= 1.0 + 1e-9).all()

    def test_temperature_softens_top_horse_prob(self):
        """temperature > 1.0 で上位馬の確率が低下すること（過信抑制）"""
        df = self._make_df({"R1": [3.0, 0.5, 0.0, 0.0, 0.0]})
        result_default = normalize_win_place_prob(df, temperature=1.0)
        result_tempered = normalize_win_place_prob(df, temperature=1.5)
        # temperature=1.0（旧アルゴリズム相当）より temperature=1.5 の方が
        # 上位馬のprobが低く、他馬に分散される
        top_prob_default = result_default.sort_values("pred_score", ascending=False)["win_place_prob"].iloc[0]
        top_prob_tempered = result_tempered.sort_values("pred_score", ascending=False)["win_place_prob"].iloc[0]
        assert top_prob_tempered <= top_prob_default

    def test_higher_score_gets_higher_prob(self):
        """スコアが高い馬ほど高いwin_place_probを得ること"""
        df = self._make_df({"R1": [3.0, 2.0, 1.0, 0.0]})
        result = normalize_win_place_prob(df).sort_values("pred_score", ascending=False).reset_index(drop=True)
        probs = result["win_place_prob"].values
        for i in range(len(probs) - 1):
            assert probs[i] >= probs[i + 1]

    def test_equal_scores_give_equal_probs(self):
        """スコアが同一の場合、win_place_probが均等（k/n）になること"""
        # 4頭均等 → 各馬 3/4 = 0.75
        df = self._make_df({"R1": [1.0, 1.0, 1.0, 1.0]})
        result = normalize_win_place_prob(df)
        assert np.allclose(result["win_place_prob"].values, 0.75)

        # 3頭均等 → 各馬 3/3 = 1.0
        df3 = self._make_df({"R2": [1.0, 1.0, 1.0]})
        result3 = normalize_win_place_prob(df3)
        assert np.allclose(result3["win_place_prob"].values, 1.0)

    def test_original_df_not_modified(self):
        """入力DataFrameが変更されないこと（コピーを返すこと）"""
        df = self._make_df({"R1": [2.0, 1.0, 0.0]})
        orig_cols = set(df.columns)
        _ = normalize_win_place_prob(df)
        assert set(df.columns) == orig_cols
        assert "win_place_prob" not in df.columns

    def test_multiple_races_independent(self):
        """複数レースが独立して正規化されること"""
        df = self._make_df({"R1": [2.0, 1.0, 0.0, 0.0], "R2": [5.0, 0.0, 0.0, 0.0]})
        result = normalize_win_place_prob(df)
        r1_sum = result[result["race_id"] == "R1"]["win_place_prob"].sum()
        r2_sum = result[result["race_id"] == "R2"]["win_place_prob"].sum()
        assert abs(r1_sum - 3.0) < 1e-6
        assert abs(r2_sum - 3.0) < 1e-6

    def test_temperature_effect(self):
        """温度パラメータが大きいほど下位馬への確率が増えること"""
        df = self._make_df({"R1": [3.0, 0.0, 0.0, 0.0, 0.0]})
        result_low = normalize_win_place_prob(df, temperature=0.5)
        result_high = normalize_win_place_prob(df, temperature=5.0)
        # 低温度の方が最下位馬の確率が低い（より上位集中）
        min_low = result_low.sort_values("pred_score")["win_place_prob"].iloc[0]
        min_high = result_high.sort_values("pred_score")["win_place_prob"].iloc[0]
        assert min_low < min_high

    def test_scale_invariance(self):
        """raw scoreスケール（分散）が変わっても複勝率が一定になること（z-score標準化の核心・Issue #397）"""
        # 同一の順位・相対関係だがスケールが2倍異なる2レース
        base = [2.0, 1.0, 0.5, 0.0, -0.5]
        df_small = self._make_df({"R1": base})
        df_large = self._make_df({"R1": [s * 3.0 for s in base]})
        probs_small = (
            normalize_win_place_prob(df_small)
            .sort_values("pred_score", ascending=False)["win_place_prob"].values
        )
        probs_large = (
            normalize_win_place_prob(df_large)
            .sort_values("pred_score", ascending=False)["win_place_prob"].values
        )
        # z-score標準化により、スコアスケールに不変な複勝率が得られる
        assert np.allclose(probs_small, probs_large, atol=1e-9)

    def test_shifted_scores_invariant(self):
        """スコアに定数を加算しても複勝率が一定になること（平均不変性）"""
        df_a = self._make_df({"R1": [-4.4, -4.5, -4.6, -4.8, -5.7]})
        df_b = self._make_df({"R1": [0.4, 0.3, 0.2, 0.0, -0.9]})  # +4.8 シフト
        probs_a = (
            normalize_win_place_prob(df_a)
            .sort_values("pred_score", ascending=False)["win_place_prob"].values
        )
        probs_b = (
            normalize_win_place_prob(df_b)
            .sort_values("pred_score", ascending=False)["win_place_prob"].values
        )
        assert np.allclose(probs_a, probs_b, atol=1e-9)
