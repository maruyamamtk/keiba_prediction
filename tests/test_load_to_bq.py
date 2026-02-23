"""
load_to_bq モジュールのユニットテスト
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.automation.data.load_to_bq import (
    BatchLoadResult,
    BigQueryLoader,
    EXPAND_DATA_TYPES,
    LOAD_HISTORY_TABLE,
    LoadResult,
    create_loader_from_env,
    extract_data_type,
    extract_file_date,
    get_table_name,
)


class TestExtractDataType:
    """extract_data_type関数のテスト"""

    def test_valid_baa_filename(self):
        assert extract_data_type("BAA260104.csv") == "BAA"

    def test_valid_kyf_filename(self):
        assert extract_data_type("KYF260104.csv") == "KYF"

    def test_valid_sec_filename(self):
        assert extract_data_type("SEC260104.csv") == "SEC"

    def test_valid_ukc_filename(self):
        assert extract_data_type("UKC260104.csv") == "UKC"

    def test_lowercase_filename(self):
        """小文字のファイル名も正しく処理される"""
        assert extract_data_type("baa260104.csv") == "BAA"

    def test_with_directory_path(self):
        """ディレクトリパスを含む場合も正しく処理される"""
        assert extract_data_type("csv/BAA260104.csv") == "BAA"
        assert extract_data_type("Baa/BAA260104.csv") == "BAA"

    def test_invalid_extension(self):
        """CSV以外の拡張子はNoneを返す"""
        assert extract_data_type("BAA260104.txt") is None

    def test_invalid_format(self):
        """不正なフォーマットはNoneを返す"""
        assert extract_data_type("invalid.csv") is None
        assert extract_data_type("BAA.csv") is None
        assert extract_data_type("BAA12345.csv") is None  # 5桁

    def test_two_letter_data_type(self):
        """2文字のデータタイプも対応"""
        # 現在のパターンは2-3文字を許容
        result = extract_data_type("OZ260104.csv")
        assert result == "OZ"


class TestGetTableName:
    """get_table_name関数のテスト"""

    def test_baa_maps_to_race_info(self):
        assert get_table_name("BAA") == "race_info"

    def test_bab_maps_to_race_info(self):
        assert get_table_name("BAB") == "race_info"

    def test_kyf_maps_to_horse_results(self):
        assert get_table_name("KYF") == "horse_results"

    def test_sec_maps_to_race_results(self):
        assert get_table_name("SEC") == "race_results"

    def test_ukc_maps_to_horse_master(self):
        assert get_table_name("UKC") == "horse_master"

    def test_kka_maps_to_horse_extended(self):
        assert get_table_name("KKA") == "horse_extended"

    def test_kaa_maps_to_venue_info(self):
        assert get_table_name("KAA") == "venue_info"

    def test_lowercase_input(self):
        """小文字の入力も正しく処理される"""
        assert get_table_name("baa") == "race_info"

    def test_hjb_maps_to_payouts(self):
        assert get_table_name("HJB") == "payouts"

    def test_hjc_maps_to_payouts(self):
        assert get_table_name("HJC") == "payouts"

    def test_oz_maps_to_odds(self):
        assert get_table_name("OZ") == "odds"

    def test_unknown_data_type(self):
        """未知のデータタイプはNoneを返す"""
        assert get_table_name("XXX") is None
        assert get_table_name("UNKNOWN") is None


class TestLoadResult:
    """LoadResultデータクラスのテスト"""

    def test_default_values(self):
        result = LoadResult(file_name="test.csv", status="success")
        assert result.file_name == "test.csv"
        assert result.status == "success"
        assert result.records_processed == 0
        assert result.table is None
        assert result.error is None
        assert result.duration_seconds == 0.0

    def test_with_all_values(self):
        result = LoadResult(
            file_name="BAA260104.csv",
            status="success",
            records_processed=100,
            table="raw.race_info",
            error=None,
            duration_seconds=1.5,
        )
        assert result.records_processed == 100
        assert result.table == "raw.race_info"
        assert result.duration_seconds == 1.5


class TestBatchLoadResult:
    """BatchLoadResultデータクラスのテスト"""

    def test_default_values(self):
        result = BatchLoadResult()
        assert result.total_files == 0
        assert result.success_count == 0
        assert result.skipped_count == 0
        assert result.failed_count == 0
        assert result.total_records == 0
        assert result.results == []
        assert result.failed_files == []
        assert result.duration_seconds == 0.0


class TestBigQueryLoaderInit:
    """BigQueryLoaderの初期化テスト"""

    def test_init_with_required_params(self):
        loader = BigQueryLoader(project_id="test-project")
        assert loader.project_id == "test-project"
        assert loader.dataset_id == "raw"
        assert loader.bucket_name == "test-project-keiba-raw-data"

    def test_init_with_all_params(self):
        loader = BigQueryLoader(
            project_id="test-project",
            dataset_id="custom_dataset",
            bucket_name="custom-bucket",
        )
        assert loader.project_id == "test-project"
        assert loader.dataset_id == "custom_dataset"
        assert loader.bucket_name == "custom-bucket"

    def test_clients_are_lazy_initialized(self):
        loader = BigQueryLoader(project_id="test-project")
        # 初期状態ではクライアントはNone
        assert loader._bq_client is None
        assert loader._storage_client is None


class TestBigQueryLoaderClients:
    """BigQueryLoaderのクライアント初期化テスト"""

    @patch("google.cloud.bigquery.Client")
    def test_bq_client_lazy_init(self, mock_bq_client):
        loader = BigQueryLoader(project_id="test-project")
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        # プロパティにアクセスしてクライアントを初期化
        client = loader.bq_client
        assert client == mock_client_instance
        mock_bq_client.assert_called_once_with(project="test-project")

    @patch("google.cloud.bigquery.Client")
    def test_bq_client_cached(self, mock_bq_client):
        loader = BigQueryLoader(project_id="test-project")
        mock_bq_client.return_value = MagicMock()

        # 2回アクセスしても初期化は1回のみ
        _ = loader.bq_client
        _ = loader.bq_client
        mock_bq_client.assert_called_once()

    @patch("google.cloud.storage.Client")
    def test_storage_client_lazy_init(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")
        mock_client_instance = MagicMock()
        mock_storage_client.return_value = mock_client_instance

        client = loader.storage_client
        assert client == mock_client_instance
        mock_storage_client.assert_called_once_with(project="test-project")


class TestBigQueryLoaderDownload:
    """BigQueryLoaderのGCSダウンロードテスト"""

    @patch("google.cloud.storage.Client")
    def test_download_file_success_utf8(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")

        # モックの設定
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_bytes.return_value = "テストデータ".encode("utf-8")

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        result = loader._download_file_from_gcs("test.csv")
        assert result == "テストデータ"

    @patch("google.cloud.storage.Client")
    def test_download_file_success_cp932(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")

        # CP932でエンコードされたデータ
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.download_as_bytes.return_value = "テストデータ".encode("cp932")

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        result = loader._download_file_from_gcs("test.csv")
        assert result == "テストデータ"

    @patch("google.cloud.storage.Client")
    def test_download_file_not_exists(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        result = loader._download_file_from_gcs("nonexistent.csv")
        assert result is None


class TestBigQueryLoaderLoadFile:
    """BigQueryLoaderのload_fileメソッドテスト"""

    @patch("src.automation.data.load_to_bq.JRDBParser", None)
    def test_load_file_no_parser(self):
        """パーサーが利用できない場合のテスト"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "_download_file_from_gcs", return_value="data"):
            result = loader.load_file("BAA260104.csv")

        assert result.status == "failed"
        assert "JRDBParser" in result.error

    def test_load_file_invalid_filename(self):
        """無効なファイル名の場合のテスト"""
        loader = BigQueryLoader(project_id="test-project")
        result = loader.load_file("invalid.csv")

        assert result.status == "failed"
        assert "無効なファイル名" in result.error

    def test_load_file_unsupported_data_type(self):
        """未サポートのデータタイプの場合のテスト"""
        loader = BigQueryLoader(project_id="test-project")
        result = loader.load_file("XXX260104.csv")

        assert result.status == "skipped"
        assert "未サポート" in result.error


class TestBigQueryLoaderListFiles:
    """BigQueryLoaderのlist_csv_filesメソッドテスト"""

    @patch("google.cloud.storage.Client")
    def test_list_csv_files(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")

        # モックblob
        mock_blobs = [
            MagicMock(name="BAA260104.csv"),
            MagicMock(name="KYF260104.csv"),
            MagicMock(name="data.txt"),  # CSVではない
        ]
        mock_blobs[0].name = "BAA260104.csv"
        mock_blobs[1].name = "KYF260104.csv"
        mock_blobs[2].name = "data.txt"

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = mock_blobs

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        result = loader.list_csv_files()

        assert len(result) == 2
        assert "BAA260104.csv" in result
        assert "KYF260104.csv" in result
        assert "data.txt" not in result

    @patch("google.cloud.storage.Client")
    def test_list_csv_files_with_data_type_filter(self, mock_storage_client):
        loader = BigQueryLoader(project_id="test-project")

        mock_blobs = [
            MagicMock(name="BAA260104.csv"),
            MagicMock(name="KYF260104.csv"),
            MagicMock(name="SEC260104.csv"),
        ]
        mock_blobs[0].name = "BAA260104.csv"
        mock_blobs[1].name = "KYF260104.csv"
        mock_blobs[2].name = "SEC260104.csv"

        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = mock_blobs

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_storage_client.return_value = mock_client

        result = loader.list_csv_files(data_types=["BAA", "KYF"])

        assert len(result) == 2
        assert "BAA260104.csv" in result
        assert "KYF260104.csv" in result
        assert "SEC260104.csv" not in result


class TestBigQueryLoaderBatchLoad:
    """BigQueryLoaderのload_files_batchメソッドテスト"""

    def test_batch_load_empty_list(self):
        loader = BigQueryLoader(project_id="test-project")
        result = loader.load_files_batch([])

        assert result.total_files == 0
        assert result.success_count == 0
        assert result.failed_count == 0

    def test_batch_load_with_failures(self):
        loader = BigQueryLoader(project_id="test-project")

        # load_fileをモック
        with patch.object(loader, "load_file") as mock_load:
            mock_load.side_effect = [
                LoadResult(file_name="file1.csv", status="success", records_processed=10),
                LoadResult(file_name="file2.csv", status="failed", error="error"),
                LoadResult(file_name="file3.csv", status="skipped", error="skipped"),
            ]

            result = loader.load_files_batch(
                ["file1.csv", "file2.csv", "file3.csv"]
            )

        assert result.total_files == 3
        assert result.success_count == 1
        assert result.failed_count == 1
        assert result.skipped_count == 1
        assert result.total_records == 10
        assert "file2.csv" in result.failed_files

    def test_batch_load_stop_on_error(self):
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "load_file") as mock_load:
            mock_load.side_effect = [
                LoadResult(file_name="file1.csv", status="success", records_processed=10),
                LoadResult(file_name="file2.csv", status="failed", error="error"),
                LoadResult(file_name="file3.csv", status="success", records_processed=5),
            ]

            result = loader.load_files_batch(
                ["file1.csv", "file2.csv", "file3.csv"],
                continue_on_error=False,
            )

        # file3は処理されない
        assert result.total_files == 3
        assert result.success_count == 1
        assert result.failed_count == 1
        assert len(result.results) == 2


class TestCreateLoaderFromEnv:
    """create_loader_from_env関数のテスト"""

    def test_missing_project_id(self):
        """プロジェクトIDがない場合はNoneを返す"""
        with patch.dict(os.environ, {}, clear=True):
            result = create_loader_from_env()
        assert result is None

    def test_with_project_id_only(self):
        """プロジェクトIDのみの場合"""
        with patch.dict(os.environ, {"GCP_PROJECT_ID": "test-project"}, clear=True):
            result = create_loader_from_env()

        assert result is not None
        assert result.project_id == "test-project"
        assert result.dataset_id == "raw"

    def test_with_all_env_vars(self):
        """全ての環境変数が設定されている場合"""
        env_vars = {
            "GCP_PROJECT_ID": "test-project",
            "BQ_DATASET_RAW": "custom_dataset",
            "GCS_BUCKET_RAW": "custom-raw-data",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            result = create_loader_from_env()

        assert result is not None
        assert result.project_id == "test-project"
        assert result.dataset_id == "custom_dataset"
        assert result.bucket_name == "test-project-custom-raw-data"


class TestBigQueryLoaderMerge:
    """BigQueryLoaderのMERGE処理テスト"""

    @patch("google.cloud.bigquery.Client")
    def test_load_to_bigquery_success(self, mock_bq_client):
        loader = BigQueryLoader(project_id="test-project")

        # モック設定
        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # テーブルスキーマ
        mock_field_1 = MagicMock()
        mock_field_1.name = "race_id"
        mock_field_2 = MagicMock()
        mock_field_2.name = "data"

        mock_table = MagicMock()
        mock_table.schema = [mock_field_1, mock_field_2]
        mock_client.get_table.return_value = mock_table

        # 一時テーブル作成
        mock_client.create_table.return_value = MagicMock()

        # insert_rows_json成功
        mock_client.insert_rows_json.return_value = []

        # クエリ成功
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        # テスト実行
        rows = [{"race_id": "12345678", "data": "test"}]
        result = loader._load_to_bigquery("race_info", rows, "BAA")

        assert result == 1
        mock_client.get_table.assert_called()
        mock_client.create_table.assert_called()
        mock_client.insert_rows_json.assert_called()
        mock_client.query.assert_called()
        mock_client.delete_table.assert_called()

    @patch("google.cloud.bigquery.Client")
    def test_load_to_bigquery_insert_error(self, mock_bq_client):
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        mock_field = MagicMock()
        mock_field.name = "race_id"
        mock_table = MagicMock()
        mock_table.schema = [mock_field]
        mock_client.get_table.return_value = mock_table
        mock_client.create_table.return_value = MagicMock()

        # insert_rows_jsonがエラーを返す
        mock_client.insert_rows_json.return_value = [{"error": "test error"}]

        rows = [{"race_id": "12345678"}]

        with pytest.raises(Exception):
            loader._load_to_bigquery("race_info", rows, "BAA")

        # 一時テーブルは削除される
        mock_client.delete_table.assert_called()


class TestGetLoadedFiles:
    """_get_loaded_filesメソッドのテスト"""

    @patch("google.cloud.bigquery.Client")
    def test_get_loaded_files_success(self, mock_bq_client):
        """ロード済みファイル一覧を取得する"""
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # クエリ結果をモック
        mock_row1 = MagicMock()
        mock_row1.file_name = "BAA260104.csv"
        mock_row2 = MagicMock()
        mock_row2.file_name = "KYF260104.csv"

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [mock_row1, mock_row2]
        mock_client.query.return_value = mock_query_job

        result = loader._get_loaded_files()

        assert result == {"BAA260104.csv", "KYF260104.csv"}
        mock_client.query.assert_called_once()
        # クエリが正しいテーブルを参照していることを確認
        query_call = mock_client.query.call_args[0][0]
        assert LOAD_HISTORY_TABLE in query_call
        assert "status = 'success'" in query_call

    @patch("google.cloud.bigquery.Client")
    def test_get_loaded_files_table_not_found(self, mock_bq_client):
        """テーブルが存在しない場合は空のセットを返す"""
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # テーブルが見つからないエラー
        mock_client.query.side_effect = Exception("Not found: Table test-project.raw.load_history")

        result = loader._get_loaded_files()

        assert result == set()

    @patch("google.cloud.bigquery.Client")
    def test_get_loaded_files_other_error(self, mock_bq_client):
        """その他のエラーは例外を発生させる"""
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client

        # その他のエラー
        mock_client.query.side_effect = Exception("Network error")

        with pytest.raises(Exception):
            loader._get_loaded_files()


class TestRecordLoadHistory:
    """_record_load_historyメソッドのテスト"""

    @patch("google.cloud.bigquery.Client")
    def test_record_load_history_success(self, mock_bq_client):
        """ロード履歴を正常に記録する"""
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client
        mock_client.insert_rows_json.return_value = []

        loader._record_load_history(
            file_name="BAA260104.csv",
            status="success",
            records_count=100,
            table_name="race_info",
            data_type="BAA",
            duration_seconds=1.5,
        )

        mock_client.insert_rows_json.assert_called_once()
        call_args = mock_client.insert_rows_json.call_args
        table_ref = call_args[0][0]
        rows = call_args[0][1]

        assert LOAD_HISTORY_TABLE in table_ref
        assert len(rows) == 1
        assert rows[0]["file_name"] == "BAA260104.csv"
        assert rows[0]["status"] == "success"
        assert rows[0]["records_count"] == 100
        assert rows[0]["table_name"] == "race_info"
        assert rows[0]["data_type"] == "BAA"

    @patch("google.cloud.bigquery.Client")
    def test_record_load_history_failure(self, mock_bq_client):
        """ロード履歴の記録失敗時はワーニングのみ（例外は発生しない）"""
        loader = BigQueryLoader(project_id="test-project")

        mock_client = MagicMock()
        mock_bq_client.return_value = mock_client
        mock_client.insert_rows_json.return_value = [{"error": "insert error"}]

        # 例外は発生しない（ワーニングのみ）
        loader._record_load_history(
            file_name="BAA260104.csv",
            status="failed",
            error_message="Test error",
        )

        mock_client.insert_rows_json.assert_called_once()


class TestIsFileAlreadyLoaded:
    """_is_file_already_loadedメソッドのテスト"""

    def test_file_is_loaded(self):
        """ファイルがロード済みの場合Trueを返す"""
        loader = BigQueryLoader(project_id="test-project")
        loaded_files = {"BAA260104.csv", "KYF260104.csv"}

        result = loader._is_file_already_loaded("BAA260104.csv", loaded_files)

        assert result is True

    def test_file_is_not_loaded(self):
        """ファイルが未ロードの場合Falseを返す"""
        loader = BigQueryLoader(project_id="test-project")
        loaded_files = {"BAA260104.csv", "KYF260104.csv"}

        result = loader._is_file_already_loaded("SEC260104.csv", loaded_files)

        assert result is False


class TestLoadFilesWithSkip:
    """重複スキップ機能を含むload_files_batchメソッドのテスト"""

    def test_batch_load_with_skip_loaded(self):
        """skip_loaded=Trueでロード済みファイルをスキップする"""
        loader = BigQueryLoader(project_id="test-project")

        # ロード済みファイルを返すモック
        with patch.object(
            loader, "_get_loaded_files", return_value={"file1.csv", "file2.csv"}
        ):
            with patch.object(loader, "load_file") as mock_load:
                mock_load.return_value = LoadResult(
                    file_name="file3.csv",
                    status="success",
                    records_processed=10,
                )

                result = loader.load_files_batch(
                    ["file1.csv", "file2.csv", "file3.csv"],
                    skip_loaded=True,
                )

        # file1.csvとfile2.csvはスキップされ、file3.csvのみロードされる
        assert result.total_files == 3
        assert result.success_count == 1
        assert result.skipped_count == 2
        assert result.failed_count == 0

        # load_fileはfile3.csvのみ呼ばれる
        mock_load.assert_called_once_with("file3.csv", record_history=True)

    def test_batch_load_without_skip_loaded(self):
        """skip_loaded=Falseで全ファイルをロードする"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "load_file") as mock_load:
            mock_load.return_value = LoadResult(
                file_name="test.csv",
                status="success",
                records_processed=10,
            )

            result = loader.load_files_batch(
                ["file1.csv", "file2.csv"],
                skip_loaded=False,
            )

        # 全ファイルがロードされる
        assert result.total_files == 2
        assert result.success_count == 2
        assert mock_load.call_count == 2

    def test_batch_load_skip_reason_in_result(self):
        """スキップされたファイルの理由が結果に含まれる"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(
            loader, "_get_loaded_files", return_value={"file1.csv"}
        ):
            with patch.object(loader, "load_file"):
                result = loader.load_files_batch(
                    ["file1.csv"],
                    skip_loaded=True,
                )

        assert len(result.results) == 1
        assert result.results[0].status == "skipped"
        assert result.results[0].error == "既にロード済み"


class TestLoadFileWithHistory:
    """履歴記録を含むload_fileメソッドのテスト"""

    def test_load_file_records_history_on_success(self):
        """成功時にロード履歴を記録する"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "_download_file_from_gcs", return_value="data"):
            with patch.object(loader, "_load_to_bigquery", return_value=100):
                with patch.object(loader, "_record_load_history") as mock_history:
                    with patch("src.automation.data.load_to_bq.JRDBParser") as mock_parser:
                        mock_parser.parse_file.return_value = [{"data": "test"}]

                        result = loader.load_file("BAA260104.csv")

        assert result.status == "success"
        mock_history.assert_called_once()
        call_kwargs = mock_history.call_args[1]
        assert call_kwargs["file_name"] == "BAA260104.csv"
        assert call_kwargs["status"] == "success"
        assert call_kwargs["records_count"] == 100

    def test_load_file_records_history_on_failure(self):
        """失敗時にロード履歴を記録する"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "_download_file_from_gcs", return_value=None):
            with patch.object(loader, "_record_load_history") as mock_history:
                result = loader.load_file("BAA260104.csv")

        assert result.status == "failed"
        mock_history.assert_called_once()
        call_kwargs = mock_history.call_args[1]
        assert call_kwargs["status"] == "failed"
        assert call_kwargs["error_message"] is not None

    def test_load_file_no_history_when_disabled(self):
        """record_history=Falseで履歴を記録しない"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "_download_file_from_gcs", return_value="data"):
            with patch.object(loader, "_load_to_bigquery", return_value=100):
                with patch.object(loader, "_record_load_history") as mock_history:
                    with patch("src.automation.data.load_to_bq.JRDBParser") as mock_parser:
                        mock_parser.parse_file.return_value = [{"data": "test"}]

                        loader.load_file("BAA260104.csv", record_history=False)

        mock_history.assert_not_called()


class TestExtractFileDate:
    """extract_file_date 関数のテスト"""

    def test_oz_filename_returns_correct_date(self):
        """OZ ファイル名から日付が正しく抽出される (2000年以降)"""
        assert extract_file_date("OZ241231.csv") == "2024-12-31"

    def test_oz_filename_2016(self):
        assert extract_file_date("OZ160101.csv") == "2016-01-01"

    def test_oz_with_directory_path(self):
        """GCS パスを含む場合も正しく抽出される"""
        assert extract_file_date("OZ/OZ241231.csv") == "2024-12-31"

    def test_baa_filename_returns_correct_date(self):
        assert extract_file_date("BAA260104.csv") == "2026-01-04"

    def test_invalid_filename_returns_none(self):
        assert extract_file_date("invalid.csv") is None
        assert extract_file_date("OZ12345.csv") is None


class TestExpandDataTypes:
    """EXPAND_DATA_TYPESの確認とHJBロード展開処理のテスト"""

    def test_expand_data_types_contains_hjb_hjc(self):
        """EXPAND_DATA_TYPESにHJBとHJCが含まれる"""
        assert "HJB" in EXPAND_DATA_TYPES
        assert "HJC" in EXPAND_DATA_TYPES

    def test_expand_data_types_contains_oz(self):
        """EXPAND_DATA_TYPES に OZ が含まれる"""
        assert "OZ" in EXPAND_DATA_TYPES

    def test_expand_data_types_does_not_contain_sec(self):
        """SECなど通常タイプはEXPAND_DATA_TYPESに含まれない"""
        assert "SEC" not in EXPAND_DATA_TYPES
        assert "BAA" not in EXPAND_DATA_TYPES

    def test_hjb_load_file_expands_payout_rows(self):
        """HJBファイルのload_fileがexpand_hjb_to_payout_rowsを呼び出し展開する"""
        loader = BigQueryLoader(project_id="test-project")

        # parse_hjb_line が返す1レース分のレコード
        hjb_parsed_record = {
            "race_id": "08251301",
            "win_payouts": [(3, 170)],
            "place_payouts": [(3, 110), (9, 110)],
            "wakuren_payouts": [],
            "umaren_payouts": [],
            "wide_payouts": [],
            "umatan_payouts": [],
            "sanrenpuku_payouts": [],
            "sanrentan_payouts": [],
            "created_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:00:00",
        }
        # expand後の期待されるペイアウト行 (win×1 + place×2 = 3行)
        expanded_rows = [
            {"race_id": "08251301", "bet_type": "win", "horse_number_1": 3,
             "horse_number_2": None, "horse_number_3": None, "payout_amount": 170,
             "rank": 1, "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00"},
            {"race_id": "08251301", "bet_type": "place", "horse_number_1": 3,
             "horse_number_2": None, "horse_number_3": None, "payout_amount": 110,
             "rank": 1, "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00"},
            {"race_id": "08251301", "bet_type": "place", "horse_number_1": 9,
             "horse_number_2": None, "horse_number_3": None, "payout_amount": 110,
             "rank": 2, "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00"},
        ]

        with patch.object(loader, "_download_file_from_gcs", return_value="data"):
            with patch.object(loader, "_load_to_bigquery", return_value=3) as mock_load_bq:
                with patch.object(loader, "_record_load_history"):
                    with patch("src.automation.data.load_to_bq.JRDBParser") as mock_parser:
                        mock_parser.parse_file.return_value = [hjb_parsed_record]
                        mock_parser.expand_hjb_to_payout_rows.return_value = expanded_rows

                        result = loader.load_file("HJB260104.csv")

        assert result.status == "success"
        assert result.records_processed == 3
        assert result.table == "raw.payouts"
        # expand_hjb_to_payout_rows が呼ばれたことを確認
        mock_parser.expand_hjb_to_payout_rows.assert_called_once_with(hjb_parsed_record)
        # _load_to_bigquery には展開後のrows が渡された
        mock_load_bq.assert_called_once_with("payouts", expanded_rows, "HJB")

    def test_hjb_load_file_empty_after_expand_is_skipped(self):
        """HJB展開結果が空の場合はskippedになる"""
        loader = BigQueryLoader(project_id="test-project")

        with patch.object(loader, "_download_file_from_gcs", return_value="data"):
            with patch.object(loader, "_record_load_history"):
                with patch("src.automation.data.load_to_bq.JRDBParser") as mock_parser:
                    mock_parser.parse_file.return_value = [{"race_id": "08251301"}]
                    mock_parser.expand_hjb_to_payout_rows.return_value = []

                    result = loader.load_file("HJB260104.csv")

        assert result.status == "skipped"
        assert result.error == "HJB展開結果が空"
