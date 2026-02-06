"""
BigQuery ロードモジュール

GCSにアップロードされたJRDBデータファイルを解析してBigQueryにロードします。
Cloud Run環境での実行を想定しています。

主な機能:
- GCSからのファイル取得
- JRDBデータのパース
- BigQueryへのUPSERT (MERGE文)
- バッチ処理対応
- エラーハンドリングと失敗ファイルの記録
- ロード履歴の記録と重複スキップ機能
"""

import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError

# 同一パッケージからパーサーをインポート
# Cloud Functions版のパーサーを再利用
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "cloud_functions" / "gcs_to_bq"))
try:
    from parser import JRDBParser
except ImportError:
    # フォールバック: cloud_functions ディレクトリがない場合
    JRDBParser = None

# ログ設定 (Cloud Run対応)
logger = logging.getLogger(__name__)

# デフォルト値
DEFAULT_PROJECT_ID = "keiba-prediction-452203"
DEFAULT_DATASET_ID = "raw"
DEFAULT_BUCKET_NAME = "keiba-prediction-452203-keiba-raw-data"

# データタイプとテーブルのマッピング
TABLE_MAPPING = {
    "BAA": "race_info",
    "BAB": "race_info",
    "BAC": "race_info",
    "KYF": "horse_results",
    "KYG": "horse_results",
    "KYH": "horse_results",
    "SEC": "race_results",
    "UKC": "horse_master",
    "KKA": "horse_extended",
    "KAA": "venue_info",
}

# テーブルごとの一意キー (MERGE文で使用)
TABLE_UNIQUE_KEYS = {
    "race_info": ["race_id"],
    "horse_results": ["race_id", "horse_number"],
    "race_results": ["race_id", "horse_number"],
    "horse_master": ["horse_id"],
    "horse_extended": ["race_id", "horse_number"],
    "venue_info": ["venue_id"],
}

# ロード履歴テーブル名
LOAD_HISTORY_TABLE = "load_history"


@dataclass
class LoadResult:
    """ファイルロード結果"""

    file_name: str
    status: str  # "success", "skipped", "failed"
    records_processed: int = 0
    table: Optional[str] = None
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class BatchLoadResult:
    """バッチロード結果"""

    total_files: int = 0
    success_count: int = 0
    skipped_count: int = 0
    failed_count: int = 0
    total_records: int = 0
    results: List[LoadResult] = field(default_factory=list)
    failed_files: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0


def extract_data_type(filename: str) -> Optional[str]:
    """
    ファイル名からデータタイプを抽出

    Args:
        filename: ファイル名 (例: BAA260104.csv, Baa/BAA260104.csv)

    Returns:
        データタイプ (BAA, KYF など) or None
    """
    basename = os.path.basename(filename)
    match = re.match(r"^([A-Z]{2,3})\d{6}\.csv$", basename, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def get_table_name(data_type: str) -> Optional[str]:
    """
    データタイプからBigQueryテーブル名を取得

    Args:
        data_type: データタイプ (BAA, KYF など)

    Returns:
        テーブル名 or None
    """
    return TABLE_MAPPING.get(data_type.upper())


class BigQueryLoader:
    """BigQueryへのデータロードを管理するクラス"""

    def __init__(
        self,
        project_id: str,
        dataset_id: str = DEFAULT_DATASET_ID,
        bucket_name: Optional[str] = None,
    ):
        """
        初期化

        Args:
            project_id: GCPプロジェクトID
            dataset_id: BigQueryデータセットID
            bucket_name: GCSバケット名 (オプション)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name or f"{project_id}-keiba-raw-data"

        self._bq_client: Optional[bigquery.Client] = None
        self._storage_client: Optional[storage.Client] = None

    @property
    def bq_client(self) -> bigquery.Client:
        """BigQueryクライアント (遅延初期化)"""
        if self._bq_client is None:
            self._bq_client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQueryクライアントを初期化しました (プロジェクト: {self.project_id})")
        return self._bq_client

    @property
    def storage_client(self) -> storage.Client:
        """Cloud Storageクライアント (遅延初期化)"""
        if self._storage_client is None:
            self._storage_client = storage.Client(project=self.project_id)
            logger.info("Cloud Storageクライアントを初期化しました")
        return self._storage_client

    def _get_loaded_files(self) -> Set[str]:
        """
        ロード履歴テーブルから成功したファイル名のセットを取得

        Returns:
            ロード済みファイル名のセット
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{LOAD_HISTORY_TABLE}"

        query = f"""
        SELECT DISTINCT file_name
        FROM `{table_ref}`
        WHERE status = 'success'
        """

        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            loaded_files = {row.file_name for row in results}
            logger.info(f"ロード履歴から {len(loaded_files)} 件の成功ファイルを取得しました")
            return loaded_files
        except Exception as e:
            # テーブルが存在しない場合は空のセットを返す
            if "Not found" in str(e):
                logger.warning(
                    f"ロード履歴テーブルが見つかりません: {table_ref}. "
                    "スキップ機能は無効になります。"
                )
                return set()
            logger.error(f"ロード履歴の取得に失敗しました: {e}")
            raise

    def _record_load_history(
        self,
        file_name: str,
        status: str,
        records_count: int = 0,
        table_name: Optional[str] = None,
        data_type: Optional[str] = None,
        error_message: Optional[str] = None,
        duration_seconds: float = 0.0,
        file_size_bytes: Optional[int] = None,
    ) -> None:
        """
        ロード履歴をBigQueryに記録

        Args:
            file_name: ファイル名
            status: ステータス (success/failed)
            records_count: ロードしたレコード数
            table_name: ロード先テーブル名
            data_type: データタイプ
            error_message: エラーメッセージ
            duration_seconds: 処理時間
            file_size_bytes: ファイルサイズ
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{LOAD_HISTORY_TABLE}"

        row = {
            "file_name": file_name,
            "loaded_at": datetime.utcnow().isoformat(),
            "records_count": records_count,
            "table_name": table_name,
            "data_type": data_type,
            "status": status,
            "error_message": error_message,
            "duration_seconds": duration_seconds,
            "file_size_bytes": file_size_bytes,
        }

        try:
            errors = self.bq_client.insert_rows_json(table_ref, [row])
            if errors:
                logger.warning(f"ロード履歴の記録に失敗しました: {errors}")
            else:
                logger.debug(f"ロード履歴を記録しました: {file_name} ({status})")
        except Exception as e:
            # ロード履歴の記録失敗はワーニングに留める
            logger.warning(f"ロード履歴の記録に失敗しました: {e}")

    def _is_file_already_loaded(
        self, file_name: str, loaded_files: Optional[Set[str]] = None
    ) -> bool:
        """
        ファイルが既にロード済みかどうかを確認

        Args:
            file_name: ファイル名
            loaded_files: ロード済みファイルのセット (キャッシュ用)

        Returns:
            ロード済みの場合True
        """
        if loaded_files is None:
            loaded_files = self._get_loaded_files()
        return file_name in loaded_files

    def _download_file_from_gcs(self, blob_name: str) -> Optional[str]:
        """
        GCSからファイルをダウンロードして文字列として返す

        Args:
            blob_name: GCS上のファイルパス

        Returns:
            ファイル内容 (文字列) or None
        """
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)

            if not blob.exists():
                logger.error(f"ファイルが存在しません: gs://{self.bucket_name}/{blob_name}")
                return None

            file_bytes = blob.download_as_bytes()

            # UTF-8でデコードを試行
            try:
                content = file_bytes.decode("utf-8")
                logger.debug(f"ファイル {blob_name} をUTF-8でデコードしました")
            except UnicodeDecodeError:
                # UTF-8で失敗した場合はCP932でデコード
                content = file_bytes.decode("cp932", errors="replace")
                logger.debug(f"ファイル {blob_name} をCP932でデコードしました")

                if "�" in content:
                    logger.warning(
                        f"CP932デコードエラーが検出されました: {blob_name}. "
                        "一部の文字が '�' に置換されています。"
                    )

            return content

        except Exception as e:
            logger.error(f"GCSからのダウンロードに失敗しました: {blob_name} - {e}")
            return None

    def _load_to_bigquery(
        self,
        table_id: str,
        rows: List[Dict],
        data_type: str,
    ) -> int:
        """
        BigQueryにデータをロード (MERGE文で重複を防止)

        既存レコードがあればUPDATE、なければINSERTを実行します。

        Args:
            table_id: テーブルID
            rows: ロードするデータのリスト
            data_type: データタイプ

        Returns:
            ロードされた行数

        Raises:
            GoogleCloudError: BigQueryエラー
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        temp_table_ref = (
            f"{self.project_id}.{self.dataset_id}._temp_{table_id}_"
            f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        )

        try:
            # ターゲットテーブルのスキーマを取得
            target_table = self.bq_client.get_table(table_ref)
            schema = target_table.schema

            # 一時テーブルを作成
            temp_table = bigquery.Table(temp_table_ref, schema=schema)
            temp_table = self.bq_client.create_table(temp_table)
            logger.debug(f"一時テーブルを作成しました: {temp_table_ref}")

            try:
                # 一時テーブルにデータをStreaming Insert
                errors = self.bq_client.insert_rows_json(temp_table, rows)
                if errors:
                    error_msgs = [str(e) for e in errors[:5]]
                    logger.error(f"BigQuery insert エラー: {error_msgs}")
                    raise GoogleCloudError(f"Insert errors: {error_msgs}")

                logger.debug(f"一時テーブルに {len(rows)} 行を挿入しました")

                # Streaming Insertのバッファ反映を待つ
                time.sleep(5)

                # MERGE文を構築
                unique_keys = TABLE_UNIQUE_KEYS.get(table_id, ["race_id"])
                columns = [field.name for field in schema]

                # JOIN条件
                join_conditions = " AND ".join(
                    [f"T.{key} = S.{key}" for key in unique_keys]
                )

                # UPDATE SET句 (一意キー以外のカラムを更新)
                update_columns = [col for col in columns if col not in unique_keys]
                update_set = ", ".join([f"T.{col} = S.{col}" for col in update_columns])

                # INSERT句
                insert_columns = ", ".join(columns)
                insert_values = ", ".join([f"S.{col}" for col in columns])

                merge_query = f"""
                MERGE `{table_ref}` T
                USING `{temp_table_ref}` S
                ON {join_conditions}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
                """

                logger.debug("MERGEクエリを実行中...")
                query_job = self.bq_client.query(merge_query)
                query_job.result()  # 完了を待つ

                logger.info(f"MERGE完了: {len(rows)} 行を {table_ref} に処理しました")

            finally:
                # 一時テーブルを削除
                self.bq_client.delete_table(temp_table_ref, not_found_ok=True)
                logger.debug(f"一時テーブルを削除しました: {temp_table_ref}")

            return len(rows)

        except GoogleCloudError as e:
            logger.error(f"BigQuery ロードエラー: {e}")
            raise

    def load_file(
        self, blob_name: str, record_history: bool = True
    ) -> LoadResult:
        """
        単一ファイルをGCSからBigQueryにロード

        Args:
            blob_name: GCS上のファイルパス
            record_history: ロード履歴を記録するか (デフォルト: True)

        Returns:
            LoadResult: ロード結果
        """
        start_time = time.time()
        result = LoadResult(file_name=blob_name, status="failed")
        data_type = None
        table_name = None

        try:
            # ファイル名からデータタイプを抽出
            data_type = extract_data_type(blob_name)
            if not data_type:
                result.error = "無効なファイル名形式"
                logger.warning(f"ファイル名からデータタイプを抽出できません: {blob_name}")
                return result

            # テーブル名を取得
            table_name = get_table_name(data_type)
            if not table_name:
                result.status = "skipped"
                result.error = f"未サポートのデータタイプ: {data_type}"
                logger.info(f"未サポートのデータタイプをスキップ: {data_type}")
                return result

            logger.info(
                f"ファイルを処理中: {blob_name} (タイプ: {data_type}, テーブル: {table_name})"
            )

            # GCSからファイルをダウンロード
            file_content = self._download_file_from_gcs(blob_name)
            if file_content is None:
                result.error = "GCSからのダウンロード失敗"
                return result

            # パーサーが利用可能か確認
            if JRDBParser is None:
                result.error = "JRDBParserが利用できません"
                logger.error("JRDBParserをインポートできませんでした")
                return result

            # データを解析
            parsed_data = JRDBParser.parse_file(file_content, data_type)

            if not parsed_data:
                result.status = "skipped"
                result.error = "パース結果が空"
                logger.warning(f"ファイルからデータをパースできませんでした: {blob_name}")
                return result

            # BigQueryにロード
            records_loaded = self._load_to_bigquery(table_name, parsed_data, data_type)

            result.status = "success"
            result.records_processed = records_loaded
            result.table = f"{self.dataset_id}.{table_name}"

            logger.info(
                f"処理完了: {blob_name} - {records_loaded} レコードを {table_name} にロード"
            )

        except Exception as e:
            logger.error(f"ファイル処理エラー: {blob_name} - {e}")
            result.error = str(e)

        finally:
            result.duration_seconds = time.time() - start_time

            # ロード履歴を記録
            if record_history:
                self._record_load_history(
                    file_name=blob_name,
                    status=result.status,
                    records_count=result.records_processed,
                    table_name=table_name,
                    data_type=data_type,
                    error_message=result.error,
                    duration_seconds=result.duration_seconds,
                )

        return result

    def load_files_batch(
        self,
        blob_names: List[str],
        continue_on_error: bool = True,
        skip_loaded: bool = False,
        record_history: bool = True,
    ) -> BatchLoadResult:
        """
        複数ファイルをバッチでロード

        Args:
            blob_names: GCS上のファイルパスのリスト
            continue_on_error: エラー時に処理を継続するか
            skip_loaded: 既にロード済みのファイルをスキップするか
            record_history: ロード履歴を記録するか

        Returns:
            BatchLoadResult: バッチロード結果
        """
        start_time = time.time()
        batch_result = BatchLoadResult(total_files=len(blob_names))

        logger.info(f"バッチロード開始: {len(blob_names)} ファイル")

        # 重複スキップが有効な場合、ロード済みファイルを取得
        loaded_files: Set[str] = set()
        if skip_loaded:
            loaded_files = self._get_loaded_files()
            logger.info(f"重複スキップ機能: 有効 ({len(loaded_files)} 件のロード済みファイル)")

        for i, blob_name in enumerate(blob_names, 1):
            # 重複スキップ判定
            if skip_loaded and blob_name in loaded_files:
                logger.info(f"スキップ ({i}/{len(blob_names)}): {blob_name} (ロード済み)")
                result = LoadResult(
                    file_name=blob_name,
                    status="skipped",
                    error="既にロード済み",
                )
                batch_result.results.append(result)
                batch_result.skipped_count += 1
                continue

            logger.info(f"処理中 ({i}/{len(blob_names)}): {blob_name}")

            result = self.load_file(blob_name, record_history=record_history)
            batch_result.results.append(result)

            if result.status == "success":
                batch_result.success_count += 1
                batch_result.total_records += result.records_processed
            elif result.status == "skipped":
                batch_result.skipped_count += 1
            else:
                batch_result.failed_count += 1
                batch_result.failed_files.append(blob_name)

                if not continue_on_error:
                    logger.error(f"エラーにより処理を中断: {blob_name}")
                    break

        batch_result.duration_seconds = time.time() - start_time

        logger.info(
            f"バッチロード完了: "
            f"成功={batch_result.success_count}, "
            f"スキップ={batch_result.skipped_count}, "
            f"失敗={batch_result.failed_count}, "
            f"合計レコード={batch_result.total_records}, "
            f"所要時間={batch_result.duration_seconds:.1f}秒"
        )

        return batch_result

    def list_csv_files(
        self,
        prefix: Optional[str] = None,
        data_types: Optional[List[str]] = None,
    ) -> List[str]:
        """
        GCSバケット内のCSVファイルを一覧取得

        Args:
            prefix: GCS上のプレフィックス (オプション)
            data_types: フィルタするデータタイプのリスト (オプション)

        Returns:
            ファイルパスのリスト
        """
        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        csv_files = []
        for blob in blobs:
            if not blob.name.lower().endswith(".csv"):
                continue

            # データタイプフィルタ
            if data_types:
                data_type = extract_data_type(blob.name)
                if data_type and data_type.upper() not in [
                    dt.upper() for dt in data_types
                ]:
                    continue

            csv_files.append(blob.name)

        logger.info(f"GCSから {len(csv_files)} 個のCSVファイルを検出しました")
        return csv_files


def create_loader_from_env() -> Optional[BigQueryLoader]:
    """
    環境変数からBigQueryLoaderを作成

    必要な環境変数:
    - GCP_PROJECT_ID: プロジェクトID (必須)
    - BQ_DATASET_RAW: BigQueryデータセットID (オプション、デフォルト: raw)
    - GCS_BUCKET_NAME: GCSバケット名 (オプション)

    Returns:
        BigQueryLoader or None (エラー時)
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error("GCP_PROJECT_ID環境変数が設定されていません")
        return None

    dataset_id = os.environ.get("BQ_DATASET_RAW", DEFAULT_DATASET_ID)
    bucket_name = os.environ.get("GCS_BUCKET_NAME")

    return BigQueryLoader(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name=bucket_name,
    )


def main():
    """CLI エントリーポイント"""
    import argparse

    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="GCSからBigQueryにJRDBデータをロードします"
    )
    parser.add_argument(
        "--project-id",
        help="GCPプロジェクトID (環境変数GCP_PROJECT_IDからも取得可能)",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_ID,
        help=f"BigQueryデータセットID (デフォルト: {DEFAULT_DATASET_ID})",
    )
    parser.add_argument(
        "--bucket",
        help="GCSバケット名 (デフォルト: {project_id}-keiba-raw-data)",
    )
    parser.add_argument(
        "--prefix",
        help="GCS上のプレフィックス (例: csv/)",
    )
    parser.add_argument(
        "--data-types",
        nargs="+",
        help="ロードするデータタイプ (例: BAA KYF SEC)",
    )
    parser.add_argument(
        "--file",
        dest="files",
        nargs="+",
        help="ロードする特定のファイル (GCS上のパス)",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="エラー時に処理を中断する",
    )
    parser.add_argument(
        "--skip-loaded",
        action="store_true",
        help="既にロード済みのファイルをスキップする (重複スキップ機能)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="ロード履歴を記録しない",
    )

    args = parser.parse_args()

    # プロジェクトIDの決定
    project_id = args.project_id or os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        logger.error(
            "プロジェクトIDが指定されていません。"
            "--project-id引数またはGCP_PROJECT_ID環境変数を設定してください。"
        )
        sys.exit(1)

    # ローダーを作成
    loader = BigQueryLoader(
        project_id=project_id,
        dataset_id=args.dataset,
        bucket_name=args.bucket,
    )

    # ロード対象ファイルを決定
    if args.files:
        files_to_load = args.files
    else:
        files_to_load = loader.list_csv_files(
            prefix=args.prefix,
            data_types=args.data_types,
        )

    if not files_to_load:
        logger.warning("ロードするファイルが見つかりませんでした")
        sys.exit(0)

    # バッチロード実行
    result = loader.load_files_batch(
        files_to_load,
        continue_on_error=not args.stop_on_error,
        skip_loaded=args.skip_loaded,
        record_history=not args.no_history,
    )

    # 結果出力
    print("\n" + "=" * 60)
    print("ロード結果サマリー")
    print("=" * 60)
    print(f"合計ファイル数: {result.total_files}")
    print(f"成功: {result.success_count}")
    print(f"スキップ: {result.skipped_count}")
    print(f"失敗: {result.failed_count}")
    print(f"合計レコード数: {result.total_records}")
    print(f"所要時間: {result.duration_seconds:.1f}秒")

    if result.failed_files:
        print("\n失敗ファイル:")
        for f in result.failed_files:
            print(f"  - {f}")

    sys.exit(0 if result.failed_count == 0 else 1)


if __name__ == "__main__":
    main()
