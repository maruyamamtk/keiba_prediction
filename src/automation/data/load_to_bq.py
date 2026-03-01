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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from google.cloud import bigquery, storage
from google.cloud.exceptions import GoogleCloudError

from src.automation.data.jrdb_parser import JRDBParser

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
    "HJB": "payouts",
    "HJC": "payouts",
    "OZ": "odds",
}

# テーブルごとの一意キー (MERGE文で使用)
TABLE_UNIQUE_KEYS = {
    "race_info": ["race_id"],
    "horse_results": ["race_id", "horse_number"],
    "race_results": ["race_id", "horse_number"],
    "horse_master": ["horse_id"],
    "horse_extended": ["race_id", "horse_number"],
    "venue_info": ["venue_id"],
    "payouts": ["race_id", "bet_type", "rank"],
    "odds": ["race_id", "horse_number", "odds_type"],
}

# パース後に行展開が必要なデータタイプ
EXPAND_DATA_TYPES = {"HJB", "HJC", "OZ"}

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

    def merge(self, other: "BatchLoadResult") -> "BatchLoadResult":
        """別のバッチ結果をマージして新しい BatchLoadResult を返す"""
        return BatchLoadResult(
            total_files=self.total_files + other.total_files,
            success_count=self.success_count + other.success_count,
            skipped_count=self.skipped_count + other.skipped_count,
            failed_count=self.failed_count + other.failed_count,
            total_records=self.total_records + other.total_records,
            results=self.results + other.results,
            failed_files=self.failed_files + other.failed_files,
            duration_seconds=self.duration_seconds + other.duration_seconds,
        )


def _ensure_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """timezone-naive datetime を UTC-aware に変換する。None はそのまま返す。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


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


def extract_file_date(filename: str) -> Optional[str]:
    """
    ファイル名から日付文字列 (YYYY-MM-DD) を抽出する

    ファイル名の形式: <TYPE><YYMMDD>.csv (例: OZ241231.csv → 2024-12-31)

    Args:
        filename: ファイル名またはGCSパス

    Returns:
        ISO形式の日付文字列 (YYYY-MM-DD) or None
    """
    basename = os.path.basename(filename)
    match = re.match(r"^[A-Z]{2,3}(\d{6})\.csv$", basename, re.IGNORECASE)
    if not match:
        return None
    yymmdd = match.group(1)
    try:
        year = int(yymmdd[0:2])
        month = int(yymmdd[2:4])
        day = int(yymmdd[4:6])
        year += 2000 if year <= 50 else 1900
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


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
        # GCS blob の更新タイムスタンプキャッシュ (list_csv_files() が設定する)
        self._blob_updated_cache: Dict[str, Optional[datetime]] = {}

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

    def _get_loaded_files(self) -> Dict[str, datetime]:
        """
        ロード履歴テーブルから成功したファイルの最終ロード日時を取得

        GCS更新タイムスタンプとの比較に使用するため、
        ファイルごとに最新の loaded_at を返す。

        Returns:
            {file_name: loaded_at (UTC-aware)} のDict
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{LOAD_HISTORY_TABLE}"

        query = f"""
        SELECT file_name, MAX(loaded_at) AS loaded_at
        FROM `{table_ref}`
        WHERE status = 'success'
        GROUP BY file_name
        """

        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            loaded_files: Dict[str, datetime] = {}
            for row in results:
                loaded_at = _ensure_utc_aware(row.loaded_at)
                if loaded_at is None:
                    loaded_at = datetime.min.replace(tzinfo=timezone.utc)
                loaded_files[row.file_name] = loaded_at
            logger.info(f"ロード履歴から {len(loaded_files)} 件の成功ファイルを取得しました")
            return loaded_files
        except Exception as e:
            # テーブルが存在しない場合は空のDictを返す
            if "Not found" in str(e):
                logger.warning(
                    f"ロード履歴テーブルが見つかりません: {table_ref}. "
                    "スキップ機能は無効になります。"
                )
                return {}
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

    @staticmethod
    def _append_skipped(batch_result: "BatchLoadResult", blob_name: str) -> None:
        """スキップ結果を batch_result に追記する"""
        batch_result.results.append(
            LoadResult(file_name=blob_name, status="skipped", error="既にロード済み")
        )
        batch_result.skipped_count += 1

    def _is_file_already_loaded(
        self, file_name: str, loaded_files: Optional[Dict[str, datetime]] = None
    ) -> bool:
        """
        ファイルが既にロード済みかどうかを確認

        Args:
            file_name: ファイル名
            loaded_files: {file_name: loaded_at} のDict (キャッシュ用)

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

        from google.api_core.exceptions import NotFound

        try:
            # ターゲットテーブルのスキーマを取得
            try:
                target_table = self.bq_client.get_table(table_ref)
            except NotFound:
                raise GoogleCloudError(
                    f"テーブルが存在しません: {table_ref}。"
                    f"scripts/setup_bigquery.sh を実行してテーブルを作成してください。"
                )
            schema = target_table.schema

            # 一時テーブルを作成（1時間後に自動削除される有効期限を設定）
            temp_table = bigquery.Table(temp_table_ref, schema=schema)
            temp_table.expires = datetime.utcnow() + timedelta(hours=1)
            temp_table = self.bq_client.create_table(temp_table)
            logger.debug(f"一時テーブルを作成しました: {temp_table_ref}")

            try:
                # 一時テーブルにデータをStreaming Insert
                errors = self.bq_client.insert_rows_json(temp_table, rows)
                if errors:
                    error_msgs = [str(e) for e in errors[:5]]
                    logger.error(
                        f"BigQuery Streaming Insert エラー (テーブル: {table_id}, "
                        f"データタイプ: {data_type}): {error_msgs}"
                    )
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
            logger.error(
                f"BigQuery ロードエラー (テーブル: {table_id}, データタイプ: {data_type}): {e}"
            )
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

            # HJB/HJC/OZ は1レコードを複数行に展開する
            if data_type.upper() in EXPAND_DATA_TYPES:
                expanded: List[Dict] = []
                if data_type.upper() in {"HJB", "HJC"}:
                    for record in parsed_data:
                        expanded.extend(JRDBParser.expand_hjb_to_payout_rows(record))
                    empty_label = "HJB"
                elif data_type.upper() == "OZ":
                    file_date = extract_file_date(blob_name)
                    for record in parsed_data:
                        expanded.extend(JRDBParser.expand_oz_to_odds_rows(record, file_date))
                    empty_label = "OZ"
                else:
                    empty_label = data_type.upper()
                parsed_data = expanded
                if not parsed_data:
                    result.status = "skipped"
                    result.error = f"{empty_label}展開結果が空"
                    logger.warning(f"{empty_label}データの展開結果が空でした: {blob_name}")
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
            logger.error(
                f"ファイル処理エラー: {blob_name} "
                f"(データタイプ: {data_type}, テーブル: {table_name}) - {e}",
                exc_info=True,
            )
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

        # 重複スキップが有効な場合、ロード済みファイルと最終ロード日時を取得
        loaded_files: Dict[str, datetime] = {}
        if skip_loaded:
            loaded_files = self._get_loaded_files()
            logger.info(f"重複スキップ機能: 有効 ({len(loaded_files)} 件のロード済みファイル)")

        total = len(blob_names)
        for i, blob_name in enumerate(blob_names, 1):
            # 重複スキップ判定: ロード済み かつ GCS更新なし の場合にスキップ
            if skip_loaded and blob_name in loaded_files:
                gcs_updated = self._blob_updated_cache.get(blob_name)
                if gcs_updated is not None:
                    # GCS更新タイムスタンプと最終ロード日時を比較 (両者はUTC-aware)
                    loaded_at = loaded_files[blob_name]
                    if gcs_updated <= loaded_at:
                        # GCSが古いまま → スキップ
                        logger.info(
                            f"スキップ ({i}/{total}): {blob_name} (ロード済み・GCS更新なし)"
                        )
                        self._append_skipped(batch_result, blob_name)
                        continue
                    else:
                        # GCSが新しい → 再ロード
                        logger.info(
                            f"GCS更新検出 ({i}/{total}): {blob_name} を再ロードします "
                            f"(GCS更新: {gcs_updated}, 最終ロード: {loaded_at})"
                        )
                else:
                    # キャッシュなし（list_csv_files()を経由しない呼び出し）→ 保守的にスキップ
                    logger.info(f"スキップ ({i}/{total}): {blob_name} (ロード済み)")
                    self._append_skipped(batch_result, blob_name)
                    continue

            logger.info(f"処理中 ({i}/{total}): {blob_name}")

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
        date_filter: Optional[str] = None,
    ) -> List[str]:
        """
        GCSバケット内のCSVファイルを一覧取得

        Args:
            prefix: GCS上のプレフィックス (オプション)
            data_types: フィルタするデータタイプのリスト (オプション)
            date_filter: ファイル名の日付文字列 yymmdd 形式 (例: "260301")

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

            # 日付フィルタ（ファイル名に yymmdd が含まれるか）
            if date_filter and date_filter not in blob.name:
                continue

            csv_files.append(blob.name)
            # GCS更新タイムスタンプをキャッシュ (load_files_batch() でのスキップ判定に使用)
            self._blob_updated_cache[blob.name] = _ensure_utc_aware(blob.updated)

        logger.info(f"GCSから {len(csv_files)} 個のCSVファイルを検出しました")
        return csv_files

    def check_tables_exist(self) -> Dict[str, bool]:
        """
        TABLE_MAPPINGで定義されたBigQueryテーブルの存在を確認

        Returns:
            テーブル名と存在可否のDict
        """
        from google.api_core.exceptions import NotFound

        target_tables = set(TABLE_MAPPING.values())
        results = {}

        for table_name in sorted(target_tables):
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
            try:
                self.bq_client.get_table(table_ref)
                results[table_name] = True
            except NotFound:
                results[table_name] = False
                logger.warning(
                    f"テーブルが存在しません: {table_ref}。"
                    f"scripts/setup_bigquery.sh を実行してテーブルを作成してください。"
                )

        missing = [t for t, exists in results.items() if not exists]
        if missing:
            logger.error(
                f"以下のテーブルが未作成です: {missing}。"
                f"scripts/setup_bigquery.sh を実行してください。"
            )
        else:
            logger.info("全テーブルの存在を確認しました")

        return results


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
    bucket_suffix = os.environ.get("GCS_BUCKET_RAW", "keiba-raw-data")
    bucket_name = f"{project_id}-{bucket_suffix}"

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
        "--date",
        help="ロード対象日 YYYY-MM-DD 形式 (例: 2026-03-01)。指定するとその日付のファイルのみ対象",
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

    # .envファイルから環境変数を読み込み
    from dotenv import load_dotenv

    load_dotenv()

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

    # --date を yymmdd 形式に変換
    date_filter = None
    if args.date:
        import datetime as _dt
        d = _dt.date.fromisoformat(args.date)
        date_filter = d.strftime("%y%m%d")  # "260301"

    # ロード対象ファイルを決定
    if args.files:
        files_to_load = args.files
    else:
        files_to_load = loader.list_csv_files(
            prefix=args.prefix,
            data_types=args.data_types,
            date_filter=date_filter,
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
