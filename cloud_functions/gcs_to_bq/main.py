"""
GCS to BigQuery Cloud Function

GCSにファイルがアップロードされた際にトリガーされ、
JRDBデータを解析してBigQueryにロードします。

トリガー: GCSオブジェクト作成 (google.storage.object.finalize)
対象バケット: ${PROJECT_ID}-keiba-raw-data
"""

import os
import logging
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from parser import JRDBParser

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'keiba-prediction-452203')
BQ_DATASET_RAW = os.environ.get('BQ_DATASET_RAW', 'raw')

# データタイプとテーブルのマッピング
# KAA/KABは開催データ（日単位）でレース単位のテーブルと構造が異なるため除外
TABLE_MAPPING = {
    'BAA': 'race_info',
    'BAB': 'race_info',
    'BAC': 'race_info',
    'KYF': 'horse_results',
    'KYG': 'horse_results',
    'KYH': 'horse_results',
    'SEC': 'race_results',  # 成績データは専用テーブルへ
    'UKC': 'horse_master',  # 馬基本データ
    'KKA': 'horse_extended',  # 競走馬拡張データ
    'KAA': 'venue_info',  # 開催データ
}

# テーブルごとの一意キー (MERGE文で使用)
TABLE_UNIQUE_KEYS = {
    'race_info': ['race_id'],
    'horse_results': ['race_id', 'horse_number'],
    'race_results': ['race_id', 'horse_number'],
    'horse_master': ['horse_id'],
    'horse_extended': ['race_id', 'horse_number'],
    'venue_info': ['venue_id'],
}


def extract_data_type(filename: str) -> Optional[str]:
    """
    ファイル名からデータタイプを抽出

    Args:
        filename: ファイル名 (例: BAA260104.csv, Baa/BAA260104.csv)

    Returns:
        データタイプ (BAA, KYF など) or None
    """
    # ディレクトリパスが含まれる場合はベース名のみを取得
    basename = os.path.basename(filename)
    match = re.match(r'^([A-Z]{2,3})\d{6}\.csv$', basename, re.IGNORECASE)
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


def load_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: List[Dict],
    data_type: str
) -> int:
    """
    BigQueryにデータをロード (MERGE文で重複を防止)

    既存レコードがあればUPDATE、なければINSERTを実行します。
    一意キーはTABLE_UNIQUE_KEYSで定義されています。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID
        table_id: テーブルID
        rows: ロードするデータのリスト
        data_type: データタイプ

    Returns:
        ロードされた行数

    Raises:
        GoogleCloudError: BigQueryエラー
    """
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    temp_table_ref = f"{project_id}.{dataset_id}._temp_{table_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    try:
        client = bigquery.Client(project=project_id)

        # ターゲットテーブルのスキーマを取得
        target_table = client.get_table(table_ref)
        schema = target_table.schema

        # 一時テーブルを作成
        temp_table = bigquery.Table(temp_table_ref, schema=schema)
        temp_table = client.create_table(temp_table)
        logger.info(f"Created temp table: {temp_table_ref}")

        try:
            # 一時テーブルにデータをStreaming Insert
            errors = client.insert_rows_json(temp_table, rows)
            if errors:
                error_msgs = [str(e) for e in errors[:5]]
                logger.error(f"BigQuery insert errors: {error_msgs}")
                raise GoogleCloudError(f"Insert errors: {error_msgs}")

            logger.info(f"Inserted {len(rows)} rows to temp table")

            # Streaming Insertのバッファ反映を待つ
            time.sleep(5)

            # MERGE文を構築
            unique_keys = TABLE_UNIQUE_KEYS.get(table_id, ['race_id'])
            columns = [field.name for field in schema]

            # JOIN条件
            join_conditions = ' AND '.join([f"T.{key} = S.{key}" for key in unique_keys])

            # UPDATE SET句 (一意キー以外のカラムを更新)
            update_columns = [col for col in columns if col not in unique_keys]
            update_set = ', '.join([f"T.{col} = S.{col}" for col in update_columns])

            # INSERT句
            insert_columns = ', '.join(columns)
            insert_values = ', '.join([f"S.{col}" for col in columns])

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

            logger.info(f"Executing MERGE query...")
            query_job = client.query(merge_query)
            query_job.result()  # 完了を待つ

            logger.info(f"MERGE completed: {len(rows)} rows processed to {table_ref}")

        finally:
            # 一時テーブルを削除
            client.delete_table(temp_table_ref, not_found_ok=True)
            logger.info(f"Deleted temp table: {temp_table_ref}")

        return len(rows)

    except GoogleCloudError as e:
        logger.error(f"BigQuery load error: {e}", exc_info=True)
        raise


def process_file(bucket_name: str, file_name: str) -> Dict[str, any]:
    """
    GCSファイルを処理してBigQueryにロード

    Args:
        bucket_name: バケット名
        file_name: ファイル名

    Returns:
        処理結果の辞書
    """
    result = {
        'status': 'failed',
        'bucket': bucket_name,
        'file': file_name,
        'records_processed': 0,
        'error': None
    }

    try:
        # ファイル名からデータタイプを抽出
        data_type = extract_data_type(file_name)
        if not data_type:
            logger.warning(f"Cannot extract data type from filename: {file_name}")
            result['error'] = 'Invalid filename format'
            return result

        # テーブル名を取得
        table_name = get_table_name(data_type)
        if not table_name:
            logger.warning(f"No table mapping for data type: {data_type}")
            result['error'] = f'Unsupported data type: {data_type}'
            return result

        logger.info(f"Processing file: {file_name} (type: {data_type}, table: {table_name})")

        # GCSからファイルを取得
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # ファイル内容を取得 (UTF-8優先、失敗時はCP932にフォールバック)
        try:
            file_bytes = blob.download_as_bytes()

            # UTF-8でデコードを試行
            try:
                file_content = file_bytes.decode('utf-8')
                logger.info(f"File {file_name} decoded as UTF-8")
            except UnicodeDecodeError:
                # UTF-8で失敗した場合はCP932でデコード
                file_content = file_bytes.decode('cp932', errors='replace')
                logger.info(f"File {file_name} decoded as CP932")

                if '�' in file_content:
                    logger.warning(
                        f"CP932 decode errors detected in {file_name}. "
                        f"Some characters may be replaced with '�'."
                    )
        except Exception as e:
            logger.error(f"Failed to decode file {file_name}: {e}", exc_info=True)
            result['error'] = f'File decode error: {str(e)}'
            return result

        # データを解析
        parsed_data = JRDBParser.parse_file(file_content, data_type)

        if not parsed_data:
            logger.warning(f"No data parsed from file: {file_name}")
            result['error'] = 'No data parsed'
            return result

        # BigQueryにロード
        records_loaded = load_to_bigquery(
            PROJECT_ID,
            BQ_DATASET_RAW,
            table_name,
            parsed_data,
            data_type
        )

        result['status'] = 'success'
        result['records_processed'] = records_loaded
        result['table'] = f"{BQ_DATASET_RAW}.{table_name}"

        logger.info(
            f"Successfully processed {file_name}: "
            f"{records_loaded} records loaded to {table_name}"
        )

    except Exception as e:
        logger.error(f"Error processing file {file_name}: {e}", exc_info=True)
        result['error'] = str(e)

    return result


def gcs_to_bq(event, context):
    """
    Cloud Function エントリーポイント (GCSトリガー)

    Args:
        event: GCSイベントデータ
        context: イベントコンテキスト
    """
    bucket_name = event['bucket']
    file_name = event['name']

    logger.info(
        f"Cloud Function triggered: "
        f"bucket={bucket_name}, file={file_name}"
    )

    # ファイル名チェック (CSVファイルのみ処理)
    if not file_name.lower().endswith('.csv'):
        logger.info(f"Skipping non-CSV file: {file_name}")
        return

    # ファイルを処理
    result = process_file(bucket_name, file_name)

    if result['status'] == 'success':
        logger.info(f"Processing completed successfully: {result}")
    else:
        logger.error(f"Processing failed: {result}")

    return result


def http_trigger(request):
    """
    Cloud Function エントリーポイント (HTTP トリガー、テスト用)

    Args:
        request: HTTPリクエスト

    Returns:
        HTTPレスポンス
    """
    request_json = request.get_json(silent=True)

    if not request_json or 'bucket' not in request_json or 'file' not in request_json:
        return {'error': 'Missing required parameters: bucket, file'}, 400

    bucket_name = request_json['bucket']
    file_name = request_json['file']

    result = process_file(bucket_name, file_name)

    status_code = 200 if result['status'] == 'success' else 500
    return result, status_code
