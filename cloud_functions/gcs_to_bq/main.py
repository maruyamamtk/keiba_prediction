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
TABLE_MAPPING = {
    'BAA': 'race_info',
    'BAB': 'race_info',
    'BAC': 'race_info',
    'KYF': 'horse_results',
    'KYG': 'horse_results',
    'KYH': 'horse_results',
    'SEC': 'horse_results',
    'KAA': 'race_info',
    'KAB': 'race_info',
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
    BigQueryにデータをロード

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

    # ジョブ設定
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,  # スキーマは既存テーブルから取得
        ignore_unknown_values=True,
    )

    try:
        # リソースを適切に管理するためwithステートメントを使用
        with bigquery.Client(project=project_id) as client:
            # データをロード
            load_job = client.load_table_from_json(
                rows,
                table_ref,
                job_config=job_config
            )

            # ジョブ完了を待機
            load_job.result()

            logger.info(
                f"Loaded {len(rows)} rows to {table_ref}. "
                f"Job ID: {load_job.job_id}"
            )

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

        # リソースを適切に管理するためwithステートメントを使用
        with storage.Client() as storage_client:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)

            # ファイル内容を取得 (CP932でデコード)
            try:
                file_bytes = blob.download_as_bytes()
                file_content = file_bytes.decode('cp932', errors='replace')

                # デコードエラーがある場合は警告を出力
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
