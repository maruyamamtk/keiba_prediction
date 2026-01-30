#!/usr/bin/env python3
"""
GCSの既存ファイルをBigQueryに再ロードするスクリプト

Cloud FunctionはGCSオブジェクト作成時にのみトリガーされるため、
既存のファイルを処理するにはこのスクリプトを使用します。
"""

import os
import sys
import logging
import argparse
from typing import List, Optional
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import storage, bigquery

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cloud_functions', 'gcs_to_bq'))

from parser import JRDBParser

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# テーブルマッピング
TABLE_MAPPING = {
    'BAA': 'race_info',
    'BAB': 'race_info',
    'BAC': 'race_info',
    'KYF': 'horse_results',
    'KYG': 'horse_results',
    'KYH': 'horse_results',
    'SEC': 'race_results',
    'UKC': 'horse_master',
    'KKA': 'horse_extended',
    'KAA': 'venue_info',
}

TABLE_UNIQUE_KEYS = {
    'race_info': ['race_id'],
    'horse_results': ['race_id', 'horse_number'],
    'race_results': ['race_id', 'horse_number'],
    'horse_master': ['horse_id'],
    'horse_extended': ['race_id', 'horse_number'],
    'venue_info': ['venue_id'],
}


def get_gcs_files(bucket_name: str, prefix: str, data_type: str) -> List[str]:
    """GCSから指定プレフィックス・データタイプのファイル一覧を取得"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for blob in blobs:
        basename = os.path.basename(blob.name)
        if basename.upper().startswith(data_type.upper()) and basename.lower().endswith('.csv'):
            files.append(blob.name)

    return sorted(files)


def load_file_to_bigquery(
    project_id: str,
    bucket_name: str,
    file_name: str,
    dataset_id: str = 'raw'
) -> dict:
    """GCSファイルをBigQueryにロード"""
    result = {
        'file': file_name,
        'status': 'failed',
        'records': 0,
        'error': None
    }

    try:
        # データタイプを抽出
        basename = os.path.basename(file_name)
        data_type = basename[:3].upper()

        table_name = TABLE_MAPPING.get(data_type)
        if not table_name:
            result['error'] = f'Unknown data type: {data_type}'
            return result

        # GCSからファイルを取得
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        file_bytes = blob.download_as_bytes()
        try:
            file_content = file_bytes.decode('utf-8')
        except UnicodeDecodeError:
            file_content = file_bytes.decode('cp932', errors='replace')

        # パース
        parsed_data = JRDBParser.parse_file(file_content, data_type)

        if not parsed_data:
            result['error'] = 'No data parsed'
            return result

        # BigQueryにロード（MERGE）
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        # 一時テーブルを作成
        temp_table_ref = f"{project_id}.{dataset_id}._temp_{table_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
        target_table = bq_client.get_table(table_ref)
        schema = target_table.schema

        temp_table = bigquery.Table(temp_table_ref, schema=schema)
        temp_table = bq_client.create_table(temp_table)

        try:
            # Streaming Insert
            errors = bq_client.insert_rows_json(temp_table, parsed_data)
            if errors:
                result['error'] = f'Insert errors: {errors[:3]}'
                return result

            # バッファ反映を待つ
            import time
            time.sleep(3)

            # MERGE
            unique_keys = TABLE_UNIQUE_KEYS.get(table_name, ['race_id'])
            columns = [field.name for field in schema]

            join_conditions = ' AND '.join([f"T.{key} = S.{key}" for key in unique_keys])
            update_columns = [col for col in columns if col not in unique_keys]
            update_set = ', '.join([f"T.{col} = S.{col}" for col in update_columns])
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

            query_job = bq_client.query(merge_query)
            query_job.result()

            result['status'] = 'success'
            result['records'] = len(parsed_data)
            result['table'] = table_name

        finally:
            bq_client.delete_table(temp_table_ref, not_found_ok=True)

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error processing {file_name}: {e}")

    return result


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description='GCSファイルをBigQueryに再ロード')
    parser.add_argument('--data-type', required=True, help='データタイプ (SEC, BAA, KYF等)')
    parser.add_argument('--prefix', default='', help='GCSプレフィックス (例: Sec/)')
    parser.add_argument('--limit', type=int, default=0, help='処理ファイル数上限 (0=無制限)')
    parser.add_argument('--dry-run', action='store_true', help='処理対象ファイルの確認のみ')
    parser.add_argument('--project-id', default=os.environ.get('GCP_PROJECT_ID'), help='GCPプロジェクトID')

    args = parser.parse_args()

    if not args.project_id:
        logger.error('GCP_PROJECT_ID is not set')
        return 1

    bucket_name = f'{args.project_id}-keiba-raw-data'

    # ファイル一覧を取得
    logger.info(f'Getting file list from gs://{bucket_name}/{args.prefix} (type: {args.data_type})')
    files = get_gcs_files(bucket_name, args.prefix, args.data_type)

    if args.limit > 0:
        files = files[:args.limit]

    logger.info(f'Found {len(files)} files to process')

    if args.dry_run:
        for f in files:
            print(f'  {f}')
        return 0

    # ファイルを処理
    success = 0
    failed = 0
    total_records = 0

    for i, file_name in enumerate(files):
        logger.info(f'[{i+1}/{len(files)}] Processing {file_name}')
        result = load_file_to_bigquery(args.project_id, bucket_name, file_name)

        if result['status'] == 'success':
            success += 1
            total_records += result['records']
            logger.info(f"  -> Success: {result['records']} records")
        else:
            failed += 1
            logger.error(f"  -> Failed: {result['error']}")

    logger.info(f'\n{"="*60}')
    logger.info(f'処理完了')
    logger.info(f'{"="*60}')
    logger.info(f'成功: {success} ファイル')
    logger.info(f'失敗: {failed} ファイル')
    logger.info(f'総レコード数: {total_records}')

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
