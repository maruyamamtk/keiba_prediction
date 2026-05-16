#!/usr/bin/env python3
"""
特徴量パイプライン（SQL駆動方式）

BigQuery SQLによる一括特徴量生成を行い、
features.training_dataテーブルに出力する。

機能:
- SQL テンプレートによる特徴量一括生成
- 日付範囲指定による差分更新（DELETE + INSERT）
- リトライ処理によるエラー耐性
"""

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
import logging
import re
import time
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

# ロガー設定
logger = logging.getLogger(__name__)


# リトライ対象の例外
RETRYABLE_EXCEPTIONS = (
    google_exceptions.ServiceUnavailable,
    google_exceptions.InternalServerError,
    google_exceptions.DeadlineExceeded,
    google_exceptions.TooManyRequests,
    ConnectionError,
    TimeoutError,
)

# SQLテンプレートファイルのパス
SQL_TEMPLATE_PATH = Path(__file__).parent / "feature_query_raw.sql"


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = RETRYABLE_EXCEPTIONS,
):
    """
    指数バックオフ付きでリトライを行う

    Args:
        func: 実行する関数
        max_retries: 最大リトライ回数
        base_delay: 初期待機時間（秒）
        max_delay: 最大待機時間（秒）
        exceptions: リトライ対象の例外

    Returns:
        関数の戻り値
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries:
                delay = min(base_delay * (2**attempt), max_delay)
                logger.warning(
                    f"Retry attempt {attempt + 1}/{max_retries} after error: {e}. "
                    f"Waiting {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} retries failed: {e}")
    raise last_exception


@dataclass
class FeaturePipelineConfig:
    """特徴量パイプラインの設定"""

    # 出力先
    output_dataset: str = "features"
    output_table: str = "training_data"

    # リトライ設定
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0


class FeaturePipeline:
    """SQL駆動の特徴量パイプラインを実行するクラス"""

    def __init__(
        self,
        project_id: str,
        config: FeaturePipelineConfig | None = None,
    ):
        """
        Args:
            project_id: GCPプロジェクトID
            config: パイプライン設定
        """
        self.project_id = project_id
        self.config = config or FeaturePipelineConfig()
        self.client = bigquery.Client(project=project_id)

        # SQLテンプレートを読み込み
        self.sql_template = self._load_sql_template()

    def _load_sql_template(self) -> str:
        """SQLテンプレートファイルを読み込む"""
        if not SQL_TEMPLATE_PATH.exists():
            raise FileNotFoundError(
                f"SQLテンプレートが見つかりません: {SQL_TEMPLATE_PATH}"
            )
        return SQL_TEMPLATE_PATH.read_text(encoding="utf-8")

    @staticmethod
    def _validate_date(date_str: str) -> None:
        """
        日付文字列がYYYY-MM-DD形式であることを検証する

        Args:
            date_str: 検証する日付文字列

        Raises:
            ValueError: 不正な日付形式の場合
        """
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            raise ValueError(
                f"日付はYYYY-MM-DD形式で指定してください: '{date_str}'"
            )

    def _build_query(self, start_date: str, end_date: str) -> str:
        """
        SQLテンプレートにパラメータを埋め込んでクエリを生成

        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)

        Returns:
            パラメータ埋め込み済みのSQL文字列

        Raises:
            ValueError: 日付形式が不正な場合
        """
        self._validate_date(start_date)
        self._validate_date(end_date)
        return self.sql_template.format(
            project_id=self.project_id,
            start_date=start_date,
            end_date=end_date,
        )

    def run(self, start_date: str, end_date: str) -> dict:
        """
        指定期間の特徴量を生成してBigQueryに保存

        処理フロー:
        1. テーブルが存在すれば対象日付範囲の既存データをDELETE
        2. SQLテンプレートで特徴量を生成し、結果をテーブルに書き込み
           （テーブルが存在しない場合は自動作成される）

        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)

        Returns:
            処理結果の統計
        """
        pipeline_start = time.time()
        logger.info(f"Feature pipeline started: {start_date} to {end_date}")

        table_ref = (
            f"{self.project_id}."
            f"{self.config.output_dataset}.{self.config.output_table}"
        )

        # Step 1: テーブルが存在する場合、既存データを削除
        deleted_rows = self._delete_existing_data_if_table_exists(
            table_ref, start_date, end_date
        )
        logger.info(f"Deleted {deleted_rows} existing rows for date range")

        # Step 2: 特徴量生成SQLを実行して結果をテーブルに書き込み
        feature_query = self._build_query(start_date, end_date)
        logger.info("Executing feature generation SQL...")
        inserted_rows = self._execute_query_to_table(feature_query, table_ref)
        logger.info(f"Inserted {inserted_rows} rows")

        elapsed = time.time() - pipeline_start
        result = {
            "start_date": start_date,
            "end_date": end_date,
            "deleted_rows": deleted_rows,
            "inserted_rows": inserted_rows,
            "elapsed_time": elapsed,
        }

        logger.info(f"Feature pipeline completed: {result}")
        return result

    def _delete_existing_data_if_table_exists(
        self, table_ref: str, start_date: str, end_date: str
    ) -> int:
        """テーブルが存在する場合、指定日付範囲の既存データを削除"""
        try:
            self.client.get_table(table_ref)
        except google_exceptions.NotFound:
            logger.info("Table does not exist yet, skipping delete")
            return 0

        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
        """
        return self._execute_dml_with_retry(delete_query)

    def _execute_query_to_table(self, query: str, table_ref: str) -> int:
        """クエリ結果をテーブルに書き込む（テーブル自動作成対応）"""
        def execute():
            job_config = bigquery.QueryJobConfig(
                destination=table_ref,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
                time_partitioning=bigquery.TimePartitioning(
                    field="race_date",
                ),
            )
            job = self.client.query(query, job_config=job_config)
            result = job.result()
            return result.total_rows

        return retry_with_backoff(
            func=execute,
            max_retries=self.config.max_retries,
            base_delay=self.config.retry_base_delay,
            max_delay=self.config.retry_max_delay,
        )

    def _execute_dml_with_retry(self, query: str) -> int:
        """リトライ付きでDMLクエリを実行し、影響行数を返す"""
        def execute():
            job = self.client.query(query)
            job.result()
            return job.num_dml_affected_rows or 0

        return retry_with_backoff(
            func=execute,
            max_retries=self.config.max_retries,
            base_delay=self.config.retry_base_delay,
            max_delay=self.config.retry_max_delay,
        )

    def generate_query(self, start_date: str, end_date: str) -> str:
        """
        特徴量生成SQLを返す（デバッグ・確認用）

        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)

        Returns:
            パラメータ埋め込み済みのSQL文字列
        """
        return self._build_query(start_date, end_date)


def main():
    """メイン関数（CLIから実行）"""
    import argparse
    import os
    from dotenv import load_dotenv

    # .envファイルから環境変数を読み込み
    load_dotenv()

    parser = argparse.ArgumentParser(description="特徴量パイプラインを実行（SQL駆動方式）")
    parser.add_argument("--start-date", required=True, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="最大リトライ回数",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="SQLを表示するのみ（実行しない）",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを出力")

    args = parser.parse_args()

    # ロギング設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_ID is not set")
        return 1

    # 設定
    config = FeaturePipelineConfig(
        max_retries=args.max_retries,
    )

    # パイプライン初期化
    pipeline = FeaturePipeline(args.project_id, config)

    # dry-runモード
    if args.dry_run:
        query = pipeline.generate_query(args.start_date, args.end_date)
        print(query)
        return 0

    # パイプライン実行
    result = pipeline.run(args.start_date, args.end_date)

    # 結果出力
    print("\n" + "=" * 60)
    print("特徴量パイプライン実行結果")
    print("=" * 60)
    print(f"対象期間: {result['start_date']} ~ {result['end_date']}")
    print(f"削除行数: {result['deleted_rows']}")
    print(f"挿入行数: {result['inserted_rows']}")
    print(f"処理時間: {result['elapsed_time']:.1f}秒")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
