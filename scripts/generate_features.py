#!/usr/bin/env python3
"""
特徴量生成スクリプト

特徴量パイプラインを実行し、指定期間のレースに対して特徴量を生成する。
生成された特徴量はBigQueryのfeatures.training_dataテーブルに保存される。

使用例:
    # 基本的な使用方法
    python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31

    # 並列処理ワーカー数を指定
    python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-12-31 --max-workers 8

    # 順次処理で実行（デバッグ用）
    python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31 --no-parallel

    # ドライラン（実際には保存しない）
    python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

from src.ml.features.feature_pipeline import (
    FeaturePipeline,
    FeaturePipelineConfig,
)

# ロガー設定
logger = logging.getLogger(__name__)


def validate_date(date_str: str) -> str:
    """日付形式を検証"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"無効な日付形式です: {date_str}。YYYY-MM-DD形式で指定してください。"
        )


def setup_logging(verbose: bool = False, log_file: str = None) -> None:
    """ロギングを設定"""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="競馬予測用の特徴量を生成する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 1ヶ月分の特徴量を生成
  python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31

  # 1年分の特徴量を並列処理で生成（ワーカー数8）
  python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-12-31 --max-workers 8

  # 詳細ログを出力しながら実行
  python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31 -v

  # ログファイルに出力
  python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-01-31 --log-file features.log
        """,
    )

    # 必須引数
    parser.add_argument(
        "--start-date",
        type=validate_date,
        required=True,
        help="開始日 (YYYY-MM-DD形式)",
    )
    parser.add_argument(
        "--end-date",
        type=validate_date,
        required=True,
        help="終了日 (YYYY-MM-DD形式)",
    )

    # GCP設定
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID（環境変数GCP_PROJECT_IDから取得可能）",
    )

    # 処理設定
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="バッチサイズ（一度にBigQueryに保存するレース数）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="並列処理のワーカー数",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="並列処理を無効化（デバッグ時に便利）",
    )

    # リトライ設定
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="エラー時の最大リトライ回数",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help="リトライ時の初期待機時間（秒）",
    )

    # 出力設定
    parser.add_argument(
        "--output-dataset",
        default="features",
        help="出力先BigQueryデータセット",
    )
    parser.add_argument(
        "--output-table",
        default="training_data",
        help="出力先BigQueryテーブル",
    )

    # ロギング設定
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="詳細ログを出力",
    )
    parser.add_argument(
        "--log-file",
        help="ログファイルパス",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=10,
        help="進捗ログの出力間隔（レース数）",
    )

    # その他
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="ドライラン（BigQueryへの保存をスキップ）",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help=(
            "実行前にテーブルをTRUNCATEしてからWRITE_TRUNCATEで書き込む（重複防止）。"
            "全期間を対象とするフルロード（retrain-and-deploy等）で必ず指定すること。"
        ),
    )

    return parser.parse_args()


def print_summary(result: dict, args: argparse.Namespace) -> None:
    """結果サマリーを出力"""
    print()
    print("=" * 70)
    print("特徴量生成 完了")
    print("=" * 70)
    print(f"期間: {args.start_date} 〜 {args.end_date}")
    print("-" * 70)
    print(f"削除行数:       {result['deleted_rows']:,}")
    print(f"挿入行数:       {result['inserted_rows']:,}")
    print(f"処理時間:       {result['elapsed_time']:.1f}秒")
    print("=" * 70)


def main() -> int:
    """メイン関数"""
    # .envファイルから環境変数を読み込み
    load_dotenv()

    # 引数パース
    args = parse_args()

    # ロギング設定
    setup_logging(verbose=args.verbose, log_file=args.log_file)

    # プロジェクトIDの確認
    if not args.project_id:
        logger.error(
            "GCPプロジェクトIDが設定されていません。"
            "--project-id オプションまたは環境変数 GCP_PROJECT_ID を設定してください。"
        )
        return 1

    # 日付の検証
    if args.start_date > args.end_date:
        logger.error("開始日は終了日より前である必要があります。")
        return 1

    logger.info("=" * 50)
    logger.info("特徴量生成パイプライン開始")
    logger.info("=" * 50)
    logger.info(f"プロジェクトID: {args.project_id}")
    logger.info(f"期間: {args.start_date} 〜 {args.end_date}")
    logger.info(f"バッチサイズ: {args.batch_size}")
    logger.info(f"ワーカー数: {args.max_workers}")
    logger.info(f"並列処理: {'無効' if args.no_parallel else '有効'}")
    logger.info(f"最大リトライ: {args.max_retries}回")
    if args.dry_run:
        logger.info("ドライランモード: BigQueryへの保存はスキップされます")
    logger.info("=" * 50)

    try:
        # パイプライン設定
        config = FeaturePipelineConfig(
            output_dataset=args.output_dataset,
            output_table=args.output_table,
            max_retries=args.max_retries,
            retry_base_delay=args.retry_delay,
        )

        # パイプライン実行
        pipeline = FeaturePipeline(args.project_id, config)

        if args.dry_run:
            # ドライランモード：生成されるSQLを表示するのみ
            query = pipeline.generate_query(args.start_date, args.end_date)
            logger.info("ドライランモード: 生成SQLを表示します（BQ保存なし）")
            print(query)
            return 0
        else:
            result = pipeline.run(
                start_date=args.start_date,
                end_date=args.end_date,
                truncate_before_run=args.truncate,
            )

        # サマリー出力
        print_summary(result, args)

        return 0

    except KeyboardInterrupt:
        logger.warning("ユーザーによって処理が中断されました")
        return 130
    except Exception as e:
        logger.exception(f"予期しないエラーが発生しました: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
