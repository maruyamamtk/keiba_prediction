"""
ローカル月次モデル再学習・本番反映オーケストレータ

毎月第1月曜 AM1:00 に launchd（scripts/monthly_retrain_local.sh 経由）から起動され、
以下を無人で実行する。各ステップ失敗・品質ゲート不合格で即停止し、結果を通知する。

  1. 特徴量再生成      : scripts/generate_features.py --truncate（全期間）
  2. 学習             : src.models.train.train_pipeline(tune=True) → 指標取得
     ├─ 品質ゲート①   : NDCG@3 / Recall@3 / AUC が閾値以上か（未満ならデプロイせず停止）
  3. 戦略再最適化      : scripts/optimize_strategy.py（校正済み確率・prob_weight_r=1.0 固定）
  4. ホールドアウト検証: run_full_strategy_backtest_pipeline で OOS 回収率を算出
     ├─ 品質ゲート②   : 回収率が閾値以上か（未満ならデプロイせず停止）
  5. デプロイ         : build_and_push.sh → deploy_cloud_run.sh

背景: 旧 weekly-model-retrain（Cloud Run Job）は毎週 OOM でサイレント失敗していたため廃止し、
本フローに移行した。校正器はモデル meta.json に保存され本番予測で適用される（PR #421 と整合）。

使い方:
    .venv/bin/python scripts/monthly_retrain.py                # フル実行（デプロイまで）
    .venv/bin/python scripts/monthly_retrain.py --dry-run      # 実行順とコマンドを表示のみ
    .venv/bin/python scripts/monthly_retrain.py --skip-deploy  # ゲート②まで検証しデプロイしない
"""

import argparse
import datetime
import logging
import os
import subprocess
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_backtest import (  # noqa: E402
    _load_strategy_config,
    run_full_strategy_backtest_pipeline,
)
from src.models.train import load_config, train_pipeline  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("monthly_retrain")

# --- 品質ゲート既定値（参考水準 NDCG@3≈0.57 / AUC≈0.81 / Recall@3≈0.51 に安全マージン） ---
DEFAULT_AUC_MIN = 0.78
DEFAULT_NDCG_MIN = 0.54
DEFAULT_RECALL_MIN = 0.47
DEFAULT_RECOVERY_MIN = 95.0  # ホールドアウト OOS 回収率 (%)

# --- 期間の既定値（今日を基準に相対計算） ---
FEATURE_START = "2016-01-01"
OPTIMIZE_LOOKBACK_DAYS = 365  # 戦略最適化期間の開始（today - N日）
OPTIMIZE_GAP_DAYS = 60        # 最適化期間の終了（today - N日）。以降をホールドアウトに使う
HOLDOUT_LOOKBACK_DAYS = 60    # ホールドアウト（OOS）期間の開始（today - N日）


def notify(subject: str, body: str) -> None:
    """結果を通知する。LINE 設定があれば push、無ければログのみ（best-effort）。"""
    message = f"[月次再学習] {subject}\n{body}"
    logger.info(message.replace("\n", " | "))

    token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
    to = os.environ.get("LINE_NOTIFY_TO")
    if not token or not to:
        return
    try:
        from src.utils.line_notify import push_messages, text_message

        push_messages(token, to, [text_message(message[:4900])])
    except Exception as exc:  # 通知失敗で本処理を止めない
        logger.warning(f"LINE通知に失敗しました: {exc}")


def fail(subject: str, body: str) -> None:
    """通知して異常終了する。"""
    notify(f"❌ {subject}", body)
    sys.exit(1)


def run_cmd(cmd: list[str], dry_run: bool) -> None:
    """サブプロセスを実行する。dry_run 時はコマンド表示のみ。失敗で fail。"""
    printable = " ".join(cmd)
    if dry_run:
        logger.info(f"[dry-run] {printable}")
        return
    logger.info(f"$ {printable}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        fail("コマンド失敗", f"exit={result.returncode}\n{printable}")


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser(description="ローカル月次モデル再学習・本番反映")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--python", default=sys.executable, help="使用するPython実行ファイル")
    parser.add_argument("--dry-run", action="store_true", help="実行順とコマンドを表示のみ")
    parser.add_argument("--skip-deploy", action="store_true", help="ゲート②まで検証しデプロイしない")
    parser.add_argument("--n-trials", type=int, default=500, help="戦略最適化のOptuna試行回数")
    parser.add_argument("--auc-min", type=float, default=DEFAULT_AUC_MIN)
    parser.add_argument("--ndcg-min", type=float, default=DEFAULT_NDCG_MIN)
    parser.add_argument("--recall-min", type=float, default=DEFAULT_RECALL_MIN)
    parser.add_argument("--recovery-min", type=float, default=DEFAULT_RECOVERY_MIN)
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数が必要です")

    today = datetime.date.today()
    date_str = today.strftime("%Y%m%d")
    py = args.python

    optimize_start = (today - datetime.timedelta(days=OPTIMIZE_LOOKBACK_DAYS)).isoformat()
    optimize_end = (today - datetime.timedelta(days=OPTIMIZE_GAP_DAYS)).isoformat()
    holdout_start = today - datetime.timedelta(days=HOLDOUT_LOOKBACK_DAYS)
    holdout_end = today

    logger.info("=" * 60)
    logger.info("ローカル月次モデル再学習を開始します")
    logger.info(f"  プロジェクト: {args.project_id}")
    logger.info(f"  ゲート①: AUC>={args.auc_min} NDCG@3>={args.ndcg_min} Recall@3>={args.recall_min}")
    logger.info(f"  ゲート②: 回収率>={args.recovery_min}%（ホールドアウト {holdout_start}〜{holdout_end}）")
    logger.info(f"  dry_run={args.dry_run} skip_deploy={args.skip_deploy}")
    logger.info("=" * 60)
    notify("開始", f"{today} 再学習を開始（dry_run={args.dry_run}, skip_deploy={args.skip_deploy}）")

    # --- ステップ1: 特徴量再生成（全期間・TRUNCATE） ---
    logger.info("[1/5] 特徴量再生成 features.training_data")
    run_cmd(
        [
            py, "scripts/generate_features.py",
            "--project-id", args.project_id,
            "--start-date", FEATURE_START,
            "--end-date", today.isoformat(),
            "--truncate",
        ],
        args.dry_run,
    )

    # --- ステップ2: 学習（Optunaチューニング・GCSアップロード） ---
    logger.info("[2/5] モデル学習 train_pipeline(tune=True)")
    if args.dry_run:
        logger.info(f"[dry-run] train_pipeline(project_id={args.project_id}, tune=True)")
        gcs_uri = f"gs://{args.project_id}-keiba-models/lgbm_ranker_multi/{date_str}/lgbm_ranker_multi_{date_str}.txt"
        metrics = {"ndcg@3": 0.0, "recall@3": 0.0, "auc": 0.0}
    else:
        config = load_config()
        result = train_pipeline(
            project_id=args.project_id,
            execution_date=today,
            config=config,
            tune=True,
        )
        gcs_uri = result["gcs_uri"]
        metrics = result["metrics"]
        logger.info(f"学習完了: metrics={metrics} gcs_uri={gcs_uri}")

        # --- 品質ゲート① ---
        gate1 = (
            metrics["auc"] >= args.auc_min
            and metrics["ndcg@3"] >= args.ndcg_min
            and metrics["recall@3"] >= args.recall_min
        )
        summary = (
            f"AUC={metrics['auc']:.4f}(≥{args.auc_min}) "
            f"NDCG@3={metrics['ndcg@3']:.4f}(≥{args.ndcg_min}) "
            f"Recall@3={metrics['recall@3']:.4f}(≥{args.recall_min})"
        )
        if not gate1:
            fail(
                "品質ゲート①不合格（モデル劣化）→ デプロイ中止",
                f"{summary}\nモデルはGCSに保存済みだが本番反映しない。"
                f"戦略が旧モデル前提のまま race-day-predict が新モデルを拾わないよう、"
                f"必要なら該当GCSフォルダ {date_str} を削除すること。",
            )
        logger.info(f"品質ゲート①合格: {summary}")

        if not gcs_uri:
            fail("GCSアップロード未検出", "train_pipeline が gcs_uri を返しませんでした")

    # --- ステップ3: 戦略再最適化（校正済み確率・prob_weight_r=1.0 固定） ---
    logger.info("[3/5] 戦略パラメータ再最適化 optimize_strategy.py")
    run_cmd(
        [
            py, "scripts/optimize_strategy.py",
            "--project-id", args.project_id,
            "--model-path", gcs_uri,
            "--start-date", optimize_start,
            "--end-date", optimize_end,
            "--n-trials", str(args.n_trials),
        ],
        args.dry_run,
    )

    # --- ステップ4: ホールドアウト検証（OOS 回収率で品質ゲート②） ---
    logger.info(f"[4/5] ホールドアウト検証 {holdout_start}〜{holdout_end}")
    if args.dry_run:
        logger.info("[dry-run] run_full_strategy_backtest_pipeline(...) で OOS 回収率を検証")
    else:
        strat = _load_strategy_config()
        model_config = load_config()
        _, bt_metrics = run_full_strategy_backtest_pipeline(
            project_id=args.project_id,
            model_path=gcs_uri,
            start_date=holdout_start,
            end_date=holdout_end,
            config=model_config,
            budget_per_race=float(strat.get("budget_per_race", 3000)),
            min_prob_threshold=float(strat.get("min_prob_threshold", 0.0)),
            expected_return_threshold=float(strat.get("expected_return_threshold", 1.2)),
            prob_weight_r=float(strat.get("prob_weight_r", 1.0)),
            top_n=int(strat.get("top_n", 5)),
            max_wide_odds=strat.get("max_wide_odds"),
            enabled_bet_types=strat.get("enabled_bet_types"),
        )
        recovery = float(bt_metrics.get("recovery_rate", 0.0)) if bt_metrics else 0.0
        total_bets = int(bt_metrics.get("total_bets", 0)) if bt_metrics else 0
        logger.info(f"ホールドアウト回収率={recovery:.1f}% 賭け数={total_bets}")
        if recovery < args.recovery_min:
            fail(
                "品質ゲート②不合格（回収率劣化）→ デプロイ中止",
                f"OOS回収率={recovery:.1f}% < {args.recovery_min}%（賭け数={total_bets}）\n"
                f"config/strategy_config.yaml を git で元に戻すこと（git checkout -- config/strategy_config.yaml）。",
            )
        logger.info(f"品質ゲート②合格: 回収率={recovery:.1f}% (≥{args.recovery_min}%)")

    # --- ステップ5: デプロイ ---
    if args.skip_deploy:
        logger.info("[5/5] --skip-deploy 指定のためデプロイをスキップ")
        notify("✅ 検証完了（デプロイ省略）", f"モデル {date_str} は両ゲート合格。デプロイは手動で実施してください。")
        return 0

    logger.info("[5/5] Cloud Run デプロイ")
    run_cmd(["bash", "infrastructure/scripts/build_and_push.sh"], args.dry_run)
    run_cmd(["bash", "infrastructure/scripts/deploy_cloud_run.sh"], args.dry_run)

    notify(
        "✅ 本番反映完了",
        f"モデル {date_str} を学習・検証・デプロイしました。\n{gcs_uri}",
    )
    logger.info("月次再学習・本番反映が完了しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
