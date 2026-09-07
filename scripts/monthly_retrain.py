"""
ローカル月次モデル再学習・本番反映オーケストレータ

毎月第1月曜 AM1:00 に launchd（scripts/monthly_retrain_local.sh 経由）から起動され、
以下を無人で実行する。各ステップ失敗・品質ゲート不合格で即停止し、結果を通知する。

  1. 特徴量再生成      : scripts/generate_features.py --truncate（全期間）
  2. 学習             : src.models.train.train_pipeline(tune=True) → 指標取得
     ├─ 品質ゲート①   : NDCG@3 / Recall@3 / AUC が閾値以上か（未満ならデプロイせず停止）
  3. 戦略再最適化      : scripts/optimize_strategy.py（モデルの検証期間 valid_from〜valid_to と
                        同一期間で最適化。校正済み確率・prob_weight_r=1.0 固定）
  4. デプロイ         : build_and_push.sh → deploy_cloud_run.sh

戦略最適化期間の設計（Issue #430）:
  最適化期間は「今日からN日前」という独立した日数計算ではなく、必ずステップ2で
  学習したモデル自身の training_period（valid_from〜valid_to）と一致させる。
  これはモデルのハイパーパラメータ選定・Early StoppingがValidation期間の成績を
  基準に行われるため、戦略最適化を別の独立した期間（特にValidation期間と重なる
  期間）で行うと、モデル選択で「見た」データの上でさらに戦略を最適化する二重の
  リークが生じるため。ホールドアウト検証（旧ステップ4・品質ゲート②）は、
  Validation期間より後の真に未見なデータがほぼ存在しない（Validation期間が
  「今日」の直前まで伸びるように学習されるため）ことから信頼できる形で実施できず、
  廃止した。品質ゲート①（モデル指標）のみで学習の可否を判断する。

背景: 旧 weekly-model-retrain（Cloud Run Job）は毎週 OOM でサイレント失敗していたため廃止し、
本フローに移行した。校正器はモデル meta.json に保存され本番予測で適用される（PR #421 と整合）。

使い方:
    .venv/bin/python scripts/monthly_retrain.py                # フル実行（デプロイまで）
    .venv/bin/python scripts/monthly_retrain.py --dry-run      # 実行順とコマンドを表示のみ
    .venv/bin/python scripts/monthly_retrain.py --skip-deploy  # ゲート①まで検証しデプロイしない
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

from scripts.run_backtest import _load_strategy_config  # noqa: E402
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

# --- 期間の既定値 ---
FEATURE_START = "2016-01-01"


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
    parser.add_argument("--skip-deploy", action="store_true", help="ゲート①まで検証しデプロイしない")
    parser.add_argument("--n-trials", type=int, default=500, help="戦略最適化のOptuna試行回数")
    parser.add_argument("--auc-min", type=float, default=DEFAULT_AUC_MIN)
    parser.add_argument("--ndcg-min", type=float, default=DEFAULT_NDCG_MIN)
    parser.add_argument("--recall-min", type=float, default=DEFAULT_RECALL_MIN)
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数が必要です")

    today = datetime.date.today()
    date_str = today.strftime("%Y%m%d")
    py = args.python

    logger.info("=" * 60)
    logger.info("ローカル月次モデル再学習を開始します")
    logger.info(f"  プロジェクト: {args.project_id}")
    logger.info(f"  ゲート①: AUC>={args.auc_min} NDCG@3>={args.ndcg_min} Recall@3>={args.recall_min}")
    logger.info(f"  dry_run={args.dry_run} skip_deploy={args.skip_deploy}")
    logger.info("=" * 60)
    notify("開始", f"{today} 再学習を開始（dry_run={args.dry_run}, skip_deploy={args.skip_deploy}）")

    # --- ステップ1: 特徴量再生成（全期間・TRUNCATE） ---
    logger.info("[1/4] 特徴量再生成 features.training_data")
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
    logger.info("[2/4] モデル学習 train_pipeline(tune=True)")
    if args.dry_run:
        logger.info(f"[dry-run] train_pipeline(project_id={args.project_id}, tune=True)")
        gcs_uri = f"gs://{args.project_id}-keiba-models/lgbm_ranker_multi/{date_str}/lgbm_ranker_multi_{date_str}.txt"
        metrics = {"ndcg@3": 0.0, "recall@3": 0.0, "auc": 0.0}
        # dry-runではモデルを学習しないため、検証期間はダミー値（直近6ヶ月）で代用する
        training_period = {
            "valid_from": (today - datetime.timedelta(days=180)).isoformat(),
            "valid_to": today.isoformat(),
        }
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
        training_period = result["training_period"]
        logger.info(f"学習完了: metrics={metrics} gcs_uri={gcs_uri}")
        logger.info(f"検証期間: {training_period['valid_from']} 〜 {training_period['valid_to']}")

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
    # 最適化期間は、今回学習したモデル自身の検証期間（valid_from〜valid_to）と
    # 完全に一致させる（Issue #430）。モデルのハイパーパラメータ選定・Early Stopping
    # がこの期間の成績を基準に行われるため、別の独立した期間で戦略最適化をしても
    # 実質的な独立性は得られず、むしろ境界がずれて中途半端に重なるだけになる。
    # 既存configのuse_harvilleを引き継ぐ: optimize_strategy.pyは実行時の
    # フラグでconfig/strategy_config.yamlのuse_harville/gammaを上書き保存するため、
    # ここで--use-harvilleを付けずに実行すると、運用者が手動でHarvilleモデルに
    # 切り替えていた場合でも毎月の再学習で独立積へ無言で戻ってしまう。
    optimize_start = training_period["valid_from"]
    optimize_end = training_period["valid_to"]
    prev_strat = _load_strategy_config()
    prev_use_harville = bool(prev_strat.get("use_harville", False))
    logger.info(
        f"[3/4] 戦略パラメータ再最適化 optimize_strategy.py "
        f"（期間={optimize_start}〜{optimize_end}, use_harville={prev_use_harville}引き継ぎ）"
    )
    optimize_cmd = [
        py, "scripts/optimize_strategy.py",
        "--project-id", args.project_id,
        "--model-path", gcs_uri,
        "--start-date", optimize_start,
        "--end-date", optimize_end,
        "--n-trials", str(args.n_trials),
    ]
    if prev_use_harville:
        optimize_cmd.append("--use-harville")
    run_cmd(optimize_cmd, args.dry_run)

    # --- ステップ4: デプロイ ---
    if args.skip_deploy:
        logger.info("[4/4] --skip-deploy 指定のためデプロイをスキップ")
        notify("✅ 検証完了（デプロイ省略）", f"モデル {date_str} はゲート①合格。デプロイは手動で実施してください。")
        return 0

    logger.info("[4/4] Cloud Run デプロイ")
    run_cmd(["bash", "infrastructure/scripts/build_and_push.sh"], args.dry_run)
    run_cmd(["bash", "infrastructure/scripts/deploy_cloud_run.sh"], args.dry_run)

    notify(
        "✅ 本番反映完了",
        f"モデル {date_str} を学習・戦略最適化・デプロイしました。\n{gcs_uri}",
    )
    logger.info("月次再学習・本番反映が完了しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
