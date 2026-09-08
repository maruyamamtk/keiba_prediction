"""
ローカル月次モデル再学習・本番反映オーケストレータ

毎月第1月曜 AM1:00 に launchd（scripts/monthly_retrain_local.sh 経由）から起動され、
以下を無人で実行する。各ステップ失敗・品質ゲート不合格で即停止し、結果を通知する。

  1. 特徴量再生成      : scripts/generate_features.py --truncate（全期間）
  2. 学習             : src.models.train.train_pipeline(tune=True, test_days=...)
     ├─ train/valid/testの3分割。train+validでハイパラ調整・Early Stopping
     ├─ train+validを結合し、決まったラウンド数固定でリフィット（testには一切触れない）
     ├─ リフィットモデルをtest（真に未見データ）で評価 → 品質ゲート①: NDCG@3/Recall@3/AUC
  3. 戦略再最適化      : scripts/optimize_strategy.py（testの前半・校正済み確率）
  4. ホールドアウト検証: run_full_strategy_backtest_pipeline（testの後半・真に未見）
     ├─ 品質ゲート②   : 回収率が閾値以上か（未満ならデプロイせず停止）
  5. デプロイ         : build_and_push.sh → deploy_cloud_run.sh

データ分割設計（Issue #430）:
  train/valid の2分割だけだと、validはEarly Stopping・Optunaハイパーパラメータ
  選定に使われるため「モデル選択済み」のデータになる。このvalidと同じ・重なる期間で
  戦略パラメータを最適化・検証すると、モデル選択で既に「見た」データの上でさらに
  戦略を最適化するリークが生じる（実際に発生していた不具合）。

  対応として train/valid/test の3分割にする:
    1. train/valid/testに分割する（testはtrain_pipeline()にtest_daysを渡すことで
       validの終端を実行日からその日数分手前に切り上げて確保する）
    2. train+validでハイパーパラメータ調整・Early Stopping（従来通り）
    3. train+validを結合し、Early Stoppingで決まったラウンド数固定でリフィット
       （testには一切触れない）
    4. リフィットしたモデルをtest（真に未見データ）で評価する（＝品質ゲート①）
    5. このモデルを使って戦略パラメータを最適化する。最適化データはtestを使い、
       test内でさらに前半（最適化用）/後半（ホールドアウト用）に分割する
  testはモデルの学習・選定のどちらにも一切使われていないため、モデルの汎化性能評価
  （ゲート①）と戦略パラメータ最適化・ホールドアウト検証（ゲート②）の両方に
  安全に使い回せる。optimize_strategy.py 自身のdocstringが定める「学習・検証期間に
  含まれない日付を指定すること」というOOS原則にも、この設計で整合する。

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
from src.models.train import (  # noqa: E402
    compute_validation_boundaries,
    load_config,
    train_pipeline,
)

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
DEFAULT_HOLDOUT_MIN_BETS = 150  # ホールドアウト賭け数の参考下限（60日分の目安。未達は警告のみ）
# これを下回る賭け数では回収率が実質ノイズ（数件のまぐれ的中で95%ラインを超えうる）となるため、
# 回収率の値によらずデプロイを止める絶対下限（Issue #430レビュー対応）
DEFAULT_HOLDOUT_MIN_BETS_HARD = 30

# --- 期間の既定値 ---
FEATURE_START = "2016-01-01"
# train/validのどちらにも使わず確保するtest期間（日数）。旧設計（Issue #422時点）の
# ホールドアウト期間（60日）と同等の統計的サンプル数を確保するため、
# test期間=前半90日(戦略最適化)+後半60日(ホールドアウト)=150日とする。
TEST_DAYS = 150
STRATEGY_OPTIMIZE_DAYS = 90  # test期間のうち前半を戦略最適化に使う日数（残りはホールドアウト）
assert STRATEGY_OPTIMIZE_DAYS < TEST_DAYS, (
    "STRATEGY_OPTIMIZE_DAYS は TEST_DAYS 未満である必要があります"
    "（ホールドアウト用に少なくとも1日は残す）"
)


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

    logger.info("=" * 60)
    logger.info("ローカル月次モデル再学習を開始します")
    logger.info(f"  プロジェクト: {args.project_id}")
    logger.info(f"  ゲート①: AUC>={args.auc_min} NDCG@3>={args.ndcg_min} Recall@3>={args.recall_min}（test期間で評価）")
    logger.info(f"  ゲート②: 回収率>={args.recovery_min}%（test期間後半のホールドアウト）")
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

    # --- ステップ2: 学習（train/valid/test3分割・リフィット・Optunaチューニング・GCSアップロード） ---
    logger.info("[2/5] モデル学習 train_pipeline(tune=True)")
    config = load_config()
    if args.dry_run:
        logger.info(
            f"[dry-run] train_pipeline(project_id={args.project_id}, tune=True, "
            f"test_days={TEST_DAYS}, calibration_days={STRATEGY_OPTIMIZE_DAYS})"
        )
        gcs_uri = f"gs://{args.project_id}-keiba-models/lgbm_ranker_multi/{date_str}/lgbm_ranker_multi_{date_str}.txt"
        metrics = {"ndcg@3": 0.0, "recall@3": 0.0, "auc": 0.0}
        # dry-runではモデルを学習しないため、train_pipeline内部と全く同じ関数
        # （compute_validation_boundaries）で検証期間・test期間を再現する
        # （日付計算ロジックの二重実装によるドリフトを防ぐ・Issue #430）。
        # 実行時（train_pipeline()の戻り値）にはtrain_from/train_rows/test_rows等も
        # 含まれるが、ここではステップ3以降が参照する日付4項目のみを意図的に用意する
        # （dry-run専用の簡略版。他のキーへのアクセスを追加する場合は実行時分岐も要確認）。
        validation_months = config["model"]["training"]["validation_months"]
        boundaries = compute_validation_boundaries(today, validation_months, TEST_DAYS)
        training_period = {
            "valid_from": boundaries["valid_start"].isoformat(),
            "valid_to": boundaries["valid_end"].isoformat(),
            "test_from": boundaries["test_start"].isoformat(),
            "test_to": boundaries["test_end"].isoformat(),
        }
    else:
        result = train_pipeline(
            project_id=args.project_id,
            execution_date=today,
            config=config,
            tune=True,
            test_days=TEST_DAYS,
            # test期間の末尾（ホールドアウト）が真に未見のままになるよう、
            # キャリブレーションもtest期間の先頭90日（戦略最適化と同じ範囲）だけでフィットする。
            # ここをSTRATEGY_OPTIMIZE_DAYSからずらすと、ステップ4のホールドアウトの実績が
            # キャリブレーションに混入する「一段深いモデル選択リーク」が再発する（Issue #430追加修正）。
            calibration_days=STRATEGY_OPTIMIZE_DAYS,
        )
        gcs_uri = result["gcs_uri"]
        metrics = result["metrics"]
        training_period = result["training_period"]
        logger.info(f"学習完了: metrics(test評価)={metrics} gcs_uri={gcs_uri}")
        logger.info(f"検証期間: {training_period['valid_from']} 〜 {training_period['valid_to']}")
        logger.info(
            f"test期間（真に未見データ）: {training_period['test_from']} 〜 "
            f"{training_period['test_to']}"
        )

        # --- 品質ゲート①（test期間で評価したリフィットモデルの指標） ---
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
    # 最適化期間はステップ2で確保したtest期間の前半（STRATEGY_OPTIMIZE_DAYS日）を使う。
    # test期間はモデルの学習・検証・選定のどちらにも使われていないため、
    # optimize_strategy.py 自身が定めるOOS原則（学習・検証期間に含まれない日付を
    # 指定すること）に適合する（Issue #430）。
    # 既存configのuse_harvilleを引き継ぐ: optimize_strategy.pyは実行時の
    # フラグでconfig/strategy_config.yamlのuse_harville/gammaを上書き保存するため、
    # ここで--use-harvilleを付けずに実行すると、運用者が手動でHarvilleモデルに
    # 切り替えていた場合でも毎月の再学習で独立積へ無言で戻ってしまう。
    test_from = datetime.date.fromisoformat(training_period["test_from"])
    # test_to は saturday-1 由来のため、週前半（月〜水）の実行では「今日」より
    # 数日先になり得る（split_train_valid_predictのvalid_end等と同じ既存の性質）。
    # 未来日を指定してもクエリ側は該当データなしで自然に空振りするだけで実害はないが、
    # ログ・実際の問い合わせ範囲としては「今日」で頭打ちにしておく方が誤解がない。
    test_to = min(datetime.date.fromisoformat(training_period["test_to"]), today)
    optimize_start = test_from
    optimize_end = min(
        test_from + datetime.timedelta(days=STRATEGY_OPTIMIZE_DAYS - 1), test_to
    )
    holdout_start = optimize_end + datetime.timedelta(days=1)
    holdout_end = test_to
    if holdout_start > holdout_end:
        fail(
            "ホールドアウト期間の計算が不正（設定不整合）→ デプロイ中止",
            f"holdout_start={holdout_start} > holdout_end={holdout_end}。"
            f"TEST_DAYS({TEST_DAYS})とSTRATEGY_OPTIMIZE_DAYS({STRATEGY_OPTIMIZE_DAYS})の"
            f"関係、またはtest_to({test_to})のクランプを確認すること。",
        )

    prev_strat = _load_strategy_config()
    prev_use_harville = bool(prev_strat.get("use_harville", False))
    logger.info(
        f"[3/5] 戦略パラメータ再最適化 optimize_strategy.py "
        f"（期間={optimize_start}〜{optimize_end}, use_harville={prev_use_harville}引き継ぎ）"
    )
    # optimize_strategy.py の --min-total-bets 既定値(600)は約6ヶ月の期間を前提とした
    # 値のため、STRATEGY_OPTIMIZE_DAYS（既定90日）の期間比で按分した値を明示的に渡す
    # （指定しないと600固定のままとなり、短い期間では大半の試行が制約落ちしてしまう）。
    optimize_min_bets = max(50, round(600 * STRATEGY_OPTIMIZE_DAYS / 180))
    optimize_cmd = [
        py, "scripts/optimize_strategy.py",
        "--project-id", args.project_id,
        "--model-path", gcs_uri,
        "--start-date", optimize_start.isoformat(),
        "--end-date", optimize_end.isoformat(),
        "--n-trials", str(args.n_trials),
        "--min-total-bets", str(optimize_min_bets),
    ]
    if prev_use_harville:
        optimize_cmd.append("--use-harville")
    run_cmd(optimize_cmd, args.dry_run)

    # --- ステップ4: ホールドアウト検証（test期間の後半・真に未見データでのOOS回収率） ---
    logger.info(f"[4/5] ホールドアウト検証 {holdout_start}〜{holdout_end}")
    if args.dry_run:
        logger.info("[dry-run] run_full_strategy_backtest_pipeline(...) で OOS 回収率を検証")
    else:
        strat = _load_strategy_config()
        _, bt_metrics = run_full_strategy_backtest_pipeline(
            project_id=args.project_id,
            model_path=gcs_uri,
            start_date=holdout_start,
            end_date=holdout_end,
            config=config,
            budget_per_race=float(strat.get("budget_per_race", 3000)),
            min_prob_threshold=float(strat.get("min_prob_threshold", 0.0)),
            expected_return_threshold=float(strat.get("expected_return_threshold", 1.2)),
            prob_weight_r=float(strat.get("prob_weight_r", 1.0)),
            top_n=int(strat.get("top_n", 5)),
            max_wide_odds=strat.get("max_wide_odds"),
            enabled_bet_types=strat.get("enabled_bet_types"),
            gamma=float(strat.get("gamma", 1.0)),
            use_harville=bool(strat.get("use_harville", False)),
        )
        recovery = float(bt_metrics.get("recovery_rate", 0.0)) if bt_metrics else 0.0
        total_bets = int(bt_metrics.get("total_bets", 0)) if bt_metrics else 0
        logger.info(f"ホールドアウト回収率={recovery:.1f}% 賭け数={total_bets}")
        if total_bets < DEFAULT_HOLDOUT_MIN_BETS_HARD:
            fail(
                "ホールドアウトの賭け数が少なすぎる（統計的に無意味）→ デプロイ中止",
                f"賭け数={total_bets} < {DEFAULT_HOLDOUT_MIN_BETS_HARD}（回収率={recovery:.1f}%）。"
                f"少数のまぐれ的中で回収率ゲートを通過しうるため、回収率の値によらず停止する。",
            )
        if total_bets < DEFAULT_HOLDOUT_MIN_BETS:
            logger.warning(
                f"ホールドアウトの賭け数が少なく（{total_bets}件）統計的信頼性は低いが、"
                f"デプロイ停止の絶対下限（{DEFAULT_HOLDOUT_MIN_BETS_HARD}件）は上回っているため継続する"
                f"（参考値扱い）。"
            )
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
        f"モデル {date_str} を学習・戦略最適化・検証・デプロイしました。\n{gcs_uri}",
    )
    logger.info("月次再学習・本番反映が完了しました")
    return 0


if __name__ == "__main__":
    sys.exit(main())
