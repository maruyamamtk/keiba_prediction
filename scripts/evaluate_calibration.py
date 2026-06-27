"""win_place_prob のキャリブレーション評価スクリプト（Issue #414）

最新（または指定）モデルを過去の out-of-sample 期間に遡及適用し、
予測複勝率ビン別の実際の複勝率・信頼性曲線・ECE・Brier・log-loss を出力する。
キャリブレーション温度（meta.json 保存値 or --temperature 指定）適用前後を比較できる。

本番予測モデルは週次再学習で頻繁に入れ替わるため、保存済み daily_predictions
（複数モデル世代混在）ではなく、1つのモデルを固定して過去に遡及適用することで
「現行モデルの校正」を一貫した基準で評価する。

Usage:
    .venv/bin/python scripts/evaluate_calibration.py \\
        --project-id <PROJECT_ID> \\
        --model gs://<PROJECT_ID>-keiba-models/lgbm_ranker_multi/20260620/lgbm_ranker_multi_20260620.txt \\
        --start 2025-12-20 --end 2026-06-14

    # 期間未指定時は meta.json の検証期間（valid_from/valid_to）を自動使用
    .venv/bin/python scripts/evaluate_calibration.py --project-id <PROJECT_ID> --model <GCS_URI>
"""

import argparse
import json
import logging
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import bigquery

from src.models.calibration import (
    apply_isotonic_calibration,
    compute_calibration_metrics,
    fit_calibration_isotonic,
    fit_calibration_temperature,
    normalize_win_place_prob,
)
from src.models.lgbm_ranker_multi import LGBMRankerMulti
from src.models.predict import _load_model
from src.models.train import build_feature_matrix, load_config

logger = logging.getLogger(__name__)


def _read_meta_period(model_path: str, tmp_dir: str) -> tuple[str | None, str | None]:
    """meta.json から検証期間（valid_from/valid_to）を読む。"""
    local_meta = Path(tmp_dir) / (Path(model_path).stem + ".meta.json")
    if not local_meta.exists():
        return None, None
    meta = json.loads(local_meta.read_text())
    tp = meta.get("training_period", {})
    return tp.get("valid_from"), tp.get("valid_to")


def _print_reliability(title: str, m: dict) -> None:
    print(f"\n=== {title} ===")
    rel = m["reliability"].copy()
    rel["pred_mean"] = (rel["pred_mean"] * 100).round(1).astype(str) + "%"
    rel["actual_rate"] = (rel["actual_rate"] * 100).round(1).astype(str) + "%"
    rel["gap"] = (m["reliability"]["gap"] * 100).round(1).map(lambda x: f"{x:+.1f}pt")
    rel.columns = ["予測確率ビン", "件数", "予測平均", "実際の複勝率", "乖離(実-予測)"]
    print(rel.to_string(index=False))
    print(
        f"全体: n={m['n']}  実複勝率={m['mean_actual']:.3f}  予測平均={m['mean_pred']:.3f}  "
        f"Brier={m['brier']:.4f}  LogLoss={m['log_loss']:.4f}  ECE={m['ece']:.4f}"
    )


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="win_place_prob キャリブレーション評価")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--model", required=True, help="モデルパス（GCS URI またはローカル）")
    parser.add_argument("--start", default=None, help="評価開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="評価終了日 (YYYY-MM-DD)")
    parser.add_argument(
        "--temperature",
        type=float,
        default=None,
        help="校正温度（未指定時は meta.json 保存値、それも無ければ out-of-sample でフィット）",
    )
    parser.add_argument(
        "--method",
        choices=["isotonic", "temperature", "both"],
        default="both",
        help="校正手法。isotonic（Issue #416・既定推奨）/ temperature（Issue #414）/ both（両方比較）",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if not args.project_id:
        logger.error("GCP_PROJECT_ID が設定されていません")
        return 1

    config = load_config()
    data_cfg = config["data"]
    client = bigquery.Client(project=args.project_id)

    with tempfile.TemporaryDirectory() as tmp:
        ranker = _load_model(LGBMRankerMulti, args.project_id, args.model, tmp)

        start, end = args.start, args.end
        if not (start and end):
            meta_start, meta_end = _read_meta_period(args.model, tmp)
            start = start or meta_start
            end = end or meta_end
        if not (start and end):
            logger.error("評価期間を特定できません（--start/--end か meta.json が必要）")
            return 1
        logger.info(f"評価期間: {start} ~ {end}")

        # 1. 特徴量取得
        df = client.query(
            f"""
            SELECT * FROM `{args.project_id}.features.training_data`
            WHERE race_date BETWEEN DATE '{start}' AND DATE '{end}'
            """
        ).to_dataframe()
        df["race_date"] = pd.to_datetime(df["race_date"]).dt.date
        df = df.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)
        logger.info(f"特徴量: {len(df)} 行 / {df['race_id'].nunique()} レース")

        # 2. 予測
        X = build_feature_matrix(
            df,
            exclude_columns=data_cfg["exclude_columns"],
            categorical_columns=data_cfg.get("categorical_columns", []),
        )
        df["pred_score"] = ranker.predict(X)

    # 3. 実績結合
    res = client.query(
        f"""
        SELECT race_id, horse_number, finish_position
        FROM `{args.project_id}.raw.race_results`
        WHERE race_date BETWEEN DATE '{start}' AND DATE '{end}'
        """
    ).to_dataframe()
    df = df.merge(res, on=["race_id", "horse_number"], how="inner")
    df = df[df["finish_position"] > 0].copy()
    df["is_place"] = (df["finish_position"] <= 3).astype(int)
    logger.info(f"実績結合後: {len(df)} 行")

    labels = df["is_place"].values

    # 4. 校正前（temperature=1.0・未校正）の指標
    probs_before = normalize_win_place_prob(df, temperature=1.0)["win_place_prob"].values
    m_before = compute_calibration_metrics(probs_before, labels)
    _print_reliability(f"校正前（temperature=1.0）モデル={Path(args.model).stem}", m_before)

    # 5. 各手法の校正後指標を算出
    results: dict[str, dict] = {}

    if args.method in ("temperature", "both"):
        # 校正温度の決定
        if args.temperature is not None:
            temperature, t_src = args.temperature, "CLI指定"
        elif ranker.calibration_temperature is not None:
            temperature, t_src = ranker.calibration_temperature, "meta.json"
        else:
            temperature, t_src = fit_calibration_temperature(df), "out-of-sampleフィット"
        logger.info(f"校正温度={temperature:.4f}（{t_src}）")
        probs_t = normalize_win_place_prob(df, temperature=temperature)["win_place_prob"].values
        m_t = compute_calibration_metrics(probs_t, labels)
        results["温度"] = m_t
        _print_reliability(f"校正後（temperature={temperature:.4f}・{t_src}）", m_t)

    if args.method in ("isotonic", "both"):
        # meta.json に保存済みの校正器があれば使用、無ければ out-of-sample でフィット
        if getattr(ranker, "calibration_isotonic", None):
            calibrator, i_src = ranker.calibration_isotonic, "meta.json"
        else:
            calibrator, i_src = fit_calibration_isotonic(df), "out-of-sampleフィット"
        logger.info(f"アイソトニック校正（{i_src}・閾値{len(calibrator['x_thresholds'])}点）")
        probs_i = apply_isotonic_calibration(df, calibrator)["win_place_prob"].values
        m_i = compute_calibration_metrics(probs_i, labels)
        results["アイソトニック"] = m_i
        _print_reliability(f"校正後（アイソトニック・{i_src}）", m_i)

    print("\n=== 改善サマリ（校正前 → 校正後） ===")
    for name, m in results.items():
        print(f"[{name}]")
        print(f"  ECE     : {m_before['ece']:.4f} → {m['ece']:.4f} ({m['ece'] - m_before['ece']:+.4f})")
        print(f"  Brier   : {m_before['brier']:.4f} → {m['brier']:.4f} ({m['brier'] - m_before['brier']:+.4f})")
        print(f"  LogLoss : {m_before['log_loss']:.4f} → {m['log_loss']:.4f} ({m['log_loss'] - m_before['log_loss']:+.4f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
