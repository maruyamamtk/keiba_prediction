#!/usr/bin/env python3
"""
日次投資戦略策定スクリプト

config/strategy_config.yaml のパラメータを読み込み、
当日の予測結果（predictions.daily_predictions）とリアルタイムオッズ
（predictions.daily_odds）を JOIN して投資判断を実行し、
predictions.investment_decisions テーブルに保存する。

Cloud Scheduler からは POST /api/v1/strategy/daily で自動実行されるが、
このスクリプトを手動実行することで内容を確認できる。

前提条件:
  - scripts/run_strategy_optimization.py を一度実行して
    config/strategy_config.yaml にパラメータが保存されていること
  - 当日の predictions.daily_predictions が存在すること
  - 当日の predictions.daily_odds が存在すること

Usage:
    # 当日分を実行
    python scripts/run_strategy.py --project-id <PROJECT_ID>

    # 特定日を指定
    python scripts/run_strategy.py --project-id <PROJECT_ID> --target-date 2026-03-07

    # BQ保存をスキップ（確認のみ）
    python scripts/run_strategy.py --project-id <PROJECT_ID> --dry-run

Issue #105: 投資戦略モジュールをバックテストパイプラインに統合
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.strategy import select_bets_for_race

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

STRATEGY_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "strategy_config.yaml"


def load_strategy_config() -> dict:
    """config/strategy_config.yaml を読み込む"""
    if not STRATEGY_CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"strategy_config.yaml が見つかりません: {STRATEGY_CONFIG_PATH}\n"
            "先に scripts/run_strategy_optimization.py を実行してください。"
        )
    with open(STRATEGY_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config


def fetch_daily_predictions(
    client: bigquery.Client,
    project_id: str,
    target_date: datetime.date,
) -> pd.DataFrame:
    """predictions.daily_predictions から当日の予測を取得する"""
    query = f"""
    SELECT
        race_id, race_date, horse_id, horse_number, horse_name,
        venue_code, race_number, win_place_prob, pred_score, rank_in_race
    FROM `{project_id}.predictions.daily_predictions`
    WHERE race_date = '{target_date.isoformat()}'
    ORDER BY race_id, rank_in_race
    """
    df = client.query(query).to_dataframe()
    logger.info(f"予測データ取得: {len(df)}行 ({target_date})")
    return df


def fetch_daily_odds(
    client: bigquery.Client,
    project_id: str,
    target_date: datetime.date,
) -> pd.DataFrame:
    """predictions.daily_odds から当日のオッズを取得する"""
    query = f"""
    SELECT race_id, horse_number, win_odds, place_odds_min, place_odds_max, scraped_at
    FROM `{project_id}.predictions.daily_odds`
    WHERE race_date = '{target_date.isoformat()}'
    """
    df = client.query(query).to_dataframe()
    logger.info(f"オッズデータ取得: {len(df)}行 ({target_date})")
    return df


def build_race_df(
    predictions_df: pd.DataFrame,
    odds_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    predictions と odds を JOIN して strategy が要求するカラム構成にする

    strategy.select_bets_for_race が要求する必須カラム:
      horse_id, horse_number, win_place_prob, odds（複勝オッズ）
    """
    if odds_df.empty:
        logger.warning("オッズデータが空です。place_odds_min=NaN として続行します。")
        predictions_df = predictions_df.copy()
        predictions_df["odds"] = float("nan")
        return predictions_df

    merged = predictions_df.merge(
        odds_df[["race_id", "horse_number", "place_odds_min"]],
        on=["race_id", "horse_number"],
        how="left",
    )
    merged = merged.rename(columns={"place_odds_min": "odds"})

    no_odds = merged["odds"].isna().sum()
    if no_odds > 0:
        logger.warning(f"{no_odds}頭のオッズが取得できていません（スキップ対象）")

    return merged


def save_decisions_to_bq(
    decisions: list[dict],
    project_id: str,
) -> int:
    """
    投資判断結果を predictions.investment_decisions テーブルに UPSERT 保存する

    Args:
        decisions: 投資判断リスト
        project_id: GCP プロジェクト ID

    Returns:
        保存した行数
    """
    if not decisions:
        logger.info("投資判断なし（保存スキップ）")
        return 0

    df = pd.DataFrame(decisions)
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.predictions.investment_decisions"
    temp_table = f"{table_ref}_temp_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("race_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("race_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("horse_id", "STRING"),
            bigquery.SchemaField("horse_number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("horse_name", "STRING"),
            bigquery.SchemaField("venue_code", "STRING"),
            bigquery.SchemaField("race_number", "INTEGER"),
            bigquery.SchemaField("race_pattern", "STRING"),
            bigquery.SchemaField("bet_type", "STRING"),
            bigquery.SchemaField("bet_amount", "FLOAT64"),
            bigquery.SchemaField("win_place_prob", "FLOAT64"),
            bigquery.SchemaField("place_odds", "FLOAT64"),
            bigquery.SchemaField("expected_return", "FLOAT64"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_query = f"""
    MERGE `{table_ref}` AS target
    USING `{temp_table}` AS source
    ON target.race_id = source.race_id
       AND target.horse_number = source.horse_number
       AND target.bet_type = source.bet_type
    WHEN MATCHED THEN UPDATE SET
        race_date = source.race_date,
        horse_id = source.horse_id,
        horse_name = source.horse_name,
        venue_code = source.venue_code,
        race_number = source.race_number,
        race_pattern = source.race_pattern,
        bet_type = source.bet_type,
        bet_amount = source.bet_amount,
        win_place_prob = source.win_place_prob,
        place_odds = source.place_odds,
        expected_return = source.expected_return,
        created_at = source.created_at
    WHEN NOT MATCHED THEN INSERT ROW
    """
    client.query(merge_query).result()
    client.delete_table(temp_table, not_found_ok=True)

    logger.info(f"投資判断を保存: {len(df)}行 → {table_ref}")
    return len(df)


def run_daily_strategy(
    project_id: str,
    target_date: datetime.date,
    dry_run: bool = False,
    initial_capital: float = 100_000.0,
) -> list[dict]:
    """
    当日の投資戦略を実行し、投資判断リストを返す

    Args:
        project_id: GCP プロジェクト ID
        target_date: 対象日
        dry_run: True の場合 BQ への保存をスキップ
        initial_capital: 初期資金（Kelly 計算用）

    Returns:
        投資判断リスト
    """
    config = load_strategy_config()
    p1 = config["p1"]
    threshold = config["expected_return_threshold"]
    max_bet_ratio = config["max_bet_ratio"]
    min_bet_amount = config.get("min_bet_amount", 100.0)
    top_n = config.get("top_n", 5)

    opt = config.get("optimization", {})
    logger.info(f"=== 日次投資戦略策定 ({target_date}) ===")
    logger.info(f"パラメータ: p1={p1}, threshold={threshold}, "
                f"max_bet_ratio={max_bet_ratio}, min_bet_amount={min_bet_amount}, top_n={top_n}")
    if opt.get("last_run"):
        logger.info(f"最終最適化: {opt['last_run']} "
                    f"(回収率={opt.get('recovery_rate')}%, 的中率={opt.get('hit_rate')}%)")

    client = bigquery.Client(project=project_id)
    predictions_df = fetch_daily_predictions(client, project_id, target_date)
    odds_df = fetch_daily_odds(client, project_id, target_date)

    if predictions_df.empty:
        logger.warning(f"予測データが見つかりません: {target_date}")
        return []

    merged = build_race_df(predictions_df, odds_df)

    decisions: list[dict] = []
    capital = initial_capital
    created_at = datetime.datetime.now(datetime.timezone.utc)

    for race_id, race_group in merged.groupby("race_id"):
        race_group = race_group.dropna(subset=["win_place_prob", "odds"])
        if len(race_group) < 3:
            continue

        try:
            bets, race_pattern = select_bets_for_race(
                race_df=race_group,
                capital=capital,
                p1=p1,
                expected_return_threshold=threshold,
                max_bet_ratio=max_bet_ratio,
                min_bet_amount=min_bet_amount,
                top_n=top_n,
            )
        except ValueError as e:
            logger.debug(f"レース {race_id} スキップ: {e}")
            continue

        if not bets:
            continue

        meta_row = race_group.iloc[0]
        for bet in bets:
            horse_numbers = bet.get("horse_numbers", [])
            if not horse_numbers:
                continue
            bet_type = bet.get("bet_type", "place")
            odds = float(bet["odds"])
            bet_amount = float(bet["bet_amount"])
            capital -= bet_amount
            # 馬ごとに1行保存（マルチ馬券は関連する各馬を個別に記録）
            for hn in horse_numbers:
                horse_row = race_group[race_group["horse_number"] == hn]
                if horse_row.empty:
                    continue
                hr = horse_row.iloc[0]
                prob = float(hr["win_place_prob"])
                horse_id = str(bet.get("horse_id") or hr.get("horse_id", ""))
                decisions.append({
                    "race_id": str(race_id),
                    "race_date": target_date,
                    "horse_id": horse_id,
                    "horse_number": int(hn),
                    "horse_name": str(hr.get("horse_name", "")),
                    "venue_code": str(meta_row.get("venue_code", "")),
                    "race_number": int(meta_row.get("race_number", 0)),
                    "race_pattern": race_pattern.pattern,
                    "bet_type": bet_type,
                    "bet_amount": bet_amount,
                    "win_place_prob": prob,
                    "place_odds": odds,
                    "expected_return": round(prob * odds, 4),
                    "created_at": created_at,
                })

    logger.info(f"投資判断: {len(decisions)}件")
    if decisions:
        total_bet = sum(d["bet_amount"] for d in decisions)
        patterns = {}
        for d in decisions:
            patterns[d["race_pattern"]] = patterns.get(d["race_pattern"], 0) + 1
        logger.info(f"  総賭け金: {total_bet:,.0f}円")
        logger.info(f"  パターン別: {patterns}")

    if not dry_run:
        save_decisions_to_bq(decisions, project_id)
    else:
        logger.info("(--dry-run: BQ保存をスキップ)")
        for d in decisions:
            logger.info(
                f"  race={d['race_id']} 馬番={d['horse_number']} "
                f"パターン={d['race_pattern']} 賭={d['bet_amount']:.0f}円 "
                f"prob={d['win_place_prob']:.2%} odds={d['place_odds']:.1f} "
                f"期待回収率={d['expected_return']:.2f}"
            )

    return decisions


def main() -> None:
    parser = argparse.ArgumentParser(description="日次投資戦略策定")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument(
        "--target-date",
        help="対象日（YYYY-MM-DD、省略時は当日）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="BQへの保存をスキップして結果を表示のみ",
    )
    parser.add_argument("--initial-capital", type=float, default=100_000.0)
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID 環境変数を設定してください")

    target_date = (
        datetime.date.fromisoformat(args.target_date)
        if args.target_date
        else datetime.date.today()
    )

    run_daily_strategy(
        project_id=args.project_id,
        target_date=target_date,
        dry_run=args.dry_run,
        initial_capital=args.initial_capital,
    )


if __name__ == "__main__":
    main()
