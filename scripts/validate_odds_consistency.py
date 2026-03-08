#!/usr/bin/env python3
"""
JRDB基準オッズ（raw.combo_odds）と netkeiba当日オッズ（predictions.daily_odds_combo）の
整合性を検証するスクリプト。

Usage:
    python scripts/validate_odds_consistency.py \\
        --project-id <PROJECT_ID> \\
        --date 2026-03-08 \\
        --bet-type wide \\
        --deviation-threshold 0.5
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_consistency(
    project_id: str,
    date: str,
    bet_type: str,
    deviation_threshold: float = 0.5,
    output_csv: Optional[str] = None,
) -> None:
    client = bigquery.Client(project=project_id)

    # JRDB基準オッズと netkeiba当日オッズを照合
    # race_id形式変換: raw.race_info経由でJRDB(8文字) ↔ netkeiba(12桁)を橋渡し
    query = f"""
    WITH jrdb AS (
        SELECT
            j.race_id AS jrdb_race_id,
            j.bet_type,
            j.horse_number_1,
            j.horse_number_2,
            j.horse_number_3,
            j.odds_value AS jrdb_odds,
            -- netkeiba race_id への変換 (raw.race_info 経由)
            ri.netkeiba_race_id
        FROM `{project_id}.raw.combo_odds` j
        LEFT JOIN (
            SELECT
                race_id,
                -- netkeiba形式: YYYY + venue_code(2桁) + 回(2桁) + 日(2桁) + R(2桁)
                -- ※ 実際のJOINキーはraw.race_infoのスキーマに合わせて調整
                CONCAT(
                    FORMAT('%04d', CAST(SUBSTR(race_id, 3, 2) AS INT64) + 2000),
                    LPAD(CAST(SUBSTR(race_id, 1, 2) AS STRING), 2, '0'),
                    '01',
                    LPAD(CAST(SUBSTR(race_id, 7, 2) AS STRING), 2, '0'),
                    '01'
                ) AS netkeiba_race_id
            FROM `{project_id}.raw.race_info`
            WHERE race_date = '{date}'
        ) ri ON j.race_id = ri.race_id
        WHERE j.race_date = '{date}'
          AND j.bet_type = '{bet_type}'
    ),
    netkeiba AS (
        SELECT
            race_id AS netkeiba_race_id,
            ticket_type AS bet_type,
            horse_number_1,
            horse_number_2,
            horse_number_3,
            odds AS netkeiba_odds
        FROM `{project_id}.predictions.daily_odds_combo`
        WHERE race_date = '{date}'
          AND ticket_type = '{bet_type}'
    )
    SELECT
        j.jrdb_race_id,
        j.bet_type,
        j.horse_number_1,
        j.horse_number_2,
        j.horse_number_3,
        j.jrdb_odds,
        n.netkeiba_odds,
        CASE
            WHEN j.jrdb_odds > 0 AND n.netkeiba_odds IS NOT NULL
            THEN ABS(j.jrdb_odds - n.netkeiba_odds) / j.jrdb_odds
            ELSE NULL
        END AS deviation_rate
    FROM jrdb j
    LEFT JOIN netkeiba n
        ON j.netkeiba_race_id = n.netkeiba_race_id
        AND j.horse_number_1 = n.horse_number_1
        AND j.horse_number_2 = n.horse_number_2
        AND COALESCE(j.horse_number_3, -1) = COALESCE(n.horse_number_3, -1)
    ORDER BY deviation_rate DESC NULLS LAST
    """

    logger.info(f"整合性検証実行: 日付={date}, 馬券種={bet_type}")
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.warning("データが見つかりませんでした")
        return

    # サマリー表示
    total = len(df)
    matched = df["netkeiba_odds"].notna().sum()
    jrdb_only = total - matched
    high_deviation = (df["deviation_rate"] > deviation_threshold).sum()
    avg_deviation = df["deviation_rate"].mean()

    logger.info("=== 整合性検証サマリー ===")
    logger.info(f"  総組み合わせ数 (JRDB): {total}")
    logger.info(f"  netkeiba照合済み: {matched} ({matched/total*100:.1f}%)")
    logger.info(f"  JRDBのみ (netkeiba欠損): {jrdb_only}")
    logger.info(f"  乖離率 > {deviation_threshold*100:.0f}%: {high_deviation}件")
    if not pd.isna(avg_deviation):
        logger.info(f"  平均乖離率: {avg_deviation:.3f}")
    else:
        logger.info("  平均乖離率: N/A")

    if output_csv:
        df.to_csv(output_csv, index=False)
        logger.info(f"結果を保存: {output_csv}")
    else:
        # 上位10件を表示
        logger.info("\n=== 乖離率上位10件 ===")
        top = df[df["deviation_rate"].notna()].head(10)
        for _, row in top.iterrows():
            h3_str = f"-{int(row['horse_number_3'])}" if pd.notna(row.get('horse_number_3')) else ""
            logger.info(
                f"  {row['jrdb_race_id']} {bet_type} "
                f"{int(row['horse_number_1'])}-{int(row['horse_number_2'])}{h3_str}"
                f": JRDB={row['jrdb_odds']:.1f} netkeiba={row['netkeiba_odds']:.1f} "
                f"乖離率={row['deviation_rate']:.1%}"
            )


def main():
    parser = argparse.ArgumentParser(description="JRDB vs netkeiba オッズ整合性検証")
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--date", required=True, help="検証対象日 (YYYY-MM-DD)")
    parser.add_argument("--bet-type", choices=["wide", "sanrenpuku", "umaren"], default="wide")
    parser.add_argument("--deviation-threshold", type=float, default=0.5)
    parser.add_argument("--output-csv", help="結果のCSV出力先")
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または GCP_PROJECT_ID を設定してください")

    validate_consistency(
        project_id=args.project_id,
        date=args.date,
        bet_type=args.bet_type,
        deviation_threshold=args.deviation_threshold,
        output_csv=args.output_csv,
    )


if __name__ == "__main__":
    main()
