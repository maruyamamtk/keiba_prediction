#!/usr/bin/env python3
"""
raw.pedigree テーブルを horse_master から構築するスクリプト。

## 背景と設計思想

JRDBのUKCファイルは親馬の「名前」しか保持しておらず、
親馬のhorse_id（dam_id / sire_id）は直接取得できない。

このスクリプトでは raw.horse_master を自己JOINし、
  dam_name = horse_master.horse_name
という名前マッチングで dam_id を解決する。

## 制約事項

- 母馬がJRDB収録レース（raw.horse_master）に存在する場合のみ dam_id が付与される。
  → 外国産馬の母、JRDB収録前に引退した母はNULL。
- 同名の馬が複数存在する場合は牝馬（sex_code=2）を優先して1件を選択する。
- sire_id / dam_sire_id は名前マッチングが不安定（牡馬は数が多い）なため NULL とする。
  → feature_query_raw.sql は dam_id のみを使用しており実用上問題なし。

## 実行タイミング

UKCファイルの大量ロード（full_load / 定期更新）後に実行することで
horse_master の最新情報を pedigree に反映できる。

Usage:
    python scripts/build_pedigree_table.py --project-id <PROJECT_ID>
    python scripts/build_pedigree_table.py --project-id <PROJECT_ID> --dry-run
"""
import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


BUILD_QUERY = """
CREATE OR REPLACE TABLE `{project_id}`.raw.pedigree
AS
WITH dam_lookup AS (
  -- 牝馬優先で dam_name に対応する horse_id を1件に絞る
  SELECT
    horse_name,
    horse_id,
    ROW_NUMBER() OVER (
      PARTITION BY horse_name
      ORDER BY sex_code = 2 DESC, horse_id
    ) AS rn
  FROM `{project_id}`.raw.horse_master
  WHERE horse_name IS NOT NULL
)
SELECT
  h.horse_id,
  h.horse_name,
  NULL        AS sire_id,
  h.sire_name,
  d.horse_id  AS dam_id,
  h.dam_name,
  NULL        AS dam_sire_id,
  h.broodmare_sire_name AS dam_sire_name,
  CAST(h.sire_line_code AS STRING) AS sire_line,
  EXTRACT(YEAR FROM h.birth_date) AS birth_year,
  h.sex,
  CAST(h.coat_color_code AS STRING) AS coat_color,
  h.breeder_name AS breeder,
  h.owner_name   AS owner,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM `{project_id}`.raw.horse_master AS h
LEFT JOIN dam_lookup AS d
  ON h.dam_name = d.horse_name
  AND d.rn = 1
"""


def build_pedigree(project_id: str, dry_run: bool = False) -> None:
    client = bigquery.Client(project=project_id)

    query = BUILD_QUERY.format(project_id=project_id)

    if dry_run:
        logger.info("[DRY RUN] 以下のクエリを実行します:\n%s", query)
        return

    logger.info("raw.pedigree を horse_master から再構築します...")
    job = client.query(query)
    job.result()

    # 結果確認クエリ
    stats_query = f"""
    SELECT
      COUNT(*) AS total,
      COUNTIF(dam_id IS NOT NULL) AS with_dam_id,
      COUNTIF(dam_id IS NULL) AS without_dam_id
    FROM `{project_id}`.raw.pedigree
    """
    rows = list(client.query(stats_query).result())
    row = rows[0]
    logger.info(
        "raw.pedigree 再構築完了: "
        "合計=%d行, dam_id解決=%d件 (%.1f%%), 未解決=%d件",
        row.total,
        row.with_dam_id,
        row.with_dam_id / row.total * 100 if row.total > 0 else 0,
        row.without_dam_id,
    )
    logger.info(
        "注意: dam_id が未解決の馬は母馬がJRDB未収録のため。"
        "horse_master 更新後に本スクリプトを再実行することで解決率が上がる場合があります。"
    )


def main():
    parser = argparse.ArgumentParser(
        description="raw.pedigree テーブルを raw.horse_master から再構築する"
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実行せずにクエリを表示する",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id または環境変数 GCP_PROJECT_ID を設定してください")

    build_pedigree(args.project_id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
