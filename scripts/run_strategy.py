#!/usr/bin/env python3
"""
日次投資戦略策定スクリプト

config/strategy_config.yaml のパラメータを読み込み、
当日の予測結果（predictions.daily_predictions）とリアルタイムオッズ
（predictions.daily_odds / predictions.daily_odds_combo / raw.combo_odds）を JOIN して
投資判断を実行し、predictions.investment_decisions テーブルに保存する。

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
Issue #161: 全馬券種（複勝/ワイド/三連複/単勝/馬連）の正しい保存対応
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


def fetch_combo_odds_for_date(
    client: bigquery.Client,
    project_id: str,
    race_ids: list[str],
) -> pd.DataFrame:
    """
    コンボオッズを取得する（当日分）

    取得優先度:
      - 馬連・ワイド: predictions.daily_odds_combo を優先し、未取得レースは raw.combo_odds で補完
      - 三連複: predictions.daily_odds_combo は人気順上位100件の上限があるため、
                daily_odds_combo（リアルタイムオッズ）+ raw.combo_odds（全件）をマージして使用。
                同一組み合わせは daily_odds_combo（netkeiba実オッズ）を優先する。

    返り値の統一スキーマ:
        race_id, bet_type, horse_number_1, horse_number_2, horse_number_3, odds_value

    Args:
        client: BigQuery クライアント
        project_id: GCP プロジェクト ID
        race_ids: オッズを取得するレース ID のリスト

    Returns:
        コンボオッズ DataFrame（なければ空 DataFrame）
    """
    if not race_ids:
        return pd.DataFrame()

    ticket_types = ["wide", "sanrenpuku", "umaren"]
    ids_str = ", ".join(f"'{r}'" for r in race_ids)
    types_str = ", ".join(f"'{t}'" for t in ticket_types)
    _UNIFIED_SCHEMA = ["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3", "odds_value"]

    def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
        for col in _UNIFIED_SCHEMA:
            if col not in df.columns:
                df[col] = None
        return df[_UNIFIED_SCHEMA]

    # Stage 1: predictions.daily_odds_combo（netkeibaリアルタイムオッズ）
    df1 = pd.DataFrame()
    try:
        query1 = f"""
        SELECT race_id, ticket_type AS bet_type,
               horse_number_1, horse_number_2, horse_number_3,
               odds AS odds_value
        FROM `{project_id}.predictions.daily_odds_combo`
        WHERE ticket_type IN ({types_str})
          AND race_id IN ({ids_str})
        """
        df1 = client.query(query1).to_dataframe()
        if len(df1) > 0:
            df1 = _ensure_schema(df1)
            logger.info(f"predictions.daily_odds_combo から {len(df1)} 件取得")
        else:
            logger.info("predictions.daily_odds_combo にデータなし → raw.combo_odds へ")
            df1 = pd.DataFrame()
    except Exception as e:
        logger.info(f"predictions.daily_odds_combo が存在しないか取得失敗: {e} → raw.combo_odds へ")

    # Stage 2: raw.combo_odds（JRDB基準オッズ・全組み合わせ）
    # 三連複は housiki=c99 でも上位100件までしか取得できないため、常に raw.combo_odds でマージする
    df2 = pd.DataFrame()
    try:
        query2 = f"""
        SELECT race_id, bet_type,
               horse_number_1, horse_number_2, horse_number_3,
               odds_value
        FROM `{project_id}.raw.combo_odds`
        WHERE bet_type IN ({types_str})
          AND race_id IN ({ids_str})
        """
        df2 = client.query(query2).to_dataframe()
        if len(df2) > 0:
            df2 = _ensure_schema(df2)
            logger.info(f"raw.combo_odds から {len(df2)} 件取得")
        else:
            logger.info("raw.combo_odds にもデータなし")
    except Exception as e:
        logger.info(f"raw.combo_odds が存在しないか取得失敗: {e}")

    if df1.empty and df2.empty:
        return pd.DataFrame()

    if df1.empty:
        return df2

    if df2.empty:
        return df1

    # Stage 1 を優先してマージ: Stage 1 にない組み合わせのみ Stage 2 から補完
    df1_keys = set(
        zip(df1["race_id"], df1["bet_type"], df1["horse_number_1"],
            df1["horse_number_2"], df1["horse_number_3"])
    )
    df2_supplement = df2[
        ~df2.apply(
            lambda r: (r["race_id"], r["bet_type"], r["horse_number_1"],
                       r["horse_number_2"], r["horse_number_3"]) in df1_keys,
            axis=1,
        )
    ]
    merged = pd.concat([df1, df2_supplement], ignore_index=True)
    logger.info(
        f"コンボオッズマージ完了: daily_odds_combo {len(df1)} 件"
        f" + raw.combo_odds補完 {len(df2_supplement)} 件 = 計 {len(merged)} 件"
    )
    return merged[_UNIFIED_SCHEMA]


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

    cols = ["race_id", "horse_number", "place_odds_min"]
    if "win_odds" in odds_df.columns:
        cols.append("win_odds")
    merged = predictions_df.merge(
        odds_df[cols],
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
    target_date: datetime.date | None = None,
) -> int:
    """
    投資判断結果を predictions.investment_decisions テーブルに保存する。

    target_date を指定した場合（日次全件置換モード）:
        対象日の全レコードを race_date で DELETE してから INSERT する。
        ベットなしになったレースの旧データも確実に消去できる。

    target_date を省略した場合（1レース更新モード）:
        decisions に含まれる race_id のみを DELETE してから MERGE する。
        発走直前リフレッシュ（_refresh_investment_decisions_for_race）向け。

    Args:
        decisions: 投資判断リスト（各 dict は _build_decision_row() で生成）
        project_id: GCP プロジェクト ID
        target_date: 対象日。指定時は対象日の全レコードを先に削除する

    Returns:
        保存した行数
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.predictions.investment_decisions"

    if target_date is not None:
        # 日次全件置換: 対象日の全レコードを削除（ベットなしになったレースも確実に消去）
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE race_date = '{target_date.isoformat()}'
        """
        client.query(delete_query).result()
        logger.info(f"対象日 {target_date} の既存行を全削除")

    if not decisions:
        logger.info("投資判断なし（保存スキップ）")
        return 0

    df = pd.DataFrame(decisions)
    temp_table = f"{table_ref}_temp_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"

    _SCHEMA = [
        bigquery.SchemaField("race_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("race_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("horse_numbers", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("horse_names", "STRING"),
        bigquery.SchemaField("venue_code", "STRING"),
        bigquery.SchemaField("race_number", "INTEGER"),
        bigquery.SchemaField("race_pattern", "STRING"),
        bigquery.SchemaField("bet_type", "STRING"),
        bigquery.SchemaField("bet_amount", "FLOAT64"),
        bigquery.SchemaField("win_place_prob", "FLOAT64"),
        bigquery.SchemaField("place_odds", "FLOAT64"),
        bigquery.SchemaField("expected_return", "FLOAT64"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=_SCHEMA,
    )
    client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    if target_date is not None:
        # 日付単位で全削除済みなので単純 INSERT
        insert_query = f"INSERT INTO `{table_ref}` SELECT * FROM `{temp_table}`"
        client.query(insert_query).result()
    else:
        # 1レース更新: 対象 race_id の既存行を削除してから MERGE
        # （BigQuery は WHEN 句での相関サブクエリ非対応のため先に DELETE）
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE race_id IN (SELECT DISTINCT race_id FROM `{temp_table}`)
        """
        client.query(delete_query).result()

        merge_query = f"""
        MERGE `{table_ref}` AS target
        USING `{temp_table}` AS source
        ON target.race_id = source.race_id
           AND target.bet_type = source.bet_type
           AND target.horse_numbers = source.horse_numbers
        WHEN MATCHED THEN UPDATE SET
            race_date = source.race_date,
            horse_names = source.horse_names,
            venue_code = source.venue_code,
            race_number = source.race_number,
            race_pattern = source.race_pattern,
            bet_type = source.bet_type,
            bet_amount = source.bet_amount,
            win_place_prob = source.win_place_prob,
            place_odds = source.place_odds,
            expected_return = source.expected_return,
            created_at = source.created_at
        WHEN NOT MATCHED BY TARGET THEN INSERT ROW
        """
        client.query(merge_query).result()

    client.delete_table(temp_table, not_found_ok=True)

    logger.info(f"投資判断を保存: {len(df)}行 → {table_ref}")
    return len(df)


def _build_decision_row(
    race_id: str,
    target_date: datetime.date,
    meta_row: pd.Series,
    race_group: pd.DataFrame,
    bet: dict,
    race_pattern_str: str,
    created_at: datetime.datetime,
) -> dict:
    """
    1馬券分の投資判断 dict を生成する（1行/馬券形式）

    Args:
        race_id: レース ID
        target_date: 対象日
        meta_row: レースのメタデータ行（venue_code, race_number）
        race_group: レースの全馬データ
        bet: select_bets_for_race() が返した馬券 dict
        race_pattern_str: レースパターン文字列
        created_at: レコード作成日時

    Returns:
        investment_decisions テーブルの1行に対応する dict
    """
    horse_numbers = bet.get("horse_numbers", [])
    bet_type = bet.get("bet_type", "place")
    odds = float(bet["odds"])
    bet_amount = float(bet["bet_amount"])

    # 馬名リストを取得
    horse_names_list = []
    for hn in horse_numbers:
        rows = race_group[race_group["horse_number"] == hn]
        if not rows.empty:
            horse_names_list.append(str(rows.iloc[0].get("horse_name", "")))
        else:
            horse_names_list.append("")

    horse_numbers_str = ",".join(str(h) for h in horse_numbers)
    horse_names_str = ",".join(horse_names_list)

    # 単複（単一馬番）のみ win_place_prob / expected_return を設定
    # 単勝（win）は win_prob がないため expected_return を計算しない（複勝率ベースの計算は誤り）
    win_place_prob = None
    expected_return = None
    if len(horse_numbers) == 1:
        rows = race_group[race_group["horse_number"] == horse_numbers[0]]
        if not rows.empty:
            prob = float(rows.iloc[0]["win_place_prob"])
            win_place_prob = prob
            if bet_type != "win":
                expected_return = round(prob * odds, 4)

    return {
        "race_id": str(race_id),
        "race_date": target_date,
        "horse_numbers": horse_numbers_str,
        "horse_names": horse_names_str,
        "venue_code": str(meta_row.get("venue_code", "")),
        "race_number": int(meta_row.get("race_number", 0)),
        "race_pattern": race_pattern_str,
        "bet_type": bet_type,
        "bet_amount": bet_amount,
        "win_place_prob": win_place_prob,
        "place_odds": odds,
        "expected_return": expected_return,
        "created_at": created_at,
    }


def run_daily_strategy(
    project_id: str,
    target_date: datetime.date,
    dry_run: bool = False,
) -> list[dict]:
    """
    当日の投資戦略を実行し、投資判断リストを返す。

    全馬券種（複勝/ワイド/三連複/馬連）に対応するため、
    combo_odds_df を取得して select_bets_for_race() に渡す。

    Args:
        project_id: GCP プロジェクト ID
        target_date: 対象日
        dry_run: True の場合 BQ への保存をスキップ

    Returns:
        投資判断リスト（1行/馬券形式）
    """
    config = load_strategy_config()
    expected_return_threshold = float(config.get("expected_return_threshold", 1.2))
    top_n = int(config.get("top_n", 5))
    budget_per_race = float(config.get("budget_per_race", 3000.0))
    min_bet_amount = config.get("min_bet_amount", 100.0)
    min_prob_threshold = config.get("min_prob_threshold", 0.10)
    prob_weight_r = float(config.get("prob_weight_r", 1.0))

    opt = config.get("optimization", {})
    logger.info(f"=== 日次投資戦略策定 ({target_date}) ===")
    logger.info(
        f"パラメータ: expected_return_threshold={expected_return_threshold}, "
        f"top_n={top_n}, budget_per_race={budget_per_race}, "
        f"min_bet_amount={min_bet_amount}, min_prob_threshold={min_prob_threshold}, "
        f"prob_weight_r={prob_weight_r}"
    )
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

    # コンボオッズ取得（ワイド/三連複/馬連に必要）
    race_ids = merged["race_id"].unique().tolist()
    combo_odds_df = fetch_combo_odds_for_date(client, project_id, race_ids)
    logger.info(f"コンボオッズ取得: {len(combo_odds_df)}件")

    # ループ内の O(n*m) フィルタを避けるため、レースIDでグループ化しておく
    combo_odds_by_race: dict[str, pd.DataFrame] = (
        {str(k): v for k, v in combo_odds_df.groupby("race_id")}
        if not combo_odds_df.empty else {}
    )

    decisions: list[dict] = []
    created_at = datetime.datetime.now(datetime.timezone.utc)

    for race_id, race_group in merged.groupby("race_id"):
        race_group = race_group.dropna(subset=["win_place_prob", "odds"])
        if len(race_group) < 3:
            continue

        race_id_str = str(race_id)
        race_combo_df = combo_odds_by_race.get(race_id_str, pd.DataFrame())

        try:
            bets = select_bets_for_race(
                race_df=race_group,
                combo_odds_df=race_combo_df if not race_combo_df.empty else None,
                budget_per_race=budget_per_race,
                expected_return_threshold=expected_return_threshold,
                min_bet_amount=min_bet_amount,
                min_prob_threshold=min_prob_threshold,
                prob_weight_r=prob_weight_r,
                top_n=top_n,
            )
        except ValueError as e:
            logger.debug(f"レース {race_id} スキップ: {e}")
            continue

        if not bets:
            continue

        meta_row = race_group.iloc[0]
        for bet in bets:
            if not bet.get("horse_numbers"):
                continue
            row = _build_decision_row(
                race_id=race_id_str,
                target_date=target_date,
                meta_row=meta_row,
                race_group=race_group,
                bet=bet,
                race_pattern_str="unified",
                created_at=created_at,
            )
            decisions.append(row)

    logger.info(f"投資判断: {len(decisions)}件")
    if decisions:
        total_bet = 0.0
        bet_types: dict[str, int] = {}
        for d in decisions:
            total_bet += d["bet_amount"]
            bet_types[d["bet_type"]] = bet_types.get(d["bet_type"], 0) + 1
        logger.info(f"  総賭け金: {total_bet:,.0f}円")
        logger.info(f"  馬券種別: {bet_types}")

    if not dry_run:
        save_decisions_to_bq(decisions, project_id, target_date=target_date)
    else:
        logger.info("(--dry-run: BQ保存をスキップ)")
        for d in decisions:
            prob_str = f"{d['win_place_prob']:.2%}" if d["win_place_prob"] is not None else "N/A"
            ev_str = f"{d['expected_return']:.2f}" if d["expected_return"] is not None else "N/A"
            logger.info(
                f"  race={d['race_id']} [{d['bet_type']}] 馬番={d['horse_numbers']} "
                f"({d['horse_names']}) パターン={d['race_pattern']} "
                f"賭={d['bet_amount']:.0f}円 prob={prob_str} "
                f"odds={d['place_odds']:.1f} 期待回収率={ev_str}"
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
    )


if __name__ == "__main__":
    main()
