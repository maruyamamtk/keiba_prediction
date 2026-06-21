#!/usr/bin/env python3
"""
バックテスト実行スクリプト

指定期間の過去データに対してモデル予測を生成し、
複勝・ワイド・三連複・馬連を対象とした投資シミュレーション（バックテスト）を実行する。
strategy_config.yaml と同一ロジック（select_bets_for_race）を使用する。

Usage:
    python scripts/run_backtest.py \\
        --project-id <PROJECT_ID> \\
        --model-path <MODEL_PATH> \\
        --start-date 2023-01-01 \\
        --end-date 2023-12-31

    python scripts/run_backtest.py \\
        --project-id <PROJECT_ID> \\
        --model-path <MODEL_PATH> \\
        --start-date 2023-01-01 \\
        --end-date 2023-12-31 \\
        --output-csv results/backtest_2023.csv \\
        --save-to-bq \\
        --initial-capital 200000 \\
        --threshold 1.2
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import bigquery

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtest.metrics import compute_metrics
from src.backtest.strategy_optimizer import StrategyOptimizer
from src.models.lgbm_ranker_multi import LGBMRankerMulti
from src.models.predict import normalize_win_place_prob
from src.models.train import (
    build_feature_matrix,
    load_config,
)

logger = logging.getLogger(__name__)

_STRATEGY_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "strategy_config.yaml"


# ---------------------------------------------------------------------------
# BigQuery データ取得
# ---------------------------------------------------------------------------

def fetch_historical_features(
    project_id: str,
    dataset: str,
    table: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """
    指定期間の特徴量データを BigQuery から取得する

    Args:
        project_id: GCP プロジェクト ID
        dataset: データセット名
        table: テーブル名
        start_date: 開始日（含む）
        end_date: 終了日（含む）

    Returns:
        特徴量 DataFrame
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT *
    FROM `{project_id}.{dataset}.{table}`
    WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY race_date, race_id, horse_number
    """
    logger.info(
        f"Fetching features from {project_id}.{dataset}.{table} "
        f"({start_date} ~ {end_date})"
    )
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns")
    return df


def fetch_historical_results(
    project_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """
    指定期間のレース結果（着順）を raw.race_results から取得する

    Args:
        project_id: GCP プロジェクト ID
        start_date: 開始日
        end_date: 終了日

    Returns:
        着順 DataFrame (race_id, horse_id, finish_position)
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT race_id, horse_id, finish_position
    FROM `{project_id}.raw.race_results`
    WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
    """
    logger.info(f"Fetching race results ({start_date} ~ {end_date})")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} race result rows")
    return df


def fetch_place_odds(
    project_id: str,
    race_ids: list[str],
) -> pd.DataFrame:
    """
    複勝オッズを以下の優先順位で取得する

    優先順位:
      1. predictions.daily_odds（netkeibaスクレイプ）: 精度が高い。スクレイプ開始日以降のみ有効
      2. raw.odds（JRDB基準オッズ）: 過去データを含む事前オッズ

    レースIDでカバー済みのレースは後段のステージをスキップする。
    両ソースにデータがない場合は空 DataFrame を返す。

    Args:
        project_id: GCP プロジェクト ID
        race_ids: オッズを取得するレース ID のリスト

    Returns:
        複勝・単勝オッズ DataFrame (race_id, horse_number, place_odds, win_odds)
        win_odds は predictions.daily_odds または raw.odds に存在する場合のみ付与される。
        存在しない場合は NaN となる。
    """
    if not race_ids:
        return pd.DataFrame()

    _BASE_SCHEMA = ["race_id", "horse_number", "place_odds"]
    client = bigquery.Client(project=project_id)
    ids_str = ", ".join(f"'{r}'" for r in race_ids)

    all_results: list[pd.DataFrame] = []
    remaining_ids = list(race_ids)

    # Stage 1: predictions.daily_odds（netkeibaスクレイプ）
    # win_odds も取得する（単勝が一切購入されないバグを修正: Issue #176）
    try:
        query1 = f"""
        SELECT race_id, horse_number, place_odds_min AS place_odds, win_odds
        FROM `{project_id}.predictions.daily_odds`
        WHERE race_id IN ({ids_str})
        """
        df1 = client.query(query1).to_dataframe()
        if len(df1) > 0:
            select_cols = [c for c in _BASE_SCHEMA + ["win_odds"] if c in df1.columns]
            all_results.append(df1[select_cols])
            covered_ids = set(df1["race_id"].unique())
            remaining_ids = [r for r in remaining_ids if r not in covered_ids]
            logger.info(
                f"predictions.daily_odds から {len(df1)} 件取得"
                f"（カバー: {len(covered_ids)} レース）"
            )
        else:
            logger.info("predictions.daily_odds にデータなし → raw.odds へ")
    except Exception as e:
        logger.info(f"predictions.daily_odds が存在しないか取得失敗: {e} → raw.odds へ")

    if not remaining_ids:
        return pd.concat(all_results, ignore_index=True)

    # Stage 2: raw.odds（JRDB基準オッズ）
    # place と win を同時に取得してピボットする（Issue #176）
    rem_ids_str = ", ".join(f"'{r}'" for r in remaining_ids)
    query2 = f"""
    SELECT
        race_id, horse_number,
        MAX(CASE WHEN odds_type = 'place' THEN odds_value END) AS place_odds,
        MAX(CASE WHEN odds_type = 'win'   THEN odds_value END) AS win_odds
    FROM (
        SELECT race_id, horse_number, odds_type, odds_value,
               ROW_NUMBER() OVER (
                   PARTITION BY race_id, horse_number, odds_type
                   ORDER BY odds_timestamp DESC
               ) AS rn
        FROM `{project_id}.raw.odds`
        WHERE odds_type IN ('place', 'win')
          AND race_id IN ({rem_ids_str})
    )
    WHERE rn = 1
    GROUP BY race_id, horse_number
    """
    try:
        df2 = client.query(query2).to_dataframe()
        if len(df2) > 0:
            select_cols = [c for c in _BASE_SCHEMA + ["win_odds"] if c in df2.columns]
            all_results.append(df2[select_cols])
            logger.info(f"raw.odds から {len(df2)} 件取得")
        else:
            logger.info("raw.odds にもデータなし")
    except Exception as e:
        logger.warning(f"raw.odds からのオッズ取得に失敗しました: {e}")

    if not all_results:
        return pd.DataFrame(columns=_BASE_SCHEMA)
    return pd.concat(all_results, ignore_index=True)


def fetch_place_payouts(
    project_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """
    指定期間の複勝払戻データを raw.payouts から取得する

    raw.payouts に race_date カラムがないため、raw.race_results と
    JOIN して日付フィルタリングする。

    Args:
        project_id: GCP プロジェクト ID
        start_date: 開始日
        end_date: 終了日

    Returns:
        複勝払戻 DataFrame (race_id, horse_number_1, payout_amount, bet_type)
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT DISTINCT p.race_id, p.horse_number_1, p.payout_amount, p.bet_type
    FROM `{project_id}.raw.payouts` p
    WHERE p.bet_type = 'place'
      AND p.race_id IN (
        SELECT DISTINCT race_id
        FROM `{project_id}.raw.race_results`
        WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
      )
    """
    logger.info(f"Fetching place payouts ({start_date} ~ {end_date})")
    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} place payout rows")
        return df
    except Exception as e:
        logger.warning(f"複勝払戻データの取得に失敗しました: {e}")
        return pd.DataFrame()


def fetch_combo_odds(
    project_id: str,
    race_ids: list[str],
    ticket_types: list[str] | None = None,
) -> pd.DataFrame:
    """
    コンボオッズを以下の優先順位で取得し、統一スキーマで返す。

    優先順位:
      1. predictions.daily_odds_combo（netkeiba当日オッズ）: 精度が高い。スクレイプ開始日以降のみ有効
      2. raw.combo_odds（JRDB基準オッズ）: 過去データを含む事前オッズ
      3. raw.payouts（確定払戻金）: ヒット馬券のみ、事後データ（最終フォールバック）

    返り値の統一スキーマ:
        race_id, bet_type, horse_number_1, horse_number_2, horse_number_3, odds_value

    Args:
        project_id: GCP プロジェクト ID
        race_ids: オッズを取得するレース ID のリスト
        ticket_types: 取得する馬券種別（None の場合は ['wide', 'sanrenpuku', 'umaren']）

    Returns:
        コンボオッズ DataFrame
    """
    if ticket_types is None:
        ticket_types = ["wide", "sanrenpuku", "umaren"]

    if not race_ids:
        return pd.DataFrame()

    _UNIFIED_SCHEMA = ["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3", "odds_value"]

    def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
        """統一スキーマを確保する（欠損列はNoneで補完）"""
        for col in _UNIFIED_SCHEMA:
            if col not in df.columns:
                df[col] = None
        return df[_UNIFIED_SCHEMA]

    all_results = []
    remaining_ids = list(race_ids)

    client = bigquery.Client(project=project_id)
    ids_str = ", ".join(f"'{r}'" for r in remaining_ids)
    types_str = ", ".join(f"'{t}'" for t in ticket_types)

    # Stage 1: predictions.daily_odds_combo
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
            all_results.append(df1)
            covered_ids = set(df1["race_id"].unique())
            remaining_ids = [r for r in remaining_ids if r not in covered_ids]
            logger.info(
                f"predictions.daily_odds_combo から {len(df1)} 件取得"
                f"（カバー: {len(covered_ids)} レース）"
            )
        else:
            logger.info("predictions.daily_odds_combo にデータなし → Stage 2 へ")
    except Exception as e:
        logger.info(f"predictions.daily_odds_combo が存在しないか取得失敗: {e} → Stage 2 へ")

    if not remaining_ids:
        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame(columns=_UNIFIED_SCHEMA)

    # Stage 2: raw.combo_odds
    rem_ids_str = ", ".join(f"'{r}'" for r in remaining_ids)
    try:
        query2 = f"""
        SELECT race_id, bet_type,
               horse_number_1, horse_number_2, horse_number_3,
               odds_value
        FROM `{project_id}.raw.combo_odds`
        WHERE bet_type IN ({types_str})
          AND race_id IN ({rem_ids_str})
        """
        df2 = client.query(query2).to_dataframe()
        if len(df2) > 0:
            df2 = _ensure_schema(df2)
            all_results.append(df2)
            covered_ids2 = set(df2["race_id"].unique())
            remaining_ids = [r for r in remaining_ids if r not in covered_ids2]
            logger.info(
                f"raw.combo_odds から {len(df2)} 件取得"
                f"（カバー: {len(covered_ids2)} レース）"
            )
        else:
            logger.info("raw.combo_odds にデータなし → Stage 3 へ")
    except Exception as e:
        logger.info(f"raw.combo_odds が存在しないか取得失敗: {e} → Stage 3 へ")

    if not remaining_ids:
        return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame(columns=_UNIFIED_SCHEMA)

    # Stage 3: raw.payouts（フォールバック）
    logger.warning(
        "raw.payoutsを使用: 的中馬券のみ存在するため回収率が過大評価になる可能性あり"
    )
    rem_ids_str3 = ", ".join(f"'{r}'" for r in remaining_ids)
    try:
        query3 = f"""
        SELECT race_id, bet_type,
               horse_number_1, horse_number_2, horse_number_3,
               (payout_amount / 100.0) AS odds_value
        FROM `{project_id}.raw.payouts`
        WHERE bet_type IN ({types_str})
          AND race_id IN ({rem_ids_str3})
        """
        df3 = client.query(query3).to_dataframe()
        if len(df3) > 0:
            df3 = _ensure_schema(df3)
            all_results.append(df3)
            logger.info(f"raw.payouts から {len(df3)} 件取得（フォールバック）")
    except Exception as e:
        logger.warning(f"raw.payouts からの取得に失敗しました: {e}")

    if not all_results:
        return pd.DataFrame(columns=_UNIFIED_SCHEMA)

    return pd.concat(all_results, ignore_index=True)


# ---------------------------------------------------------------------------
# 予測生成
# ---------------------------------------------------------------------------

def generate_predictions(
    features_df: pd.DataFrame,
    results_df: pd.DataFrame,
    model_path: str,
    config: dict,
) -> pd.DataFrame:
    """
    特徴量データからモデル予測を生成し、実際の着順を付与する

    Args:
        features_df: 特徴量 DataFrame
        results_df: 着順 DataFrame (race_id, horse_id, finish_position)
        model_path: モデルファイルパス
        config: 設定辞書

    Returns:
        予測結果 DataFrame (race_id, race_date, horse_id, horse_number,
                             win_place_prob, finish_position, odds_yesterday 等)
    """
    data_config = config["data"]

    ranker = LGBMRankerMulti()
    ranker.load(model_path)

    X = build_feature_matrix(
        features_df,
        exclude_columns=data_config["exclude_columns"],
        categorical_columns=data_config.get("categorical_columns", []),
    )

    scores = ranker.predict(X)

    result_df = features_df[
        ["race_id", "race_date", "horse_id", "horse_number"]
    ].copy()

    for col in ["venue_code", "race_number", "horse_name"]:
        if col in features_df.columns:
            result_df[col] = features_df[col]

    result_df["pred_score"] = scores

    # レースごとにソフトマックス正規化でwin_place_probを計算
    result_df = normalize_win_place_prob(result_df)

    # 着順の付与
    if len(results_df) > 0:
        result_df = result_df.merge(
            results_df[["race_id", "horse_id", "finish_position"]],
            on=["race_id", "horse_id"],
            how="left",
        )
    else:
        result_df["finish_position"] = np.nan

    return result_df.sort_values(
        ["race_date", "race_id", "horse_number"]
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 結果の保存・可視化
# ---------------------------------------------------------------------------

def save_to_bigquery(
    history_df: pd.DataFrame,
    metrics: dict,
    project_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    run_id: str,
) -> None:
    """
    バックテスト結果を BigQuery の backtests.backtest_results テーブルに保存する

    Args:
        history_df: 賭け記録 DataFrame
        metrics: 評価指標辞書
        project_id: GCP プロジェクト ID
        start_date: バックテスト開始日
        end_date: バックテスト終了日
        run_id: 実行 ID（バックテストの識別子）
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.backtests.backtest_results"

    save_df = history_df.copy()
    save_df["run_id"] = run_id
    save_df["backtest_start_date"] = start_date.isoformat()
    save_df["backtest_end_date"] = end_date.isoformat()
    save_df["created_at"] = pd.Timestamp.now(tz="UTC")

    # 型の整合
    for col in ["finish_position", "payout_per_100"]:
        if col in save_df.columns:
            save_df[col] = save_df[col].astype("Int64")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(save_df, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Saved {len(save_df)} records to {table_ref}")


def print_metrics(metrics: dict, initial_capital: float) -> None:
    """評価指標をコンソールに出力する"""
    print("\n" + "=" * 60)
    print("バックテスト評価指標")
    print("=" * 60)
    print(f"  初期資金:         ¥{initial_capital:>12,.0f}")
    print(f"  最終資金:         ¥{metrics['final_capital']:>12,.0f}")
    print(f"  損益:             ¥{metrics['profit']:>+12,.0f}")
    print(f"  回収率:            {metrics['recovery_rate']:>10.1f}%")
    print(f"  的中率:            {metrics['hit_rate']:>10.1f}%")
    print(f"  最大ドローダウン:  {metrics['max_drawdown']:>10.1f}%")
    print(f"  シャープレシオ:    {metrics['sharpe_ratio']:>10.3f}")
    print(f"  総賭け数:          {metrics['total_bets']:>10,d}")
    print(f"  総的中数:          {metrics['total_hits']:>10,d}")
    print(f"  総賭け金:         ¥{metrics['total_bet_amount']:>12,.0f}")
    print(f"  総払戻金:         ¥{metrics['total_return_amount']:>12,.0f}")
    print("=" * 60)


def plot_capital_curve(
    history_df: pd.DataFrame,
    initial_capital: float,
    output_path: str,
) -> None:
    """
    資金推移グラフを PNG として保存する

    Args:
        history_df: 賭け記録 DataFrame
        initial_capital: 初期資金
        output_path: 出力ファイルパス (.png)
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logger.warning("matplotlib が見つかりません。グラフ保存をスキップします。")
        return

    fig, axes = plt.subplots(2, 1, figsize=(12, 8))

    # 資金推移
    dates = pd.to_datetime(history_df["race_date"])
    capital_series = history_df["capital_after"].values
    initial_line = [initial_capital] * len(capital_series)

    ax1 = axes[0]
    ax1.plot(range(len(capital_series)), capital_series, label="資金", color="steelblue")
    ax1.axhline(y=initial_capital, color="gray", linestyle="--", alpha=0.7, label="初期資金")
    ax1.set_title("資金推移")
    ax1.set_ylabel("資金 (円)")
    ax1.legend()
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"¥{x:,.0f}"))
    ax1.grid(alpha=0.3)

    # 累積損益
    cumulative_profit = (history_df["capital_after"] - initial_capital).values
    ax2 = axes[1]
    colors = ["green" if p >= 0 else "red" for p in cumulative_profit]
    ax2.bar(range(len(cumulative_profit)), cumulative_profit, color=colors, alpha=0.6)
    ax2.axhline(y=0, color="black", linewidth=0.8)
    ax2.set_title("累積損益")
    ax2.set_ylabel("損益 (円)")
    ax2.set_xlabel("賭け回数")
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"¥{x:+,.0f}"))
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info(f"グラフを保存しました: {output_path}")


# ---------------------------------------------------------------------------
# バックテストパイプライン（select_bets_for_race 使用）
# ---------------------------------------------------------------------------

def _load_strategy_config(config_path: str | None = None) -> dict:
    """
    strategy_config.yaml を読み込む。
    存在しない場合は空辞書を返す。

    Args:
        config_path: 設定ファイルパス（None の場合はデフォルトパスを使用）

    Returns:
        設定辞書
    """
    path = Path(config_path) if config_path else _STRATEGY_CONFIG_PATH
    if not path.exists():
        logger.warning(f"strategy_config.yaml が見つかりません: {path}")
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _fetch_all_payouts(
    project_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> pd.DataFrame:
    """
    指定期間の全馬券種払戻データを raw.payouts から取得する

    フル戦略バックテスト用。place/win/wide/umaren/sanrenpuku をすべて取得する。

    Args:
        project_id: GCP プロジェクト ID
        start_date: 開始日
        end_date: 終了日

    Returns:
        払戻 DataFrame (race_id, horse_number_1, horse_number_2, horse_number_3,
                         payout_amount, bet_type)
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT DISTINCT
        p.race_id,
        p.horse_number_1,
        p.horse_number_2,
        p.horse_number_3,
        p.payout_amount,
        p.bet_type
    FROM `{project_id}.raw.payouts` p
    WHERE p.bet_type IN ('place', 'win', 'wide', 'umaren', 'sanrenpuku')
      AND p.race_id IN (
        SELECT DISTINCT race_id
        FROM `{project_id}.raw.race_results`
        WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
      )
    """
    logger.info(f"全払戻データ取得 ({start_date} ~ {end_date})")
    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Fetched {len(df)} payout rows (all bet types)")
        return df
    except Exception as e:
        logger.warning(f"全払戻データの取得に失敗しました: {e}")
        return pd.DataFrame()


def run_full_strategy_backtest_pipeline(
    project_id: str,
    model_path: str,
    start_date: datetime.date,
    end_date: datetime.date,
    config: dict,
    budget_per_race: float,
    min_prob_threshold: float,
    expected_return_threshold: float = 1.2,
    prob_weight_r: float = 1.0,
    top_n: int = 5,
    max_wide_odds: float | None = None,
    enabled_bet_types: list[str] | None = None,
    initial_capital: float = 100_000.0,
    output_csv: str | None = None,
    save_bq: bool = False,
    output_chart: str | None = None,
    run_id: str | None = None,
    # 後方互換性のための旧パラメータ（無視される）
    p1: float | None = None,
    prob_weight_r_dominant: float | None = None,
    prob_weight_r_standard: float | None = None,
    top_n_dominant: int | None = None,
    top_n_standard: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    フル戦略バックテストパイプラインを実行する

    select_bets_for_race() を使い、複勝・ワイド・三連複・馬連を含む
    フル戦略でシミュレーションを行う。
    strategy_config.yaml と同一ロジックで試算したい場合に使用する。

    Args:
        project_id: GCP プロジェクト ID
        model_path: 学習済みモデルのローカルパス
        start_date: バックテスト開始日
        end_date: バックテスト終了日
        config: モデル設定辞書
        budget_per_race: 1レースあたりの固定予算 (円)
        min_prob_threshold: 軸馬の最低複勝率
        expected_return_threshold: 期待回収率閾値
        prob_weight_r: 選定スコア係数 (score = odds * prob^r)
        top_n: 候補馬数
        max_wide_odds: ワイド購入の上限オッズ（None で無制限）
        initial_capital: 初期資金 (円)
        output_csv: CSV 出力パス (None でスキップ)
        save_bq: BigQuery 保存フラグ
        output_chart: グラフ出力パス (None でスキップ)
        run_id: 実行 ID (BigQuery 保存時に使用)

    Returns:
        (history_df, metrics) のタプル
    """
    data_config = config["data"]

    # 1. データ取得
    features_df = fetch_historical_features(
        project_id=project_id,
        dataset=data_config["dataset"],
        table=data_config["table"],
        start_date=start_date,
        end_date=end_date,
    )
    if len(features_df) == 0:
        logger.error("指定期間の特徴量データが存在しません")
        return pd.DataFrame(), {}

    results_df = fetch_historical_results(
        project_id=project_id,
        start_date=start_date,
        end_date=end_date,
    )

    # フル戦略では全馬券種の払戻が必要
    payouts_df = _fetch_all_payouts(
        project_id=project_id,
        start_date=start_date,
        end_date=end_date,
    )

    # 2. GCS URI の場合はモデルをローカルにダウンロード
    local_model_path = model_path
    if model_path.startswith("gs://"):
        import tempfile
        from google.cloud import storage as gcs_storage
        bucket_name, blob_path = model_path[5:].split("/", 1)
        local_model_path = str(Path(tempfile.mkdtemp()) / Path(blob_path).name)
        gcs_storage.Client(project=project_id).bucket(bucket_name).blob(blob_path).download_to_filename(local_model_path)
        logger.info(f"GCSからモデルをダウンロード: {local_model_path}")

    # 3. 予測生成
    predictions_df = generate_predictions(
        features_df=features_df,
        results_df=results_df,
        model_path=local_model_path,
        config=config,
    )

    # 4. 単複オッズ取得（predictions.daily_odds → raw.odds）
    race_ids = predictions_df["race_id"].unique().tolist()
    odds_df = fetch_place_odds(project_id=project_id, race_ids=race_ids)
    if len(odds_df) > 0:
        merge_odds_cols = ["race_id", "horse_number", "place_odds"]
        if "win_odds" in odds_df.columns:
            merge_odds_cols.append("win_odds")
        predictions_df = predictions_df.merge(
            odds_df[merge_odds_cols],
            on=["race_id", "horse_number"],
            how="left",
        )
        n_win_odds = predictions_df["win_odds"].notna().sum() if "win_odds" in predictions_df.columns else 0
        logger.info(f"複勝オッズを place_odds、単勝オッズを win_odds としてマージしました（win_odds付与: {n_win_odds}件）")
    else:
        logger.error(
            "predictions.daily_odds・raw.odds のどちらにも複勝オッズデータがありません。バックテストを中断します。\n"
            "  バックテストには事前オッズ（predictions.daily_odds または raw.odds）が必須です。"
        )
        return pd.DataFrame(), {}

    # 5. コンボオッズ取得（predictions.daily_odds_combo → raw.combo_odds → raw.payouts）
    combo_odds_df = fetch_combo_odds(project_id=project_id, race_ids=race_ids)
    logger.info(f"コンボオッズ取得: {len(combo_odds_df)} 件")

    # 5. StrategyOptimizer でシミュレーション実行（固定パラメータで1回）
    optimizer = StrategyOptimizer(
        predictions_df=predictions_df,
        payouts_df=payouts_df,
        initial_capital=initial_capital,
        combo_odds_df=combo_odds_df if len(combo_odds_df) > 0 else None,
        budget_per_race=budget_per_race,
        enabled_bet_types=enabled_bet_types,
    )

    history_df, _ = optimizer._run_simulation(
        expected_return_threshold=expected_return_threshold,
        min_prob_threshold=min_prob_threshold,
        prob_weight_r=prob_weight_r,
        top_n=top_n,
        max_wide_odds=max_wide_odds,
    )

    # 6. 評価指標計算
    metrics = compute_metrics(history_df, initial_capital)

    # 7. 結果保存
    if output_csv and len(history_df) > 0:
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        history_df.to_csv(output_csv, index=False)
        logger.info(f"賭け記録を CSV に保存しました: {output_csv}")

    if save_bq and len(history_df) > 0:
        _run_id = run_id or datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        save_to_bigquery(
            history_df=history_df,
            metrics=metrics,
            project_id=project_id,
            start_date=start_date,
            end_date=end_date,
            run_id=_run_id,
        )

    if output_chart and len(history_df) > 0:
        plot_capital_curve(history_df, initial_capital, output_chart)

    return history_df, metrics


# ---------------------------------------------------------------------------
# CLI エントリーポイント
# ---------------------------------------------------------------------------

def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="バックテスト実行スクリプト",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCP プロジェクト ID",
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="学習済みモデルファイルパス（ローカル）",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="バックテスト開始日",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="バックテスト終了日",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=100_000.0,
        metavar="YEN",
        help="初期資金 (円)",
    )
    parser.add_argument(
        "--budget-per-race",
        type=float,
        default=None,
        help="1レースあたりの固定予算 (円)。デフォルト: strategy_config.yaml の値 (3000)",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        metavar="PATH",
        help="賭け記録を CSV 出力するパス",
    )
    parser.add_argument(
        "--save-to-bq",
        action="store_true",
        help="結果を BigQuery (backtests.backtest_results) に保存する",
    )
    parser.add_argument(
        "--output-chart",
        default=None,
        metavar="PATH",
        help="資金推移グラフの PNG 出力パス",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="モデル設定ファイルパス",
    )
    parser.add_argument(
        "--strategy-config",
        default=None,
        metavar="PATH",
        help=f"戦略パラメータYAMLのパス（デフォルト: {_STRATEGY_CONFIG_PATH}）",
    )
    parser.add_argument(
        "--prob-weight-r",
        type=float,
        default=None,
        dest="prob_weight_r",
        help="選定スコア係数 (score = odds * prob^r)。指定時はYAMLの値より優先",
    )
    parser.add_argument(
        "--min-prob-threshold",
        type=float,
        default=None,
        dest="min_prob_threshold",
        help="軸馬の最低複勝率フィルタ。指定時はYAMLの値より優先",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        dest="top_n",
        help="ワイド/三連複/馬連の候補馬数。指定時はYAMLの値より優先",
    )
    parser.add_argument(
        "--max-wide-odds",
        type=float,
        default=None,
        dest="max_wide_odds",
        help="ワイド購入の上限オッズ（指定時はYAMLの値より優先。未指定はYAML値、YAMLに未設定なら無制限）",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログ")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not args.project_id:
        logger.error("GCP_PROJECT_ID が設定されていません")
        return 1

    try:
        start_date = datetime.date.fromisoformat(args.start_date)
        end_date = datetime.date.fromisoformat(args.end_date)
    except ValueError as e:
        logger.error(f"日付フォーマットが不正です: {e}")
        return 1

    if (end_date - start_date).days < 180:
        logger.warning(
            f"バックテスト期間が 6 ヶ月未満です ({start_date} ~ {end_date})。"
            "信頼性の高い結果のためには 6 ヶ月以上を推奨します。"
        )

    config = load_config(args.config)

    # strategy_config.yaml を読み込み、CLIオプションで上書き
    strategy_cfg = _load_strategy_config(args.strategy_config)

    budget = args.budget_per_race if args.budget_per_race is not None else \
        float(strategy_cfg.get("budget_per_race", 3000.0))
    min_prob = args.min_prob_threshold if args.min_prob_threshold is not None else \
        float(strategy_cfg.get("min_prob_threshold", 0.10))
    expected_return_threshold = float(strategy_cfg.get("expected_return_threshold", 1.2))
    top_n = args.top_n if args.top_n is not None else int(strategy_cfg.get("top_n", 5))
    prob_weight_r = args.prob_weight_r if args.prob_weight_r is not None else \
        float(strategy_cfg.get("prob_weight_r", 1.0))
    _yaml_mwo = strategy_cfg.get("max_wide_odds", None)
    max_wide_odds = args.max_wide_odds if args.max_wide_odds is not None else \
        (float(_yaml_mwo) if _yaml_mwo is not None else None)
    enabled_bet_types = strategy_cfg.get("enabled_bet_types")

    print(f"\nバックテスト設定:")
    print(f"  期間:                          {start_date} ~ {end_date}")
    print(f"  モデル:                        {args.model_path}")
    print(f"  初期資金:                      ¥{args.initial_capital:,.0f}")
    print(f"  expected_return_threshold:     {expected_return_threshold}")
    print(f"  top_n:                         {top_n}")
    print(f"  1レースあたり予算:             ¥{budget:,.0f}")
    print(f"  min_prob_threshold:            {min_prob}")
    print(f"  prob_weight_r:                 {prob_weight_r}")
    print(f"  max_wide_odds:                 {max_wide_odds if max_wide_odds is not None else '無制限'}")
    print(f"  enabled_bet_types:             {enabled_bet_types if enabled_bet_types else '全券種'}")

    history_df, metrics = run_full_strategy_backtest_pipeline(
        project_id=args.project_id,
        model_path=args.model_path,
        start_date=start_date,
        end_date=end_date,
        config=config,
        budget_per_race=budget,
        min_prob_threshold=min_prob,
        expected_return_threshold=expected_return_threshold,
        prob_weight_r=prob_weight_r,
        top_n=top_n,
        max_wide_odds=max_wide_odds,
        enabled_bet_types=enabled_bet_types,
        initial_capital=args.initial_capital,
        output_csv=args.output_csv,
        save_bq=args.save_to_bq,
        output_chart=args.output_chart,
    )

    if not metrics:
        logger.error("バックテストが中断されたため、評価指標を表示できません。")
        return 1

    print_metrics(metrics, args.initial_capital)

    return 0


if __name__ == "__main__":
    sys.exit(main())
