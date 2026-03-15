"""
ダッシュボード用 BigQuery データ取得モジュール

各画面で必要なデータを BigQuery から取得し、キャッシュする。
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

VENUE_CODE_TO_NAME: dict[str, str] = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}

BET_TYPE_LABELS: dict[str, str] = {
    "place": "複勝",
    "win": "単勝",
    "wide": "ワイド",
    "umaren": "馬連",
    "sanrenpuku": "三連複",
}


def get_project_id() -> str:
    return os.environ.get("GCP_PROJECT_ID", "")


def _get_bq_client():
    from google.cloud import bigquery
    return bigquery.Client(project=get_project_id())


# ---------------------------------------------------------------------------
# ホーム画面
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def fetch_today_investment_decisions(race_date: str) -> pd.DataFrame:
    """
    指定日の投資判断データを取得する（推奨馬券 + 期待回収率）
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        SELECT
            i.race_date,
            i.venue_code,
            i.race_number,
            i.horse_number,
            i.horse_name,
            i.bet_type,
            i.bet_amount,
            i.place_odds,
            i.expected_return,
            i.win_place_prob,
            i.race_pattern
        FROM `{project_id}.predictions.investment_decisions` i
        WHERE i.race_date = '{race_date}'
        ORDER BY i.expected_return DESC
        """
        df = client.query(query).to_dataframe()
        if not df.empty:
            df["venue_name"] = df["venue_code"].map(VENUE_CODE_TO_NAME).fillna(df["venue_code"])
            df["bet_type_label"] = df["bet_type"].map(BET_TYPE_LABELS).fillna(df["bet_type"])
        return df
    except Exception as e:
        logger.warning(f"investment_decisions 取得失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_daily_summary(race_date: str) -> dict:
    """
    指定日のサマリー統計（推奨馬券数、総投資額、期待回収額）を取得する
    """
    df = fetch_today_investment_decisions(race_date)
    if df.empty:
        return {"total_bets": 0, "total_amount": 0, "expected_return_amount": 0, "race_count": 0}
    return {
        "total_bets": len(df),
        "total_amount": int(df["bet_amount"].sum()),
        "expected_return_amount": int((df["bet_amount"] * df["expected_return"]).sum()),
        "race_count": df[["venue_code", "race_number"]].drop_duplicates().shape[0],
    }


# ---------------------------------------------------------------------------
# レース一覧
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def fetch_race_list(race_date: str) -> pd.DataFrame:
    """
    指定日のレース一覧と各レースの予測 TOP3 を取得する
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        WITH ranked AS (
            SELECT
                race_date,
                race_id,
                venue_code,
                race_number,
                horse_number,
                horse_name,
                win_place_prob,
                rank_in_race,
                ROW_NUMBER() OVER (
                    PARTITION BY race_id ORDER BY rank_in_race ASC
                ) AS pos
            FROM `{project_id}.predictions.daily_predictions`
            WHERE race_date = '{race_date}'
        )
        SELECT * FROM ranked
        WHERE pos <= 3
        ORDER BY venue_code, race_number, rank_in_race
        """
        df = client.query(query).to_dataframe()
        if not df.empty:
            df["venue_name"] = df["venue_code"].map(VENUE_CODE_TO_NAME).fillna(df["venue_code"])
        return df
    except Exception as e:
        logger.warning(f"race_list 取得失敗: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# レース詳細
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def fetch_race_detail(race_date: str, venue_code: str, race_number: int) -> pd.DataFrame:
    """
    指定レースの全馬予測 + 単複オッズを取得する
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        SELECT
            p.horse_number,
            p.horse_name,
            p.win_place_prob,
            p.pred_score,
            p.rank_in_race,
            o.win_odds,
            o.place_odds_min AS place_odds,
            ROUND(p.win_place_prob * o.place_odds_min, 3) AS expected_return
        FROM `{project_id}.predictions.daily_predictions` p
        LEFT JOIN `{project_id}.predictions.daily_odds` o
            ON p.race_date = o.race_date
            AND p.race_id = o.race_id
            AND p.horse_number = o.horse_number
        WHERE p.race_date = '{race_date}'
          AND p.venue_code = '{venue_code}'
          AND p.race_number = {race_number}
        ORDER BY p.rank_in_race ASC
        """
        return client.query(query).to_dataframe()
    except Exception as e:
        logger.warning(f"race_detail 取得失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_race_combo_odds(race_date: str, venue_code: str, race_number: int) -> pd.DataFrame:
    """
    指定レースの組み合わせオッズを取得する
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        SELECT
            bet_type,
            horse_number_1,
            horse_number_2,
            horse_number_3,
            odds_value
        FROM `{project_id}.predictions.daily_odds_combo`
        WHERE race_date = '{race_date}'
          AND venue_code = '{venue_code}'
          AND race_number = {race_number}
        ORDER BY bet_type, odds_value
        """
        df = client.query(query).to_dataframe()
        if not df.empty:
            df["bet_type_label"] = df["bet_type"].map(BET_TYPE_LABELS).fillna(df["bet_type"])
        return df
    except Exception as e:
        logger.warning(f"combo_odds 取得失敗（スキップ）: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# バックテスト
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def fetch_backtest_results(start_date: str, end_date: str) -> pd.DataFrame:
    """
    指定期間のバックテスト結果を取得する
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        SELECT *
        FROM `{project_id}.backtests.backtest_results`
        WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY race_date
        """
        return client.query(query).to_dataframe()
    except Exception as e:
        logger.warning(f"backtest_results 取得失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def fetch_backtest_summary(start_date: str, end_date: str) -> dict:
    """
    バックテスト集計サマリーを取得する
    """
    project_id = get_project_id()
    if not project_id:
        return {}
    try:
        client = _get_bq_client()
        query = f"""
        SELECT
            COUNT(*) AS total_bets,
            SUM(bet_amount) AS total_invested,
            SUM(payout) AS total_payout,
            SAFE_DIVIDE(SUM(payout), SUM(bet_amount)) * 100 AS recovery_rate,
            COUNTIF(payout > 0) AS hit_count,
            SAFE_DIVIDE(COUNTIF(payout > 0), COUNT(*)) * 100 AS hit_rate
        FROM `{project_id}.backtests.backtest_results`
        WHERE race_date BETWEEN '{start_date}' AND '{end_date}'
        """
        df = client.query(query).to_dataframe()
        if df.empty:
            return {}
        row = df.iloc[0]
        return {
            "total_bets": int(row.get("total_bets", 0) or 0),
            "total_invested": float(row.get("total_invested", 0) or 0),
            "total_payout": float(row.get("total_payout", 0) or 0),
            "recovery_rate": float(row.get("recovery_rate", 0) or 0),
            "hit_count": int(row.get("hit_count", 0) or 0),
            "hit_rate": float(row.get("hit_rate", 0) or 0),
        }
    except Exception as e:
        logger.warning(f"backtest_summary 取得失敗: {e}")
        return {}


# ---------------------------------------------------------------------------
# モデル情報
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def fetch_feature_importance(model_path: Optional[str] = None) -> pd.DataFrame:
    """
    GCS から最新モデルを読み込み、特徴量重要度を返す
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    try:
        from src.models.lgbm_ranker import LGBMRanker

        ranker = LGBMRanker()
        if model_path:
            ranker.load(model_path)
        else:
            # GCS から最新モデルを取得
            project_id = get_project_id()
            if not project_id:
                return pd.DataFrame()
            from google.cloud import storage
            gcs_client = storage.Client(project=project_id)
            bucket = gcs_client.bucket(f"{project_id}-keiba-models")
            blobs = list(bucket.list_blobs(prefix="lgbm_ranker/"))
            model_blobs = [b for b in blobs if b.name.endswith(".txt")]
            if not model_blobs:
                return pd.DataFrame()
            model_blobs.sort(key=lambda b: b.name.split("/")[-2], reverse=True)
            latest = model_blobs[0]
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
                latest.download_to_filename(f.name)
                ranker.load(f.name)

        return ranker.feature_importance()
    except Exception as e:
        logger.warning(f"feature_importance 取得失敗: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# SHAP説明
# ---------------------------------------------------------------------------

@st.cache_resource
def load_lgbm_ranker_for_shap(model_path: Optional[str] = None):
    """
    GCS から最新 LGBMRanker を読み込み、モデルオブジェクトを返す。
    @st.cache_resource でプロセスライフタイム中キャッシュする。

    Returns:
        (LGBMRanker, list[str]) — ranker インスタンスと特徴量名リスト
        失敗時は (None, [])
    """
    import sys
    import tempfile
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    try:
        from src.models.lgbm_ranker import LGBMRanker

        ranker = LGBMRanker()
        if model_path:
            ranker.load(model_path)
        else:
            project_id = get_project_id()
            if not project_id:
                return None, []
            from google.cloud import storage
            gcs_client = storage.Client(project=project_id)
            bucket = gcs_client.bucket(f"{project_id}-keiba-models")
            blobs = list(bucket.list_blobs(prefix="lgbm_ranker/"))
            model_blobs = [b for b in blobs if b.name.endswith(".txt")]
            if not model_blobs:
                return None, []
            model_blobs.sort(key=lambda b: b.name.split("/")[-2], reverse=True)
            latest = model_blobs[0]
            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
                latest.download_to_filename(f.name)
                ranker.load(f.name)

        feature_names = ranker.model.feature_name() if ranker.model else []
        return ranker, feature_names
    except Exception as e:
        logger.warning(f"LGBMRanker 読み込み失敗: {e}")
        return None, []


@st.cache_data(ttl=3600)
def fetch_horse_features(
    race_date: str,
    venue_code: str,
    race_number: int,
    horse_number: int,
) -> pd.DataFrame:
    """
    features.training_data から指定馬の特徴量行を取得する。
    """
    project_id = get_project_id()
    if not project_id:
        return pd.DataFrame()
    try:
        client = _get_bq_client()
        query = f"""
        SELECT *
        FROM `{project_id}.features.training_data`
        WHERE race_date = '{race_date}'
          AND venue_code = '{venue_code}'
          AND race_number = {race_number}
          AND horse_number = {horse_number}
        LIMIT 1
        """
        return client.query(query).to_dataframe()
    except Exception as e:
        logger.warning(f"horse_features 取得失敗: {e}")
        return pd.DataFrame()


def get_available_dates(days_back: int = 30) -> list[str]:
    """
    過去 N 日間で予測データが存在する日付リストを返す
    """
    project_id = get_project_id()
    if not project_id:
        return []
    try:
        client = _get_bq_client()
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        query = f"""
        SELECT DISTINCT CAST(race_date AS STRING) AS race_date
        FROM `{project_id}.predictions.daily_predictions`
        WHERE race_date >= '{cutoff}'
        ORDER BY race_date DESC
        """
        df = client.query(query).to_dataframe()
        return df["race_date"].tolist()
    except Exception as e:
        logger.warning(f"available_dates 取得失敗: {e}")
        return []
