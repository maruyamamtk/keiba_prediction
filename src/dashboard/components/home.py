"""
ホーム画面コンポーネント

当日のおすすめ馬券 TOP を表示する。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard.data import (
    fetch_daily_summary,
    fetch_today_investment_decisions,
)


def render(race_date: str) -> None:
    """ホーム画面を描画する"""
    st.header(f"本日のおすすめ馬券 ({race_date})")

    summary = fetch_daily_summary(race_date)
    _render_summary_metrics(summary)

    st.divider()

    df = fetch_today_investment_decisions(race_date)
    if df.empty:
        st.info("本日の投資判断データがありません。バッチ処理が完了しているか確認してください。")
        return

    top_df = df.head(10)
    _render_top_bets(top_df)

    st.divider()

    _render_by_race(df)


def _render_summary_metrics(summary: dict) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("推奨馬券数", f"{summary.get('total_bets', 0):,} 件")
    col2.metric("対象レース数", f"{summary.get('race_count', 0):,} R")
    col3.metric("総投資額", f"¥{summary.get('total_amount', 0):,}")
    col4.metric("期待回収額", f"¥{summary.get('expected_return_amount', 0):,}")


def _render_top_bets(df: pd.DataFrame) -> None:
    st.subheader("推奨馬券 TOP 10（期待回収率順）")

    display_df = df[[
        "venue_name", "race_number", "horse_number", "horse_name",
        "bet_type_label", "place_odds", "win_place_prob", "expected_return", "bet_amount",
    ]].copy()
    display_df.columns = [
        "競馬場", "R", "馬番", "馬名", "馬券種", "オッズ", "複勝率", "期待回収率", "推奨額",
    ]
    display_df["複勝率"] = (display_df["複勝率"] * 100).round(1).astype(str) + "%"
    display_df["期待回収率"] = display_df["期待回収率"].round(2)
    display_df["推奨額"] = display_df["推奨額"].apply(lambda x: f"¥{int(x):,}")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )


def _render_by_race(df: pd.DataFrame) -> None:
    st.subheader("レース別内訳")

    races = df.groupby(["venue_name", "race_number"], sort=False)
    for (venue_name, race_number), race_df in races:
        total_bet = int(race_df["bet_amount"].sum())
        exp_ret = race_df["expected_return"].mean()
        label = f"{'🔥' if exp_ret >= 1.5 else ''} {venue_name} {race_number}R — 投資: ¥{total_bet:,} / 平均期待値: {exp_ret:.2f}"

        with st.expander(label):
            for _, row in race_df.iterrows():
                col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                col1.write(f"**{row['horse_number']}番 {row['horse_name']}** ({row['bet_type_label']})")
                col2.metric("複勝率", f"{row['win_place_prob']*100:.1f}%")
                col3.metric("オッズ", f"{row['place_odds']:.1f}")
                col4.metric("推奨額", f"¥{int(row['bet_amount']):,}")
