"""
レース一覧画面コンポーネント

指定日の全レースと各レースの予測 TOP3 を一覧表示する。
"""



import streamlit as st

from src.dashboard.data import VENUE_CODE_TO_NAME, fetch_race_list


def render(race_date: str) -> None:
    """レース一覧画面を描画する"""
    st.header(f"レース一覧 ({race_date})")

    df = fetch_race_list(race_date)
    if df.empty:
        st.info("指定日の予測データがありません。")
        return

    races = df.groupby(["venue_code", "race_number"], sort=True)
    st.write(f"**{df[['venue_code','race_number']].drop_duplicates().shape[0]} レース** の予測データ")

    for (venue_code, race_number), race_df in races:
        venue_name = VENUE_CODE_TO_NAME.get(venue_code, venue_code)
        _render_race_card(venue_name, race_number, race_df)


def _render_race_card(venue_name: str, race_number: int, df) -> None:
    with st.container(border=True):
        st.markdown(f"#### {venue_name} {race_number}R")
        cols = st.columns(len(df))
        for col, (_, row) in zip(cols, df.iterrows()):
            prob_pct = row["win_place_prob"] * 100
            col.markdown(
                f"**{row['rank_in_race']}位**\n\n"
                f"馬番: {row['horse_number']}\n\n"
                f"**{row['horse_name']}**\n\n"
                f"複勝率: {prob_pct:.1f}%"
            )
