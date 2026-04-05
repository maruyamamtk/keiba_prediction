"""
レース詳細画面コンポーネント

指定レースの全馬予測・オッズ・期待回収率を詳細表示する。
"""



import pandas as pd
import streamlit as st

from src.dashboard.data import (
    VENUE_CODE_TO_NAME,
    fetch_race_combo_odds,
    fetch_race_detail,
    fetch_race_list,
)


def render(race_date: str) -> None:
    """レース詳細画面を描画する"""
    st.header("レース詳細")

    # レース選択UI
    race_list_df = fetch_race_list(race_date)
    if race_list_df.empty:
        st.info(f"{race_date} の予測データがありません。")
        return

    # 競馬場セレクト
    venue_codes = sorted(race_list_df["venue_code"].unique())
    venue_options = {VENUE_CODE_TO_NAME.get(vc, vc): vc for vc in venue_codes}
    selected_venue_name = st.selectbox("競馬場", list(venue_options.keys()))
    selected_venue_code = venue_options[selected_venue_name]

    # レース番号セレクト
    race_numbers = sorted(
        race_list_df[race_list_df["venue_code"] == selected_venue_code]["race_number"].unique()
    )
    selected_race = st.selectbox("レース番号", race_numbers, format_func=lambda r: f"{r}R")

    st.divider()

    detail_df = fetch_race_detail(race_date, selected_venue_code, int(selected_race))
    if detail_df.empty:
        st.warning("詳細データを取得できませんでした。")
        return

    _render_prediction_table(detail_df, selected_venue_name, int(selected_race))
    _render_bar_chart(detail_df)
    _render_combo_odds(race_date, selected_venue_code, int(selected_race))


def _render_prediction_table(df: pd.DataFrame, venue_name: str, race_number: int) -> None:
    st.subheader(f"{venue_name} {race_number}R — 予測結果")

    display = df.copy()
    display["win_place_prob"] = (display["win_place_prob"] * 100).round(1).astype(str) + "%"
    display["pred_score"] = display["pred_score"].round(4)
    display["expected_return"] = display["expected_return"].round(3)

    # 期待回収率 1.0 以上を強調
    def highlight_expected(row):
        color = "background-color: #d4edda" if (row.get("expected_return", 0) or 0) >= 1.0 else ""
        return [color] * len(row)

    cols_map = {
        "rank_in_race": "予測順",
        "horse_number": "馬番",
        "horse_name": "馬名",
        "win_place_prob": "複勝率",
        "pred_score": "スコア",
        "win_odds": "単勝オッズ",
        "place_odds": "複勝オッズ",
        "expected_return": "期待回収率",
    }
    show_cols = [c for c in cols_map if c in display.columns]
    display = display[show_cols].rename(columns=cols_map)

    st.dataframe(
        display.style.apply(highlight_expected, axis=1),
        use_container_width=True,
        hide_index=True,
    )


def _render_bar_chart(df: pd.DataFrame) -> None:
    try:
        import plotly.express as px

        st.subheader("複勝率 棒グラフ")
        chart_df = df.copy()
        chart_df["label"] = chart_df["horse_number"].astype(str) + "." + chart_df["horse_name"]
        chart_df = chart_df.sort_values("rank_in_race")
        fig = px.bar(
            chart_df,
            x="label",
            y="win_place_prob",
            labels={"label": "馬番・馬名", "win_place_prob": "複勝率"},
            color="win_place_prob",
            color_continuous_scale="Blues",
            text=chart_df["win_place_prob"].mul(100).round(1).astype(str) + "%",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(yaxis_tickformat=".0%", coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    except ImportError:
        st.info("plotly がインストールされていないためグラフを表示できません。")


def _render_combo_odds(race_date: str, venue_code: str, race_number: int) -> None:
    combo_df = fetch_race_combo_odds(race_date, venue_code, race_number)
    if combo_df.empty:
        return

    st.subheader("組み合わせオッズ（上位 20 件）")
    for bet_type_label, group in combo_df.groupby("bet_type_label"):
        with st.expander(f"{bet_type_label}"):
            top20 = group.nsmallest(20, "odds_value")
            disp = top20[["horse_number_1", "horse_number_2", "horse_number_3", "odds_value"]].copy()
            disp.columns = ["馬番1", "馬番2", "馬番3", "オッズ"]
            st.dataframe(disp, hide_index=True, use_container_width=True)
