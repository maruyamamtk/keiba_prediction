"""
バックテスト画面コンポーネント

過去の投資シミュレーション結果を可視化する。
"""



from datetime import date, timedelta

import streamlit as st

from src.dashboard.data import fetch_backtest_results, fetch_backtest_summary


def render() -> None:
    """バックテスト画面を描画する"""
    st.header("バックテスト結果")

    # 期間セレクタ
    col1, col2 = st.columns(2)
    default_end = date.today()
    default_start = default_end - timedelta(days=180)
    start_date = col1.date_input("開始日", value=default_start)
    end_date = col2.date_input("終了日", value=default_end)

    if start_date > end_date:
        st.error("開始日は終了日より前に設定してください。")
        return

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    summary = fetch_backtest_summary(start_str, end_str)
    if not summary:
        st.info("指定期間のバックテストデータがありません。")
        return

    _render_summary_metrics(summary)

    st.divider()

    df = fetch_backtest_results(start_str, end_str)
    if df.empty:
        return

    _render_capital_curve(df)
    _render_monthly_stats(df)


def _render_summary_metrics(summary: dict) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("総ベット数", f"{summary.get('total_bets', 0):,}")
    col2.metric("回収率", f"{summary.get('recovery_rate', 0):.1f}%")
    col3.metric("的中率", f"{summary.get('hit_rate', 0):.1f}%")
    col4.metric("的中数", f"{summary.get('hit_count', 0):,}")

    col1b, col2b = st.columns(2)
    col1b.metric("総投資額", f"¥{summary.get('total_invested', 0):,.0f}")
    col2b.metric("総払戻額", f"¥{summary.get('total_payout', 0):,.0f}")


def _render_capital_curve(df) -> None:
    try:
        import plotly.express as px
        import pandas as pd

        st.subheader("損益推移")

        if "race_date" not in df.columns or "payout" not in df.columns or "bet_amount" not in df.columns:
            st.info("損益推移グラフに必要なカラムがありません。")
            return

        daily = df.groupby("race_date").agg(
            invested=("bet_amount", "sum"),
            payout=("payout", "sum"),
        ).reset_index()
        daily["race_date"] = pd.to_datetime(daily["race_date"])
        daily["profit"] = daily["payout"] - daily["invested"]
        daily["cumulative_profit"] = daily["profit"].cumsum()

        fig = px.line(
            daily,
            x="race_date",
            y="cumulative_profit",
            labels={"race_date": "日付", "cumulative_profit": "累積損益 (円)"},
            title="累積損益推移",
        )
        fig.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="損益ゼロライン")
        st.plotly_chart(fig, use_container_width=True)
    except ImportError:
        st.info("plotly がインストールされていないためグラフを表示できません。")


def _render_monthly_stats(df) -> None:
    try:
        import pandas as pd

        st.subheader("月次集計")

        if "race_date" not in df.columns:
            return

        df = df.copy()
        df["race_date"] = pd.to_datetime(df["race_date"])
        df["month"] = df["race_date"].dt.to_period("M").astype(str)

        monthly = df.groupby("month").agg(
            total_bets=("bet_amount", "count"),
            total_invested=("bet_amount", "sum"),
            total_payout=("payout", "sum"),
            hit_count=("payout", lambda x: (x > 0).sum()),
        ).reset_index()
        monthly["recovery_rate"] = (monthly["total_payout"] / monthly["total_invested"] * 100).round(1)
        monthly["hit_rate"] = (monthly["hit_count"] / monthly["total_bets"] * 100).round(1)
        monthly["profit"] = (monthly["total_payout"] - monthly["total_invested"]).astype(int)

        display = monthly[["month", "total_bets", "recovery_rate", "hit_rate", "profit"]].copy()
        display.columns = ["月", "ベット数", "回収率(%)", "的中率(%)", "損益(円)"]

        def color_profit(val):
            color = "color: green" if val > 0 else ("color: red" if val < 0 else "")
            return color

        st.dataframe(
            display.style.applymap(color_profit, subset=["損益(円)"]),
            hide_index=True,
            use_container_width=True,
        )
    except Exception as e:
        st.warning(f"月次集計の表示に失敗しました: {e}")
