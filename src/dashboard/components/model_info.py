"""
モデル情報画面コンポーネント

特徴量重要度とモデル性能指標を表示する。
"""



import streamlit as st

from src.dashboard.data import fetch_feature_importance


def render() -> None:
    """モデル情報画面を描画する"""
    st.header("モデル情報")

    with st.spinner("GCS からモデルを読み込み中..."):
        fi_df = fetch_feature_importance()

    if fi_df.empty:
        st.info("モデルの特徴量重要度を取得できませんでした。GCS のモデルファイルを確認してください。")
        return

    _render_feature_importance(fi_df)


def _render_feature_importance(df) -> None:
    st.subheader("特徴量重要度 TOP 30 (gain)")

    top30 = df.head(30)

    try:
        import plotly.express as px

        fig = px.bar(
            top30[::-1],  # 上位が上に来るよう反転
            x="importance",
            y="feature",
            orientation="h",
            labels={"importance": "重要度 (gain)", "feature": "特徴量"},
            color="importance",
            color_continuous_scale="Viridis",
        )
        fig.update_layout(coloraxis_showscale=False, height=700)
        st.plotly_chart(fig, use_container_width=True)
    except ImportError:
        # plotly がなければテーブルで表示
        pass

    st.subheader("特徴量重要度テーブル（全件）")
    st.dataframe(df, use_container_width=True, hide_index=True)
