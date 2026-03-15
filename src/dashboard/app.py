"""
競馬予測ダッシュボード (Streamlit)

環境変数:
    GCP_PROJECT_ID: GCP プロジェクト ID

実行方法:
    streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

# ページ設定（最初の Streamlit コマンドである必要がある）
st.set_page_config(
    page_title="競馬予測ダッシュボード",
    page_icon="🐎",
    layout="wide",
    initial_sidebar_state="expanded",
)

from src.dashboard.components import backtest, home, model_info, race_detail, race_list
from src.dashboard.data import get_available_dates

MENU_ITEMS = ["ホーム", "レース一覧", "レース詳細", "バックテスト", "モデル情報"]


def main() -> None:
    # サイドバー
    with st.sidebar:
        st.title("🐎 競馬予測")
        st.caption("競馬予測MLシステム ダッシュボード")
        st.divider()

        menu = st.radio("メニュー", MENU_ITEMS)

        st.divider()

        # 日付選択（ホーム・レース一覧・レース詳細で共通）
        if menu in ("ホーム", "レース一覧", "レース詳細"):
            available_dates = get_available_dates(days_back=30)
            today_str = date.today().isoformat()

            if available_dates:
                # 今日が含まれていればデフォルト、なければ最新日
                default_idx = 0
                if today_str in available_dates:
                    default_idx = available_dates.index(today_str)
                selected_date = st.selectbox(
                    "日付",
                    available_dates,
                    index=default_idx,
                )
            else:
                selected_date = st.text_input("日付 (YYYY-MM-DD)", value=today_str)

        st.divider()
        project_id = os.environ.get("GCP_PROJECT_ID", "（未設定）")
        st.caption(f"Project: {project_id}")

    # メインコンテンツ
    if menu == "ホーム":
        home.render(selected_date)
    elif menu == "レース一覧":
        race_list.render(selected_date)
    elif menu == "レース詳細":
        race_detail.render(selected_date)
    elif menu == "バックテスト":
        backtest.render()
    elif menu == "モデル情報":
        model_info.render()


if __name__ == "__main__":
    main()
