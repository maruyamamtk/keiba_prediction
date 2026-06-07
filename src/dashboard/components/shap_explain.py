"""
SHAP説明画面コンポーネント

特定の馬がなぜ複勝率が高い（低い）のかを SHAP ウォーターフォールで可視化する。

SHAPの解釈:
  - 予測スコア = base_value（全馬平均）+ 各特徴量のSHAP値の合計
  - SHAP値 > 0: その特徴量がスコアを押し上げている（複勝率UPに寄与）
  - SHAP値 < 0: その特徴量がスコアを押し下げている（複勝率DOWNに寄与）
"""



import logging

import numpy as np
import pandas as pd
import streamlit as st

from src.dashboard.feature_labels import to_label
from src.dashboard.data import (
    VENUE_CODE_TO_NAME,
    fetch_race_features,
    fetch_race_list,
    fetch_race_place_odds,
    load_lgbm_ranker_for_shap,
)

logger = logging.getLogger(__name__)

# model_config から除外カラムを読み込む（学習時と同じ除外リスト）
_EXCLUDE_COLUMNS = [
    "race_id", "horse_id", "race_date", "target_place",
    "finish_position", "venue_code", "jockey_id", "trainer_id",
    "created_at", "race_number", "horse_number", "horse_name",
]


def render(race_date: str) -> None:
    """SHAP説明画面を描画する"""
    st.header("SHAP説明 — 予測スコアの要因分析")
    st.caption(
        "各馬の予測スコア（複勝率ランキングの根拠）を特徴量ごとに分解します。"
        " 棒が右（赤）=スコアを押し上げる要因 / 左（青）=押し下げる要因。"
    )

    # ── 1. レース・馬の選択 ────────────────────────────────────────────────
    race_list_df = fetch_race_list(race_date)
    if race_list_df.empty:
        st.info(f"{race_date} の予測データがありません。")
        return

    col1, col2, col3 = st.columns(3)

    venue_codes = sorted(race_list_df["venue_code"].unique())
    venue_options = {VENUE_CODE_TO_NAME.get(vc, vc): vc for vc in venue_codes}
    selected_venue_name = col1.selectbox("競馬場", list(venue_options.keys()), key="shap_venue")
    selected_venue_code = venue_options[selected_venue_name]

    race_numbers = sorted(
        race_list_df[race_list_df["venue_code"] == selected_venue_code]["race_number"].unique()
    )
    selected_race = col2.selectbox(
        "レース番号", race_numbers, format_func=lambda r: f"{r}R", key="shap_race"
    )

    st.divider()

    # ── 2. モデル読み込み & 全馬リアルタイム計算 ─────────────────────────
    with st.spinner("モデルを読み込み中...（初回のみ時間がかかります）"):
        ranker, feature_names = load_lgbm_ranker_for_shap()

    if ranker is None or not feature_names:
        st.error("モデルの読み込みに失敗しました。GCS 上のモデルファイルを確認してください。")
        return

    with st.spinner("全馬の特徴量を取得中..."):
        race_df = fetch_race_features(race_date, selected_venue_code, int(selected_race))

    if race_df.empty:
        st.warning(
            "features.training_data に該当レースのデータが見つかりませんでした。"
            " 特徴量生成バッチが完了しているか確認してください。"
        )
        return

    # 全馬のスコアをリアルタイム計算
    realtime_df = _compute_realtime_predictions(ranker, feature_names, race_df)

    # 複勝オッズをdaily_oddsから取得してJOIN
    place_odds_df = fetch_race_place_odds(race_date, selected_venue_code, int(selected_race))
    if not place_odds_df.empty:
        realtime_df = realtime_df.merge(
            place_odds_df[["horse_number", "place_odds_min"]],
            on="horse_number", how="left",
        )
        realtime_df["place_odds"] = realtime_df["place_odds_min"]
        realtime_df["expected_return"] = (
            realtime_df["win_place_prob"] * realtime_df["place_odds_min"]
        ).round(3)
    else:
        realtime_df["place_odds"] = float("nan")
        realtime_df["expected_return"] = float("nan")

    # 馬選択プルダウン（リアルタイム計算値で表示）
    horse_options = {
        f"{int(row['horse_number'])}番 {row['horse_name']} (複勝率 {row['win_place_prob']*100:.1f}%)": int(row["horse_number"])
        for _, row in realtime_df.sort_values("rank_in_race").iterrows()
    }
    selected_horse_label = col3.selectbox("馬", list(horse_options.keys()), key="shap_horse")
    selected_horse_number = horse_options[selected_horse_label]

    selected_horse_row = realtime_df[realtime_df["horse_number"] == selected_horse_number].iloc[0]

    # ── 3. 選択馬のSHAP計算 ──────────────────────────────────────────────
    horse_df = race_df[race_df["horse_number"] == selected_horse_number]

    shap_df, error_msg = _compute_shap(ranker, horse_df, feature_names)
    if shap_df is None:
        st.error(f"SHAP 値の計算に失敗しました。\n\n```\n{error_msg}\n```")
        return

    # ── 4. 結果表示 ─────────────────────────────────────────────────────
    _render_horse_header(selected_horse_row, selected_venue_name, int(selected_race))
    _render_waterfall(shap_df, top_n=20)
    _render_shap_table(shap_df)


# ---------------------------------------------------------------------------
# 内部関数
# ---------------------------------------------------------------------------

def _compute_realtime_predictions(ranker, feature_names: list[str], race_df: pd.DataFrame) -> pd.DataFrame:
    """
    レース全馬の pred_score・win_place_prob・rank_in_race をリアルタイム計算する。

    Returns:
        horse_number / horse_name / pred_score / win_place_prob / rank_in_race を持つ DataFrame
    """
    from src.models.predict import _scores_to_place_prob

    available = [f for f in feature_names if f in race_df.columns]
    if not available:
        result = race_df[["horse_number", "horse_name"]].copy()
        result["pred_score"] = float("nan")
        result["win_place_prob"] = float("nan")
        result["rank_in_race"] = range(1, len(result) + 1)
        return result

    X = race_df[available].copy()
    for col in X.select_dtypes(include=["object", "category"]).columns:
        X[col] = pd.factorize(X[col])[0].astype(float)
    X_np = X.to_numpy(dtype=float, na_value=0.0)

    scores = ranker.model.predict(X_np)

    probs = _scores_to_place_prob(scores, n_places=3)

    result = race_df[["horse_number", "horse_name"]].copy()
    result["pred_score"] = scores
    result["win_place_prob"] = probs
    result["rank_in_race"] = pd.Series(scores).rank(ascending=False, method="min").astype(int).values
    return result.reset_index(drop=True)


def _compute_shap(ranker, horse_df: pd.DataFrame, feature_names: list[str]):
    """
    LightGBM の pred_contrib=True で指定馬のSHAP値を計算する。

    shap.TreeExplainer は内部で pred_contrib=True を呼ぶが、
    pandas DataFrame に object/category 列があるとカテゴリ不一致エラーになる。
    numpy 配列に変換してから直接呼び出すことで回避する。

    Returns:
        (DataFrame, None)  — 成功時: (|shap_value|降順のDF, None)
        (None, str)        — 失敗時: (None, エラーメッセージ)
    """
    try:
        # 学習時と同じ除外カラムを除去して特徴量行列を作る
        available_features = [f for f in feature_names if f in horse_df.columns]
        missing = set(feature_names) - set(available_features)
        if missing:
            logger.warning(f"特徴量 {len(missing)} 件が training_data に存在しない: {list(missing)[:5]}...")

        if not available_features:
            return None, (
                f"モデルの特徴量 ({len(feature_names)} 件) が"
                " features.training_data の列と一致しませんでした。"
                " 特徴量生成バッチが最新かどうか確認してください。"
            )

        X = horse_df[available_features].copy()
        feature_vals = X.iloc[0].values  # 元の値（表示用）

        # object/category dtype の列を整数コードに変換してから numpy 配列化。
        # pandas DataFrame のまま渡すとカテゴリ列のレベル不一致で ValueError になる。
        for col in X.select_dtypes(include=["object", "category"]).columns:
            X[col] = pd.factorize(X[col])[0].astype(float)
        X_np = X.to_numpy(dtype=float, na_value=0.0)

        # pred_contrib=True: shape (n_samples, n_features + 1)
        # 最終列は base value (期待値)、それ以外が各特徴量のSHAP値
        contrib = ranker.model.predict(X_np, pred_contrib=True)
        values = contrib[0, :-1]  # 1サンプル・特徴量分のみ

        df = pd.DataFrame({
            "feature": available_features,
            "shap_value": values,
            "feature_value": feature_vals,
        })
        df["label"] = df["feature"].map(to_label)
        df["abs_shap"] = df["shap_value"].abs()
        return df.sort_values("abs_shap", ascending=False).reset_index(drop=True), None

    except Exception as e:
        import traceback
        msg = f"{type(e).__name__}: {e}\n\n{traceback.format_exc()}"
        logger.error(f"SHAP 計算失敗: {e}", exc_info=True)
        return None, msg


def _render_horse_header(row: pd.Series, venue_name: str, race_number: int) -> None:
    """馬の予測情報ヘッダーを表示する"""
    st.subheader(
        f"{venue_name} {race_number}R — "
        f"{int(row['horse_number'])}番 **{row['horse_name']}**"
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("予測順位", f"{int(row['rank_in_race'])}位")
    col2.metric("複勝率", f"{row['win_place_prob']*100:.1f}%")
    col3.metric("複勝オッズ", f"{row['place_odds']:.1f}" if pd.notna(row.get("place_odds")) else "—")
    col4.metric(
        "期待回収率",
        f"{row['expected_return']:.3f}" if pd.notna(row.get("expected_return")) else "—",
    )


def _is_numeric(v) -> bool:
    """値が数値として扱えるか判定する"""
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _render_waterfall(shap_df: pd.DataFrame, top_n: int = 20) -> None:
    """SHAP ウォーターフォール（横棒グラフ）を表示する"""
    try:
        import plotly.graph_objects as go

        top = shap_df.head(top_n).copy()
        top = top.sort_values("shap_value")  # 昇順でプロット（下が最大）

        colors = ["#d62728" if v > 0 else "#1f77b4" for v in top["shap_value"]]
        def _fmt_fv(fv):
            try:
                return f"{float(fv):.3g}"
            except (ValueError, TypeError):
                return str(fv)

        text_labels = [
            f"{v:+.4f}  ({_fmt_fv(fv)})"
            for v, fv in zip(top["shap_value"], top["feature_value"])
        ]

        fig = go.Figure(go.Bar(
            x=top["shap_value"],
            y=top["label"],
            orientation="h",
            marker_color=colors,
            text=text_labels,
            textposition="outside",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "SHAP値: %{x:.4f}<br>"
                "特徴量値: %{customdata}<extra></extra>"
            ),
            customdata=top["feature_value"].apply(_fmt_fv),
        ))

        fig.add_vline(x=0, line_width=1, line_color="black")
        fig.update_layout(
            title=f"SHAP ウォーターフォール（上位 {top_n} 特徴量）",
            xaxis_title="SHAP値（予測スコアへの寄与）",
            yaxis_title="特徴量",
            height=max(400, top_n * 30),
            margin=dict(l=260, r=130, t=60, b=40),
        )
        st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.info("plotly がインストールされていないためグラフを表示できません。")


def _render_shap_table(shap_df: pd.DataFrame) -> None:
    """SHAP値テーブルを折りたたみ表示する"""
    with st.expander("全特徴量のSHAP値テーブル"):
        display = shap_df[["label", "feature", "shap_value", "feature_value"]].copy()
        display.columns = ["特徴量(日本語)", "カラム名", "SHAP値", "特徴量値"]
        display["SHAP値"] = display["SHAP値"].round(6)
        display["特徴量値"] = display["特徴量値"].apply(
            lambda v: round(float(v), 4) if _is_numeric(v) else v
        )

        def color_shap(val):
            if val > 0:
                return "color: #d62728"
            elif val < 0:
                return "color: #1f77b4"
            return ""

        st.dataframe(
            display.style.applymap(color_shap, subset=["SHAP値"]),
            hide_index=True,
            use_container_width=True,
        )
