"""
当日 vs 過去日の特徴量NaN率・値分布比較診断スクリプト

現象1（当日予測スコアが平均的で分散が狭い）の原因を特定するため、
当日と過去比較日の features.training_data を全列にわたって比較する。

Usage:
    .venv/bin/python scripts/diagnose_prediction_nan.py \
        --project-id <PROJECT_ID> \
        --target-date 2026-05-17 \
        --compare-date 2026-05-10

Output:
    - 全特徴量のNaN率一覧（当日 vs 比較日）
    - NaN率が高い特徴量のグループ別サマリ
    - base_popularity vs win_popularity の乖離確認（現象2調査）
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def fetch_date_data(client: bigquery.Client, project_id: str, date: str) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM `{project_id}.features.training_data`
    WHERE race_date = '{date}'
    ORDER BY race_id, horse_number
    """
    return client.query(query).to_dataframe()


def print_section(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print("="*70)


def report_all_nan_rates(today_df: pd.DataFrame, past_df: pd.DataFrame) -> pd.DataFrame:
    """全列のNaN率を比較して返す（数値・カテゴリカル両方）"""
    rows = []
    all_cols = list(today_df.columns)
    for col in all_cols:
        today_nan = today_df[col].isna().mean()
        past_nan = past_df[col].isna().mean() if col in past_df.columns else float("nan")
        col_dtype = str(today_df[col].dtype)
        rows.append({
            "feature": col,
            "dtype": col_dtype,
            "today_nan_rate": today_nan,
            "past_nan_rate": past_nan,
            "diff": today_nan - past_nan,
        })
    return pd.DataFrame(rows).sort_values("today_nan_rate", ascending=False)


def classify_feature_group(col: str) -> str:
    """特徴量列をグループに分類する"""
    if col in ("idm", "jockey_index", "info_index", "total_index",
               "training_index", "stable_index", "popularity_index",
               "surge_index", "base_odds", "base_popularity"):
        return "JRDB指数"
    if col in ("cha_training_index", "training_last_3f", "training_furlongs",
               "training_intensity", "training_course_type", "training_count"):
        return "調教CHA"
    if col.endswith("_te") or col.endswith("_te_diff"):
        return "Target Encoding"
    if any(col.endswith(f"_{i}") for i in range(1, 6)) and any(
        kw in col for kw in ("finish_position", "win_odds", "win_popularity",
                              "idm", "finish_time", "last_3f", "corner",
                              "race_date_diff", "popularity_rate", "upside_rate")
    ):
        return "過去走情報"
    if any(kw in col for kw in ("top3_rate", "top2_rate", "top1_rate",
                                  "finish_rate", "mean_finish", "ema_finish")):
        return "馬成績集計"
    if any(kw in col for kw in ("is_fresh", "is_renso", "continuous_run",
                                  "idm_trend", "finish_position_trend",
                                  "race_date_diff_3", "race_date_diff_4", "race_date_diff_5")):
        return "ローテーション"
    if any(kw in col for kw in ("weight_carried", "running_style", "pace_forecast",
                                  "early_advantage", "behind_advantage",
                                  "field_size", "pace_index", "ten_index",
                                  "agari_index", "position_index")):
        return "レース内指標"
    if any(kw in col for kw in ("diff_sum", "diff", "surface", "rotation",
                                  "bracket", "season", "direction")):
        return "差分指標"
    return "その他"


def print_group_summary(nan_df: pd.DataFrame) -> None:
    """グループ別のNaN率サマリを表示する"""
    print_section("グループ別 NaN率サマリ（当日）")
    nan_df["group"] = nan_df["feature"].apply(classify_feature_group)
    summary = (
        nan_df.groupby("group")
        .agg(
            列数=("feature", "count"),
            当日NaN率_mean=("today_nan_rate", "mean"),
            当日NaN率_max=("today_nan_rate", "max"),
            過去NaN率_mean=("past_nan_rate", "mean"),
            差分_mean=("diff", "mean"),
        )
        .sort_values("当日NaN率_mean", ascending=False)
        .reset_index()
    )
    print(summary.to_string(index=False))
    return nan_df  # group列を付加して返す


def print_high_nan_features(nan_df: pd.DataFrame, threshold: float = 0.3) -> None:
    """当日NaN率がthreshold以上の全列を表示する"""
    high_nan = nan_df[nan_df["today_nan_rate"] >= threshold].copy()
    print_section(f"当日NaN率 {threshold:.0%} 以上の特徴量（全{len(high_nan)}件）")
    if len(high_nan) == 0:
        print("  なし")
        return
    print(f"{'グループ':<20} {'特徴量':<50} {'当日NaN率':>10} {'過去NaN率':>10} {'差分':>8}")
    print("-" * 100)
    for _, row in high_nan.iterrows():
        group = row.get("group", classify_feature_group(row["feature"]))
        print(f"{group:<20} {row['feature']:<50} {row['today_nan_rate']:>10.1%} "
              f"{row['past_nan_rate']:>10.1%} {row['diff']:>+8.1%}")


def print_large_diff_features(nan_df: pd.DataFrame, diff_threshold: float = 0.1) -> None:
    """当日と過去でNaN率の差が大きい特徴量を表示する"""
    large_diff = nan_df[nan_df["diff"].abs() >= diff_threshold].copy()
    large_diff = large_diff.sort_values("diff", ascending=False)
    print_section(f"NaN率の乖離が {diff_threshold:.0%} 以上の特徴量（全{len(large_diff)}件）")
    if len(large_diff) == 0:
        print("  なし（当日と過去でNaN率のパターンはほぼ同じ）")
        return
    print(f"{'グループ':<20} {'特徴量':<50} {'当日NaN率':>10} {'過去NaN率':>10} {'差分':>8}")
    print("-" * 100)
    for _, row in large_diff.iterrows():
        group = row.get("group", classify_feature_group(row["feature"]))
        print(f"{group:<20} {row['feature']:<50} {row['today_nan_rate']:>10.1%} "
              f"{row['past_nan_rate']:>10.1%} {row['diff']:>+8.1%}")


def print_full_nan_table(nan_df: pd.DataFrame) -> None:
    """全特徴量のNaN率を全件表示する"""
    print_section(f"全特徴量のNaN率一覧（{len(nan_df)}列、当日NaN率降順）")
    print(f"{'グループ':<20} {'特徴量':<50} {'dtype':<12} {'当日NaN率':>10} {'過去NaN率':>10} {'差分':>8}")
    print("-" * 114)
    for _, row in nan_df.iterrows():
        group = row.get("group", classify_feature_group(row["feature"]))
        print(f"{group:<20} {row['feature']:<50} {row['dtype']:<12} "
              f"{row['today_nan_rate']:>10.1%} {row['past_nan_rate']:>10.1%} {row['diff']:>+8.1%}")


def check_per_horse_nan(today_df: pd.DataFrame, past_df: pd.DataFrame) -> None:
    """1馬あたりのNaN特徴量数の分布を確認する"""
    print_section("1馬あたりのNaN特徴量数（全列対象）")
    all_cols = [c for c in today_df.columns]
    today_nan_counts = today_df[all_cols].isna().sum(axis=1)
    past_nan_counts = past_df[all_cols].isna().sum(axis=1) if len(past_df) > 0 else pd.Series([0])

    print(f"当日: mean={today_nan_counts.mean():.1f}, "
          f"median={today_nan_counts.median():.1f}, "
          f"max={today_nan_counts.max()}, min={today_nan_counts.min()}")
    if len(past_nan_counts) > 0:
        print(f"過去: mean={past_nan_counts.mean():.1f}, "
              f"median={past_nan_counts.median():.1f}, "
              f"max={past_nan_counts.max()}, min={past_nan_counts.min()}")

    worst_idx = today_nan_counts.idxmax()
    if pd.notna(worst_idx):
        row = today_df.loc[worst_idx]
        print(f"\nNaN最多馬: race_id={row.get('race_id','?')}, "
              f"horse_number={row.get('horse_number','?')}, "
              f"horse_name={row.get('horse_name','?')}, "
              f"NaN数={today_nan_counts[worst_idx]}")


def check_value_distribution(today_df: pd.DataFrame, past_df: pd.DataFrame,
                              key_cols: list[str]) -> None:
    """重要特徴量の値分布（mean/std）を当日と過去で比較する"""
    print_section("重要特徴量の値分布比較（NaNを除外）")
    print(f"{'特徴量':<40} {'当日mean':>10} {'当日std':>10} {'過去mean':>10} {'過去std':>10}")
    print("-" * 74)
    for col in key_cols:
        if col not in today_df.columns:
            continue
        t_vals = today_df[col].dropna()
        p_vals = past_df[col].dropna() if col in past_df.columns else pd.Series(dtype=float)
        t_mean = t_vals.mean() if len(t_vals) > 0 else float("nan")
        t_std = t_vals.std() if len(t_vals) > 0 else float("nan")
        p_mean = p_vals.mean() if len(p_vals) > 0 else float("nan")
        p_std = p_vals.std() if len(p_vals) > 0 else float("nan")
        print(f"{col:<40} {t_mean:>10.3f} {t_std:>10.3f} {p_mean:>10.3f} {p_std:>10.3f}")


def check_base_popularity_vs_actual(client: bigquery.Client, project_id: str,
                                    date: str, race_id_example: str | None) -> None:
    """
    現象2調査: base_popularity(JRDB想定人気) vs win_popularity(実際の単勝人気) の乖離確認
    """
    print_section(f"現象2調査: base_popularity vs 実際の単勝人気 (date={date})")

    where = f"r_i.race_date = '{date}'"
    if race_id_example:
        where += f" AND h_r.race_id = '{race_id_example}'"

    query = f"""
    SELECT
        h_r.race_id
        ,h_r.horse_number
        ,h_r.horse_name
        ,h_r.base_popularity
        ,r_r.win_popularity
        ,h_r.idm
        ,r_r.finish_position
    FROM `{project_id}`.raw.horse_results AS h_r
    INNER JOIN `{project_id}`.raw.race_info AS r_i
        ON h_r.race_id = r_i.race_id
    LEFT JOIN `{project_id}`.raw.race_results AS r_r
        ON h_r.race_id = r_r.race_id
        AND h_r.horse_number = r_r.horse_number
    WHERE {where}
    ORDER BY h_r.race_id, h_r.horse_number
    LIMIT 300
    """
    try:
        df = client.query(query).to_dataframe()
        if len(df) == 0:
            print("  データなし")
            return

        print(f"取得行数: {len(df)}")
        print(f"base_popularity NULL率: {df['base_popularity'].isna().mean():.1%}")
        print(f"win_popularity  NULL率: {df['win_popularity'].isna().mean():.1%}")

        valid = df.dropna(subset=["base_popularity", "win_popularity"]).copy()
        if len(valid) > 0:
            valid["diff"] = (valid["win_popularity"] - valid["base_popularity"]).abs()
            print(f"\n乖離(|win_pop - base_pop|): "
                  f"mean={valid['diff'].mean():.2f}, "
                  f"max={valid['diff'].max():.0f}, "
                  f"完全一致割合={(valid['diff']==0).mean():.1%}")

            print("\n乖離Top10:")
            top10 = valid.nlargest(10, "diff")[
                ["race_id", "horse_number", "horse_name",
                 "base_popularity", "win_popularity", "diff", "idm", "finish_position"]
            ]
            print(top10.to_string(index=False))
        else:
            print("  base_popularity/win_popularity が両方揃う行がない")
    except Exception as e:
        print(f"  クエリ失敗: {e}")


def main():
    parser = argparse.ArgumentParser(description="予測特徴量NaN診断スクリプト")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--target-date", default="2026-05-17",
                        help="当日（スコア分散が小さい日）")
    parser.add_argument("--compare-date", default="2026-05-10",
                        help="比較対象（スコア分散が大きい過去日）")
    parser.add_argument("--race-id", default=None,
                        help="現象2調査用 race_id（例: 202605101212）")
    parser.add_argument("--full", action="store_true",
                        help="全特徴量のNaN率を全件表示する")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project_id)

    print(f"当日データ取得中: {args.target_date}")
    today_df = fetch_date_data(client, args.project_id, args.target_date)
    print(f"  → {len(today_df)} 行 / {len(today_df.columns)} 列")

    print(f"比較日データ取得中: {args.compare_date}")
    past_df = fetch_date_data(client, args.project_id, args.compare_date)
    print(f"  → {len(past_df)} 行 / {len(past_df.columns)} 列")

    if len(today_df) == 0:
        print(f"\n[ERROR] 当日({args.target_date})のデータが features.training_data に存在しません。")
        print("特徴量パイプラインが実行されていない可能性があります。")
        sys.exit(1)

    # 全列のNaN率を計算
    nan_df = report_all_nan_rates(today_df, past_df)

    # グループ分類を付加
    nan_df["group"] = nan_df["feature"].apply(classify_feature_group)

    # ── 診断1: グループ別サマリ ──
    print_group_summary(nan_df)

    # ── 診断2: 当日NaN率30%以上の特徴量（全件） ──
    print_high_nan_features(nan_df, threshold=0.30)

    # ── 診断3: 当日と過去でNaN率の差が10%以上の特徴量 ──
    print_large_diff_features(nan_df, diff_threshold=0.10)

    # ── 診断4: 1馬あたりNaN数の分布 ──
    check_per_horse_nan(today_df, past_df)

    # ── 診断5: 重要特徴量の値分布比較 ──
    key_cols = [
        "idm", "jockey_index", "info_index", "training_index", "stable_index",
        "base_popularity", "base_odds", "surge_index",
        "jockey_te", "trainer_te",
        "cha_training_index", "training_last_3f",
        "finish_position_1", "win_popularity_1", "idm_1",
        "mean_finish_position", "ema_finish_position",
        "is_fresh", "is_renso", "continuous_run_count",
    ]
    check_value_distribution(today_df, past_df, key_cols)

    # ── 診断6: 全件表示（--full指定時） ──
    if args.full:
        print_full_nan_table(nan_df)

    # ── 診断7: base_popularity vs 実際の人気（現象2調査） ──
    check_base_popularity_vs_actual(
        client, args.project_id, args.compare_date, args.race_id
    )


if __name__ == "__main__":
    main()
