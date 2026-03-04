"""
モデル評価レポート生成スクリプト

学習済みモデルを読み込み、評価指標・特徴量重要度・チューニング結果を
Markdownレポートとして出力する。

Usage:
    python scripts/generate_evaluation_report.py \\
        --model-path src/models/lgbm_ranker_20260217.txt \\
        --project-id <PROJECT_ID>

    # GCS上のモデルを使う場合
    python scripts/generate_evaluation_report.py \\
        --gcs-model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260217/lgbm_ranker_20260217.txt \\
        --project-id <PROJECT_ID>

    # BigQueryへの接続なしでローカルCSVを使う場合
    python scripts/generate_evaluation_report.py \\
        --model-path src/models/lgbm_ranker_20260217.txt \\
        --local-data-path /path/to/training_data.csv
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import lightgbm as lgb
import matplotlib
matplotlib.use("Agg")  # GUIなし環境向け
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPORT_DIR = Path(__file__).resolve().parents[1] / "docs"
FIGURE_DIR = REPORT_DIR / "figures"
REPORT_PATH = REPORT_DIR / "model_evaluation_report.md"


# ---------------------------------------------------------------------------
# データ取得
# ---------------------------------------------------------------------------

def fetch_training_data_bq(project_id: str, dataset: str = "features", table: str = "training_data") -> pd.DataFrame:
    """BigQueryからtraining_dataを取得する。

    features.training_data の finish_position は常にNULLのため、
    raw.race_results から finish_position を LEFT JOIN して取得する。
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
      t.* EXCEPT(finish_position),
      r.finish_position
    FROM `{project_id}.{dataset}.{table}` t
    LEFT JOIN `{project_id}.raw.race_results` r
      ON t.race_id = r.race_id AND t.horse_number = r.horse_number
    ORDER BY t.race_date, t.race_id, t.horse_number
    """
    logger.info(f"Fetching data from {project_id}.{dataset}.{table}...")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns")
    return df


def load_model_from_gcs(gcs_uri: str, project_id: str) -> tuple[lgb.Booster, dict]:
    """GCSからモデルを読み込む"""
    from google.cloud import storage
    client = storage.Client(project=project_id)

    # gs://bucket/path 形式をパース
    without_scheme = gcs_uri[len("gs://"):]
    bucket_name, blob_name = without_scheme.split("/", 1)

    bucket = client.bucket(bucket_name)

    with tempfile.TemporaryDirectory() as tmpdir:
        model_local = Path(tmpdir) / Path(blob_name).name
        meta_local = model_local.with_suffix(".meta.json")

        bucket.blob(blob_name).download_to_filename(str(model_local))
        meta_blob_name = blob_name.replace(".txt", ".meta.json")
        meta_blob = bucket.blob(meta_blob_name)
        meta = {}
        if meta_blob.exists():
            meta_blob.download_to_filename(str(meta_local))
            meta = json.loads(meta_local.read_text())

        # LightGBMのBoosterは内部でモデルをメモリに保持するため、
        # tmpdir削除後もboosterは有効に使用できる
        booster = lgb.Booster(model_file=str(model_local))

    return booster, meta


def load_model_local(model_path: str) -> tuple[lgb.Booster, dict]:
    """ローカルからモデルを読み込む"""
    path = Path(model_path)
    if not path.exists():
        raise FileNotFoundError(f"モデルファイルが見つかりません: {path}")

    booster = lgb.Booster(model_file=str(path))
    meta_path = path.with_suffix(".meta.json")
    meta = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())

    return booster, meta


# ---------------------------------------------------------------------------
# 評価計算
# ---------------------------------------------------------------------------

def _build_feature_matrix(booster: lgb.Booster, df: pd.DataFrame) -> pd.DataFrame:
    """
    モデルの特徴量情報を使って予測用の特徴量行列を構築する。

    Booster.predict() に渡す DataFrame は、学習時のカテゴリカル特徴量と
    pandas_categorical が一致している必要があるため、モデルファイルから
    categorical_feature インデックスと pandas_categorical を取得して
    正確にエンコードする。
    """
    import re

    feature_names = booster.feature_name()
    feature_cols = [c for c in feature_names if c in df.columns]
    X = df[feature_cols].copy()

    # モデルのカテゴリカル特徴量インデックスを取得
    model_str = booster.model_to_string()
    match = re.search(r'\[categorical_feature: ([\d,]+)\]', model_str)
    if match and booster.pandas_categorical:
        cat_indices = [int(i) for i in match.group(1).split(',')]
        pandas_cat = booster.pandas_categorical
        for i, cat_idx in enumerate(cat_indices):
            if i >= len(pandas_cat):
                break
            col_name = feature_names[cat_idx]
            if col_name in X.columns:
                X[col_name] = pd.Categorical(X[col_name], categories=pandas_cat[i])

    return X


def compute_metrics(
    booster: lgb.Booster,
    df: pd.DataFrame,
    exclude_columns: list[str],
    categorical_columns: list[str],
) -> dict:
    """
    評価指標を計算する

    Returns:
        NDCG@3, Recall@3, AUC, レース数を含む辞書
    """
    from sklearn.metrics import roc_auc_score

    # finish_positionがNAの行（競走除外・中止馬など）を除外
    df = df[df["finish_position"].notna()].copy()

    X = _build_feature_matrix(booster, df)

    positions = df["finish_position"].values.astype(int)
    y_binary = np.where((positions >= 1) & (positions <= 3), 1, 0)
    groups = df.groupby("race_id", sort=False).size().tolist()

    y_pred = booster.predict(X)

    ndcg_scores = []
    recall_scores = []
    start = 0
    for group_size in groups:
        end = start + group_size
        true_pos = positions[start:end]
        pred = y_pred[start:end]

        top3_pred_idx = np.argsort(pred)[::-1][:3]
        top3_true_idx = set(np.where(true_pos <= 3)[0])

        if len(top3_true_idx) > 0:
            recall = len(set(top3_pred_idx) & top3_true_idx) / len(top3_true_idx)
            recall_scores.append(recall)

        relevance = np.where(true_pos <= 3, 1.0, 0.0)
        pred_order = np.argsort(pred)[::-1]
        dcg = sum(
            relevance[pred_order[i]] / np.log2(i + 2)
            for i in range(min(3, group_size))
        )
        ideal_order = np.argsort(relevance)[::-1]
        idcg = sum(
            relevance[ideal_order[i]] / np.log2(i + 2)
            for i in range(min(3, group_size))
        )
        if idcg > 0:
            ndcg_scores.append(dcg / idcg)

        start = end

    auc = float(roc_auc_score(y_binary, y_pred)) if len(np.unique(y_binary)) >= 2 else 0.0

    return {
        "ndcg@3": float(np.mean(ndcg_scores)) if ndcg_scores else 0.0,
        "recall@3": float(np.mean(recall_scores)) if recall_scores else 0.0,
        "auc": auc,
        "num_races": len(groups),
        "num_rows": len(df),
    }


# ---------------------------------------------------------------------------
# 可視化
# ---------------------------------------------------------------------------

def plot_feature_importance(
    booster: lgb.Booster,
    top_n: int = 20,
    output_path: Optional[str] = None,
) -> str:
    """特徴量重要度グラフを生成し、ファイルパスを返す"""
    importance = booster.feature_importance(importance_type="gain")
    names = booster.feature_name()

    df_imp = pd.DataFrame({"feature": names, "importance": importance})
    df_imp = df_imp.sort_values("importance", ascending=False).head(top_n)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(df_imp["feature"][::-1], df_imp["importance"][::-1], color="#4C72B0")
    ax.set_xlabel("Importance (Gain)", fontsize=12)
    ax.set_title(f"Feature Importance TOP {top_n}", fontsize=14)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.tight_layout()

    if output_path is None:
        FIGURE_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(FIGURE_DIR / "feature_importance.png")
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Feature importance plot saved: {output_path}")
    return output_path


def plot_monthly_metrics(
    df: pd.DataFrame,
    booster: lgb.Booster,
    exclude_columns: list[str],
    categorical_columns: list[str],
    output_path: Optional[str] = None,
) -> str:
    """月次評価指標の推移グラフを生成し、ファイルパスを返す"""
    from sklearn.metrics import roc_auc_score

    df = df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"])
    df["year_month"] = df["race_date"].dt.to_period("M")

    months = sorted(df["year_month"].unique())
    records = []

    for month in months:
        monthly = df[df["year_month"] == month]
        # finish_positionがNAの行（競走除外・中止馬など）を除外
        monthly = monthly[monthly["finish_position"].notna()].copy()
        if len(monthly) < 10:
            continue

        X = _build_feature_matrix(booster, monthly)

        positions = monthly["finish_position"].values.astype(int)
        y_binary = np.where((positions >= 1) & (positions <= 3), 1, 0)
        groups = monthly.groupby("race_id", sort=False).size().tolist()

        y_pred = booster.predict(X)

        ndcg_list, recall_list = [], []
        start = 0
        for g in groups:
            end = start + g
            true_pos = positions[start:end]
            pred = y_pred[start:end]

            top3_pred_idx = set(np.argsort(pred)[::-1][:3])
            top3_true_idx = set(np.where(true_pos <= 3)[0])
            if top3_true_idx:
                recall_list.append(len(top3_pred_idx & top3_true_idx) / len(top3_true_idx))

            relevance = np.where(true_pos <= 3, 1.0, 0.0)
            pred_order = np.argsort(pred)[::-1]
            dcg = sum(relevance[pred_order[i]] / np.log2(i + 2) for i in range(min(3, g)))
            ideal_order = np.argsort(relevance)[::-1]
            idcg = sum(relevance[ideal_order[i]] / np.log2(i + 2) for i in range(min(3, g)))
            if idcg > 0:
                ndcg_list.append(dcg / idcg)
            start = end

        auc = float(roc_auc_score(y_binary, y_pred)) if len(np.unique(y_binary)) >= 2 else None

        records.append({
            "month": str(month),
            "ndcg@3": np.mean(ndcg_list) if ndcg_list else None,
            "recall@3": np.mean(recall_list) if recall_list else None,
            "auc": auc,
        })

    if not records:
        logger.warning("月次グラフ生成に十分なデータがありません")
        return ""

    metrics_df = pd.DataFrame(records)
    metrics_df["month"] = pd.to_datetime(metrics_df["month"].astype(str))

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    for ax, col, color, label in zip(
        axes,
        ["ndcg@3", "recall@3", "auc"],
        ["#4C72B0", "#DD8452", "#55A868"],
        ["NDCG@3", "Recall@3", "AUC"],
    ):
        ax.plot(metrics_df["month"], metrics_df[col], marker="o", color=color, linewidth=2)
        ax.set_ylabel(label, fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 1.05)

    axes[-1].set_xlabel("Month", fontsize=11)
    axes[0].set_title("Monthly Evaluation Metrics", fontsize=13)
    fig.autofmt_xdate()
    plt.tight_layout()

    if output_path is None:
        FIGURE_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(FIGURE_DIR / "monthly_metrics.png")
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Monthly metrics plot saved: {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# レポート生成
# ---------------------------------------------------------------------------

def build_report(
    booster: lgb.Booster,
    meta: dict,
    train_metrics: dict,
    valid_metrics: dict,
    test_metrics: Optional[dict],
    feature_importance_path: str,
    monthly_metrics_path: str,
    train_period: tuple[str, str],
    valid_period: tuple[str, str],
    test_period: Optional[tuple[str, str]],
    model_path: str,
    generation_date: str,
) -> str:
    """Markdownレポートを生成する"""
    top20 = pd.DataFrame({
        "feature": booster.feature_name(),
        "importance": booster.feature_importance(importance_type="gain"),
    }).sort_values("importance", ascending=False).head(20)

    best_params = meta.get("params", {})
    best_iteration = meta.get("best_iteration", "N/A")
    feature_count = len(booster.feature_name())

    # 特徴量重要度テーブル
    importance_table = "| Rank | Feature | Importance (Gain) |\n|------|---------|------------------|\n"
    for i, row in top20.reset_index(drop=True).iterrows():
        importance_table += f"| {i + 1} | {row['feature']} | {row['importance']:,.1f} |\n"

    # パラメータテーブル
    param_table = "| Parameter | Value |\n|-----------|-------|\n"
    for k, v in best_params.items():
        param_table += f"| {k} | {v} |\n"

    # 評価指標テーブル
    def metrics_row(split_name: str, m: Optional[dict]) -> str:
        if m is None:
            return f"| {split_name} | - | - | - | - | - |\n"
        return (
            f"| {split_name} | {m['ndcg@3']:.4f} | {m['recall@3']:.4f} | "
            f"{m['auc']:.4f} | {m['num_races']:,} | {m['num_rows']:,} |\n"
        )

    metrics_table = (
        "| Split | NDCG@3 | Recall@3 | AUC | レース数 | 行数 |\n"
        "|-------|--------|----------|-----|----------|------|\n"
    )
    metrics_table += metrics_row("学習 (Train)", train_metrics)
    metrics_table += metrics_row("検証 (Valid)", valid_metrics)
    if test_metrics is not None:
        metrics_table += metrics_row("テスト (Test)", test_metrics)

    # 画像パスを相対パスに変換
    def rel_img(path: str) -> str:
        if not path:
            return ""
        try:
            return str(Path(path).relative_to(REPORT_DIR))
        except ValueError:
            return path

    fi_img = rel_img(feature_importance_path)
    mm_img = rel_img(monthly_metrics_path)

    report = f"""# モデル評価レポート

**生成日時**: {generation_date}
**モデルパス**: `{model_path}`

---

## 1. モデル概要

| 項目 | 内容 |
|------|------|
| アルゴリズム | LightGBM LambdaRank |
| 目的関数 | lambdarank |
| ラベル形式 | 二値ラベル（3着以内=1, それ以外=0）|
| 評価指標 | NDCG@3, Recall@3, AUC |
| グループ単位 | レースID |
| 特徴量数 | {feature_count} |
| Best Iteration | {best_iteration} |

---

## 2. 時系列分割設定

| Split | 期間 |
|-------|------|
| 学習 (Train) | {train_period[0]} 〜 {train_period[1]} |
| 検証 (Valid) | {valid_period[0]} 〜 {valid_period[1]} |
{"| テスト (Test) | " + test_period[0] + " 〜 " + test_period[1] + " |" if test_period else ""}

---

## 3. 評価指標

{metrics_table}

> **指標説明**
> - **NDCG@3**: 上位3頭のランキング品質（1.0が最高）
> - **Recall@3**: 3着以内の馬を予測TOP3に含める割合（1.0が最高）
> - **AUC**: 3着以内の二値分類性能（ROC-AUC、0.5が無作為）

---

## 4. 特徴量重要度 TOP 20

{importance_table}
{"" if not fi_img else f"![Feature Importance]({fi_img})"}

---

## 5. 評価指標の月次推移

{"![Monthly Metrics](" + mm_img + ")" if mm_img else "_月次グラフは生成されませんでした（データ不足）_"}

---

## 6. モデルパラメータ

{param_table}

---

## 7. 目標指標との比較

| 指標 | 目標値 | 実績（検証） | 達成 |
|------|--------|-------------|------|
| NDCG@3 | 0.70以上 | {valid_metrics["ndcg@3"]:.4f} | {"✓" if valid_metrics["ndcg@3"] >= 0.70 else "✗"} |
| Recall@3 | 0.80以上 | {valid_metrics["recall@3"]:.4f} | {"✓" if valid_metrics["recall@3"] >= 0.80 else "✗"} |
| AUC | 0.70以上 | {valid_metrics["auc"]:.4f} | {"✓" if valid_metrics["auc"] >= 0.70 else "✗"} |

---

*このレポートは `scripts/generate_evaluation_report.py` によって自動生成されました。*
"""
    return report


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

DEFAULT_EXCLUDE_COLUMNS = [
    "race_id", "horse_id", "race_date", "target_place",
    "finish_position", "venue_code", "jockey_id", "trainer_id",
    "created_at", "race_number",
]
DEFAULT_CATEGORICAL_COLUMNS = ["course_type", "track_condition"]


def main():
    """メイン関数（CLIから実行）"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="モデル評価レポート生成スクリプト")
    parser.add_argument(
        "--model-path",
        default=None,
        help="ローカルのモデルファイルパス (.txt)",
    )
    parser.add_argument(
        "--gcs-model-path",
        default=None,
        help="GCSのモデルURIs (gs://...)",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT_ID"),
        help="GCPプロジェクトID",
    )
    parser.add_argument(
        "--local-data-path",
        default=None,
        help="ローカルCSVデータパス（指定時はBigQueryへの接続をスキップ）",
    )
    parser.add_argument(
        "--output-report",
        default=str(REPORT_PATH),
        help=f"出力レポートパス (デフォルト: {REPORT_PATH})",
    )
    parser.add_argument(
        "--validation-months",
        type=int,
        default=6,
        help="検証期間（月数、デフォルト: 6）",
    )
    parser.add_argument(
        "--test-months",
        type=int,
        default=0,
        help="テスト期間（月数、デフォルト: 0=スキップ）",
    )
    parser.add_argument(
        "--top-n-features",
        type=int,
        default=20,
        help="特徴量重要度表示数 (デフォルト: 20)",
    )
    parser.add_argument(
        "--skip-monthly-plot",
        action="store_true",
        help="月次推移グラフの生成をスキップ",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログ")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # モデル読み込み
    if args.gcs_model_path:
        if not args.project_id:
            logger.error("GCS読み込みには --project-id が必要です")
            return 1
        logger.info(f"Loading model from GCS: {args.gcs_model_path}")
        booster, meta = load_model_from_gcs(args.gcs_model_path, args.project_id)
        model_display_path = args.gcs_model_path
    elif args.model_path:
        logger.info(f"Loading model from local: {args.model_path}")
        booster, meta = load_model_local(args.model_path)
        model_display_path = args.model_path
    else:
        logger.error("--model-path または --gcs-model-path を指定してください")
        return 1

    # データ取得
    if args.local_data_path:
        logger.info(f"Loading data from CSV: {args.local_data_path}")
        df = pd.read_csv(args.local_data_path)
    else:
        if not args.project_id:
            logger.error("BigQuery接続には --project-id が必要です（または --local-data-path を使用）")
            return 1
        df = fetch_training_data_bq(args.project_id)

    df["race_date"] = pd.to_datetime(df["race_date"])
    df = df.sort_values(["race_date", "race_id", "horse_number"])

    # 時系列分割
    max_date = df["race_date"].max()

    if args.test_months > 0:
        test_start = max_date - pd.DateOffset(months=args.test_months) + pd.Timedelta(days=1)
        valid_end = test_start - pd.Timedelta(days=1)
        valid_start = valid_end - pd.DateOffset(months=args.validation_months) + pd.Timedelta(days=1)

        test_df = df[df["race_date"] >= test_start]
        valid_df = df[(df["race_date"] >= valid_start) & (df["race_date"] <= valid_end)]
        train_df = df[df["race_date"] < valid_start]

        test_period = (str(test_start.date()), str(max_date.date()))
    else:
        valid_start = max_date - pd.DateOffset(months=args.validation_months) + pd.Timedelta(days=1)
        valid_df = df[df["race_date"] >= valid_start]
        train_df = df[df["race_date"] < valid_start]
        test_df = None
        test_period = None

    logger.info(f"Train: {len(train_df)} rows | Valid: {len(valid_df)} rows | Test: {len(test_df) if test_df is not None else 0} rows")

    # 評価
    exclude_cols = DEFAULT_EXCLUDE_COLUMNS
    cat_cols = DEFAULT_CATEGORICAL_COLUMNS

    logger.info("Computing train metrics...")
    train_metrics = compute_metrics(booster, train_df, exclude_cols, cat_cols)
    logger.info("Computing valid metrics...")
    valid_metrics = compute_metrics(booster, valid_df, exclude_cols, cat_cols)
    test_metrics = None
    if test_df is not None and len(test_df) > 0:
        logger.info("Computing test metrics...")
        test_metrics = compute_metrics(booster, test_df, exclude_cols, cat_cols)

    # 可視化
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)
    fi_path = plot_feature_importance(booster, top_n=args.top_n_features)

    mm_path = ""
    if not args.skip_monthly_plot:
        mm_path = plot_monthly_metrics(df, booster, exclude_cols, cat_cols)

    # レポート生成
    train_period = (str(train_df["race_date"].min().date()), str(train_df["race_date"].max().date()))
    valid_period = (str(valid_df["race_date"].min().date()), str(valid_df["race_date"].max().date()))

    generation_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report_md = build_report(
        booster=booster,
        meta=meta,
        train_metrics=train_metrics,
        valid_metrics=valid_metrics,
        test_metrics=test_metrics,
        feature_importance_path=fi_path,
        monthly_metrics_path=mm_path,
        train_period=train_period,
        valid_period=valid_period,
        test_period=test_period,
        model_path=model_display_path,
        generation_date=generation_date,
    )

    output_path = Path(args.output_report)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_md, encoding="utf-8")

    print("\n" + "=" * 60)
    print("評価レポート生成完了")
    print("=" * 60)
    print(f"レポート: {output_path}")
    print(f"特徴量重要度グラフ: {fi_path}")
    if mm_path:
        print(f"月次推移グラフ: {mm_path}")
    print(f"\n評価指標（検証データ）:")
    print(f"  NDCG@3:   {valid_metrics['ndcg@3']:.4f}")
    print(f"  Recall@3: {valid_metrics['recall@3']:.4f}")
    print(f"  AUC:      {valid_metrics['auc']:.4f}")
    print(f"  レース数: {valid_metrics['num_races']:,}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
