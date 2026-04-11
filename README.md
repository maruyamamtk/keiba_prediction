# 競馬予測MLシステム

JRDBデータを活用した機械学習による馬券購入支援システム

## 概要

このプロジェクトは、JRDBの競馬データをGCP（BigQuery、Cloud Storage、Cloud Run）に取り込み、機械学習による馬券購入支援システムを構築します。

### 目標

- **対象馬券**: 単勝・複勝
- **予測内容**: 3着以内に入る確率
- **目標**: 回収率100%以上

### 技術スタック

- **言語**: Python 3.13
- **機械学習**: LightGBM (Learning to Rank)
- **最適化**: Optuna (ハイパーパラメータチューニング)
- **クラウド**: GCP (BigQuery, Cloud Storage, Cloud Run, Cloud Scheduler)
- **API**: FastAPI
- **ダッシュボード**: Streamlit, Plotly
- **テスト**: pytest
- **データソース**: JRDB

詳細な仕様・設計思想は [CLAUDE.md](./CLAUDE.md) を参照してください。

---

## システムアーキテクチャ

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Cloud Scheduler                                │
│                    (毎日 AM 6:00 JST トリガー)                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                     Cloud Run (keiba-pipeline)                         │
│                  FastAPI + uvicorn (src.automation.api.app)             │
│                                                                        │
│  [データパイプライン]                                                    │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────┐              │
│  │ JRDBから    │ →  │ GCSに      │ →  │ BigQueryに   │              │
│  │ ダウンロード │    │ アップロード │    │ MERGE/UPSERT │              │
│  └─────────────┘    └─────────────┘    └──────────────┘              │
│                                                                        │
│  [特徴量生成]                                                          │
│  ┌──────────────────────────────────────────────────┐                 │
│  │ BigQuery (raw) → SQL駆動 → features.training_data │                │
│  └──────────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      Cloud Storage (GCS)                               │
│               (${PROJECT_ID}-keiba-raw-data バケット)                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                          BigQuery                                      │
│              (raw, features, predictions データセット)                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## クイックスタート

### 1. 環境セットアップ

```bash
# リポジトリをクローン
git clone https://github.com/maruyamamtk/keiba_prediction.git
cd keiba_prediction

# Python仮想環境の作成
python3 -m venv venv
source venv/bin/activate

# 依存パッケージのインストール
pip install -r requirements.txt

# 環境変数の設定
cp .env.example .env
# .env ファイルを編集してGCP_PROJECT_IDを設定
```

### 2. GCPセットアップ

```bash
# GCPプロジェクトの認証
gcloud auth application-default login

# GCP初期セットアップ（API有効化、サービスアカウント作成、権限付与）
./infrastructure/scripts/setup_gcp.sh

# BigQueryデータセット・テーブル作成
python3 -m src.manual.create_tables

# セットアップ状態の確認
./infrastructure/scripts/verify_setup.sh
```

### 3. ローカルでのデータ取得・処理

```bash
# 過去分全件を一括処理（初回セットアップ推奨）
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31

# または日次処理（特定日のみ）
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15

# データ品質チェック
python3 -m src.manual.quality_check
```

### 4. 特徴量生成

```bash
# 指定期間の特徴量を生成
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-01 --end-date 2024-12-31
```

### 5. モデル学習・推論

```bash
# モデル学習
python3 -m src.models.train --project-id <PROJECT_ID>

# 推論実行（今週の土日を自動で対象とする）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker.txt
```

詳細なオプションは [src/models/README.md](./src/models/README.md) を参照してください。

### 6. バックテスト

```bash
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path <MODEL_PATH> \
    --start-date 2023-01-01 \
    --end-date 2023-12-31
```

詳細なオプションは [src/backtest/README.md](./src/backtest/README.md) を参照してください。

### 7. 投資戦略策定

#### A. パラメータ最適化（手動、初回および月次実行）

グリッドサーチで最適な投資パラメータを探索し、`config/strategy_config.yaml` に保存する。
**この手順を一度実行するだけで、以降の日次自動実行（手順B）が正しいパラメータで動作する。**

```bash
python3 scripts/run_strategy_optimization.py \
    --project-id <PROJECT_ID> \
    --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260301/model.txt \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
```

#### B. 日次投資戦略策定（手動確認用 / Cloud Schedulerで自動実行）

`config/strategy_config.yaml` のパラメータを読み込んで投資判断を実行する。
`POST /api/v1/strategy/daily` から毎朝 AM 8:30 に Cloud Scheduler で自動実行される（`dry_run=true` のためBQ保存なし）。

```bash
# 当日分（dry-runで結果確認）
python3 scripts/run_strategy.py --project-id <PROJECT_ID> --dry-run
```

> **前提条件**: `predictions.daily_predictions`（予測）と `predictions.daily_odds`（オッズ）に当日データが存在すること。

**investment_decisions テーブル作成（初回のみ）:**

```bash
python3 scripts/create_investment_decisions_table.py --project-id <PROJECT_ID>
```

> **スキーマ注意（Issue #161）**: `horse_numbers` カラムは STRING 型（カンマ区切り）です。単勝/複勝は `"3"`、ワイド/馬連は `"1,3"`、三連複は `"1,3,7"` のように格納します。

詳細なオプションは [src/backtest/README.md](./src/backtest/README.md) を参照してください。

### 8. Webダッシュボード閲覧

BigQueryの予測結果・バックテスト・モデル情報をブラウザで確認できます。

```bash
# 依存パッケージをインストール（初回のみ）
pip3 install streamlit==1.32.0 plotly==5.19.0

# ダッシュボードを起動
GCP_PROJECT_ID=your-project-id streamlit run src/dashboard/app.py
```

起動後、ブラウザで **http://localhost:8501** が自動的に開きます。

| 画面 | 内容 |
|------|------|
| ホーム | 指定日の推奨馬券TOP10・期待回収率・推奨投資額 |
| レース一覧 | 当日全レースの予測TOP3を一覧表示 |
| レース詳細 | 全馬の予測確率・オッズ・Plotlyグラフ・組み合わせオッズ |
| バックテスト | 累積損益推移グラフ・月次集計テーブル |
| モデル情報 | GCS最新モデルの特徴量重要度TOP30 |

---

### 9. Cloud Runデプロイ（本番環境）

```bash
# Dockerイメージのビルド・プッシュ
./infrastructure/scripts/build_and_push.sh

# Cloud Runにデプロイ
./infrastructure/scripts/deploy_cloud_run.sh

# デプロイ後の動作確認
./infrastructure/scripts/verify_deployment.sh
```

詳細な手順は [infrastructure/README.md](./infrastructure/README.md) を参照してください。

---

## スクリプト一覧

`scripts/` フォルダには、データ分析・運用・インフラ整備向けの CLIスクリプトが含まれています。
各スクリプトは `python scripts/<ファイル名> --help` で詳細なオプションを確認できます。

---

### `scripts/run_backtest.py` — バックテスト実行

学習済みモデルを使って指定期間の投資シミュレーションを行います。
Kelly基準による賭け金計算・期待回収率フィルタを適用し、回収率・的中率・最大ドローダウン等を評価します。

詳細は [src/backtest/README.md](./src/backtest/README.md) または `python scripts/run_backtest.py --help` を参照してください。

---

### `scripts/generate_evaluation_report.py` — モデル評価レポート生成

学習済みモデルを読み込み、NDCG@3・Recall@3・AUC などの評価指標と特徴量重要度グラフを含む
Markdownレポートを `docs/model_evaluation_report.md` に出力します。

```bash
python scripts/generate_evaluation_report.py \
    --model-path src/models/lgbm_ranker.txt \
    --project-id <PROJECT_ID>
```

詳細オプション: `python scripts/generate_evaluation_report.py --help`

---

### `scripts/generate_features.py` — 特徴量生成

指定期間のレースに対して特徴量パイプラインを実行し、`features.training_data` テーブルに保存します。

```bash
python scripts/generate_features.py \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
```

詳細オプション: `python scripts/generate_features.py --help`

---

### `scripts/diagnose_bq_load.py` — BQロード状態診断

BigQueryテーブルの存在確認・レコード数、GCSファイル数、`raw.load_history` のロード成否を一括診断します。

```bash
python3 scripts/diagnose_bq_load.py --show-errors
```

---

### `scripts/reload_gcs_to_bq.py` — GCSから BigQuery へ再ロード

GCS上の既存ファイルを指定データタイプでフィルタしてBigQueryに再ロードします。
Cloud Functionはオブジェクト作成時にのみトリガーされるため、既存ファイルの手動ロードに使います。

```bash
python scripts/reload_gcs_to_bq.py --data-type SEC --prefix Sec/
```

詳細オプション: `python scripts/reload_gcs_to_bq.py --help`

---

### `scripts/create_predictions_table.py` — 予測テーブル作成

`predictions` データセットと `daily_predictions` テーブルを BigQuery に作成します。
日次予測パイプライン（`POST /api/v1/predict/daily`）の実行前に一度だけ実行が必要です。

```bash
python scripts/create_predictions_table.py

# プロジェクトIDを明示する場合
python scripts/create_predictions_table.py --project-id <PROJECT_ID>
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 環境変数 | GCPプロジェクトID |

---

### `scripts/alter_odds_horse_id_nullable.py` — oddsテーブルスキーマ修正

`raw.odds` テーブルの `horse_id` カラムを `REQUIRED` から `NULLABLE` に変更します。
OZ（基準オッズ）ファイルには `horse_id` が含まれないため、初回セットアップ時に一度だけ実行します。

```bash
python3 scripts/alter_odds_horse_id_nullable.py

# プロジェクトIDを明示する場合
python3 scripts/alter_odds_horse_id_nullable.py --project-id <PROJECT_ID>
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--dataset` | `raw` | 対象データセット名 |

---

### `scripts/create_raw_combo_odds_table.py` — raw.combo_oddsテーブル作成

JRDBのOW（ワイド）・OT（三連複）・OZ（馬連）コンボ基準オッズを格納する
`raw.combo_odds` テーブルをBigQueryに作成します。初回セットアップ時に一度だけ実行します。

```bash
python3 scripts/create_raw_combo_odds_table.py --project-id <PROJECT_ID>
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 環境変数 | GCPプロジェクトID |

---

### `scripts/validate_odds_consistency.py` — JRDBとnetkeibaオッズ整合性検証

`raw.combo_odds`（JRDBコンボ基準オッズ）と `predictions.daily_odds_combo`（netkeibaスクレイプ）の
オッズ一致率を検証します。データ品質確認やモデル評価の参考として使用します。

```bash
python3 scripts/validate_odds_consistency.py --project-id <PROJECT_ID>
```

詳細オプション: `python3 scripts/validate_odds_consistency.py --help`

---

### `scripts/scrape_historical_odds.py` — 過去レース一括オッズ取得

netkeibaから指定期間（2016年以降）の全レースの単複・組み合わせ馬券オッズを一括スクレイプして
BigQueryに保存します。途中で停止しても再実行で続きから再開できます。

JRDB race_id（8文字）を netkeiba race_id（12桁）に直接変換するため、BQ照会不要で高速です。

```bash
# 単複・コンボオッズを一括取得
python3 scripts/scrape_historical_odds.py \
    --project-id <PROJECT_ID> \
    --start-date 2016-01-01 \
    --mode all
```

> **推定時間**: 単複のみ（約25,000レース）で約28時間、コンボ3種込みで約83時間。長時間処理のため `nohup` や `tmux` での実行を推奨。

詳細オプション: `python3 scripts/scrape_historical_odds.py --help`

---

## プロジェクト構成

```
keiba_prediction/
├── src/
│   ├── __init__.py
│   ├── automation/                    # 自動化データ取り込み
│   │   ├── __init__.py
│   │   ├── api/
│   │   │   ├── __init__.py
│   │   │   └── app.py                # FastAPI HTTPエンドポイント
│   │   ├── data/
│   │   │   ├── __init__.py
│   │   │   ├── jrdb_downloader.py    # JRDBダウンローダー
│   │   │   ├── jrdb_parser.py        # JRDBデータパーサー
│   │   │   ├── ipat_purchaser.py     # JRA IPAT自動馬券購入（Playwright）
│   │   │   ├── load_to_bq.py         # BigQueryロード（MERGE+重複スキップ）
│   │   │   ├── netkeiba_scraper.py   # netkeibaリアルタイムオッズスクレイパー
│   │   │   └── upload_to_gcs.py      # GCSアップロード
│   │   └── pipeline/
│   │       ├── __init__.py
│   │       ├── daily_pipeline.py     # 日次パイプライン
│   │       └── full_load_pipeline.py # 過去分全件ロード
│   ├── manual/                        # 手動実行スクリプト
│   │   ├── __init__.py
│   │   ├── create_tables.py          # BigQueryテーブル作成
│   │   ├── quality_check.py          # データ品質チェック
│   │   └── validation_rules.py       # バリデーションルール定義
│   ├── ml/                            # 機械学習（特徴量生成）
│   │   ├── __init__.py
│   │   └── features/
│   │       ├── __init__.py
│   │       ├── feature_pipeline.py    # 特徴量パイプライン
│   │       └── feature_query_raw.sql. # 特徴量集計用クエリ
│   ├── models/                        # モデル学習・推論
│   │   ├── __init__.py
│   │   ├── lgbm_ranker.py            # LightGBM LambdaRankモデル
│   │   ├── train.py                  # 学習パイプライン
│   │   ├── predict.py                # 推論パイプライン
│   │   └── tuning.py                 # Optunaハイパーパラメータチューニング
│   ├── backtest/                      # バックテストシミュレーター
│   │   ├── __init__.py
│   │   ├── simulator.py              # Kelly基準・BacktestSimulatorクラス
│   │   ├── metrics.py                # 評価指標（回収率・的中率・ドローダウン・シャープレシオ）
│   │   ├── strategy.py               # 投資戦略（Kelly基準・期待回収率フィルタ）
│   │   └── strategy_optimizer.py     # 戦略パラメータ最適化
│   └── dashboard/                     # Streamlit Webダッシュボード
│       ├── app.py                    # メインアプリ（サイドバーナビ・日付選択UI）
│       ├── data.py                   # BigQueryデータ取得（@st.cache_data TTLキャッシュ付き）
│       └── components/
│           ├── home.py               # ホーム画面（推奨馬券TOP10・レース別内訳）
│           ├── race_list.py          # レース一覧画面（当日全レースの予測TOP3）
│           ├── race_detail.py        # レース詳細（全馬予測・Plotly棒グラフ・組み合わせオッズ）
│           ├── backtest.py           # バックテスト（累積損益推移グラフ・月次集計テーブル）
│           └── model_info.py         # モデル情報（GCS最新モデルから特徴量重要度を可視化）
├── scripts/                           # ユーティリティスクリプト
│   ├── add_start_time_to_race_info.py    # raw.race_infoにstart_timeカラム追加（Issue #214）
│   ├── create_daily_odds_combo_table.py  # predictions.daily_odds_comboテーブル作成
│   ├── create_daily_odds_table.py    # predictions.daily_oddsテーブル作成
│   ├── create_predictions_table.py   # predictions.daily_predictionsテーブル作成
│   ├── create_purchase_history_table.py  # predictions.purchase_historyテーブル作成（Issue #213）
│   ├── create_raw_combo_odds_table.py  # raw.combo_oddsテーブル作成（Issue #140）
│   ├── generate_features.py
│   ├── reload_gcs_to_bq.py
│   ├── run_backtest.py               # CLIバックテスト実行スクリプト
│   ├── run_strategy.py               # 日次投資戦略策定スクリプト（手動確認用）
│   ├── run_strategy_optimization.py  # 投資パラメータ最適化スクリプト（手動実行）
│   ├── scrape_historical_odds.py       # 過去レース一括オッズ取得（netkeibaスクレイプ）
│   ├── setup_bigquery.sh
│   ├── setup_gcp.sh
│   ├── sync_to_gcs.sh
│   └── validate_odds_consistency.py    # JRDBとnetkeibaオッズの整合性検証
├── tests/                             # テストコード
├── config/                            # 設定ファイル（BigQueryスキーマ、モデル設定）
│   └── model_config.yaml             # LightGBMモデル設定
├── infrastructure/                    # GCPインフラ設定
│   ├── cloud_run_config.yaml
│   └── scripts/
├── docs/                              # ドキュメント
│   ├── GCP_SETUP.md
│   ├── BIGQUERY_SETUP.md
│   └── LINE_SETUP.md
├── reports/                           # 品質チェックレポート出力先
├── Dockerfile
├── Dockerfile.dashboard               # ダッシュボード用軽量コンテナ（Playwright不要）
├── requirements.txt
├── .env.example
├── CLAUDE.md                          # システム仕様書
├── ML_FEATURE.md                      # 特徴量設計
├── SCHEMA.md                          # JRDBスキーマ仕様書
└── README.md
```

---

## フォルダごとの役割

プロジェクトは以下の4つのカテゴリに分類されています。

### 1. `src/automation/` - 自動化処理

**目的**: Cloud Runで自動実行される処理。JRDB→GCS→BigQueryのデータ取り込みパイプライン全体を担当。

**含まれるモジュール**:
- `data/jrdb_downloader.py`: JRDBからデータをダウンロード
- `data/jrdb_parser.py`: JRDBデータの解析とパース
- `data/upload_to_gcs.py`: ローカルファイルをGCSにアップロード
- `data/load_to_bq.py`: GCSからBigQueryへのロード（MERGE処理、重複スキップ）
- `pipeline/daily_pipeline.py`: 「その日に追加されたデータのダウンロード→GCS→BigQueryへの格納→特徴量生成」を行う
- `pipeline/full_load_pipeline.py`: 「指定期間のデータをJRDBからダウンロード→GCS→BigQuery→特徴量生成」を一括実行する
- `api/app.py`: FastAPI HTTPエンドポイント（Cloud Run用）

**使用場面**:
- Cloud Schedulerから毎日自動実行（日次パイプライン）
- 初回セットアップやデータ欠損の補完（全件ロード）
- Cloud Runでのバックグラウンド実行

### 2. `src/manual/` - 手動実行スクリプト

**目的**: 開発者が必要に応じて手動で実行する管理・検証スクリプト。

**含まれるモジュール**:
- `create_tables.py`: BigQueryのデータセット・テーブルを作成
- `quality_check.py`: BigQueryデータの品質チェック（NULL、重複、範囲検証）
- `validation_rules.py`: 品質チェックのルール定義

**使用場面**:
- 初回セットアップ時のテーブル作成
- データロード後の品質確認
- 定期的なデータ検証

### 3. `src/ml/` - 機械学習（特徴量生成）

**目的**: 機械学習モデルの学習・予測と特徴量エンジニアリング。

**含まれるモジュール**:
- `features/feature_pipeline.py`: 特徴量生成パイプライン
- `features/past_performance.py`: 過去走集計特徴量
- `features/condition_features.py`: 条件適性特徴量（芝/ダート、距離帯など）

**使用場面**:
- BigQueryのrawデータから特徴量テーブルを生成
- モデル学習前の特徴量準備
- 特徴量の追加・更新

### 4. `src/models/` - モデル学習・推論

**目的**: LightGBM LambdaRankモデルの学習・推論パイプライン。

**含まれるモジュール**:
- `lgbm_ranker.py`: LightGBM LambdaRankモデルのラッパークラス（学習・予測・保存・読み込み）
- `train.py`: 学習パイプライン（BigQueryデータ取得 → 時系列分割 → 学習 → 評価 → GCS保存）
- `predict.py`: 推論パイプライン（モデル読み込み → 今週末レース予測 → 結果整形）
- `tuning.py`: Optunaベイズ最適化によるハイパーパラメータチューニング

**使用場面**:
- モデルの学習と評価
- ハイパーパラメータの自動最適化
- 今週末のレース着順予測
- モデルのGCS保存・読み込み

詳細は [src/models/README.md](./src/models/README.md) を参照してください。

### 5. `src/dashboard/` - Streamlit Webダッシュボード

**目的**: BigQueryの予測結果・バックテスト結果・モデル情報をブラウザから閲覧できるWebダッシュボードを提供する。

**含まれるモジュール**:
- `app.py`: Streamlitメインアプリ（サイドバーナビ・日付選択UI）
- `data.py`: BigQueryデータ取得モジュール（`@st.cache_data` TTLキャッシュ付き）
- `components/home.py`: ホーム画面（指定日の推奨馬券TOP10・期待回収率・推奨額・レース別内訳）
- `components/race_list.py`: レース一覧画面（当日全レースの予測TOP3を一覧表示）
- `components/race_detail.py`: レース詳細（全馬の予測確率・オッズ・期待回収率、Plotly棒グラフ、組み合わせオッズ）
- `components/backtest.py`: バックテスト（累積損益推移グラフ、月次集計テーブル）
- `components/model_info.py`: モデル情報（GCSから最新モデルを読み込み特徴量重要度TOP30を可視化）

**環境変数**:

| 変数名 | 説明 |
|--------|------|
| `GCP_PROJECT_ID` | GCP プロジェクト ID |

**起動方法**:

```bash
# ローカル起動
GCP_PROJECT_ID=your-project streamlit run src/dashboard/app.py

# Cloud Run デプロイ（dashboard-service）
docker build --platform linux/amd64 -f Dockerfile.dashboard -t dashboard-service .
```

**使用場面**:
- 当日の推奨馬券を確認する（ホーム画面）
- 全レースの予測上位馬を一覧で確認する（レース一覧画面）
- 特定レースの全馬予測と組み合わせオッズを詳細確認する（レース詳細画面）
- 過去の投資シミュレーション結果を可視化する（バックテスト画面）
- モデルの特徴量重要度を確認する（モデル情報画面）

### 6. `src/backtest/` - バックテストシミュレーター

**目的**: 過去データを用いた投資シミュレーションにより、モデルの実用性を検証する。

**含まれるモジュール**:
- `simulator.py`: Kelly基準による賭け金計算、BacktestSimulatorクラス（BigQueryデータ取得 → 予測 → 投資判断 → 損益計算）
- `metrics.py`: バックテスト評価指標（回収率・的中率・最大ドローダウン・シャープレシオ）

**使用場面**:
- モデル学習後の実用性評価（回収率シミュレーション）
- 投資戦略パラメータ（Kelly係数・期待回収率閾値）の調整
- `scripts/run_backtest.py` からCLI実行

**投資ロジック（Issue #139 改修後）**:
- **基本馬券**: 複勝・ワイド・三連複をベースに期待回収率フィルタで選定
- **パターンA（突出型）**: `top1_prob - top2_prob > p1` の場合、単勝・馬連を追加購入
- 賭け金配分: **オッズ逆数比率**方式（1レース合計 = `budget_per_race` 固定 3000円）
- コンボオッズ参照: `predictions.daily_odds_combo` → `raw.combo_odds` → `raw.payouts` の順にフォールバック
- パラメータ管理: `config/strategy_config.yaml`（`run_strategy_optimization.py` でグリッドサーチ100通り最適化: p1 × threshold × r の3次元探索）
- **`min_prob_threshold`**: 軸馬の最低複勝率（これ未満は複勝単体買いの軸馬から除外）
- **`prob_weight_r`**: 馬選定スコア係数（スコア = `odds × prob^r`。r>1 で高確率馬を優先）

---

## APIエンドポイント

Cloud RunにデプロイされたFastAPIアプリケーションは以下のエンドポイントを提供します。

| メソッド | エンドポイント | 用途 |
|---------|---------------|------|
| GET | `/` | ヘルスチェック（簡易） |
| GET | `/health` | ヘルスチェック（詳細） |
| GET | `/docs` | OpenAPI (Swagger UI) ドキュメント |
| POST | `/api/v1/load/daily` | 日次ロード（同期） |
| POST | `/api/v1/load/daily/async` | 日次ロード（非同期、Cloud Scheduler用） |
| POST | `/api/v1/load/full` | 全件ロード（非同期） |
| POST | `/api/v1/load/full/sync` | 全件ロード（同期、テスト用） |
| POST | `/api/v1/features/generate` | 特徴量生成（同期） |
| POST | `/api/v1/features/generate/async` | 特徴量生成（非同期） |
| POST | `/api/v1/predict/daily` | 翌日レース予測 + BQ/GCS保存（Cloud Scheduler用）。`model_path` 未指定時はGCSから最新モデルを自動取得。 |
| POST | `/api/v1/predict/on-demand` | 任意日付レース予測 + BQ/GCS保存（手動実行用） |
| POST | `/api/v1/odds/scrape` | netkeibaから当日オッズを取得し `predictions.daily_odds` にUPSERT保存。`include_combo=true` で組み合わせ馬券オッズも取得し `predictions.daily_odds_combo` に保存。 |
| POST | `/api/v1/strategy/daily` | 当日の投資戦略を策定。`config/strategy_config.yaml` のパラメータを使用。`dry_run` のデフォルトは `true`（BQ保存なし）。実際のBQ保存は発走直前の `race-day-purchase` が最新オッズで上書き実行する（Issue #231）。 |
| POST | `/api/v1/line/webhook` | LINE Messaging API Webhook受信。「日付 競馬場名 レース番号」形式のメッセージに対して予測テーブルと推奨馬券リストを返信。 |
| POST | `/api/v1/purchase/daily` | 発走5分前JRA IPAT自動馬券購入。`dry_run` のデフォルトは `false`（本番購入モード）。`dry_run=true` の場合はIPATログイン・購入をスキップし推奨馬券をLINE通知のみ実行。結果は `predictions.purchase_history` に保存。 |

---

## データパイプライン

### 全体フロー

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. データ取得 (JRDB → downloaded_files/)                                  │
│    - JRDBDownloader: HTTP Basic認証 + lzh解凍 + CP932→UTF-8変換       │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. GCSアップロード (ローカル → GCS)                                      │
│    - GCSUploader: MD5重複チェック + バッチアップロード                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. BigQueryロード (GCS → BigQuery)                                      │
│    - BigQueryLoader: MERGE(UPSERT)処理 + ロード履歴管理                │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. 特徴量生成 (BigQuery raw → features)                                 │
│    - FeaturePipeline: SQL駆動方式（feature_query_raw.sql）             │
└─────────────────────────────────────────────────────────────────────────┘
```

### 実行方法

#### 1. ローカルCLI実行

**日次パイプライン（統合処理）**

```bash
# 当日のデータを処理（ダウンロード→GCSアップロード→BigQueryロード）
python3 -m src.automation.pipeline.daily_pipeline

# 特定日付を指定
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15
```

**全件ロード（初回セットアップ/データ補完）**

```bash
# 全期間のデータを一括処理
python3 -m src.automation.pipeline.full_load_pipeline

# 期間を指定
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31
```

**特徴量生成**

```bash
# 指定期間の特徴量を生成
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-01 --end-date 2024-12-31

# 詳細ログ付きで実行
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-01 --end-date 2024-12-31 -v
```

**個別モジュール実行（高度な使用）**

```bash
# Step 1: JRDBダウンロードのみ
python3 -m src.automation.data.jrdb_downloader --start-date 240101

# Step 2: GCSアップロードのみ
python3 -m src.automation.data.upload_to_gcs

# Step 3: BigQueryロードのみ（重複スキップ推奨）
python3 -m src.automation.data.load_to_bq --skip-loaded
```

#### 2. FastAPI経由（ローカル/Cloud Run共通）

```bash
# 開発環境でサーバー起動
uvicorn src.automation.api.app:app --reload --port 8080

# ヘルスチェック
curl http://localhost:8080/health
```

利用可能なエンドポイントの詳細は [APIエンドポイント](#apiエンドポイント) または `GET /docs` (Swagger UI) を参照してください。

#### 3. Cloud Runデプロイと自動実行

**デプロイ手順**

詳細な手順は [infrastructure/README.md](./infrastructure/README.md) を参照してください。

```bash
# Dockerイメージのビルド・プッシュ
./infrastructure/scripts/build_and_push.sh

# Cloud Runにデプロイ
./infrastructure/scripts/deploy_cloud_run.sh

# デプロイ後の動作確認
./infrastructure/scripts/verify_deployment.sh
```

**Cloud Scheduler設定（自動実行）**

```bash
# Cloud Schedulerジョブの作成（毎日AM 6:00 JSTに日次パイプラインを自動実行）
./infrastructure/scripts/setup_scheduler.sh

# 手動で即時実行（テスト用）
gcloud scheduler jobs run daily-data-pipeline --location=asia-northeast1
```

主なジョブ一覧:

| ジョブ名 | スケジュール (JST) | 用途 |
|---------|-----------------|------|
| `daily-data-pipeline` | 毎日 AM 6:00 | 日次データロード |
| `race-day-predict` | 毎日 AM 8:00 | レース予測 |
| `race-day-odds-scrape` | 毎日 AM 8:15 | netkeibaオッズ取得 |
| `race-day-strategy` | 毎日 AM 8:30 | 投資戦略策定（dry_run=true） |
| `monthly-model-retrain` | 毎月第1月曜 AM 8:00 | モデル月次再学習 |
| `race-day-purchase` | 土日 8:00〜17:55 の5分おき | 発走直前IPAT自動馬券購入 |

詳細な設定内容・操作コマンド・障害対応は [SCHEDULE.md](./SCHEDULE.md) を参照してください。

---

## BigQueryテーブル構成

### rawデータセット

| テーブル | データソース | 説明 | 行数 |
|---------|-------------|------|------|
| `race_info` | BAA (番組データ) | レース基本情報（`start_time` STRING: HHMM形式、例: "1015" = 10:15）| ~33,400 |
| `horse_results` | KYF (競走馬データ) | 出馬表・予測指数 | ~486,500 |
| `race_results` | SEC (成績データ) | レース結果 | ~486,500 |
| `horse_extended` | KKA (拡張馬データ) | 条件別成績 | ~486,500 |
| `horse_master` | KSA (馬マスター) | 馬基本情報 | ~21,500 |
| `venue_info` | KAB (開催情報) | 馬場状態・天候 | ~418 |
| `load_history` | (管理用) | ロード履歴管理 | ~3,350 |
| `odds` | OZ (基準オッズ) | 単勝・複勝オッズ（JRDBコード表） | - |
| `combo_odds` | OW/OT/OZ | JRDBコンボ基準オッズ（ワイド/三連複/馬連） | - |

詳細なスキーマは [SCHEMA.md](./SCHEMA.md) および `config/bq_schema_*.json` を参照してください。

テーブル作成コマンド（raw.combo_odds）:

```bash
python3 scripts/create_raw_combo_odds_table.py --project-id <PROJECT_ID>
```

`raw.race_info` への `start_time` カラム追加（Issue #214。初回のみ実行）:

```bash
python3 scripts/add_start_time_to_race_info.py --project-id <PROJECT_ID>
```

### featuresデータセット

| テーブル | 説明 | カラム数 | 行数 |
|---------|------|---------|------|
| `training_data` | 学習用特徴量 | 257 | 466,265 |

特徴量の詳細は [ML_FEATURE.md](./ML_FEATURE.md) を参照してください。

### predictionsデータセット

| テーブル | 説明 | 状態 |
|---------|------|------|
| `predictions.daily_predictions` | 日次予測結果 | 実装済み（Issue #117） |
| `predictions.daily_odds` | netkeibaリアルタイム単複オッズ | 実装済み（Issue #131） |
| `predictions.daily_odds_combo` | netkeibaリアルタイム組み合わせ馬券オッズ | 実装済み（Issue #134） |
| `predictions.investment_decisions` | 日次投資判断結果（`horse_numbers` STRING型・カンマ区切り） | 実装済み（Issue #105 / スキーマ変更 Issue #161） |
| `predictions.purchase_history` | IPAT馬券購入履歴（パーティション: `race_date`） | 実装済み（Issue #213） |

テーブル作成コマンド:

```bash
python3 scripts/create_predictions_table.py
# GCP_PROJECT_IDを環境変数から読み込む。または --project-id で明示指定も可能。

python3 scripts/create_daily_odds_table.py --project-id <PROJECT_ID>
python3 scripts/create_daily_odds_combo_table.py --project-id <PROJECT_ID>
python3 scripts/create_investment_decisions_table.py --project-id <PROJECT_ID>
python3 scripts/create_purchase_history_table.py --project-id <PROJECT_ID>
```

各テーブルのスキーマ詳細は `config/bq_schema_*.json` を参照してください。

### オッズテーブルの設計意図（JRDB系 vs netkeiba系）

オッズデータは「JRDBから取得する基準オッズ（`raw.*`）」と「netkeibaからリアルタイムにスクレイプするオッズ（`predictions.*`）」の2系統が存在します。それぞれ役割が異なり、用途に応じて使い分けます。

| 比較軸 | `raw.odds` / `raw.combo_odds` | `predictions.daily_odds` / `predictions.daily_odds_combo` |
|--------|-------------------------------|----------------------------------------------------------|
| **データ源** | JRDB（OZ/OW/OTファイル） | netkeiba（Playwrightスクレイピング） |
| **オッズ種別** | 単複 / 馬連・ワイド・三連複 | 単複 / 馬連・馬単・ワイド・三連複 |
| **更新契機** | 日次JRDBロード（自動、パイプライン） | `POST /api/v1/odds/scrape`（Cloud Schedulerが毎朝実行） |
| **対象期間** | 過去全期間（JRDBデータがある限り） | スクレイプを開始した日付以降のみ |
| **主な用途** | **バックテスト**（過去シミュレーション） | **当日の投資戦略実行**（リアルタイム判断） |
| **精度** | 前日公開の参考オッズ（確定値ではない） | 発走直前の最新オッズに近い値 |

**なぜ2系統必要か:**
- バックテストは数年分の過去データを扱うため、JRDBベースの `raw.*` が必要。
- 当日の実際の馬券購入には、より正確な最新オッズが必要なため `predictions.*` をスクレイプする。

**コンボオッズのフォールバック順序（バックテスト時）:**

```
1. predictions.daily_odds_combo  ← netkeibaスクレイプ済みの日付は最優先（精度高い）
         ↓ データ不足なら
2. raw.combo_odds                ← JRDBで過去全期間をカバー
         ↓ それでも不足なら
3. raw.payouts                   ← 最終手段（的中馬券のみ・先読みバイアスに注意）
```

### backtestsデータセット

| テーブル | 説明 | 状態 |
|---------|------|------|
| `backtests.backtest_results` | バックテスト結果 | 実装済み（`--save-to-bq` オプションで保存） |

---

## データ品質チェック

```bash
# 全テーブルのチェック
python3 -m src.manual.quality_check

# 特定テーブルのみチェック
python3 -m src.manual.quality_check --table raw.race_info
```

---

## テスト

```bash
# 全テストを実行
python -m pytest tests/ -v

# 特定のテストファイルを実行
python -m pytest tests/test_quality_check.py -v

# カバレッジレポート付き
python -m pytest tests/ --cov=src --cov-report=html
```

---

## ドキュメント

| ドキュメント | 内容 |
|------------|------|
| [CLAUDE.md](./CLAUDE.md) | システム全体の仕様書（目的、設計思想、未実装機能の設計） |
| [SCHEDULE.md](./SCHEDULE.md) | Cloud Schedulerジョブ一覧（稼働ジョブ・操作コマンド・障害対応手順） |
| [SCHEMA.md](./SCHEMA.md) | JRDBデータスキーマ仕様書（データタイプ、フィールド定義、コードテーブル） |
| [ML_FEATURE.md](./ML_FEATURE.md) | 特徴量設計（特徴量リスト、Target Encoding、リーク対策） |
| [infrastructure/README.md](./infrastructure/README.md) | インフラセットアップガイド（GCP、Cloud Run、Docker） |
| [src/models/README.md](./src/models/README.md) | モデル学習・推論の詳細ドキュメント |

---

## ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)
| 2026-04-04 | Issue #214（raw.race_info に start_time カラム追加: HHMM形式、jrdb_parser.py + scripts/add_start_time_to_race_info.py 追加）・Issue #213（発走5分前JRA IPAT自動馬券購入パイプライン: POST /api/v1/purchase/daily 追加、ipat_purchaser.py・predictions.purchase_history・scripts/create_purchase_history_table.py 追加、Cloud Scheduler ジョブ race-day-purchase 追加、dry_run フラグで本番切り替え可能）の実装を反映 |
