# 競馬予測MLシステム

JRDBデータを活用した機械学習による馬券購入支援システム

## 概要

このプロジェクトは、JRDBの競馬データをGCP（BigQuery、Cloud Storage、Cloud Run）に取り込み、機械学習による馬券購入支援システムを構築します。

### 目標

- **対象馬券**: 単勝・複勝
- **予測内容**: 3着以内に入る確率
- **目標**: 回収率100%以上

### 技術スタック

- **言語**: Python 3.9+
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
# モデル学習（GCSアップロードなし）
python3 -m src.models.train --project-id <PROJECT_ID> --skip-gcs-upload --output-dir ./models

# モデル学習（GCSアップロードあり）
python3 -m src.models.train --project-id <PROJECT_ID>

# Optunaによるハイパーパラメータチューニング
python3 -m src.models.train --project-id <PROJECT_ID> --tune --n-trials 50

# チューニング（タイムアウト付き）
python3 -m src.models.train --project-id <PROJECT_ID> --tune --tune-timeout 3600

# 推論実行（今週の土日を自動で対象とする）
# --model-path にはローカルパスまたは gs:// URI を指定可能
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt

# GCS上のモデルを直接指定（自動ダウンロードされる）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker_20260215.txt

# 推論結果をCSV保存
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt --output-csv predictions.csv

# 特定の1日を指定して推論
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt \
  --target-dates 2026-01-10

# 複数の任意日付を指定して推論
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt \
  --target-dates 2026-01-10 2026-01-11 2026-01-12
```

### 6. バックテスト

```bash
# 基本的なバックテスト実行
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --start-date 2023-01-01 \
    --end-date 2023-12-31

# オプション付き（Kelly係数・閾値・出力先を指定）
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --start-date 2023-01-01 \
    --end-date 2023-12-31 \
    --initial-capital 100000 \
    --kelly-fraction 0.25 \
    --threshold 1.2 \
    --output-csv results/backtest_2023.csv \
    --output-chart results/capital_curve.png \
    --save-to-bq
```

**主なオプション:**

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 必須 | GCPプロジェクトID |
| `--model-path` | 必須 | モデルファイルパス |
| `--start-date` | 必須 | バックテスト開始日 (YYYY-MM-DD) |
| `--end-date` | 必須 | バックテスト終了日 (YYYY-MM-DD) |
| `--initial-capital` | 100000 | 初期資金（円） |
| `--kelly-fraction` | 0.25 | Fractional Kellyの係数 |
| `--threshold` | 1.2 | 期待回収率フィルタ閾値 |
| `--output-csv` | なし | 結果CSV保存先パス |
| `--output-chart` | なし | 資金推移グラフ保存先パス |
| `--save-to-bq` | False | BigQuery保存フラグ |

### 7. 投資戦略策定

#### A. パラメータ最適化（手動、初回および月次実行）

グリッドサーチで最適な投資パラメータを探索し、`config/strategy_config.yaml` に保存する。
**この手順を一度実行するだけで、以降の日次自動実行（手順B）が正しいパラメータで動作する。**

```bash
# パラメータ最適化を実行（strategy_config.yamlを更新）
python3 scripts/run_strategy_optimization.py \
    --project-id <PROJECT_ID> \
    --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260301/model.txt \
    --start-date 2024-01-01 \
    --end-date 2024-12-31

# 詳細オプション指定
python3 scripts/run_strategy_optimization.py \
    --project-id <PROJECT_ID> \
    --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260301/model.txt \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --metric recovery_rate \
    --output-csv results/optimization_2024.csv \
    --top-n 10
```

**主なオプション:**

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 必須 | GCPプロジェクトID |
| `--model-path` | 必須 | モデルファイルパス（ローカルまたは gs://） |
| `--start-date` | 必須 | 最適化期間開始日 (YYYY-MM-DD) |
| `--end-date` | 必須 | 最適化期間終了日 (YYYY-MM-DD) |
| `--metric` | recovery_rate | 最適化指標（recovery_rate / hit_rate / sharpe_ratio / max_drawdown） |
| `--initial-capital` | 100000 | 初期資金（円） |
| `--r-range` | `0.5 1.0 1.5 2.0` | `prob_weight_r` の探索値（複数指定可） |
| `--output-csv` | なし | 全グリッドサーチ結果のCSV保存先 |
| `--top-n` | 10 | 上位N件を表示 |

#### B. 日次投資戦略策定（手動確認用 / Cloud Schedulerで自動実行）

`config/strategy_config.yaml` のパラメータを読み込んで投資判断を実行する。
`POST /api/v1/strategy/daily` から毎朝 AM 9:00 に Cloud Scheduler で自動実行されるが、
スクリプトを手動実行して内容を確認することもできる。

```bash
# 当日分の投資戦略を実行（BQ保存あり）
python3 scripts/run_strategy.py --project-id <PROJECT_ID>

# 特定日を指定
python3 scripts/run_strategy.py --project-id <PROJECT_ID> --target-date 2026-03-07

# BQ保存をスキップして結果を確認のみ（dry-run）
python3 scripts/run_strategy.py --project-id <PROJECT_ID> --dry-run
```

**主なオプション:**

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 必須 | GCPプロジェクトID |
| `--target-date` | 当日 | 対象日（YYYY-MM-DD） |
| `--dry-run` | False | BQ保存をスキップして結果表示のみ |
| `--initial-capital` | 100000 | 初期資金（円） |

> **前提条件**: `predictions.daily_predictions`（予測）と `predictions.daily_odds`（オッズ）に当日データが存在すること。

**investment_decisions テーブル作成（初回のみ）:**

```bash
python3 scripts/create_investment_decisions_table.py --project-id <PROJECT_ID>

# スキーマ変更後に再作成する場合（Issue #161: horse_number → horse_numbers）
python3 scripts/create_investment_decisions_table.py --project-id <PROJECT_ID> --recreate
```

> **スキーマ注意（Issue #161）**: `horse_numbers` カラムは STRING 型（カンマ区切り）です。単勝/複勝は `"3"`、ワイド/馬連は `"1,3"`、三連複は `"1,3,7"` のように格納します。

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

```bash
# 基本的なバックテスト実行
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --start-date 2023-01-01 \
    --end-date 2023-12-31

# 詳細オプション指定
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --start-date 2023-01-01 \
    --end-date 2023-12-31 \
    --initial-capital 100000 \
    --kelly-fraction 0.25 \
    --threshold 1.2 \
    --output-csv results/backtest_2023.csv \
    --output-chart results/capital_curve.png \
    --save-to-bq
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 必須 | GCPプロジェクトID |
| `--model-path` | 必須 | モデルファイルパス（ローカル） |
| `--start-date` | 必須 | バックテスト開始日 (YYYY-MM-DD) |
| `--end-date` | 必須 | バックテスト終了日 (YYYY-MM-DD) |
| `--initial-capital` | 100000 | 初期資金（円） |
| `--kelly-fraction` | 0.25 | Fractional Kellyの係数 |
| `--threshold` | 1.2 | 期待回収率フィルタ閾値 |
| `--budget-per-race` | 3000 | 1レースあたり固定予算（円） |
| `--output-csv` | なし | 賭け記録CSV保存先パス |
| `--output-chart` | なし | 資金推移グラフ保存先パス |
| `--save-to-bq` | False | `backtests.backtest_results` への保存フラグ |

---

### `scripts/generate_evaluation_report.py` — モデル評価レポート生成

学習済みモデルを読み込み、NDCG@3・Recall@3・AUC などの評価指標と特徴量重要度グラフを含む
Markdownレポートを `docs/model_evaluation_report.md` に出力します。

```bash
# ローカルモデルを使用
python scripts/generate_evaluation_report.py \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --project-id <PROJECT_ID>

# GCS上のモデルを使用
python scripts/generate_evaluation_report.py \
    --gcs-model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260217/lgbm_ranker_20260217.txt \
    --project-id <PROJECT_ID>

# BigQueryへの接続なしでローカルCSVを使用
python scripts/generate_evaluation_report.py \
    --model-path src/models/lgbm_ranker_20260217.txt \
    --local-data-path /path/to/training_data.csv
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--model-path` | なし | ローカルモデルファイルパス (.txt) |
| `--gcs-model-path` | なし | GCSモデルURI (gs://...) |
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--local-data-path` | なし | ローカルCSVデータパス（BQ接続をスキップ） |
| `--output-report` | `docs/model_evaluation_report.md` | 出力レポートパス |
| `--validation-months` | 6 | 検証期間（月数） |
| `--test-months` | 0 | テスト期間（月数、0=スキップ） |
| `--top-n-features` | 20 | 特徴量重要度表示数 |
| `--skip-monthly-plot` | False | 月次推移グラフの生成をスキップ |

---

### `scripts/generate_features.py` — 特徴量生成

指定期間のレースに対して特徴量パイプラインを実行し、`features.training_data` テーブルに保存します。
並列処理・バッチ処理に対応し、エラー時のリトライも行います。

```bash
# 1ヶ月分の特徴量を生成
python scripts/generate_features.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-31

# 並列処理ワーカー数を指定（大量データ向け）
python scripts/generate_features.py \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --max-workers 8

# ドライラン（処理対象レース数の確認のみ）
python scripts/generate_features.py \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --dry-run
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--start-date` | 必須 | 開始日 (YYYY-MM-DD) |
| `--end-date` | 必須 | 終了日 (YYYY-MM-DD) |
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--max-workers` | 4 | 並列処理のワーカー数 |
| `--batch-size` | 100 | BigQueryへの一括保存レース数 |
| `--no-parallel` | False | 並列処理を無効化（デバッグ用） |
| `--max-retries` | 3 | エラー時の最大リトライ回数 |
| `--dry-run` | False | 処理対象確認のみ（保存しない） |
| `--log-file` | なし | ログファイルパス |

---

### `scripts/diagnose_bq_load.py` — BQロード状態診断

BigQueryテーブルの存在確認・レコード数、GCSファイル数、`raw.load_history` のロード成否を一括診断します。
データロードが正常に行われているか確認するときに使います。

```bash
# 基本診断
python3 scripts/diagnose_bq_load.py

# 失敗ファイルのエラー詳細も表示
python3 scripts/diagnose_bq_load.py --show-errors

# 特定テーブルのみ診断
python3 scripts/diagnose_bq_load.py --table race_results --show-errors
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--show-errors` | False | 失敗ファイルの詳細エラーを表示 |
| `--table` | なし | 診断対象テーブルを1つに限定 |

---

### `scripts/reload_gcs_to_bq.py` — GCSから BigQuery へ再ロード

GCS上の既存ファイルを指定データタイプでフィルタしてBigQueryに再ロードします。
Cloud Functionはオブジェクト作成時にのみトリガーされるため、既存ファイルの手動ロードに使います。

```bash
# SEC（レース結果）を全件再ロード
python scripts/reload_gcs_to_bq.py \
    --data-type SEC \
    --prefix Sec/

# ドライラン（対象ファイルの確認のみ）
python scripts/reload_gcs_to_bq.py \
    --data-type BAA \
    --prefix Baa/ \
    --dry-run

# 処理件数を制限して試す
python scripts/reload_gcs_to_bq.py \
    --data-type KYF \
    --prefix Kyf/ \
    --limit 10
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--data-type` | 必須 | データタイプ (BAA, SEC, KYF, UKC 等) |
| `--prefix` | `""` | GCS上のプレフィックス (例: `Sec/`) |
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--limit` | 0 (無制限) | 処理ファイル数上限 |
| `--dry-run` | False | 対象ファイル一覧の表示のみ |

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

# 期間を指定して検証
python3 scripts/validate_odds_consistency.py \
    --project-id <PROJECT_ID> \
    --start-date 2026-01-01 \
    --end-date 2026-03-01
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 環境変数 | GCPプロジェクトID |
| `--start-date` | なし | 検証開始日 (YYYY-MM-DD) |
| `--end-date` | なし | 検証終了日 (YYYY-MM-DD) |

---

### `scripts/scrape_historical_odds.py` — 過去レース一括オッズ取得

netkeibaから指定期間（2016年以降）の全レースの単複・組み合わせ馬券オッズを一括スクレイプして
BigQueryに保存します。途中で停止しても再実行で続きから再開できます。

JRDB race_id（8文字）を netkeiba race_id（12桁）に直接変換するため、BQ照会不要で高速です。

```bash
# ドライラン（対象件数と推定時間の確認のみ）
python3 scripts/scrape_historical_odds.py \
    --project-id <PROJECT_ID> \
    --start-date 2016-01-01 \
    --mode all \
    --dry-run

# 単複オッズのみ取得（predictions.daily_odds に保存）
python3 scripts/scrape_historical_odds.py \
    --project-id <PROJECT_ID> \
    --start-date 2016-01-01 \
    --mode win_place

# 組み合わせ馬券オッズのみ取得（predictions.daily_odds_combo に保存）
python3 scripts/scrape_historical_odds.py \
    --project-id <PROJECT_ID> \
    --start-date 2016-01-01 \
    --mode combo \
    --ticket-types b4 b5 b7

# バックグラウンド実行（長時間処理）
nohup python3 scripts/scrape_historical_odds.py \
    --project-id <PROJECT_ID> \
    --start-date 2016-01-01 \
    --mode win_place > /tmp/scrape.log 2>&1 &
```

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `--project-id` | 必須 | GCPプロジェクトID |
| `--start-date` | `2016-01-01` | 取得開始日 (YYYY-MM-DD) |
| `--end-date` | 本日 | 取得終了日 (YYYY-MM-DD) |
| `--mode` | `all` | 取得モード（`win_place` / `combo` / `all`） |
| `--ticket-types` | `b4 b5 b7` | comboモードで取得する馬券種（b4=馬連 b5=ワイド b6=馬単 b7=三連複） |
| `--sleep-sec` | `2.0` | ページ間スリープ秒数 |
| `--batch-size` | `50` | BQへのバッチ保存間隔（レース数） |
| `--dry-run` | False | 対象件数・推定時間の確認のみ（スクレイプなし） |

> **推定時間**: 単複のみ（約25,000レース）で約28時間、コンボ3種込みで約83時間。長時間処理のため `nohup` や `tmux` での実行を推奨。

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
- モデルの学習と評価（Phase 4）
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
| POST | `/api/v1/strategy/daily` | 当日の投資戦略を策定し `predictions.investment_decisions` にUPSERT保存。`config/strategy_config.yaml` のパラメータを使用。 |
| POST | `/api/v1/line/webhook` | LINE Messaging API Webhook受信。「日付 競馬場名 レース番号」形式のメッセージに対して予測テーブルと推奨馬券リストを返信。 |
| POST | `/api/v1/purchase/daily` | 発走5分前JRA IPAT自動馬券購入。`dry_run=true`（デフォルト）の場合はIPATログイン・購入をスキップし推奨馬券をLINE通知のみ実行。結果は `predictions.purchase_history` に保存。 |

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

### 実装状況

**Phase 1: データ基盤構築 ✅ 完了**
- JRDBダウンローダー（HTTP + lzh解凍 + エンコーディング変換）
- GCSアップロード（MD5重複チェック）
- BigQueryロード（MERGE処理、ロード履歴管理）
- データ品質チェック
- ロード履歴スキップロジック改善（GCS更新タイムスタンプ比較による強制再ロード） ← Issue #116

**Phase 2: 特徴量エンジニアリング ✅ 完了**
- SQL駆動型特徴量パイプライン（257カラム、5段階CTE）
- 過去走統計、条件適性、騎手×条件別成績、血統指標、差分指標
- BigQuery: features.training_data (466,265行)

**Phase 3: Cloud Run統合 ✅ 完了**
- FastAPI HTTPエンドポイント
- 日次パイプライン（同期/非同期）
- 全件ロード（同期/非同期）
- 特徴量生成API（同期/非同期） ← Issue #59で追加
- Docker化とCloud Runデプロイスクリプト ← Issue #71で整備
- デプロイ検証スクリプト ← Issue #71で追加
- Cloud Schedulerセットアップスクリプト ← Issue #60で追加

**Phase 4: モデル開発 ✅ 完了**
- LightGBM ランク学習 (LambdaRank) ✅
- 二値ラベル化（3着以内=1, それ以外=0） ✅ ← Issue #85
- AUC評価指標の追加 ✅ ← Issue #85
- Optunaハイパーパラメータチューニング ✅ ← Issue #86
- 時系列分割・推論パイプライン ✅
- バックテストシミュレーター（Kelly基準・回収率評価） ✅ ← Issue #17

**Phase 5: 運用システム構築 ✅ 完了**
- ✅ 日次予測パイプライン（Cloud Schedulerからの自動推論・BQ/GCS保存） - Issue #117
  - `POST /api/v1/predict/daily`: `model_path` 未指定時はGCSから最新モデルを自動取得
  - `predictions.daily_predictions` テーブルへのUPSERT保存
  - GCS保存（`gs://{project}-keiba-predictions/{date}/predictions.csv`）
- ✅ `raw.race_info` に `start_time` カラム追加 - Issue #214
- ✅ 発走5分前JRA IPAT自動馬券購入 - Issue #213
  - `POST /api/v1/purchase/daily`: `dry_run`（デフォルト: true）で本番切り替え可能
  - `predictions.purchase_history` テーブルへの購入履歴保存
- ✅ Webダッシュボード（Streamlit） - Issue #24
- ✅ LINE Messaging API Webhook Bot - Issue #25

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

**APIサーバーの起動（ローカル）**

```bash
# 開発環境でサーバー起動
uvicorn src.automation.api.app:app --reload --port 8080
```

**日次ロード**

```bash
# 同期ロード（処理完了まで待機）
curl -X POST http://localhost:8080/api/v1/load/daily \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# 非同期ロード（バックグラウンド処理）
curl -X POST http://localhost:8080/api/v1/load/daily/async \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'
```

**全件ロード**

```bash
# 非同期実行（推奨）
curl -X POST http://localhost:8080/api/v1/load/full \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'

# 同期実行（テスト用）
curl -X POST http://localhost:8080/api/v1/load/full/sync \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

**特徴量生成**

```bash
# 同期実行
curl -X POST http://localhost:8080/api/v1/features/generate \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'

# 非同期実行
curl -X POST http://localhost:8080/api/v1/features/generate/async \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2024-01-01", "end_date": "2024-12-31"}'
```

**日次予測（Cloud Scheduler用）**

`model_path` を省略すると GCS から最新モデルを自動取得します。
Cloud Scheduler は空のボディ `{}` を送信するため、明示的な指定は不要です。

```bash
# model_path 省略（最新モデルを自動取得）
curl -X POST http://localhost:8080/api/v1/predict/daily \
  -H "Content-Type: application/json" \
  -d '{}'

# model_path を明示的に指定する場合
curl -X POST http://localhost:8080/api/v1/predict/daily \
  -H "Content-Type: application/json" \
  -d '{
    "model_path": "gs://my-project-keiba-models/models/20260201/lgbm_ranker.txt",
    "save_to_bq": true,
    "save_to_gcs": true
  }'
```

**レスポンス例（日次予測）**

```json
{
  "status": "success",
  "target_dates": ["2026-03-07", "2026-03-08"],
  "num_races": 24,
  "num_horses": 384,
  "saved_to_bq": true,
  "saved_rows": 384,
  "saved_to_gcs": true,
  "gcs_uri": "gs://my-project-keiba-predictions/2026-03-07/predictions.csv"
}
```

**IPAT自動馬券購入（Cloud Scheduler用）**

`dry_run` のデフォルトは `true` です。IPATへの実際の購入なしに推奨馬券をLINE通知だけ行います。

```bash
# dry_run モード（IPATログイン・購入なし。推奨馬券をLINE通知のみ）
curl -X POST http://localhost:8080/api/v1/purchase/daily \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2026-04-05", "dry_run": true}'

# 本番モード（実際にIPATで馬券購入）
curl -X POST http://localhost:8080/api/v1/purchase/daily \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2026-04-05", "dry_run": false}'
```

**レスポンス例（IPAT自動購入）**

```json
{
  "status": "success",
  "execution_date": "2026-04-05",
  "dry_run": true,
  "purchased_races": 0,
  "total_amount": 0,
  "results": [...]
}
```

> **本番切り替え**: Cloud Schedulerジョブのリクエストボディを `{"dry_run": false}` に更新することで本番購入に切り替えできます。
>
> ```bash
> gcloud scheduler jobs update http race-day-purchase \
>   --location=asia-northeast1 \
>   --message-body='{"dry_run": false}' \
>   --project=<PROJECT_ID>
> ```

**ヘルスチェック**

```bash
curl http://localhost:8080/health
```

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

| ジョブ名 | スケジュール | 用途 |
|---------|------------|------|
| `daily-data-pipeline` | 毎日 AM 6:00 JST | 日次データロード |
| `race-day-predict` | 毎日 AM 8:00 JST | レース予測 |
| `race-day-purchase` | 土日 8:00〜17:00 の5分おき | 発走5分前IPAT自動馬券購入（Issue #213） |

詳細な設定内容は [infrastructure/README.md](./infrastructure/README.md#7-cloud-schedulerの設定) を参照してください。

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
| `pedigree` | 血統データ | 血統情報 | 0 (未実装) |
| `odds` | OZ (基準オッズ) | 単勝・複勝オッズ（JRDBコード表） | 実装済み |
| `combo_odds` | OW/OT/OZ | JRDBコンボ基準オッズ（ワイド/三連複/馬連） | 実装済み（Issue #140） |

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
| [SCHEMA.md](./SCHEMA.md) | JRDBデータスキーマ仕様書（データタイプ、フィールド定義、コードテーブル） |
| [ML_FEATURE.md](./ML_FEATURE.md) | 特徴量設計（特徴量リスト、Target Encoding、リーク対策） |
| [infrastructure/README.md](./infrastructure/README.md) | インフラセットアップガイド（GCP、Cloud Run、Docker） |
| [src/models/README.md](./src/models/README.md) | モデル学習・推論の詳細ドキュメント |

---

## 実装状況

### Phase 1: データ基盤構築 ✅ 完了

- ✅ GCP初期セットアップ（API有効化、サービスアカウント作成）
- ✅ GCSバケット作成
- ✅ BigQueryデータセット・テーブル作成
- ✅ JRDBダウンローダー（HTTP + lzh解凍 + エンコーディング変換）
- ✅ GCSアップロード（MD5重複チェック）
- ✅ BigQueryロード（MERGE処理、ロード履歴管理）
- ✅ データ品質チェック

### Phase 2: 特徴量エンジニアリング ✅ 完了

- ✅ SQL駆動型特徴量パイプライン（257カラム、5段階CTE）
- ✅ 過去走統計、条件適性、騎手×条件別成績、血統指標、差分指標
- ✅ BigQuery: features.training_data (466,265行)

### Phase 3: Cloud Run統合 ✅ 完了

- ✅ FastAPI HTTPエンドポイント
- ✅ 日次パイプライン（同期/非同期）
- ✅ 全件ロード（同期/非同期）
- ✅ 特徴量生成API（同期/非同期） - Issue #59
- ✅ Docker化とCloud Runデプロイスクリプト - Issue #71
- ✅ デプロイ検証スクリプト - Issue #71
- ✅ Cloud Schedulerセットアップスクリプト - Issue #60

### Phase 4: モデル開発 ✅ 完了

- ✅ LightGBM ランク学習 (LambdaRank) - Issue #14
- ✅ 二値ラベル化（3着以内=1, それ以外=0） - Issue #85
- ✅ AUC評価指標の追加 - Issue #85
- ✅ Optunaハイパーパラメータチューニング - Issue #86
- ✅ 時系列分割（学習・検証・推論） - Issue #14
- ✅ 推論パイプライン - Issue #14
- ✅ バックテストシミュレーター（Kelly基準・回収率評価） - Issue #17

### Phase 5: 運用システム構築 🔧 一部実装済み

- ✅ 日次予測パイプライン（Cloud Schedulerからの自動推論・BQ/GCS保存） - Issue #117
  - `POST /api/v1/predict/daily`: `model_path` 未指定時GCSから最新モデルを自動取得
  - `predictions.daily_predictions` テーブルへのUPSERT保存
  - GCS保存（`gs://{project}-keiba-predictions/{date}/predictions.csv`）
- ✅ netkeibaリアルタイムオッズスクレイパー - Issue #131
  - `POST /api/v1/odds/scrape`: netkeibaから当日全レースの単複オッズを取得
  - `predictions.daily_odds` テーブルへのUPSERT保存（race_id + horse_numberキー）
  - Playwright (Chromium) によるJS描画ページ対応
- ✅ netkeibaスクレイパー拡張（組み合わせ馬券オッズ） - Issue #134
  - `POST /api/v1/odds/scrape` の `include_combo=true`: 馬連・馬単・ワイド・三連複オッズ取得
  - `predictions.daily_odds_combo` テーブルへのUPSERT保存
- ✅ 日次投資戦略策定 - Issue #105
  - `POST /api/v1/strategy/daily`: 予測×オッズからKelly基準で投資判断を実行
  - `predictions.investment_decisions` テーブルへのUPSERT保存
  - `config/strategy_config.yaml` でパラメータ管理（`run_strategy_optimization.py` で最適化）
- ✅ JRDBコンボ基準オッズ取得（raw.combo_odds） - Issue #140
  - OW（ワイド153通り）・OT（三連複816通り）・OZ（馬連153通り）のJRDB基準オッズを解析
  - `raw.combo_odds` テーブルにMERGE/UPSERT保存
  - `scripts/create_raw_combo_odds_table.py` でテーブル作成
- ✅ 投資戦略を複勝+ワイド+三連複中心に改修（パターンA追加） - Issue #139
  - 複勝のみ → 複勝+ワイド+三連複をベース馬券に変更
  - パターンA（突出型）: top1確率が top2 より p1 以上高い場合に単勝+馬連を追加
  - 賭け金配分: オッズ逆数比率方式（トリガミ防止）
  - バックテスト `fetch_combo_odds`: predictions.daily_odds_combo → raw.combo_odds → raw.payouts の3段フォールバック
  - グリッドサーチ: 450通り → 100通りに拡張（p1 × threshold × r の3次元探索、Issue #162）
- ✅ 過去レース一括オッズ取得スクリプト - PR #143
  - `scripts/scrape_historical_odds.py`: 2016年以降の全レース（約25,000レース）を一括スクレイプ
  - JRDB race_id（8文字）→ netkeiba race_id（12桁）のローカル変換（BQ照会不要）
  - 既スクレイプ済みレースのスキップによる再開対応
  - 単複オッズ（predictions.daily_odds）とコンボオッズ（predictions.daily_odds_combo）両対応
- ✅ LINE Messaging API Webhook Bot - Issue #25
  - `POST /api/v1/line/webhook`: HMAC-SHA256署名検証 + 「日付 競馬場名 レース番号」形式のメッセージ解析
  - メッセージ1: 予測テーブル（予測順・馬番・馬名・スコア・複勝率・オッズ・期待値）
  - メッセージ2: 推奨馬券リスト（馬券種・馬番・馬名・オッズ・賭け金・合計投資額）
  - `src/automation/api/line_webhook.py` + `src/utils/line_notify.py` に実装
  - 必要な環境変数: `LINE_CHANNEL_SECRET`（Webhook署名検証用）、`LINE_CHANNEL_ACCESS_TOKEN`（リプライ送信用）
- ✅ Webダッシュボード（Streamlit） - Issue #24
  - 5画面構成: ホーム / レース一覧 / レース詳細 / バックテスト / モデル情報
  - `src/dashboard/app.py`: メインアプリ（サイドバーナビ・日付選択UI）
  - `src/dashboard/data.py`: BigQueryデータ取得（`@st.cache_data` TTLキャッシュ）
  - `src/dashboard/components/`: 各画面コンポーネント（Plotly グラフ対応）
  - `Dockerfile.dashboard`: Playwright不要の軽量Cloud Runコンテナ（`dashboard-service`）
  - ローカル起動: `GCP_PROJECT_ID=your-project streamlit run src/dashboard/app.py`
- ✅ `raw.race_info` に `start_time` カラム追加 - Issue #214
  - `src/automation/data/jrdb_parser.py`: `parse_baa_line()` の return dict に `start_time`（HHMM形式）追加
  - `scripts/add_start_time_to_race_info.py`: BQ ALTER TABLE スクリプト（初回のみ実行）
- ✅ 発走5分前JRA IPAT自動馬券購入パイプライン - Issue #213
  - `POST /api/v1/purchase/daily`: 対象レースを順次処理し、発走5分前にIPATで馬券購入
  - `dry_run=true`（デフォルト）: IPATログイン・購入をスキップし推奨馬券をLINE通知のみ
  - `dry_run=false`: 実際にIPATへログインして馬券を購入（本番切り替えは Cloud Scheduler ジョブ更新で行う）
  - `predictions.purchase_history` テーブルへの購入履歴保存
  - Cloud Scheduler ジョブ `race-day-purchase`（土日 8:00〜17:00 の5分おき）で自動実行
  - 本番切り替えコマンド: `gcloud scheduler jobs update http race-day-purchase --message-body='{"dry_run": false}'`
  - `src/automation/data/ipat_purchaser.py`: IpatPurchaserクラス（Playwright自動化）
  - 必要な環境変数（Secret Manager経由）: `IPAT_MEMBER_ID`（加入者番号）・`IPAT_PIN`（暗証番号4桁）・`IPAT_PAT_NUMBER`（PAT番号）

---

## ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)

---

## 変更履歴

| 日付 | 変更内容 |
|------|----------|
| 2026-02-14 | README.mdとCLAUDE.mdの重複除外。Issue #59（特徴量生成API）とIssue #71（デプロイスクリプト整備）の内容を反映 |
| 2026-02-16 | Issue #85（二値ラベル化・AUC追加）とIssue #86（Optunaチューニング）の内容を反映 |
| 2026-02-22 | Issue #17（バックテストシミュレーター）の実装を反映。src/backtest/追加、scripts/run_backtest.py追加、Phase 4完了に更新 |
| 2026-03-01 | Issue #21（Cloud Runデプロイ設定）の実装を反映。予測エンドポイント2件をAPIエンドポイント一覧に追記、--model-pathへのgs://URI指定対応を追記 |
| 2026-03-01 | Issue #116（ロード履歴スキップロジック改善）・Issue #117（日次予測パイプライン完成）の実装を反映。predictions.daily_predictionsテーブル追加、POST /api/v1/predict/dailyのmodel_pathをOptional化、Phase 5を一部実装済みに更新 |
| 2026-03-07 | Issue #131（netkeibaリアルタイムオッズスクレイパー）の実装を反映。netkeiba_scraper.py追加、predictions.daily_oddsテーブル追加、POST /api/v1/odds/scrapeエンドポイント追加、strategy.py/strategy_optimizer.pyをプロジェクト構成に追記 |
| 2026-03-08 | Issue #140（JRDBコンボ基準オッズ raw.combo_odds）・Issue #139（投資戦略複勝+ワイド+三連複改修、パターンA追加）・PR #143（過去レース一括オッズ取得スクリプト）の実装を反映 |
| 2026-03-10 | Issue #25（LINE Messaging API Webhook Bot）の実装を反映。POST /api/v1/line/webhookエンドポイント追加、line_webhook.py/line_notify.py追加、Phase 5通知システムを実装済みに更新 |
| 2026-03-15 | Issue #24（Streamlit Webダッシュボード）の実装を反映。src/dashboard/追加、Dockerfile.dashboard追加、技術スタックにStreamlit/Plotly追記、Phase 5 Webダッシュボードを実装済みに更新 |
| 2026-03-17 | Issue #161（investment_decisions スキーマ変更: horse_number INTEGER → horse_numbers STRING カンマ区切り）・Issue #162（馬選定スコアに prob_weight_r 導入、min_prob_threshold フィルタ追加、グリッドサーチ3次元化 100通り）の実装を反映 |
| 2026-03-20 | Issue #167（netkeibaスクレイパー COMBO_TICKET_TYPESマッピング修正: b5=ワイド/b6=馬単/b7=三連複）・Issue #168（1レースあたり投資予算を capital×max_bet_ratio 方式から budget_per_race=3000円固定方式に変更: `--max-bet-ratio` → `--budget-per-race`、strategy_config.yaml更新）・Issue #165（prob_weight_r が期待値フィルタに影響しないことを検証するテスト追加）・Issue #166（build_race_df の win_odds JOIN 動作を検証する tests/test_run_strategy.py 新規作成）の実装を反映 |
| 2026-04-04 | Issue #214（raw.race_info に start_time カラム追加: HHMM形式、jrdb_parser.py + scripts/add_start_time_to_race_info.py 追加）・Issue #213（発走5分前JRA IPAT自動馬券購入パイプライン: POST /api/v1/purchase/daily 追加、ipat_purchaser.py・predictions.purchase_history・scripts/create_purchase_history_table.py 追加、Cloud Scheduler ジョブ race-day-purchase 追加、dry_run フラグで本番切り替え可能）の実装を反映 |
