# 競馬予測MLシステム

競馬の馬券購入を支援する機械学習システム

## 概要

このプロジェクトは、JRDBの競馬データを活用し、機械学習による馬券購入支援システムを構築します。

- **対象馬券**: 単勝・複勝
- **予測内容**: 3着以内に入る確率
- **目標**: 回収率100%以上
- **技術**: Python, LightGBM, GCP (BigQuery, Cloud Run)

詳細な仕様は [CLAUDE.md](./CLAUDE.md) を参照してください。

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
# .env ファイルを編集してGCP_PROJECT_IDとJRDB認証情報を設定
```

### 2. データダウンロード

```bash
cd downloader

# 環境変数の設定
cp .env.example .env
# .env ファイルを編集してJRDBの認証情報を設定

# スキーマ（仕様書）のダウンロード
sh download_schema.sh

# 指定日付以降のデータを全タイプ一括ダウンロード
sh download_all_from_date.sh
```

### 3. GCPセットアップ

```bash
# GCPプロジェクトの認証
gcloud auth application-default login

# BigQueryデータセット・テーブル作成
python -m src.manual.create_tables
```

### 4. データアップロード

```bash
# GCSへのアップロード
python -m src.automation.data.upload_to_gcs

# データ品質チェック
python -m src.manual.quality_check
```

## プロジェクト構成

```
keiba_prediction/
├── src/
│   ├── automation/          # 自動化処理（GCP運用向け）
│   │   ├── data/            # データ取得・アップロード・ロード
│   │   │   ├── jrdb_downloader.py  # JRDBダウンローダー
│   │   │   ├── jrdb_parser.py      # JRDBデータパーサー
│   │   │   ├── upload_to_gcs.py    # GCSアップロード
│   │   │   └── load_to_bq.py       # BigQueryロード（MERGE処理・重複スキップ）
│   │   ├── pipeline/        # 統合パイプライン
│   │   │   ├── daily_pipeline.py   # 日次パイプライン（DL→GCS→BQ）
│   │   │   └── full_load_pipeline.py # 過去分全件ロード
│   │   └── api/             # FastAPI HTTPエンドポイント
│   │       └── app.py       # APIサーバー（日次・全件ロード）
│   ├── manual/              # 手動実行スクリプト
│   │   ├── create_tables.py        # BigQueryテーブル作成
│   │   ├── quality_check.py        # データ品質チェック
│   │   └── validation_rules.py     # バリデーションルール定義
│   └── ml/                  # 機械学習・特徴量エンジニアリング
│       └── features/        # 特徴量モジュール
│           ├── feature_pipeline.py # 特徴量パイプライン
│           ├── past_performance.py # 過去走特徴量
│           └── condition_features.py # 条件適性特徴量
├── scripts/                 # ユーティリティスクリプト
│   ├── reload_gcs_to_bq.py         # 既存GCSファイルの再ロード
│   └── generate_features.py        # 特徴量生成スクリプト
├── tests/                   # テストコード
├── legacy/                  # レガシーコード（参照用）
│   ├── downloader/          # 旧シェルスクリプト版ダウンローダー
│   ├── cloud_functions/     # 旧Cloud Functions（Cloud Runに移行済み）
│   ├── main.py              # 旧Flaskエントリーポイント（FastAPIに移行済み）
│   └── data_pipeline.py     # 旧パイプライン（DailyPipeline等に移行済み）
├── notebooks/               # Jupyter Notebook (EDA用)
├── config/                  # BigQueryスキーマ定義
├── reports/                 # 品質チェックレポート出力先
├── Dockerfile               # Dockerイメージ定義（Cloud Run用）
├── CLAUDE.md                # システム仕様書
├── SCHEMA.md                # JRDBデータスキーマ仕様書
├── ML_FEATURE.md            # 特徴量設計ドキュメント
└── README.md                # このファイル
```

## データパイプライン

### 全体フロー

現在は手動実行ですが、将来的にはCloud Run Functions で完全自動化する予定です。

#### 現状（手動実行）

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. データ取得 (JRDB → ローカル)                                          │
│    $ sh downloader/download_all_from_date.sh                            │
│    または                                                                │
│    $ python -m src.automation.data.jrdb_downloader --start-date 240101             │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. GCSアップロード (ローカル → GCS)                                      │
│    $ python -m src.automation.data.upload_to_gcs                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. BigQueryロード (GCS → BigQuery)                                      │
│    - 新規ファイル: Cloud Functionが自動トリガー                          │
│    - 既存ファイル: $ python scripts/reload_gcs_to_bq.py                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. 特徴量生成 (BigQuery raw → features)                                 │
│    $ python -m src.ml.features.feature_pipeline --start-date ... --end-date│
└─────────────────────────────────────────────────────────────────────────┘
```

#### 自動化の実現状況

**実現済み:**
- ✅ Cloud Run: FastAPI HTTPエンドポイント（`/api/v1/load/daily`, `/api/v1/load/daily/async`）
- ✅ Step 1+2+3の統合: JRDBダウンロード → GCSアップロード → BigQueryロード（`DailyPipeline`）
- ✅ 一時ディレクトリを使用、完了後自動削除
- ✅ HTTPリクエスト経由でのトリガー対応（Cloud Scheduler連携可能）
- ✅ 過去分全件ロード（`FullLoadPipeline`）による初回セットアップ・データ補完

**未実装:**
- ❌ Cloud Scheduler設定（Cloud Runはデプロイ済み前提でSchedulerジョブの作成が必要）
- ❌ Step 4: 特徴量生成の自動化（パイプラインには未統合）
- ❌ Secret Managerでの認証情報管理
- ❌ Cloud Loggingとの統合

**実現済みアーキテクチャ:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Cloud Scheduler (毎日AM 6:00)                                            │
│   ↓ HTTPリクエスト                                                       │
│ Cloud Run: /api/v1/load/daily/async エンドポイント                      │
│   ├─ Step 1: JRDBダウンロード (一時ディレクトリ使用)                     │
│   ├─ Step 2: GCSアップロード                                             │
│   └─ Step 3: BigQueryロード (DailyPipelineが直接実行)                   │
│              ├─ 重複スキップ機能                                        │
│              └─ ロード履歴管理 (raw.load_history)                       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ 全件ロード (手動トリガー)                                                │
│   ↓ HTTPリクエスト                                                       │
│ Cloud Run: /api/v1/load/full エンドポイント                             │
│   ├─ Step 1: 指定期間の全データをJRDBからダウンロード                    │
│   ├─ Step 2: GCSアップロード                                             │
│   └─ Step 3: BigQueryロード (日付フィルタ + 重複スキップ)                │
└─────────────────────────────────────────────────────────────────────────┘
```

**自動化のメリット:**
- 人手不要の完全自動運用（Cloud Scheduler連携時）
- 一時ディレクトリによるディスク容量の節約
- 統合パイプラインによるエラーハンドリング
- ロード履歴による重複防止・処理効率化

### 手動実行の手順

#### Step 1: データ取得 (JRDB → ローカル)

**方法A: シェルスクリプト（従来の方法）**

```bash
cd downloader

# 環境変数の設定（初回のみ）
cp .env.example .env
# .env にJRDB認証情報を設定

# 指定日付以降のデータをダウンロード
sh download_all_from_date.sh
```

**方法B: Pythonモジュール（推奨）**

```bash
# 全データタイプをダウンロード
python -m src.automation.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python -m src.automation.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python -m src.automation.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
```

#### Step 2: GCSアップロード

```bash
# 全データをアップロード（差分のみ）
python -m src.automation.data.upload_to_gcs

# 特定タイプのみアップロード
python -m src.automation.data.upload_to_gcs --data-type Sec

# ドライラン（実際にはアップロードしない）
python -m src.automation.data.upload_to_gcs --dry-run
```

#### Step 1+2 統合: ダウンロード→アップロード

```bash
# ダウンロード→アップロードを一括実行
python -m legacy.data_pipeline --start-date 240101

# 特定のデータタイプのみ
python -m legacy.data_pipeline --start-date 240101 --datatype BAA

# 既存のdownloaded_filesを使用（一時ディレクトリを使わない）
python -m legacy.data_pipeline --start-date 240101 --no-temp-dir
```

#### Step 1+2+3 統合: 日次パイプライン（推奨）

**CLIからの実行:**

```bash
# 当日のデータを処理（ダウンロード→GCSアップロード→BigQueryロード）
python -m src.automation.pipeline.daily_pipeline

# 特定日付を指定
python -m src.automation.pipeline.daily_pipeline --date 2024-01-15

# JSON形式で結果を出力
python -m src.automation.pipeline.daily_pipeline --date 2024-01-15 --json
```

**FastAPI経由での実行:**

```bash
# APIサーバーを起動
uvicorn src.automation.api.app:app --reload --port 8080

# 同期ロード（処理完了まで待機）
curl -X POST http://localhost:8080/api/v1/load/daily \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# 非同期ロード（バックグラウンド処理）
curl -X POST http://localhost:8080/api/v1/load/daily/async \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# ヘルスチェック
curl http://localhost:8080/health
```

**レスポンス例:**

```json
{
  "status": "success",
  "target_date": "2024-01-15",
  "files_downloaded": 5,
  "files_uploaded": 5,
  "files_loaded": 3,
  "records_loaded": 100,
  "duration_seconds": 10.5,
  "steps": {
    "download": {
      "status": "success",
      "files": 5,
      "duration": 3.2
    },
    "upload": {
      "status": "success",
      "files": 5,
      "duration": 5.1
    },
    "load": {
      "status": "success",
      "files": 3,
      "records": 100,
      "duration": 2.2
    }
  }
}
```

**日次パイプラインの特徴:**
- ダウンロード→GCSアップロード→BigQueryロードの統合処理
- ステップごとの詳細な結果追跡
- エラーハンドリングと自動クリーンアップ
- 重複スキップ機能との連携（既にロード済みのファイルは自動スキップ）
- Cloud Scheduler連携対応（REST API経由での自動実行）

#### 全件ロード: 過去データの一括処理

初回セットアップやデータ欠損の補完時に使用します。

```bash
# 全期間のデータを一括処理
python -m src.automation.pipeline.full_load_pipeline

# 期間を指定して処理
python -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31

# API経由で非同期実行
curl -X POST http://localhost:8080/api/v1/load/full \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

#### Step 3: BigQueryロード

**A) 新規ファイルの場合（自動）**

GCSにファイルがアップロードされると、Cloud Functionが自動的にトリガーされBigQueryにロードされます。

**B) 既存ファイルの場合（手動）**

Cloud Functionはアップロード時のみトリガーされるため、既存ファイルは手動でロードが必要です。

```bash
# SECファイル（成績データ）を全件ロード
python scripts/reload_gcs_to_bq.py --data-type SEC --prefix Sec/

# ドライラン（処理対象の確認のみ）
python scripts/reload_gcs_to_bq.py --data-type SEC --prefix Sec/ --dry-run

# 5ファイルのみテスト
python scripts/reload_gcs_to_bq.py --data-type SEC --prefix Sec/ --limit 5
```

**C) 重複スキップ機能（推奨）**

`raw.load_history`テーブルのロード履歴を参照し、既にロード済みのファイルを自動でスキップします。

```bash
# 重複スキップを有効にしてロード（推奨）
python -m src.automation.data.load_to_bq --prefix Sec/ --skip-loaded

# 特定のデータタイプのみ
python -m src.automation.data.load_to_bq --data-types SEC --skip-loaded

# 履歴記録を無効化（テスト用）
python -m src.automation.data.load_to_bq --prefix Sec/ --no-history

# 重複スキップと組み合わせ
python -m src.automation.data.load_to_bq --prefix Sec/ --skip-loaded --data-types BAA KYF SEC
```

**重複スキップ機能の利点:**
- 既にロード済みのファイルをスキップし、処理時間を短縮
- ロード履歴を`raw.load_history`テーブルで管理
- 失敗したファイルのリトライが簡単（履歴上は失敗扱いなので再ロードされる）
- バッチロード時のコスト削減

#### Step 4: 特徴量生成

```bash
# 指定期間の特徴量を生成
python -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-01-06

# 詳細ログ付きで実行
python -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-12-31 -v
```

#### Step 5: データ品質チェック

```bash
# 全テーブルのチェック
python -m src.manual.quality_check

# 特定テーブルのみチェック
python -m src.manual.quality_check --table raw.race_info
```

### 対応データタイプ

| データタイプ | 説明 | BigQueryテーブル |
|-------------|------|-----------------|
| BAA/BAB/BAC | 番組データ (レース基本情報) | `raw.race_info` |
| KYF/KYG/KYH | 競走馬データ (出馬表・予測指数) | `raw.horse_results` |
| SEC | 成績データ (レース結果) | `raw.race_results` |

### Cloud Runでの実行（将来の自動化に向けた準備）

Cloud Run環境でパイプライン全体を実行できます。

#### ローカルでのテスト

```bash
# Flaskサーバーを起動
python main.py

# 別のターミナルで実行
# ヘルスチェック
curl http://localhost:8080/

# ダウンロード→アップロード
curl -X POST http://localhost:8080/download \
  -H "Content-Type: application/json" \
  -d '{"start_date": "240101"}'

# フルパイプライン実行
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"start_date": "240101", "end_date": "2024-01-01"}'
```

#### Cloud Runへのデプロイ

```bash
# イメージをビルド＆デプロイ
gcloud builds submit --tag gcr.io/${PROJECT_ID}/keiba-pipeline
gcloud run deploy keiba-pipeline \
  --image gcr.io/${PROJECT_ID}/keiba-pipeline \
  --platform managed \
  --region asia-northeast1 \
  --memory 2Gi \
  --timeout 900 \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID} \
  --set-secrets JRDB_USER=jrdb-user:latest,JRDB_PASSWORD=jrdb-password:latest

# Cloud Schedulerで定期実行（毎日AM 6:00）
gcloud scheduler jobs create http daily-data-pipeline \
  --location asia-northeast1 \
  --schedule "0 6 * * *" \
  --uri "https://keiba-pipeline-xxxxx.a.run.app/run" \
  --http-method POST \
  --headers "Content-Type=application/json" \
  --message-body '{"steps": ["download_upload", "features"]}'
```

---

## 特徴量パイプライン

BigQueryの`raw`テーブルから特徴量を生成し、`features.training_data`テーブルに保存します。

### 生成される特徴量

| カテゴリ | 特徴量例 |
|---------|---------|
| 過去走統計 | past_3_avg_position, past_5_avg_last3f |
| 条件適性 | turf_place_rate, dirt_place_rate, dist_sprint_place_rate |
| 脚質 | front_rate, closer_rate, avg_corner4_position |

**注意**: 特徴量パイプラインは`race_results`テーブルにデータが存在する日付でのみ動作します。

詳細は [ML_FEATURE.md](./ML_FEATURE.md) を参照してください。

---

## 主要機能

### 日次パイプライン (`src/pipeline/daily_pipeline.py`)

JRDBダウンロード→GCSアップロード→BigQueryロードの統合処理を実行します。

**CLIからの実行:**

```bash
# 当日のデータを処理
python -m src.automation.pipeline.daily_pipeline

# 特定日付を指定
python -m src.automation.pipeline.daily_pipeline --date 2024-01-15

# JSON形式で結果を出力
python -m src.automation.pipeline.daily_pipeline --date 2024-01-15 --json
```

**FastAPI HTTPエンドポイント (`src/api/app.py`):**

APIサーバーを起動し、HTTPリクエストでパイプラインを実行できます。

```bash
# 開発環境でサーバー起動
uvicorn src.automation.api.app:app --reload --port 8080

# 本番環境（Cloud Run）
python -m src.automation.api.app
```

**エンドポイント一覧:**

| エンドポイント | メソッド | 説明 |
|-------------|---------|------|
| `/health` | GET | ヘルスチェック |
| `/api/v1/load/daily` | POST | 同期日次ロード（処理完了まで待機） |
| `/api/v1/load/daily/async` | POST | 非同期日次ロード（バックグラウンド処理） |
| `/api/v1/load/full` | POST | 非同期全件ロード（バックグラウンド処理） |
| `/api/v1/load/full/sync` | POST | 同期全件ロード（テスト用） |

**リクエスト例:**

```bash
# 同期ロード
curl -X POST http://localhost:8080/api/v1/load/daily \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# 非同期ロード
curl -X POST http://localhost:8080/api/v1/load/daily/async \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# target_dateを省略すると当日の処理
curl -X POST http://localhost:8080/api/v1/load/daily \
  -H "Content-Type: application/json" \
  -d '{}'
```

**レスポンス例:**

```json
{
  "status": "success",
  "target_date": "2024-01-15",
  "files_downloaded": 5,
  "files_uploaded": 5,
  "files_loaded": 3,
  "records_loaded": 100,
  "duration_seconds": 10.5,
  "steps": {
    "download": {
      "status": "success",
      "files": 5,
      "duration": 3.2
    },
    "upload": {
      "status": "success",
      "files": 5,
      "duration": 5.1
    },
    "load": {
      "status": "success",
      "files": 3,
      "records": 100,
      "duration": 2.2
    }
  }
}
```

**主な機能:**
- **統合処理**: ダウンロード→アップロード→ロードを一括実行
- **詳細な結果追跡**: 各ステップの成功/失敗、処理ファイル数、レコード数を記録
- **エラーハンドリング**: 各ステップで失敗した場合も、他のステップの結果を保持
- **自動クリーンアップ**: 一時ディレクトリの自動削除
- **重複スキップ**: `load_to_bq`の重複スキップ機能と連携
- **冪等性**: 同じ日付で複数回実行しても安全（既にロード済みのファイルはスキップ）
- **Cloud Scheduler連携**: REST API経由での自動実行に対応

**Cloud Runへのデプロイ:**

```bash
# イメージをビルド＆デプロイ
gcloud builds submit --tag gcr.io/${PROJECT_ID}/keiba-daily-pipeline
gcloud run deploy keiba-daily-pipeline \
  --image gcr.io/${PROJECT_ID}/keiba-daily-pipeline \
  --platform managed \
  --region asia-northeast1 \
  --memory 2Gi \
  --timeout 900 \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID} \
  --set-secrets JRDB_USER=jrdb-user:latest,JRDB_PASSWORD=jrdb-password:latest

# Cloud Schedulerで定期実行（毎日AM 6:00）
gcloud scheduler jobs create http daily-data-pipeline \
  --location asia-northeast1 \
  --schedule "0 6 * * *" \
  --uri "https://keiba-daily-pipeline-xxxxx.a.run.app/api/v1/load/daily" \
  --http-method POST \
  --headers "Content-Type=application/json" \
  --message-body '{}'
```

### 過去分全件ロードパイプライン (`src/pipeline/full_load_pipeline.py`)

指定期間のデータをJRDBからダウンロード→GCS→BigQueryに一括ロードします。
初回セットアップやデータ欠損の補完に使用します。

**CLIからの実行:**

```bash
# 全期間のデータを処理
python -m src.automation.pipeline.full_load_pipeline

# 期間を指定
python -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31

# JSON形式で結果を出力
python -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31 --json
```

**FastAPI経由での実行:**

```bash
# 非同期実行（バックグラウンド処理、推奨）
curl -X POST http://localhost:8080/api/v1/load/full \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'

# 同期実行（処理完了まで待機、テスト用）
curl -X POST http://localhost:8080/api/v1/load/full/sync \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

**レスポンス例（非同期）:**

```json
{
  "status": "started",
  "job_id": "a1b2c3d4",
  "start_date": "2020-01-01",
  "end_date": "2024-12-31",
  "message": "全件ロードを開始しました（2020-01-01〜2024-12-31）",
  "files_downloaded": 0,
  "files_uploaded": 0,
  "files_loaded": 0,
  "records_loaded": 0,
  "duration_seconds": 0.0,
  "error_message": null
}
```

**主な機能:**
- **一括処理**: 指定期間のデータを一括でダウンロード→アップロード→ロード
- **日付フィルタ**: ファイル名のyymmdd部分で日付範囲を自動フィルタ
- **重複スキップ**: 既にロード済みのファイルは自動スキップ
- **ジョブID追跡**: 各実行にユニークなジョブIDを付与
- **バックグラウンド実行**: 長時間処理はバックグラウンドで実行し即座にレスポンス
- **エラー耐性**: 一部ファイルが失敗しても残りの処理を継続

### データ品質チェック (`src/data/quality_check.py`)

BigQueryにロードされたデータの品質を自動チェックします。

```bash
# 全テーブルのチェック
python -m src.manual.quality_check

# 特定テーブルのみチェック
python -m src.manual.quality_check --table raw.race_info

# レポート出力先を指定
python -m src.manual.quality_check --output reports/my_report.json

# アラートを無効化
python -m src.manual.quality_check --no-alert
```

**チェック項目:**
- テーブル存在確認
- レコード数チェック（最低期待値との比較）
- NULL値チェック（必須カラムの検証）
- 重複レコードチェック（主キーの検証）
- 日付範囲チェック（2016-01-01〜未来7日以内）
- 数値範囲チェック（各カラムの妥当な範囲）

**出力例:**
```
============================================================
データ品質チェックレポート
============================================================

レポートID: 20260126_123456
生成日時: 2026-01-26T12:34:56
プロジェクト: keiba-prediction-452203

--- サマリー ---
総チェック数: 25
成功: 23
失敗: 2
  - ERROR: 1
  - WARNING: 1
  - INFO: 0

--- 失敗したチェック ---

[ERROR] raw.race_info
  チェック: null_check
  詳細: カラム 'race_id': NULL件数 5 / 10,000 (0.05%)

============================================================
ステータス: FAILED (ERROR検出)
============================================================
```

### JRDBダウンローダー (`src/data/jrdb_downloader.py`)

JRDBからデータをダウンロードし、解凍・エンコーディング変換を行います。

```bash
# 全データタイプをダウンロード
python -m src.automation.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python -m src.automation.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python -m src.automation.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
```

**特徴:**
- lzhファイルの自動解凍
- CP932からUTF-8へのエンコーディング変換
- 環境変数からの認証情報取得（Cloud Run対応）
- 一時ディレクトリのサポート

### GCSアップロード (`src/data/upload_to_gcs.py`)

ローカルのダウンロードデータをGCSにアップロードします。

```bash
# 全データをアップロード
python -m src.automation.data.upload_to_gcs

# 特定のデータタイプのみアップロード
python -m src.automation.data.upload_to_gcs --data-type Baa

# ドライラン（実際にはアップロードしない）
python -m src.automation.data.upload_to_gcs --dry-run

# 強制アップロード（差分チェックをスキップ）
python -m src.automation.data.upload_to_gcs --force
```

**特徴:**
- MD5チェックによる差分アップロード
- リトライ機能（最大3回）
- プログレス表示
- 詳細なアップロードレポート

### パイプライン統合 (`src/data/pipeline.py`)

ダウンロード→GCSアップロードを一括実行します。

```bash
# ダウンロード→アップロードを一括実行
python -m legacy.data_pipeline --start-date 240101

# 特定のデータタイプのみ
python -m legacy.data_pipeline --start-date 240101 --datatype BAA
```

**特徴:**
- 一時ディレクトリの自動管理（Cloud Run環境向け）
- エラーハンドリングとクリーンアップ
- 統合されたログ出力

### BigQueryロード (`src/data/load_to_bq.py`)

GCSにアップロードされたJRDBデータをBigQueryにロードします。

```bash
# 全CSVファイルをロード
python -m src.automation.data.load_to_bq

# 重複スキップを有効化（推奨）
python -m src.automation.data.load_to_bq --skip-loaded

# 特定のデータタイプのみロード
python -m src.automation.data.load_to_bq --data-types BAA KYF SEC

# 特定のプレフィックス配下のファイルをロード
python -m src.automation.data.load_to_bq --prefix Sec/

# エラー時に処理を中断
python -m src.automation.data.load_to_bq --stop-on-error
```

**主な機能:**
- **重複スキップ機能**: `--skip-loaded`オプションで、既にロード済みのファイルを自動スキップ
- **ロード履歴管理**: `raw.load_history`テーブルにロード履歴を記録
- **MERGE処理**: 既存レコードはUPDATE、新規レコードはINSERTで重複を防止
- **バッチ処理**: 複数ファイルを一括処理
- **エラーハンドリング**: 失敗したファイルを記録し、リトライが容易

**ロード履歴テーブル (`raw.load_history`)**

ロード履歴は以下の情報を記録します:

| カラム | 説明 |
|--------|------|
| file_name | ロードされたファイル名 (GCS上のパス) |
| loaded_at | ロード実行日時 |
| records_count | ロードされたレコード数 |
| table_name | ロード先テーブル名 |
| data_type | データタイプ (BAA, KYF, SEC等) |
| status | ステータス (success/failed) |
| error_message | エラーメッセージ (失敗時) |
| duration_seconds | 処理時間(秒) |
| file_size_bytes | ファイルサイズ(バイト) |

履歴の確認:
```sql
-- 最近のロード履歴を確認
SELECT * FROM `raw.load_history`
ORDER BY loaded_at DESC
LIMIT 100;

-- 失敗したファイルを確認
SELECT file_name, error_message, loaded_at
FROM `raw.load_history`
WHERE status = 'failed'
ORDER BY loaded_at DESC;
```

### BigQueryテーブル作成 (`src/data/create_tables.py`)

BigQueryのデータセットとテーブルを作成します。

```bash
python -m src.data.create_tables
```

**作成されるリソース:**
- データセット: `raw`, `features`, `predictions`, `backtests`
- テーブル: `race_info`, `horse_results`, `race_results`, `training_data`, `load_history` など

### Cloud Functionデプロイ

GCSにファイルがアップロードされた際に自動でBigQueryにロードするCloud Functionをデプロイします。

```bash
cd cloud_functions/gcs_to_bq

# デプロイ
gcloud functions deploy gcs-to-bq \
  --runtime python39 \
  --trigger-resource ${PROJECT_ID}-keiba-raw-data \
  --trigger-event google.storage.object.finalize \
  --entry-point gcs_to_bq \
  --region asia-northeast1 \
  --memory 512MB \
  --timeout 300s \
  --set-env-vars GCP_PROJECT_ID=${PROJECT_ID}
```

**注意**: Cloud Functionはファイルアップロード時のみトリガーされます。既存ファイルをロードするには `scripts/reload_gcs_to_bq.py` を使用してください。

## テスト

```bash
# 全テストを実行
python -m pytest tests/ -v

# 特定のテストファイルを実行
python -m pytest tests/test_quality_check.py -v

# カバレッジレポート付き
python -m pytest tests/ --cov=src --cov-report=html
```

## Claude Code GitHub Action

このリポジトリでは、Claude Code GitHub Actionを使用してPRのレビューや自動修正を行います。

### セットアップ

1. **Anthropic APIキーの取得**
   - [Anthropic Console](https://console.anthropic.com/)でAPIキーを作成

2. **GitHub Secretsに設定**
   ```
   リポジトリ設定 → Secrets and variables → Actions → New repository secret

   Name: ANTHROPIC_API_KEY
   Value: <your-api-key>
   ```

3. **使用方法**
   - PRを作成すると自動的にClaudeがコードをレビュー
   - PRコメントで `@claude <指示>` とメンションすると対応
   - 例: `@claude この関数を最適化して`

### ワークフロー設定

`.github/workflows/claude.yml` が自動的に設定されています。

詳細は [Claude Code Action Documentation](https://github.com/anthropics/claude-code-action) を参照。

## ドキュメント

- [CLAUDE.md](./CLAUDE.md) - システム全体の仕様書
  - アーキテクチャ
  - データパイプライン
  - モデル設計
  - 運用フロー
  - 実装計画

- [SCHEMA.md](./SCHEMA.md) - JRDBデータスキーマ仕様書
  - データタイプの詳細
  - フィールド定義
  - コードテーブル

- [ML_FEATURE.md](./ML_FEATURE.md) - 特徴量設計
  - 特徴量の詳細リスト
  - Target Encoding設計
  - リーク対策

- [downloader/README.md](./downloader/README.md) - データダウンローダー
  - 使用方法
  - スクリプト一覧
  - データ形式

- [cloud_functions/gcs_to_bq/README.md](./cloud_functions/gcs_to_bq/README.md) - Cloud Functions
  - デプロイ方法
  - テスト手順

## 技術スタック

- **言語**: Python 3.9+
- **機械学習**: LightGBM (Learning to Rank)
- **クラウド**: GCP (BigQuery, Cloud Storage, Cloud Run, Cloud Functions)
- **テスト**: pytest
- **通知**: SendGrid, LINE Notify
- **可視化**: Streamlit

## 実装計画

### Phase 1: データ基盤構築 ✅

- [x] GCSバケット作成
- [x] BigQueryデータセット・テーブル作成 (`src/data/create_tables.py`)
- [x] JRDBダウンローダー (`src/data/jrdb_downloader.py`)
- [x] GCSアップロードスクリプト (`src/data/upload_to_gcs.py`)
- [x] パイプライン統合 (`src/data/pipeline.py`)
- [x] GCS→BigQuery自動ロード (`cloud_functions/gcs_to_bq/`)
- [x] BigQueryロードモジュール (`src/data/load_to_bq.py`)
  - [x] ロード履歴管理 (`raw.load_history`テーブル)
  - [x] 重複スキップ機能 (`--skip-loaded`オプション)
- [x] データ品質チェックスクリプト (`src/data/quality_check.py`)
- [x] 既存ファイル再ロードスクリプト (`scripts/reload_gcs_to_bq.py`)

### Phase 2: 特徴量エンジニアリング ✅

- [x] 過去走集計特徴量 (`src/features/past_performance.py`)
- [x] 条件適性特徴量 (`src/features/condition_features.py`)
- [x] 特徴量パイプライン (`src/features/feature_pipeline.py`)
- [ ] Target Encoding実装 (Phase 3で実装予定)

### Phase 3: Cloud Run統合 🚧

- [x] Cloud Runエントリーポイント (`main.py`)
- [x] Dockerfile作成
- [x] フルパイプライン統合（ダウンロード→アップロード→特徴量生成）
- [x] 日次パイプライン (`src/pipeline/daily_pipeline.py`)
- [x] 過去分全件ロードパイプライン (`src/pipeline/full_load_pipeline.py`)
- [x] FastAPI HTTPエンドポイント (`src/api/app.py`)
- [ ] Cloud Scheduler設定
- [ ] Secret Managerでの認証情報管理
- [ ] Cloud Loggingとの統合

### Phase 4: モデル開発

- [ ] LightGBM ランク学習
- [ ] 時系列クロスバリデーション
- [ ] バックテスト

### Phase 5: 運用システム構築

- [ ] 予測パイプライン
- [ ] Webダッシュボード
- [ ] 通知システム

## ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)
