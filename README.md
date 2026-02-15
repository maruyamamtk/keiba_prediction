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
- **クラウド**: GCP (BigQuery, Cloud Storage, Cloud Run, Cloud Scheduler)
- **API**: FastAPI
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

# 推論実行
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt

# 推論結果をCSV保存
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt --output-csv predictions.csv
```

### 6. Cloud Runデプロイ（本番環境）

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
│   │   │   ├── load_to_bq.py         # BigQueryロード（MERGE+重複スキップ）
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
│   └── models/                        # モデル学習・推論
│       ├── __init__.py
│       ├── lgbm_ranker.py            # LightGBM LambdaRankモデル
│       ├── train.py                  # 学習パイプライン
│       └── predict.py                # 推論パイプライン
├── scripts/                           # ユーティリティスクリプト
│   ├── generate_features.py
│   ├── reload_gcs_to_bq.py
│   ├── setup_bigquery.sh
│   ├── setup_gcp.sh
│   └── sync_to_gcs.sh
├── tests/                             # テストコード
├── config/                            # 設定ファイル（BigQueryスキーマ、モデル設定）
│   └── model_config.yaml             # LightGBMモデル設定
├── infrastructure/                    # GCPインフラ設定
│   ├── cloud_run_config.yaml
│   └── scripts/
├── docs/                              # ドキュメント
│   ├── GCP_SETUP.md
│   └── BIGQUERY_SETUP.md
├── reports/                           # 品質チェックレポート出力先
├── Dockerfile
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
- `pipeline/daily_pipeline.py`: ダウンロード→アップロード→ロードの統合処理
- `pipeline/full_load_pipeline.py`: 過去分全件ロード
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

**使用場面**:
- モデルの学習と評価（Phase 4）
- 今週末のレース着順予測
- モデルのGCS保存・読み込み

詳細は [src/models/README.md](./src/models/README.md) を参照してください。

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

**Phase 3: 未実装**
- Secret Managerでの認証情報管理
- Cloud Loggingとの統合

**Phase 4: モデル開発 🔧 進行中**
- LightGBM ランク学習 (LambdaRank) ✅
- 時系列分割・推論パイプライン ✅
- バックテスト ⬜

**Phase 5: 運用システム構築 ⬜ 未着手**
- Webダッシュボード
- 通知システム

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

詳細な設定内容は [infrastructure/README.md](./infrastructure/README.md#7-cloud-schedulerの設定) を参照してください。

---

## BigQueryテーブル構成

### rawデータセット

| テーブル | データソース | 説明 | 行数 |
|---------|-------------|------|------|
| `race_info` | BAA (番組データ) | レース基本情報 | ~33,400 |
| `horse_results` | KYF (競走馬データ) | 出馬表・予測指数 | ~486,500 |
| `race_results` | SEC (成績データ) | レース結果 | ~486,500 |
| `horse_extended` | KKA (拡張馬データ) | 条件別成績 | ~486,500 |
| `horse_master` | KSA (馬マスター) | 馬基本情報 | ~21,500 |
| `venue_info` | KAB (開催情報) | 馬場状態・天候 | ~418 |
| `load_history` | (管理用) | ロード履歴管理 | ~3,350 |
| `pedigree` | 血統データ | 血統情報 | 0 (未実装) |
| `odds` | OZ (オッズデータ) | オッズ情報 | 0 (未実装) |

詳細なスキーマは [SCHEMA.md](./SCHEMA.md) を参照してください。

#### ロード履歴テーブル (`raw.load_history`)

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

### featuresデータセット

| テーブル | 説明 | カラム数 | 行数 |
|---------|------|---------|------|
| `training_data` | 学習用特徴量 | 257 | 466,265 |

特徴量の詳細は [ML_FEATURE.md](./ML_FEATURE.md) を参照してください。

### predictions/backtestsデータセット

テーブルのみ作成済み。Phase 4-5で実装予定。

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
- ⬜ Secret Managerでの認証情報管理
- ⬜ Cloud Loggingとの統合

### Phase 4: モデル開発 🔧 進行中

- ✅ LightGBM ランク学習 (LambdaRank) - Issue #14
- ✅ 時系列分割（学習・検証・推論） - Issue #14
- ✅ 推論パイプライン - Issue #14
- ⬜ バックテスト

### Phase 5: 運用システム構築 ⬜ 未着手

- ⬜ 予測パイプライン
- ⬜ Webダッシュボード
- ⬜ 通知システム

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
