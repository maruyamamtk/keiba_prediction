# 競馬予測MLシステム

競馬の馬券購入を支援する機械学習システム

## 概要

このプロジェクトは、JRDBの競馬データを活用し、機械学習による馬券購入支援システムを構築します。

- **対象馬券**: 単勝・複勝
- **予測内容**: 3着以内に入る確率
- **目標**: 回収率100%以上
- **技術**: Python 3.9+, LightGBM, GCP (BigQuery, Cloud Run, Cloud Storage)

詳細な仕様は [CLAUDE.md](./CLAUDE.md) を参照してください。

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
# .env ファイルを編集してGCP_PROJECT_IDとJRDB認証情報を設定
```

### 2. GCPセットアップ

```bash
# GCPプロジェクトの認証
gcloud auth application-default login

# BigQueryデータセット・テーブル作成
python3 -m src.manual.create_tables
```

### 3. データ取得

```bash
# JRDBから全データタイプをダウンロード
python3 -m src.automation.data.jrdb_downloader --start-date 240101

# または過去分全件を一括処理（推奨）
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31
```

### 4. データ品質チェック

```bash
# BigQueryロード後の品質チェック
python3 -m src.manual.quality_check
```

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
│   └── ml/                            # 機械学習
│       ├── __init__.py
│       └── features/
│           ├── __init__.py
│           ├── condition_features.py  # 条件適性特徴量
│           ├── feature_pipeline.py    # 特徴量パイプライン
│           └── past_performance.py    # 過去走特徴量
├── legacy/                            # レガシーコード（参照用）
│   ├── README.md
│   ├── main.py                        # 旧Flaskエントリーポイント
│   ├── data_pipeline.py              # 旧パイプライン
│   ├── cloud_functions/              # 旧Cloud Functions
│   └── downloader/                   # 旧シェルスクリプト版
├── scripts/                           # ユーティリティスクリプト
│   ├── generate_features.py
│   ├── reload_gcs_to_bq.py
│   ├── setup_bigquery.sh
│   ├── setup_gcp.sh
│   └── sync_to_gcs.sh
├── tests/                             # テストコード
├── notebooks/                         # Jupyter Notebook（EDA）
│   ├── 01_data_exploration.ipynb
│   ├── 02_race_analysis.ipynb
│   ├── 03_horse_analysis.ipynb
│   └── 04_feature_correlation.ipynb
├── config/                            # BigQueryスキーマ定義JSON
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

### 3. `src/ml/` - 機械学習

**目的**: 機械学習モデルの学習・予測と特徴量エンジニアリング。

**含まれるモジュール**:
- `features/feature_pipeline.py`: 特徴量生成パイプライン
- `features/past_performance.py`: 過去走集計特徴量
- `features/condition_features.py`: 条件適性特徴量（芝/ダート、距離帯など）

**使用場面**:
- BigQueryのrawデータから特徴量テーブルを生成
- モデル学習前の特徴量準備
- 特徴量の追加・更新

### 4. `legacy/` - レガシーコード

**目的**: 旧アーキテクチャのコード。参照目的で保持しており、新規開発では使用しない。

**含まれるファイル**:
- `main.py`: 旧Flaskエントリーポイント → FastAPI (`src/automation/api/app.py`) に移行済み
- `data_pipeline.py`: 旧パイプライン → `src/automation/pipeline/` に移行済み
- `cloud_functions/`: 旧Cloud Functions → Cloud Run統合パイプラインに移行済み
- `downloader/`: 旧シェルスクリプト版 → Pythonモジュール (`src/automation/data/jrdb_downloader.py`) に移行済み

**注意**: legacy配下のコードは実行せず、src配下の新しいモジュールを使用してください。

---

## データパイプライン

### 全体フロー

```
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. データ取得 (JRDB → downloaded_files/)                                  │
│    $ python3 -m src.automation.data.jrdb_downloader --start-date 240101 │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. GCSアップロード (ローカル → GCS)                                      │
│    $ python3 -m src.automation.data.upload_to_gcs                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. BigQueryロード (GCS → BigQuery)                                      │
│    $ python3 -m src.automation.data.load_to_bq --skip-loaded            │
│      - MERGE処理で重複防止                                              │
│      - ロード履歴管理 (raw.load_history)                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. 特徴量生成 (BigQuery raw → features)                                 │
│    $ python3 -m src.ml.features.feature_pipeline --start-date ... \     │
│      --end-date ...                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 自動化の実現状況

**実現済み:**
- ✅ Cloud Run: FastAPI HTTPエンドポイント（`/api/v1/load/daily`, `/api/v1/load/full`）
- ✅ Step 1+2+3の統合: JRDBダウンロード → GCSアップロード → BigQueryロード（`DailyPipeline`, `FullLoadPipeline`）
- ✅ downloaded_files/ に保存（永続）
- ✅ HTTPリクエスト経由でのトリガー対応（Cloud Scheduler連携可能）
- ✅ 過去分全件ロードによる初回セットアップ・データ補完

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
│   ├─ Step 1: JRDBダウンロード (downloaded_files/)                        │
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
- downloaded_files/ への永続保存でデータの再利用が容易
- 統合パイプラインによるエラーハンドリング
- ロード履歴による重複防止・処理効率化

### 手動実行の手順

#### Step 1: データ取得 (JRDB → ローカル)

```bash
# 全データタイプをダウンロード
python3 -m src.automation.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python3 -m src.automation.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python3 -m src.automation.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
```

#### Step 2: GCSアップロード

```bash
# 全データをアップロード（差分のみ）
python3 -m src.automation.data.upload_to_gcs

# 特定タイプのみアップロード
python3 -m src.automation.data.upload_to_gcs --data-type Sec

# ドライラン（実際にはアップロードしない）
python3 -m src.automation.data.upload_to_gcs --dry-run
```

#### Step 1+2+3 統合: 日次パイプライン（推奨）

**CLIからの実行:**

```bash
# 当日のデータを処理（ダウンロード→GCSアップロード→BigQueryロード）
python3 -m src.automation.pipeline.daily_pipeline

# 特定日付を指定
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15

# JSON形式で結果を出力
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15 --json
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

#### 全件ロード: 過去データの一括処理

初回セットアップやデータ欠損の補完時に使用します。

```bash
# 全期間のデータを一括処理
python3 -m src.automation.pipeline.full_load_pipeline

# 期間を指定して処理
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31

# API経由で非同期実行
curl -X POST http://localhost:8080/api/v1/load/full \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

#### Step 3: BigQueryロード（重複スキップ機能）

`raw.load_history`テーブルのロード履歴を参照し、既にロード済みのファイルを自動でスキップします。

```bash
# 全データタイプを一括ロード（推奨）
python3 -m src.automation.data.load_to_bq --skip-loaded

# 特定プレフィックス配下のみロード
python3 -m src.automation.data.load_to_bq --prefix Sec/ --skip-loaded

# 特定のデータタイプのみ
python3 -m src.automation.data.load_to_bq --data-types SEC --skip-loaded

# 履歴記録を無効化（テスト用）
python3 -m src.automation.data.load_to_bq --prefix Sec/ --no-history

# 重複スキップと組み合わせ
python3 -m src.automation.data.load_to_bq --prefix Sec/ --skip-loaded --data-types BAA KYF SEC
```

**重複スキップ機能の利点:**
- 既にロード済みのファイルをスキップし、処理時間を短縮
- ロード履歴を`raw.load_history`テーブルで管理
- 失敗したファイルのリトライが簡単（履歴上は失敗扱いなので再ロードされる）
- バッチロード時のコスト削減

#### Step 4: 特徴量生成

```bash
# 指定期間の特徴量を生成
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-01-06

# 詳細ログ付きで実行
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-12-31 -v
```

#### Step 5: データ品質チェック

```bash
# 全テーブルのチェック
python3 -m src.manual.quality_check

# 特定テーブルのみチェック
python3 -m src.manual.quality_check --table raw.race_info
```

### 対応データタイプ

| データタイプ | 説明 | BigQueryテーブル |
|-------------|------|-----------------|
| BAA/BAB/BAC | 番組データ (レース基本情報) | `raw.race_info` |
| KYF/KYG/KYH | 競走馬データ (出馬表・予測指数) | `raw.horse_results` |
| SEC | 成績データ (レース結果) | `raw.race_results` |

---

## 主要機能

### JRDBダウンローダー

JRDBからデータをダウンロードし、解凍・エンコーディング変換を行います。

```bash
# 全データタイプをダウンロード
python3 -m src.automation.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python3 -m src.automation.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python3 -m src.automation.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
```

**特徴:**
- lzhファイルの自動解凍
- CP932からUTF-8へのエンコーディング変換
- 環境変数からの認証情報取得（Cloud Run対応）
- downloaded_files/ への永続保存

### GCSアップロード

ローカルのダウンロードデータをGCSにアップロードします。

```bash
# 全データをアップロード
python3 -m src.automation.data.upload_to_gcs

# 特定のデータタイプのみアップロード
python3 -m src.automation.data.upload_to_gcs --data-type Baa

# ドライラン（実際にはアップロードしない）
python3 -m src.automation.data.upload_to_gcs --dry-run

# 強制アップロード（差分チェックをスキップ）
python3 -m src.automation.data.upload_to_gcs --force
```

**特徴:**
- MD5チェックによる差分アップロード
- リトライ機能（最大3回）
- プログレス表示
- 詳細なアップロードレポート

### BigQueryロード

GCSにアップロードされたJRDBデータをBigQueryにロードします。

```bash
# 全CSVファイルをロード
python3 -m src.automation.data.load_to_bq

# 重複スキップを有効化（推奨）
python3 -m src.automation.data.load_to_bq --skip-loaded

# 特定のデータタイプのみロード
python3 -m src.automation.data.load_to_bq --data-types BAA KYF SEC

# 特定のプレフィックス配下のファイルをロード
python3 -m src.automation.data.load_to_bq --prefix Sec/

# エラー時に処理を中断
python3 -m src.automation.data.load_to_bq --stop-on-error
```

**主な機能:**
- **重複スキップ機能**: `--skip-loaded`オプションで、既にロード済みのファイルを自動スキップ
- **ロード履歴管理**: `raw.load_history`テーブルにロード履歴を記録
- **MERGE処理**: 既存レコードはUPDATE、新規レコードはINSERTで重複を防止
- **バッチ処理**: 複数ファイルを一括処理
- **エラーハンドリング**: 失敗したファイルを記録し、リトライが容易

**ロード履歴テーブル (`raw.load_history`)**

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

### 日次パイプライン

JRDBダウンロード→GCSアップロード→BigQueryロードの統合処理を実行します。

**CLIからの実行:**

```bash
# 当日のデータを処理
python3 -m src.automation.pipeline.daily_pipeline

# 特定日付を指定
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15

# JSON形式で結果を出力
python3 -m src.automation.pipeline.daily_pipeline --date 2024-01-15 --json
```

**FastAPI HTTPエンドポイント:**

APIサーバーを起動し、HTTPリクエストでパイプラインを実行できます。

```bash
# 開発環境でサーバー起動
uvicorn src.automation.api.app:app --reload --port 8080

# 本番環境（Cloud Run）
python3 -m src.automation.api.app
```

**エンドポイント一覧:**

| エンドポイント | メソッド | 説明 |
|-------------|---------|------|
| `/health` | GET | ヘルスチェック |
| `/api/v1/load/daily` | POST | 同期日次ロード（処理完了まで待機） |
| `/api/v1/load/daily/async` | POST | 非同期日次ロード（バックグラウンド処理） |
| `/api/v1/load/full` | POST | 非同期全件ロード（バックグラウンド処理） |
| `/api/v1/load/full/sync` | POST | 同期全件ロード（テスト用） |

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
- **永続保存**: downloaded_files/ にデータを保持
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
  --uri "https://keiba-daily-pipeline-xxxxx.a.run.app/api/v1/load/daily/async" \
  --http-method POST \
  --headers "Content-Type=application/json" \
  --message-body '{}'
```

### 過去分全件ロードパイプライン

指定期間のデータをJRDBからダウンロード→GCS→BigQueryに一括ロードします。
初回セットアップやデータ欠損の補完に使用します。

**CLIからの実行:**

```bash
# 全期間のデータを処理
python3 -m src.automation.pipeline.full_load_pipeline

# 期間を指定
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31

# JSON形式で結果を出力
python3 -m src.automation.pipeline.full_load_pipeline --start-date 2020-01-01 --end-date 2024-12-31 --json
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

**主な機能:**
- **一括処理**: 指定期間のデータを一括でダウンロード→アップロード→ロード
- **日付フィルタ**: ファイル名のyymmdd部分で日付範囲を自動フィルタ
- **重複スキップ**: 既にロード済みのファイルは自動スキップ
- **ジョブID追跡**: 各実行にユニークなジョブIDを付与
- **バックグラウンド実行**: 長時間処理はバックグラウンドで実行し即座にレスポンス
- **エラー耐性**: 一部ファイルが失敗しても残りの処理を継続

### データ品質チェック

BigQueryにロードされたデータの品質を自動チェックします。

```bash
# 全テーブルのチェック
python3 -m src.manual.quality_check

# 特定テーブルのみチェック
python3 -m src.manual.quality_check --table raw.race_info

# レポート出力先を指定
python3 -m src.manual.quality_check --output reports/my_report.json

# アラートを無効化
python3 -m src.manual.quality_check --no-alert
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

### BigQueryテーブル作成

BigQueryのデータセットとテーブルを作成します。

```bash
python3 -m src.manual.create_tables
```

**作成されるリソース:**
- データセット: `raw`, `features`, `predictions`, `backtests`
- テーブル: `race_info`, `horse_results`, `race_results`, `training_data`, `load_history` など

---

## 特徴量パイプライン

BigQueryの`raw`テーブルから特徴量を生成し、`features.training_data`テーブルに保存します。

```bash
# 指定期間の特徴量を生成
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-01-06

# 詳細ログ付きで実行
python3 -m src.ml.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-12-31 -v
```

### 生成される特徴量

| カテゴリ | 特徴量例 |
|---------|---------|
| 過去走統計 | past_3_avg_position, past_5_avg_last3f |
| 条件適性 | turf_place_rate, dirt_place_rate, dist_sprint_place_rate |
| 脚質 | front_rate, closer_rate, avg_corner4_position |

**注意**: 特徴量パイプラインは`race_results`テーブルにデータが存在する日付でのみ動作します。

詳細は [ML_FEATURE.md](./ML_FEATURE.md) を参照してください。

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

---

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

---

## 技術スタック

- **言語**: Python 3.9+
- **機械学習**: LightGBM (Learning to Rank)
- **クラウド**: GCP (BigQuery, Cloud Storage, Cloud Run)
- **API**: FastAPI
- **テスト**: pytest
- **通知**: SendGrid, LINE Notify
- **可視化**: Streamlit

---

## 実装計画

### Phase 1: データ基盤構築 ✅

- [x] GCSバケット作成
- [x] BigQueryデータセット・テーブル作成 (`src/manual/create_tables.py`)
- [x] JRDBダウンローダー (`src/automation/data/jrdb_downloader.py`)
- [x] GCSアップロードスクリプト (`src/automation/data/upload_to_gcs.py`)
- [x] BigQueryロードモジュール (`src/automation/data/load_to_bq.py`)
  - [x] ロード履歴管理 (`raw.load_history`テーブル)
  - [x] 重複スキップ機能 (`--skip-loaded`オプション)
- [x] データ品質チェックスクリプト (`src/manual/quality_check.py`)
- [x] 既存ファイル再ロードスクリプト (`scripts/reload_gcs_to_bq.py`)

### Phase 2: 特徴量エンジニアリング ✅

- [x] 過去走集計特徴量 (`src/ml/features/past_performance.py`)
- [x] 条件適性特徴量 (`src/ml/features/condition_features.py`)
- [x] 特徴量パイプライン (`src/ml/features/feature_pipeline.py`)
- [ ] Target Encoding実装 (Phase 3で実装予定)

### Phase 3: Cloud Run統合 🚧

- [x] FastAPI HTTPエンドポイント (`src/automation/api/app.py`)
- [x] Dockerfile作成
- [x] 日次パイプライン (`src/automation/pipeline/daily_pipeline.py`)
- [x] 過去分全件ロードパイプライン (`src/automation/pipeline/full_load_pipeline.py`)
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

---

## ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)
