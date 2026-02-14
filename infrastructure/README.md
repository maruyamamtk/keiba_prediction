# Infrastructure セットアップガイド

このディレクトリには、データパイプライン自動化に必要なGCPインフラのセットアップスクリプトが含まれています。

## 前提条件

- Google Cloud SDK (`gcloud`) がインストール済み
- GCPプロジェクトが作成済み
- 適切な権限を持つユーザーでログイン済み
- Docker がインストール済み（ローカルビルドの場合）

## アーキテクチャ概要

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Cloud Scheduler                                │
│                    (毎日 AM 6:00 JST トリガー)                         │
│                POST /api/v1/load/daily/async                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                     Cloud Run (keiba-pipeline)                         │
│                  FastAPI + uvicorn (src.automation.api.app)             │
│                                                                        │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────┐              │
│  │ JRDBから    │ →  │ GCSに      │ →  │ BigQueryに   │              │
│  │ ダウンロード │    │ アップロード │    │ MERGE/UPSERT │              │
│  └─────────────┘    └─────────────┘    └──────────────┘              │
│         (JRDBDownloader)  (GCSUploader)    (BigQueryLoader)            │
│                                                                        │
│  ┌──────────────┐                                                     │
│  │ 特徴量生成   │  (FeaturePipeline - SQL駆動)                        │
│  └──────────────┘                                                     │
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

## APIエンドポイント

| メソッド | エンドポイント | 用途 |
|---------|---------------|------|
| GET | `/` | ヘルスチェック（簡易） |
| GET | `/health` | ヘルスチェック（詳細） |
| POST | `/api/v1/load/daily` | 日次ロード（同期） |
| POST | `/api/v1/load/daily/async` | 日次ロード（非同期、Cloud Scheduler用） |
| POST | `/api/v1/load/full` | 全件ロード（非同期） |
| POST | `/api/v1/load/full/sync` | 全件ロード（同期、テスト用） |
| POST | `/api/v1/features/generate` | 特徴量生成（同期） |
| POST | `/api/v1/features/generate/async` | 特徴量生成（非同期） |
| GET | `/docs` | OpenAPI (Swagger UI) ドキュメント |

## セットアップ手順

### 1. 環境変数の設定

`.env` ファイルを編集して、GCPプロジェクトIDを設定してください。

```bash
GCP_PROJECT_ID=your-project-id
```

### 2. GCP APIの有効化とサービスアカウント作成

```bash
./infrastructure/scripts/setup_gcp.sh
```

このスクリプトは以下を実行します：
- 必要なGCP APIを有効化
- Artifact Registryリポジトリの作成
- Cloud Run用サービスアカウントの作成
- 必要な権限の付与
- Secret ManagerへのJRDB認証情報の登録

### 3. 設定の確認

```bash
./infrastructure/scripts/verify_setup.sh
```

### 4. Dockerイメージのビルド・プッシュ

```bash
./infrastructure/scripts/build_and_push.sh [image-tag]
```

`image-tag`を省略した場合は`latest`が使用されます。

このスクリプトは以下を実行します：
- Artifact Registryへの認証設定
- `linux/amd64`プラットフォームでのDockerイメージビルド（Apple Silicon Mac対応）
- Artifact Registryへのプッシュ

> **注意（Apple Silicon Mac使用時）**: M1/M2/M3/M4 Macでは`--platform linux/amd64`オプションが必須です。スクリプトは自動的にこのオプションを付与します。

### 5. Cloud Runへデプロイ

```bash
./infrastructure/scripts/deploy_cloud_run.sh [image-tag]
```

このスクリプトは以下の環境変数を自動的にCloud Runに設定します：
- `GCP_PROJECT_ID`: プロジェクトID
- `GCP_REGION`: リージョン
- `GCS_BUCKET_RAW`: GCSバケット名（フルパス）
- `BQ_DATASET_RAW`: rawデータセット名
- `BQ_DATASET_FEATURES`: 特徴量データセット名
- Secret Managerからの認証情報 (`JRDB_USER`, `JRDB_PASSWORD`)

### 6. デプロイ後の動作確認

```bash
./infrastructure/scripts/verify_deployment.sh
```

このスクリプトは以下の動作確認を行います：
- ルートエンドポイント (`GET /`) の疎通
- ヘルスチェック (`GET /health`) の疎通
- 日次ロード (`POST /api/v1/load/daily`) のテスト実行（対話的に選択可能）
- OpenAPIドキュメント (`GET /docs`) の疎通

### 7. Cloud Schedulerの設定

日次データ取得を自動化するCloud Schedulerジョブを設定します：

```bash
./infrastructure/scripts/setup_scheduler.sh
```

このスクリプトは以下を実行します：
- Cloud Scheduler APIの有効化確認
- サービスアカウントへのCloud Run Invoker権限付与
- Cloud Schedulerジョブの作成（既存なら更新）

#### ジョブ設定

| 項目 | 値 |
|------|-----|
| ジョブ名 | `daily-data-pipeline` |
| スケジュール | `0 6 * * *`（毎日AM 6:00 JST） |
| ターゲット | `POST /api/v1/load/daily/async` |
| 認証 | OIDCトークン（`keiba-pipeline-sa`） |
| タイムアウト | 900秒（15分） |
| リトライ | 最大3回、バックオフ5秒〜300秒 |

#### ジョブの操作

```bash
# 手動で即時実行（テスト用）
gcloud scheduler jobs run daily-data-pipeline --location=asia-northeast1

# ジョブの一時停止
gcloud scheduler jobs pause daily-data-pipeline --location=asia-northeast1

# ジョブの再開
gcloud scheduler jobs resume daily-data-pipeline --location=asia-northeast1

# ジョブの削除
gcloud scheduler jobs delete daily-data-pipeline --location=asia-northeast1
```

## スクリプト一覧

| スクリプト | 用途 |
|-----------|------|
| `setup_gcp.sh` | GCP初期セットアップ（API有効化、SA作成、権限付与） |
| `verify_setup.sh` | セットアップ状態の確認 |
| `build_and_push.sh` | Dockerイメージのビルド・プッシュ |
| `deploy_cloud_run.sh` | Cloud Runサービスのデプロイ |
| `verify_deployment.sh` | デプロイ後の動作確認 |
| `setup_scheduler.sh` | Cloud Schedulerジョブの作成・更新 |

## サービスアカウント

### keiba-pipeline-sa (Cloud Run用)

データパイプライン実行に使用するサービスアカウントです。

**付与される権限:**
- `roles/storage.objectAdmin` - GCS読み書き
- `roles/bigquery.dataEditor` - BigQuery読み書き
- `roles/bigquery.jobUser` - BigQueryジョブ実行
- `roles/secretmanager.secretAccessor` - Secret Manager読み取り
- `roles/logging.logWriter` - Cloud Logging書き込み
- `roles/monitoring.metricWriter` - Cloud Monitoring書き込み

## Secret Manager

以下のシークレットが登録されます：

| シークレット名 | 説明 |
|---------------|------|
| `jrdb-user` | JRDB認証ユーザー名 |
| `jrdb-password` | JRDBパスワード |

## 環境変数一覧

Cloud Runサービスに設定される環境変数：

| 変数名 | 説明 | デフォルト値 |
|--------|------|-------------|
| `GCP_PROJECT_ID` | GCPプロジェクトID | - |
| `GCP_REGION` | GCPリージョン | `asia-northeast1` |
| `GCS_BUCKET_RAW` | rawデータ用バケット | `${PROJECT_ID}-keiba-raw-data` |
| `BQ_DATASET_RAW` | rawデータセット | `raw` |
| `BQ_DATASET_FEATURES` | 特徴量データセット | `features` |
| `LOG_LEVEL` | ログレベル | `INFO` |

## Cloud Run設定

| 項目 | 値 | 説明 |
|------|-----|------|
| メモリ | 2Gi | JRDBデータの処理に十分な量 |
| CPU | 2 | 並列処理対応 |
| タイムアウト | 900秒 | 全件ロード等の長時間処理対応 |
| 同時実行数 | 1 | パイプラインは逐次処理 |
| 最大インスタンス | 1 | 同時実行の防止 |
| 認証 | 必須 | Cloud Scheduler/内部呼び出しのみ |

## トラブルシューティング

### API有効化エラー

```
ERROR: (gcloud.services.enable) PERMISSION_DENIED
```

→ プロジェクトオーナー権限があることを確認してください。

### サービスアカウント作成エラー

```
ERROR: (gcloud.iam.service-accounts.create) Resource already exists
```

→ サービスアカウントは既に存在しています。スクリプトは既存のアカウントに権限を追加します。

### Dockerビルドエラー

#### アーキテクチャエラー（Apple Silicon Mac）

```
Cloud Run does not support image: Container manifest type must support amd64/linux.
```

→ `build_and_push.sh`スクリプトは自動的に`--platform linux/amd64`を指定します。手動ビルドの場合はこのオプションを必ず付けてください。

#### Docker認証エラー

```
denied: Permission denied
```

→ Docker認証が設定されているか確認してください：

```bash
gcloud auth configure-docker asia-northeast1-docker.pkg.dev
```

### Cloud Runデプロイエラー

#### イメージが見つからない

```
Dockerイメージが見つかりません
```

→ イメージがArtifact Registryにプッシュされているか確認してください：

```bash
source .env
gcloud artifacts docker images list \
  asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline
```

#### ポートエラー

```
Container failed to start. Failed to start and then listen on the port defined by the PORT environment variable.
```

→ uvicornがポート8080でリッスンしているか確認してください。Dockerfileの`CMD`が正しく設定されていることを確認します。

### Cloud Runログの確認

```bash
gcloud run services logs read keiba-pipeline --region=asia-northeast1 --limit=50
```
