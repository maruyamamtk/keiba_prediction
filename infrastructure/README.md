# Infrastructure セットアップガイド

このディレクトリには、データパイプライン自動化に必要なGCPインフラのセットアップスクリプトが含まれています。

## 前提条件

- Google Cloud SDK (`gcloud`) がインストール済み
- GCPプロジェクトが作成済み
- 適切な権限を持つユーザーでログイン済み

## アーキテクチャ概要

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Cloud Scheduler                                  │
│                    (毎日 AM 6:00 JST トリガー)                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                           Cloud Run                                      │
│                    (データパイプラインサービス)                            │
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│  │ JRDBから    │ →  │ GCSに      │ →  │ 特徴量生成  │                 │
│  │ ダウンロード │    │ アップロード │    │ パイプライン │                 │
│  └─────────────┘    └─────────────┘    └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      Cloud Storage (GCS)                                 │
│                   (keiba-raw-data バケット)                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓ (自動トリガー)
┌─────────────────────────────────────────────────────────────────────────┐
│                       Cloud Functions                                    │
│                    (gcs_to_bq: GCS → BigQuery)                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                          BigQuery                                        │
│              (raw, features, predictions データセット)                   │
└─────────────────────────────────────────────────────────────────────────┘
```

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
- Cloud Run用サービスアカウントの作成
- 必要な権限の付与
- Secret ManagerへのJRDB認証情報の登録

### 3. 設定の確認

```bash
./infrastructure/scripts/verify_setup.sh
```

## サービスアカウント

### keiba-pipeline-sa (Cloud Run用)

データパイプライン実行に使用するサービスアカウントです。

**付与される権限:**
- `roles/storage.objectAdmin` - GCS読み書き
- `roles/bigquery.dataEditor` - BigQuery読み書き
- `roles/bigquery.jobUser` - BigQueryジョブ実行
- `roles/secretmanager.secretAccessor` - Secret Manager読み取り
- `roles/logging.logWriter` - Cloud Logging書き込み

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

### Secret Manager登録エラー

```
ERROR: (gcloud.secrets.create) ALREADY_EXISTS
```

→ シークレットは既に存在しています。バージョンを追加する場合は手動で実行してください。
