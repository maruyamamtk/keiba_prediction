# GCPプロジェクトのセットアップ手順

このドキュメントでは、競馬予測MLシステムのためのGCPプロジェクトとCloud Storageバケットのセットアップ手順を説明します。

## 前提条件

- Google Cloudアカウントを持っていること
- `gcloud` CLIがインストールされていること
- `gcloud auth login` でログイン済みであること
- プロジェクトの課金が有効化されていること

### gcloud CLIのインストール

まだインストールしていない場合は、以下の手順でインストールしてください：

```bash
# macOS
brew install --cask google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# インストール後、初期化
gcloud init
```

## セットアップ手順

### 1. 環境変数ファイルの作成

`.env.example` をコピーして `.env` ファイルを作成します：

```bash
cp .env.example .env
```

`.env` ファイルを編集し、以下の必須項目を設定してください：

```bash
# GCPプロジェクトID（必須）
GCP_PROJECT_ID=your-actual-project-id

# その他の設定はデフォルトのまま、または必要に応じて変更
GCP_REGION=asia-northeast1
```

**重要**: `.env` ファイルは `.gitignore` に含まれており、Gitにコミットされません。

### 2. GCPプロジェクトの作成（初回のみ）

新しいプロジェクトを作成する場合：

```bash
# プロジェクトIDを決定（グローバルに一意である必要があります）
export PROJECT_ID="keiba-prediction-$(date +%s)"

# プロジェクトを作成
gcloud projects create $PROJECT_ID --name="Keiba Prediction"

# 作成したプロジェクトIDを.envファイルに設定
echo "GCP_PROJECT_ID=$PROJECT_ID" >> .env
```

既存のプロジェクトを使用する場合は、そのプロジェクトIDを `.env` に設定してください。

### 3. 課金アカウントの設定（初回のみ）

プロジェクトに課金アカウントをリンクします：

```bash
# 利用可能な課金アカウントを確認
gcloud billing accounts list

# 課金アカウントをプロジェクトにリンク
gcloud billing projects link $PROJECT_ID --billing-account=BILLING_ACCOUNT_ID
```

### 4. セットアップスクリプトの実行

自動セットアップスクリプトを実行します：

```bash
./scripts/setup_gcp.sh
```

このスクリプトは以下の処理を実行します：

1. ✅ GCPプロジェクトの設定
2. ✅ 必要なAPIの有効化
   - Cloud Storage API
   - BigQuery API
   - Cloud Functions API
   - Cloud Run API
   - Cloud Scheduler API
   - Cloud Build API
   - Cloud Logging API
   - Cloud Monitoring API
3. ✅ サービスアカウントの作成と権限設定
4. ✅ Cloud Storageバケットの作成
   - `${PROJECT_ID}-keiba-raw-data`: JRDBダウンロード生データ（90日後自動削除）
   - `${PROJECT_ID}-keiba-processed-data`: 加工済みデータ
   - `${PROJECT_ID}-keiba-models`: 学習済みモデル（バージョニング有効）
   - `${PROJECT_ID}-keiba-predictions`: 予測結果（バージョニング有効）

### 5. セットアップの確認

セットアップが正常に完了したことを確認します：

```bash
# プロジェクトが設定されているか確認
gcloud config get-value project

# 有効化されたAPIを確認
gcloud services list --enabled

# サービスアカウントを確認
gcloud iam service-accounts list

# バケットを確認
gsutil ls
```

## セットアップ後の作業

### サービスアカウントキーのダウンロード（ローカル実行用）

ローカルでスクリプトを実行する場合は、サービスアカウントキーをダウンロードします：

```bash
# サービスアカウントのメールアドレスを確認
SERVICE_ACCOUNT_EMAIL=$(gcloud iam service-accounts list --filter="displayName:Keiba Prediction Service Account" --format="value(email)")

# キーを作成してダウンロード
gcloud iam service-accounts keys create key.json \
  --iam-account=$SERVICE_ACCOUNT_EMAIL

# 環境変数に設定
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/key.json"
```

**セキュリティ上の注意**:
- `key.json` は `.gitignore` に含まれており、Gitにコミットされません
- キーファイルは安全に保管し、第三者と共有しないでください
- 本番環境ではWorkload Identityの使用を推奨します

### 環境変数の更新

`.env` ファイルのバケット名を実際の名前に更新してください：

```bash
# スクリプトが作成したバケット名を確認
gsutil ls

# .envファイルを更新（例）
# GCS_BUCKET_RAW=your-project-id-keiba-raw-data
# GCS_BUCKET_PROCESSED=your-project-id-keiba-processed-data
# GCS_BUCKET_MODELS=your-project-id-keiba-models
# GCS_BUCKET_PREDICTIONS=your-project-id-keiba-predictions
```

## トラブルシューティング

### エラー: "API is not enabled"

APIの有効化には数分かかる場合があります。少し待ってから再実行してください。

### エラー: "Permission denied"

gcloud CLIで正しいアカウントでログインしているか確認してください：

```bash
gcloud auth list
gcloud config list
```

必要に応じて再ログインしてください：

```bash
gcloud auth login
```

### エラー: "Bucket name already exists"

バケット名はグローバルに一意である必要があります。`.env` ファイルのバケット名を変更するか、異なるプロジェクトIDを使用してください。

### スクリプトの再実行

セットアップスクリプトは冪等性があるため、何度実行しても安全です。既に存在するリソースはスキップされます。

## コスト管理

### 予算アラートの設定

想定外のコストを防ぐため、予算アラートを設定することを推奨します：

```bash
# GCPコンソールから設定
# https://console.cloud.google.com/billing/budgets
```

### コストの確認

```bash
# 現在のコストを確認
gcloud billing accounts list
```

## 次のステップ

GCPのセットアップが完了したら、次のIssueに進んでください：

- **Issue #4**: BigQueryデータセットとテーブルの作成
- **Issue #5**: GCS→BigQuery自動ロードCloud Functionsの実装

## 参考リンク

- [GCP公式ドキュメント](https://cloud.google.com/docs)
- [Cloud Storage公式ドキュメント](https://cloud.google.com/storage/docs)
- [gcloud CLIリファレンス](https://cloud.google.com/sdk/gcloud/reference)
- [サービスアカウントのベストプラクティス](https://cloud.google.com/iam/docs/best-practices-service-accounts)
