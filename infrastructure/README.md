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

### 4. Cloud Runへのデプロイ

#### 4.1 必要なファイルの作成

Cloud Runにデプロイするために、以下のファイルをプロジェクトルートに作成します。

##### 4.1.1 Dockerfile

プロジェクトルートに `Dockerfile` を作成します：

```dockerfile
# Dockerfile
FROM python:3.9-slim

# 作業ディレクトリを設定
WORKDIR /app

# システム依存パッケージのインストール
# - curl: ヘルスチェック用
# - p7zip-full: lzhファイルの展開用
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    p7zip-full \
    && rm -rf /var/lib/apt/lists/*

# 依存パッケージをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY downloader/ ./downloader/
COPY config/ ./config/
COPY main.py .

# 環境変数のデフォルト値
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# ポートを公開
EXPOSE 8080

# アプリケーションを起動
CMD ["python", "main.py"]
```

##### 4.1.2 .dockerignore

ビルドコンテキストから不要なファイルを除外するため、プロジェクトルートに `.dockerignore` を作成します：

```
# .dockerignore

# Git
.git
.gitignore

# Python
__pycache__
*.py[cod]
*$py.class
*.so
.Python
venv/
.venv/
*.egg-info/
.eggs/

# IDE
.idea/
.vscode/
*.swp
*.swo

# テスト
.pytest_cache/
.coverage
htmlcov/
tests/

# ドキュメント
*.md
docs/
reports/

# ローカル環境
.env
.env.local
*.log

# ダウンロードデータ（大容量）
downloaded_files/

# Jupyter
notebooks/
*.ipynb
.ipynb_checkpoints/

# その他
.DS_Store
*.json
!config/*.json
infrastructure/
cloud_functions/
```

##### 4.1.3 main.py（エントリーポイント）

Cloud Runで実行されるエントリーポイントとして、プロジェクトルートに `main.py` を作成します：

```python
#!/usr/bin/env python3
"""
Cloud Run エントリーポイント

HTTPリクエストを受け付け、データパイプラインを実行する。
Cloud Schedulerからのトリガーで定期実行される。
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, request

# ロギング設定
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/", methods=["GET"])
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/run", methods=["POST"])
def run_pipeline():
    """
    データパイプラインを実行するエンドポイント

    Cloud Schedulerから呼び出される。
    """
    try:
        logger.info("パイプライン実行を開始します")

        # TODO: 以下の処理を実装
        # 1. JRDBからデータをダウンロード
        # 2. GCSにアップロード
        # 3. 特徴量生成パイプラインを実行

        # 現時点ではプレースホルダーとして成功を返す
        result = {
            "status": "success",
            "message": "パイプライン実行が完了しました",
            "timestamp": datetime.utcnow().isoformat(),
        }

        logger.info("パイプライン実行が完了しました")
        return jsonify(result), 200

    except Exception as e:
        logger.error(f"パイプライン実行中にエラーが発生しました: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"サーバーをポート {port} で起動します")
    app.run(host="0.0.0.0", port=port)
```

##### 4.1.4 requirements.txt の更新

`requirements.txt` に Flask を追加します（未追加の場合）：

```bash
# requirements.txt に以下を追加
echo "flask==3.0.0" >> requirements.txt
```

または、Cloud Run用の依存関係を別ファイルで管理する場合は `requirements-cloudrun.txt` を作成：

```
# requirements-cloudrun.txt
flask==3.0.0
gunicorn==21.2.0
```

#### 4.2 Dockerイメージのビルド・プッシュ

**前提条件:**
- 4.1 で必要なファイル（Dockerfile, .dockerignore, main.py）を作成済み
- Dockerがインストール済み（またはCloud Buildを使用）
- `setup_gcp.sh`が実行済み（Artifact Registryリポジトリが作成されていること）

**手順:**

1. **Docker認証の設定**（初回のみ）

   ```bash
   gcloud auth configure-docker asia-northeast1-docker.pkg.dev
   ```

2. **イメージのビルド**

   プロジェクトルートで以下を実行します：

   ```bash
   # 環境変数を読み込み
   source .env

   # イメージをビルド（タグはlatestまたは任意のバージョン）
   # ※ Cloud Runはamd64アーキテクチャが必要なため --platform オプションを指定
   docker build --platform linux/amd64 -t asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest .
   ```

   > **注意（Apple Silicon Mac使用時）**: M1/M2/M3 Macでは`--platform linux/amd64`オプションが必須です。このオプションがないとarm64イメージがビルドされ、Cloud Runデプロイ時に「must support amd64/linux」エラーが発生します。

3. **ローカルでの動作確認**（オプション）

   ```bash
   # コンテナをローカルで起動
   docker run -p 8080:8080 \
     -e GCP_PROJECT_ID=${GCP_PROJECT_ID} \
     asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest

   # 別ターミナルでヘルスチェック
   curl http://localhost:8080/
   ```

   **ポート8080が使用中の場合:**

   別のポート（例：8081）を使用してください：

   ```bash
   # ポート8081でコンテナを起動
   docker run -p 8081:8080 \
     -e GCP_PROJECT_ID=${GCP_PROJECT_ID} \
     asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest

   # 別ターミナルでヘルスチェック
   curl http://localhost:8081/
   ```

   使用中のポートを確認するには：

   ```bash
   lsof -i :8080
   ```

4. **Artifact Registryへプッシュ**

   ```bash
   docker push asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest
   ```

**代替方法: Cloud Buildを使用したビルド**

ローカルにDockerがない場合は、Cloud Buildを使用してビルドできます：

```bash
# 環境変数を読み込み
source .env

# Cloud Buildでビルド・プッシュを一括実行
gcloud builds submit \
  --tag asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest \
  .
```

#### 4.3 Cloud Runサービスのデプロイ

イメージのプッシュ後、以下を実行してCloud Runにデプロイします：

```bash
./infrastructure/scripts/deploy_cloud_run.sh [image-tag]
```

`image-tag`を省略した場合は`latest`が使用されます。

このスクリプトは以下の環境変数を自動的にCloud Runに設定します：
- `GCP_PROJECT_ID`: プロジェクトID
- `GCP_REGION`: リージョン
- `GCS_BUCKET_RAW`: GCSバケット名（フルパス）
- Secret Managerからの認証情報

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

### Dockerビルドエラー

#### ファイルが見つからない

```
COPY failed: file not found in build context
```

→ Dockerfile内で指定しているファイル/ディレクトリが存在するか確認してください。特に以下のファイルが必要です：
- `requirements.txt`
- `main.py`
- `src/` ディレクトリ
- `scripts/` ディレクトリ
- `downloader/` ディレクトリ
- `config/` ディレクトリ

#### Docker認証エラー

```
denied: Permission denied
```

→ Docker認証が設定されているか確認してください：

```bash
gcloud auth configure-docker asia-northeast1-docker.pkg.dev
```

#### イメージプッシュエラー

```
denied: Unauthenticated request
```

→ gcloudにログインしているか確認してください：

```bash
gcloud auth login
gcloud auth application-default login
```

### Cloud Runデプロイエラー

#### アーキテクチャエラー（Apple Silicon Mac）

```
Cloud Run does not support image: Container manifest type 'application/vnd.oci.image.index.v1+json' must support amd64/linux.
```

→ Apple Silicon Mac（M1/M2/M3）でビルドしたイメージはarm64アーキテクチャのため、Cloud Runで動作しません。`--platform linux/amd64`オプションを付けて再ビルドしてください：

```bash
source .env
docker build --platform linux/amd64 -t asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest .
docker push asia-northeast1-docker.pkg.dev/${GCP_PROJECT_ID}/keiba-pipeline/keiba-pipeline:latest
```

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

→ main.pyが環境変数`PORT`で指定されたポートでリッスンしているか確認してください。Cloud Runはデフォルトでポート8080を使用します。
