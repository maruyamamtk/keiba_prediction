# 環境変数管理ガイド

データパイプライン自動化システムにおける環境変数の管理方法を定義します。

## 環境変数の分類

### 1. 公開設定（環境変数として直接設定）

機密性がなく、ソースコードにコミットしても問題ない設定値。

| 変数名 | 説明 | 設定場所 |
|--------|------|----------|
| `GCP_PROJECT_ID` | GCPプロジェクトID | Cloud Run環境変数 |
| `GCP_REGION` | GCPリージョン | Cloud Run環境変数 |
| `BQ_DATASET_RAW` | rawデータセット名 | Cloud Run環境変数 |
| `BQ_DATASET_FEATURES` | 特徴量データセット名 | Cloud Run環境変数 |
| `LOG_LEVEL` | ログレベル | Cloud Run環境変数 |

### 2. 機密情報（Secret Manager経由）

認証情報やAPIキーなど、ソースコードにコミットしてはいけない情報。

| シークレット名 | 環境変数名 | 説明 |
|---------------|-----------|------|
| `jrdb-user` | `JRDB_USER` | JRDB認証ユーザー名 |
| `jrdb-password` | `JRDB_PASSWORD` | JRDBパスワード |
| `sendgrid-api-key` | `SENDGRID_API_KEY` | SendGrid APIキー（将来用） |
| `line-notify-token` | `LINE_NOTIFY_TOKEN` | LINE Notifyトークン（将来用） |

## 環境別設定

### ローカル開発環境

`.env` ファイルを使用：

```bash
# .env
GCP_PROJECT_ID=keiba-prediction-452203
GCP_REGION=asia-northeast1
JRDB_USER=actual-user
JRDB_PASSWORD=actual-password
```

**注意**: `.env` ファイルは `.gitignore` に含まれており、リポジトリにはコミットされません。

### Cloud Run環境

1. **環境変数**: `cloud_run_config.yaml` で定義
2. **シークレット**: Secret Managerからマウント

```yaml
# cloud_run_config.yaml
set-env-vars:
  GCP_REGION: asia-northeast1
  BQ_DATASET_RAW: raw

set-secrets:
  JRDB_USER: jrdb-user:latest
  JRDB_PASSWORD: jrdb-password:latest
```

## Secret Managerの操作

### シークレットの作成

```bash
# 新規作成
echo -n 'secret-value' | gcloud secrets create SECRET_NAME \
    --replication-policy="automatic" \
    --data-file=-

# 例: JRDB認証情報
echo -n 'your-jrdb-user' | gcloud secrets create jrdb-user \
    --replication-policy="automatic" \
    --data-file=-
```

### シークレットの更新（新バージョン追加）

```bash
echo -n 'new-secret-value' | gcloud secrets versions add SECRET_NAME --data-file=-
```

### シークレットの確認

```bash
# シークレット一覧
gcloud secrets list

# バージョン一覧
gcloud secrets versions list SECRET_NAME

# 値の確認（注意: 機密情報が表示されます）
gcloud secrets versions access latest --secret=SECRET_NAME
```

### シークレットの削除

```bash
# バージョン削除
gcloud secrets versions destroy VERSION_ID --secret=SECRET_NAME

# シークレット削除
gcloud secrets delete SECRET_NAME
```

## Pythonコードでの使用

### ローカル環境（python-dotenv使用）

```python
from dotenv import load_dotenv
import os

# .envファイルを読み込み
load_dotenv()

project_id = os.environ.get('GCP_PROJECT_ID')
jrdb_user = os.environ.get('JRDB_USER')
```

### Cloud Run環境

Cloud RunではSecret Managerのシークレットが自動的に環境変数としてマウントされるため、
同じコードで動作します：

```python
import os

# Cloud RunではSecret Managerから自動マウント
project_id = os.environ.get('GCP_PROJECT_ID')
jrdb_user = os.environ.get('JRDB_USER')
```

### 環境検出

```python
import os

def is_cloud_run() -> bool:
    """Cloud Run環境かどうかを判定"""
    return os.environ.get('K_SERVICE') is not None

def get_config():
    """環境に応じた設定を取得"""
    if is_cloud_run():
        # Cloud Run環境
        return {
            'project_id': os.environ['GCP_PROJECT_ID'],
            'region': os.environ.get('GCP_REGION', 'asia-northeast1'),
        }
    else:
        # ローカル環境
        from dotenv import load_dotenv
        load_dotenv()
        return {
            'project_id': os.environ.get('GCP_PROJECT_ID'),
            'region': os.environ.get('GCP_REGION', 'asia-northeast1'),
        }
```

## セキュリティベストプラクティス

1. **機密情報は必ずSecret Managerを使用**
   - パスワード、APIキー、トークンは絶対にソースコードにコミットしない
   - `.env` ファイルも `.gitignore` に含める

2. **最小権限の原則**
   - サービスアカウントには必要最小限の権限のみ付与
   - `roles/secretmanager.secretAccessor` は必要なシークレットのみにバインド

3. **シークレットのローテーション**
   - 定期的にシークレットを更新
   - 古いバージョンは無効化

4. **監査ログの有効化**
   - Secret Managerへのアクセスログを有効化
   - 不正アクセスの検知
