# BigQueryデータセットとテーブルのセットアップ手順

このドキュメントでは、競馬予測MLシステムのためのBigQueryデータセットとテーブルのセットアップ手順を説明します。

## 前提条件

- GCPプロジェクトのセットアップが完了していること (Issue #3)
- `gcloud` CLIがインストールされ、認証済みであること
- Python 3.9以上がインストールされていること
- `.env` ファイルに `GCP_PROJECT_ID` が設定されていること

## セットアップ手順

### 1. 環境変数の確認

`.env` ファイルにGCPプロジェクトIDが設定されていることを確認します:

```bash
cat .env | grep GCP_PROJECT_ID
```

設定されていない場合は、`.env` ファイルを編集してください:

```bash
GCP_PROJECT_ID=your-actual-project-id
```

### 2. セットアップスクリプトの実行

自動セットアップスクリプトを実行します:

```bash
./scripts/setup_bigquery.sh
```

このスクリプトは以下の処理を実行します:

1. ✅ 必要なPythonパッケージのインストール
2. ✅ BigQueryデータセットの作成
   - `raw`: 生データテーブル
   - `features`: 特徴量テーブル
   - `predictions`: 予測結果テーブル
   - `backtests`: バックテスト結果テーブル
3. ✅ BigQueryテーブルの作成
   - `raw.race_info`: レース情報
   - `raw.horse_results`: 競走馬成績データ
   - `raw.pedigree`: 血統データ
   - `raw.odds`: オッズデータ
   - `features.training_data`: 特徴量テーブル

### 3. セットアップの確認

BigQueryコンソールで作成されたデータセットとテーブルを確認します:

```bash
# ブラウザでBigQueryコンソールを開く
open "https://console.cloud.google.com/bigquery?project=$GCP_PROJECT_ID"
```

または、`bq` コマンドラインツールで確認:

```bash
# データセット一覧を表示
bq ls

# 各データセットのテーブル一覧を表示
bq ls raw
bq ls features
bq ls predictions
bq ls backtests
```

## データセット構成

### raw (生データ)

JRDBから取得した生データを格納します。

| テーブル名 | 説明 | パーティション | クラスタリング |
|-----------|------|--------------|---------------|
| `race_info` | レース情報 (BAA: 番組データ) | race_date | venue_code, race_number |
| `horse_results` | 競走馬成績データ (KYF) | なし | horse_id, jockey_id, trainer_id |
| `pedigree` | 血統データ | なし | sire_id, dam_sire_id |
| `odds` | オッズデータ | odds_timestamp | race_id, horse_id, odds_type |

### features (特徴量)

機械学習用の特徴量データを格納します。

| テーブル名 | 説明 | パーティション | クラスタリング |
|-----------|------|--------------|---------------|
| `training_data` | 機械学習用特徴量 | race_date | venue_code, horse_id |

### predictions (予測結果)

モデルの予測結果を格納します。

### backtests (バックテスト結果)

バックテスト結果を格納します。

## テーブルスキーマ

各テーブルの詳細なスキーマは `config/` ディレクトリ内のJSONファイルを参照してください:

- `config/bq_schema_race_info.json`
- `config/bq_schema_horse_results.json`
- `config/bq_schema_pedigree.json`
- `config/bq_schema_odds.json`
- `config/bq_schema_training_data.json`

## パーティショニングとクラスタリング

### パーティショニング

パーティショニングにより、クエリのコストとパフォーマンスが最適化されます。

- **日付ベースのパーティション**: `race_date`, `odds_timestamp`
- **パーティション保持期間**: 無制限 (データ削除はライフサイクルポリシーで管理)

### クラスタリング

クラスタリングにより、特定のカラムでのフィルタリングが高速化されます。

- `race_info`: `venue_code`, `race_number`
- `horse_results`: `horse_id`, `jockey_id`, `trainer_id`
- `pedigree`: `sire_id`, `dam_sire_id`
- `odds`: `race_id`, `horse_id`, `odds_type`
- `training_data`: `venue_code`, `horse_id`

## トラブルシューティング

### エラー: "Permission denied"

BigQuery APIの権限が不足している可能性があります。サービスアカウントに適切な権限が付与されているか確認してください:

```bash
gcloud projects get-iam-policy $GCP_PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*keiba-prediction*"
```

### エラー: "Table already exists"

テーブルが既に存在する場合、スクリプトはスキップします。テーブルを再作成したい場合は、まず削除してください:

```bash
# テーブルを削除
bq rm -f -t $GCP_PROJECT_ID:raw.race_info

# スクリプトを再実行
./scripts/setup_bigquery.sh
```

### エラー: "ModuleNotFoundError: No module named 'google.cloud'"

Pythonパッケージがインストールされていません:

```bash
pip install -r requirements.txt
```

## Pythonスクリプトの直接実行

必要に応じて、Pythonスクリプトを直接実行することもできます:

```bash
# 環境変数を設定
export GCP_PROJECT_ID=your-project-id

# スクリプトを実行
python src/data/create_tables.py
```

## コスト管理

### クエリコストの確認

BigQueryのクエリコストは、スキャンされたデータ量に基づいて課金されます。

```bash
# 月間使用量を確認
bq ls --max_results=100
```

### コスト削減のベストプラクティス

1. **パーティションフィルタを使用**: クエリで `WHERE race_date = '2024-01-01'` のようにパーティションフィールドでフィルタリング
2. **SELECT *を避ける**: 必要なカラムのみを選択
3. **プレビュー機能を活用**: クエリを実行する前にプレビューでデータを確認

## 次のステップ

BigQueryのセットアップが完了したら、次のIssueに進んでください:

- **Issue #5**: GCS→BigQuery自動ロードCloud Functionsの実装
- **Issue #6**: ローカル→GCS自動アップロードスクリプトの実装

## 参考リンク

- [BigQuery公式ドキュメント](https://cloud.google.com/bigquery/docs)
- [パーティション分割テーブル](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [クラスタ化テーブル](https://cloud.google.com/bigquery/docs/clustered-tables)
- [BigQuery料金](https://cloud.google.com/bigquery/pricing)
