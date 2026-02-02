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
python -m src.data.create_tables
```

### 4. データアップロード

```bash
# GCSへのアップロード
python -m src.data.upload_to_gcs

# データ品質チェック
python -m src.data.quality_check
```

## プロジェクト構成

```
keiba_prediction/
├── downloader/              # データダウンロードスクリプト
│   ├── download_from_date.sh       # 指定日付以降のファイルをダウンロード
│   ├── download_all_from_date.sh   # 全データタイプ一括ダウンロード
│   └── README.md
├── src/
│   ├── data/                # データパイプライン
│   │   ├── jrdb_downloader.py     # JRDBダウンローダー
│   │   ├── upload_to_gcs.py       # GCSアップロード
│   │   ├── pipeline.py            # パイプライン統合（ダウンロード→アップロード）
│   │   ├── create_tables.py       # BigQueryテーブル作成
│   │   ├── quality_check.py       # データ品質チェック
│   │   └── validation_rules.py    # バリデーションルール定義
│   └── features/            # 特徴量エンジニアリング
│       ├── feature_pipeline.py    # 特徴量パイプライン
│       ├── past_performance.py    # 過去走特徴量
│       └── condition_features.py  # 条件適性特徴量
├── main.py                  # Cloud Runエントリーポイント
├── Dockerfile               # Dockerイメージ定義
├── scripts/                 # ユーティリティスクリプト
│   └── reload_gcs_to_bq.py        # 既存GCSファイルの再ロード
├── tests/                   # テストコード
├── cloud_functions/         # Cloud Functions
│   └── gcs_to_bq/           # GCS→BigQuery自動ロード
├── notebooks/               # Jupyter Notebook (EDA用)
├── config/                  # BigQueryスキーマ定義
├── reports/                 # 品質チェックレポート出力先
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
│    $ python -m src.data.jrdb_downloader --start-date 240101             │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. GCSアップロード (ローカル → GCS)                                      │
│    $ python -m src.data.upload_to_gcs                                   │
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
│    $ python -m src.features.feature_pipeline --start-date ... --end-date│
└─────────────────────────────────────────────────────────────────────────┘
```

#### 将来計画（Cloud Run Functions での自動化）

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Cloud Scheduler (毎日AM 6:00)                                            │
│   ↓ HTTPリクエスト                                                       │
│ Cloud Run: /run エンドポイント                                           │
│   ├─ Step 1+2: JRDBダウンロード → GCSアップロード                        │
│   │            (一時ディレクトリを使用、完了後自動削除)                    │
│   ├─ Step 3: BigQueryロード (Cloud Functionが自動トリガー)               │
│   └─ Step 4: 特徴量生成 (BigQuery raw → features)                       │
└─────────────────────────────────────────────────────────────────────────┘
```

**自動化のメリット:**
- 人手不要の完全自動運用
- 一時ディレクトリによるディスク容量の節約
- エラー時の自動リトライ
- Cloud Loggingでの一元的なログ管理

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
python -m src.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python -m src.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python -m src.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
```

#### Step 2: GCSアップロード

```bash
# 全データをアップロード（差分のみ）
python -m src.data.upload_to_gcs

# 特定タイプのみアップロード
python -m src.data.upload_to_gcs --data-type Sec

# ドライラン（実際にはアップロードしない）
python -m src.data.upload_to_gcs --dry-run
```

#### Step 1+2 統合: ダウンロード→アップロード

```bash
# ダウンロード→アップロードを一括実行
python -m src.data.pipeline --start-date 240101

# 特定のデータタイプのみ
python -m src.data.pipeline --start-date 240101 --datatype BAA

# 既存のdownloaded_filesを使用（一時ディレクトリを使わない）
python -m src.data.pipeline --start-date 240101 --no-temp-dir
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

#### Step 4: 特徴量生成

```bash
# 指定期間の特徴量を生成
python -m src.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-01-06

# 詳細ログ付きで実行
python -m src.features.feature_pipeline --start-date 2024-01-06 --end-date 2024-12-31 -v
```

#### Step 5: データ品質チェック

```bash
# 全テーブルのチェック
python -m src.data.quality_check

# 特定テーブルのみチェック
python -m src.data.quality_check --table raw.race_info
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

### データ品質チェック (`src/data/quality_check.py`)

BigQueryにロードされたデータの品質を自動チェックします。

```bash
# 全テーブルのチェック
python -m src.data.quality_check

# 特定テーブルのみチェック
python -m src.data.quality_check --table raw.race_info

# レポート出力先を指定
python -m src.data.quality_check --output reports/my_report.json

# アラートを無効化
python -m src.data.quality_check --no-alert
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
python -m src.data.jrdb_downloader --start-date 240101

# 特定のデータタイプのみ
python -m src.data.jrdb_downloader --start-date 240101 --datatype BAA

# 出力先を指定
python -m src.data.jrdb_downloader --start-date 240101 --output-dir /path/to/dir
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
python -m src.data.upload_to_gcs

# 特定のデータタイプのみアップロード
python -m src.data.upload_to_gcs --data-type Baa

# ドライラン（実際にはアップロードしない）
python -m src.data.upload_to_gcs --dry-run

# 強制アップロード（差分チェックをスキップ）
python -m src.data.upload_to_gcs --force
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
python -m src.data.pipeline --start-date 240101

# 特定のデータタイプのみ
python -m src.data.pipeline --start-date 240101 --datatype BAA
```

**特徴:**
- 一時ディレクトリの自動管理（Cloud Run環境向け）
- エラーハンドリングとクリーンアップ
- 統合されたログ出力

### BigQueryテーブル作成 (`src/data/create_tables.py`)

BigQueryのデータセットとテーブルを作成します。

```bash
python -m src.data.create_tables
```

**作成されるリソース:**
- データセット: `raw`, `features`, `predictions`, `backtests`
- テーブル: `race_info`, `horse_results`, `race_results`, `training_data` など

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
