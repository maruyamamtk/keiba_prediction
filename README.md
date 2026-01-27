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
│   └── README.md                   # ダウンローダーの詳細ドキュメント
├── src/                     # Pythonソースコード
│   └── data/                # データパイプライン
│       ├── create_tables.py       # BigQueryテーブル作成
│       ├── upload_to_gcs.py       # GCSアップロード
│       ├── quality_check.py       # データ品質チェック
│       └── validation_rules.py    # バリデーションルール定義
├── tests/                   # テストコード
│   ├── test_upload_to_gcs.py      # GCSアップロードのテスト
│   └── test_quality_check.py      # 品質チェックのテスト (27テストケース)
├── cloud_functions/         # Cloud Functions
│   └── gcs_to_bq/           # GCS→BigQuery自動ロード
│       ├── main.py          # Cloud Function エントリーポイント
│       ├── parser.py        # JRDBデータパーサー
│       └── requirements.txt
├── config/                  # 設定ファイル
│   ├── bq_schema_race_info.json     # race_infoテーブルスキーマ
│   ├── bq_schema_horse_results.json # horse_resultsテーブルスキーマ
│   ├── bq_schema_race_results.json  # race_resultsテーブルスキーマ
│   └── ...                          # その他のスキーマファイル
├── reports/                 # 品質チェックレポート出力先
├── CLAUDE.md                # システム仕様書
├── SCHEMA.md                # JRDBデータスキーマ仕様書
├── ML_FEATURE.md            # 特徴量設計ドキュメント
└── README.md                # このファイル
```

## データパイプライン

### 対応データタイプ

GCS→BigQuery自動ロード機能が対応しているJRDBデータタイプの一覧です。

| データタイプ | 説明 | BigQueryテーブル |
|-------------|------|-----------------|
| BAA | 番組データ (レース基本情報) | `raw.race_info` |
| BAB | 番組データ (詳細) | `raw.race_info` |
| BAC | 番組データ (追加情報) | `raw.race_info` |
| KYF | 競走馬データ (出走馬情報・指数) | `raw.horse_results` |
| KYG | 競走馬データ (詳細) | `raw.horse_results` |
| KYH | 競走馬データ (追加情報) | `raw.horse_results` |
| SEC | 成績データ (レース結果) | `raw.race_results` |

### データフロー

```
[ローカル]
  ↓ download_all_from_date.sh
[ローカル] downloaded_files/
  ↓ python -m src.data.upload_to_gcs
[GCS] gs://${PROJECT_ID}-keiba-raw-data/
  ↓ Cloud Functions (自動トリガー)
[BigQuery]
  ├── raw.race_info        ← BAA/BAB/BAC (レース情報)
  ├── raw.horse_results    ← KYF/KYG/KYH (出馬表・予測指数)
  └── raw.race_results     ← SEC (レース成績・結果)
  ↓ python -m src.data.quality_check
[レポート] reports/quality_report_*.json
```

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

### BigQueryテーブル作成 (`src/data/create_tables.py`)

BigQueryのデータセットとテーブルを作成します。

```bash
python -m src.data.create_tables
```

**作成されるリソース:**
- データセット: `raw`, `features`, `predictions`, `backtests`
- テーブル: `race_info`, `horse_results`, `pedigree`, `odds`, `training_data`

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
- [x] GCSアップロードスクリプト (`src/data/upload_to_gcs.py`)
- [x] GCS→BigQuery自動ロード (`cloud_functions/gcs_to_bq/`)
- [x] データ品質チェックスクリプト (`src/data/quality_check.py`)

### Phase 2: 特徴量エンジニアリング 🚧

- [ ] 過去走集計特徴量
- [ ] Target Encoding実装
- [ ] 特徴量パイプライン

### Phase 3: モデル開発

- [ ] LightGBM ランク学習
- [ ] 時系列クロスバリデーション
- [ ] バックテスト

### Phase 4: 運用システム構築

- [ ] 予測パイプライン
- [ ] Webダッシュボード
- [ ] 通知システム

## ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)
