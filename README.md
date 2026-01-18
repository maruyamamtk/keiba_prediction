# 競馬予測MLシステム

競馬の馬券購入を支援する機械学習システム

## 📋 概要

このプロジェクトは、JRDBの競馬データを活用し、機械学習による馬券購入支援システムを構築します。

- **対象馬券**: 単勝・複勝
- **予測内容**: 3着以内に入る確率
- **目標**: 回収率100%以上
- **技術**: Python, LightGBM, GCP (BigQuery, Cloud Run)

詳細な仕様は [CLAUDE.md](./CLAUDE.md) を参照してください。

## 🚀 クイックスタート

### 1. データダウンロード

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

### 2. GCPセットアップ（準備中）

```bash
# GCPプロジェクトの作成
gcloud projects create keiba-prediction

# BigQueryデータセット作成
bq mk --dataset keiba-prediction:raw
bq mk --dataset keiba-prediction:features
```

## 📁 プロジェクト構成

```
keiba_prediction/
├── downloader/              # データダウンロードスクリプト
│   ├── download_from_date.sh       # 指定日付以降のファイルをダウンロード
│   ├── download_all_from_date.sh   # 全データタイプ一括ダウンロード
│   └── README.md                    # ダウンローダーの詳細ドキュメント
├── CLAUDE.md                # システム仕様書
├── ML_FEATURE.md            # 特徴量設計ドキュメント
└── README.md                # このファイル
```

## 🤖 Claude Code GitHub Action

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

## 📖 ドキュメント

- [CLAUDE.md](./CLAUDE.md) - システム全体の仕様書
  - アーキテクチャ
  - データパイプライン
  - モデル設計
  - 運用フロー
  - 実装計画

- [ML_FEATURE.md](./ML_FEATURE.md) - 特徴量設計
  - 特徴量の詳細リスト
  - Target Encoding設計
  - リーク対策

- [downloader/README.md](./downloader/README.md) - データダウンローダー
  - 使用方法
  - スクリプト一覧
  - データ形式

## 🛠 技術スタック

- **言語**: Python 3.9+
- **機械学習**: LightGBM (Learning to Rank)
- **クラウド**: GCP (BigQuery, Cloud Storage, Cloud Run)
- **通知**: SendGrid, LINE Notify
- **可視化**: Streamlit

## 📅 実装計画

- [x] Phase 1: データダウンロード基盤
- [ ] Phase 2: データパイプライン (GCS + BigQuery)
- [ ] Phase 3: 特徴量エンジニアリング
- [ ] Phase 4: モデル開発
- [ ] Phase 5: 運用システム構築

## 📝 ライセンス

このプロジェクトは個人用です。JRDBデータの利用はJRDBの利用規約に従ってください。

## 🔗 関連リンク

- [JRDB公式サイト](http://www.jrdb.com/)
- [LightGBM Documentation](https://lightgbm.readthedocs.io/)
- [Claude Code](https://claude.ai/code)
