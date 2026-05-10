# 競馬予測MLシステム 設計仕様書

このドキュメントは、競馬予測MLシステムの設計思想、目的、および実装方針を記載します。
**機能の実行手順については [README.md](./README.md) を参照してください。**

---

## 1. プロジェクト概要

### 1.1 目的
競馬の馬券購入を支援する機械学習システムを構築し、回収率100%以上を目指す。

### 1.2 対象馬券
- **主対象**: 単勝・複勝
- **予測内容**: 各馬の3着以内に入る確率を予測
- **購入判断**: 予測確率とオッズを考慮し、期待回収率が高い馬券を選定

### 1.3 目標指標
- **回収率**: 100%以上
- **評価指標**: NDCG@3, Recall@3
- **バックテスト期間**: 最低6ヶ月以上

---

## 2. システムアーキテクチャ

### 2.1 実装状況

現在のシステムは以下の3つのレイヤーで構成されています。

#### Data Layer ✅ 実装済み
- Cloud Run（FastAPI）による日次/全件データパイプライン
- JRDBダウンロード → GCSアップロード → BigQueryロード（MERGE UPSERT）
- ロード履歴管理による重複スキップ

#### Feature Engineering Layer ✅ 実装済み
- BigQuery SQL駆動方式（feature_query_raw.sql）
- 5段階CTE: ベース → 過去走 → 集計 → 馬マスター → 差分指標
- 出力: features.training_data（257カラム、466,265行）
- Cloud Run APIエンドポイント（同期/非同期）

#### Model Training Layer ✅ 実装済み
- LightGBM LambdaRank（ランク学習）
- 二値ラベル化（3着以内=1, それ以外=0）による複勝券予測
- Optunaベイズ最適化によるハイパーパラメータチューニング
- 時系列分割（学習/検証/推論）
- 評価指標: NDCG@3, Recall@3, AUC
- モデルファイルのGCS保存・読み込み

#### Backtest Layer ✅ 実装済み
- 期待回収率フィルタ（win_place_prob × odds > threshold）
- 評価指標: 回収率・的中率・最大ドローダウン・シャープレシオ
- BigQuery（features.training_data, raw.race_results, raw.payouts）からの期間指定データ取得
- 結果のCSV保存・BigQuery保存・グラフ出力
- `select_bets_for_race()` を使用（`budget_per_race` 固定・オッズ逆数比率配分・複勝/ワイド/三連複/馬連）← **本番と同じロジック**

#### Prediction & Operation Layer 🔧 一部実装済み
- 日次予測パイプライン（Cloud Schedulerからの自動推論・BQ/GCS保存） ✅ Issue #117
  - `POST /api/v1/predict/daily`: `model_path` 未指定時はGCSから最新モデルを自動取得
  - `predictions.daily_predictions` テーブルへのUPSERT保存
  - GCS保存（`gs://{project}-keiba-predictions/{date}/predictions.csv`）
- netkeibaリアルタイムオッズスクレイパー ✅ Issue #131
  - `POST /api/v1/odds/scrape`: Playwright (Chromium) で当日全レースの単複オッズ取得
  - `predictions.daily_odds` テーブルへのUPSERT保存（race_id + horse_numberキー）
  - `src/automation/data/netkeiba_scraper.py` に実装
- 通知システム（LINE Messaging API） ✅ 実装済み Issue #25
  - `POST /api/v1/line/webhook`: HMAC-SHA256署名検証 + 「日付 競馬場名 レース番号」形式のメッセージ解析
  - メッセージ1: 予測テーブル（予測順・馬番・馬名・スコア・複勝率・オッズ・期待値）
  - メッセージ2: 推奨馬券リスト（馬券種・馬番・馬名・オッズ・賭け金・合計投資額）
  - `src/automation/api/line_webhook.py` + `src/utils/line_notify.py` に実装
- Webダッシュボード（Streamlit） ✅ 実装済み Issue #24

詳細なアーキテクチャ図と実行手順は [README.md](./README.md) を参照してください。

### 2.2 GCPリソース構成（設計）

#### 2.2.1 Cloud Storage (GCS)
- `gs://${PROJECT_ID}-keiba-raw-data/`: JRDBダウンロード生データ
- `gs://${PROJECT_ID}-keiba-models/`: 学習済みモデル
- `gs://${PROJECT_ID}-keiba-predictions/`: 予測結果

#### 2.2.2 BigQuery
- `raw`: 生データテーブル（実装済み）
- `features`: 特徴量テーブル（実装済み）
- `predictions`: 予測結果テーブル（実装済み）。`predictions.daily_predictions` で日次予測を保存、`predictions.daily_odds` でリアルタイムオッズを保存。
- `backtests`: バックテスト結果テーブル（実装済み）

詳細なテーブル構成は [README.md](./README.md#bigqueryテーブル構成) を参照してください。

#### 2.2.3 Cloud Run
- `keiba-pipeline`: データパイプライン・特徴量生成
- `dashboard-service`: Webダッシュボード

APIエンドポイントの詳細は [README.md](./README.md#apiエンドポイント) を参照してください。

#### 2.2.4 Cloud Scheduler
稼働中ジョブの詳細は [SCHEDULE.md](./SCHEDULE.md) を参照してください。

---

## 3. データ管理方針

### 3.1 データ取得フロー

JRDB → GCS → BigQuery → features.training_data の流れでデータを取得・加工します。

**実装済みの詳細は [README.md](./README.md#データパイプライン) を参照してください。**

### 3.2 BigQueryテーブル構成

詳細なテーブル構成は [README.md](./README.md#bigqueryテーブル構成) を参照してください。
スキーマ仕様は [SCHEMA.md](./SCHEMA.md) を参照してください。

---

## 4. 特徴量エンジニアリング

### 4.1 実装方針

特徴量生成はBigQuery SQL駆動方式を採用しています。
- SQL（`src/ml/features/feature_query_raw.sql`）で一括処理
- 5段階CTE: ベース → 過去走 → 集計 → 馬マスター → 差分指標
- 出力: features.training_data（257カラム、466,265行）

**実装済みの詳細は [README.md](./README.md) および [ML_FEATURE.md](./ML_FEATURE.md) を参照してください。**

### 4.2 リーク対策チェックリスト
- [ ] 発走後に確定する情報を使用していない（確定オッズ、確定馬体重など）
- [ ] 条件別成績は当該レースを除外して計算
- [ ] 同一レース内の情報漏洩がない
- [ ] 時系列分割でバックテスト実施

---

## 5. モデル設計

### 5.1 LightGBM ランク学習 ✅ 実装済み

#### 5.1.1 モデル概要
- **アルゴリズム**: LightGBM LambdaRank
- **目的関数**: `lambdarank`
- **評価指標**: `ndcg@3`, `auc`
- **グループ単位**: レースID
- **ラベル形式**: 二値ラベル（3着以内=1, それ以下=0）

**重要な設計変更（Issue #85）:**
**二値ラベル（3着以内=1, それ以外=0）**を採用。これにより、複勝券予測に特化したモデルとなり、評価にAUC指標を追加しています。

#### 5.1.2 実装済みの内容
- `src/models/lgbm_ranker.py`: モデルクラス（学習・予測・保存・読み込み）
- `src/models/train.py`: 学習パイプライン（時系列分割、評価、GCS保存）
- `src/models/predict.py`: 推論パイプライン（今週末レース予測）
- `src/models/tuning.py`: Optunaハイパーパラメータチューニング（Issue #86）
- `config/model_config.yaml`: 設定ファイル（パラメータ、チューニング範囲）

詳細は [src/models/README.md](./src/models/README.md) を参照してください。

### 5.2 モデル評価
1. **NDCG@3**: ランキングの質を評価
2. **Recall@3**: 上位3頭の中に複勝圏内の馬が含まれる割合
3. **AUC**: 3着以内予測の二値分類性能（ROC-AUC）
4. **回収率**: 実際の投資に基づく評価
5. **的中率**: 3着以内予測の精度


## 6. バックテスト設計 ✅ 実装済み

### 6.0 バックテスト概要
- **目的**: 過去データで実際の投資をシミュレーション
- **期間**: 最低6ヶ月以上
- **評価**: 回収率、的中率、最大ドローダウン

### 6.1 実装済みファイル
- `src/backtest/__init__.py`: モジュール初期化
- `src/backtest/simulator.py`: Kelly基準・BacktestSimulatorクラス
- `src/backtest/metrics.py`: 評価指標（回収率・的中率・最大ドローダウン・シャープレシオ）
- `src/backtest/strategy.py`: 投資戦略（Kelly基準・期待回収率フィルタ）
- `src/backtest/strategy_optimizer.py`: 戦略パラメータ最適化
- `scripts/run_backtest.py`: CLIバックテスト実行スクリプト
- `tests/test_backtest_simulator.py`: シミュレーターテスト（24件）
- `tests/test_backtest_metrics.py`: 指標テスト（24件）

**実行手順は [README.md](./README.md#6-バックテスト) を参照してください。**

### 6.2 投資戦略

#### 6.2.1 投資ルール
1. **閾値設定**: 予測確率 > 閾値 の馬のみ購入
2. **期待値フィルタ**: 期待回収率 = 予測確率 × オッズ > 1.2 の馬のみ
3. **賭け金配分**: オッズ逆数比率方式（1レース合計 = `budget_per_race` 固定）
4. **1レースあたり固定予算**: 3000円（`budget_per_race`）

---

## 7. 運用設計

### 7.1 実装済み機能

日次データパイプライン、Cloud Runデプロイ、APIエンドポイントの詳細は [README.md](./README.md) および [infrastructure/README.md](./infrastructure/README.md) を参照してください。

### 7.2 予測・オッズ取得パイプライン設計

#### 7.2.1 実行タイミング
- **当日AM 8:00**: 推論実行 → `predictions.daily_predictions` 保存
- **当日AM 8:15**: netkeibaオッズ取得 → `predictions.daily_odds` 保存
- **当日AM 8:30**: 投資戦略策定（dry_run=true、BQ保存なし）
- **土日 8:00〜17:55 の5分おき**: 発走5分前レースの投資判断を最新オッズで上書き → IPAT購入

#### 7.2.2 実装方針
- Cloud Schedulerから Cloud Run HTTPエンドポイントをトリガー
- 予測結果を `predictions.daily_predictions` に保存
- リアルタイムオッズを `predictions.daily_odds` に保存
- 発走直前に `_refresh_investment_decisions_for_race()` で最新オッズで投資判断を再計算しBQ保存
- 詳細は [SCHEDULE.md](./SCHEDULE.md) を参照

---

## 8. 通知システム

### 8.1 LINE通知（✅ 実装済み - Issue #25）

LINE Messaging APIを使った双方向Botを実装済み。

**実装済み機能:**
- **オンデマンド問い合わせ**: ユーザーがLINEで「日付 競馬場名 レース番号」形式のメッセージ送信 → Webhookで受信 → 予測テーブルと推奨馬券リストを返信

**実装ファイル:**
- `src/automation/api/line_webhook.py`: LINE Webhookハンドラ
  - `verify_line_signature()`: HMAC-SHA256署名検証
  - `parse_race_query()`: 「日付 競馬場名 レース番号」形式のメッセージ解析
  - `handle_race_query()`: BQデータ取得 + 2通の返答メッセージ生成
  - `process_webhook_events()`: Webhookイベント処理
- `src/utils/__init__.py`: utilsパッケージ初期化
- `src/utils/line_notify.py`: LINE リプライ/プッシュ通知ヘルパー
  - `reply_messages()`: リプライ API
  - `push_messages()`: プッシュ API
  - `text_message()`: テキストメッセージオブジェクト生成
- `tests/test_line_webhook.py`: ユニットテスト35件

**必要な環境変数:**

| 変数名 | 説明 |
|--------|------|
| `LINE_CHANNEL_SECRET` | Webhook署名検証用シークレット |
| `LINE_CHANNEL_ACCESS_TOKEN` | リプライ送信用アクセストークン |

**APIエンドポイント:**
- Webhook受信: `POST /api/v1/line/webhook`（Cloud Runに実装済み）
- `predictions.daily_predictions` と `predictions.daily_odds` を参照し投資戦略を計算
- `src/backtest/strategy.py` の `select_bets_for_race()` を流用

---

## 9. リスク管理

### 11.1 技術的リスク

| リスク | 対策 |
|--------|------|
| データ品質問題 | データ検証パイプライン、異常値検知 |
| モデル過学習 | 時系列CV、正則化、Early Stopping |
| リーク | チェックリスト、レビュー体制 |
| 予測遅延 | タイムアウト設定、非同期処理 |

### 11.2 運用リスク

| リスク | 対策 |
|--------|------|
| GCPコスト超過 | 予算アラート、クエリ最適化 |
| モデル性能劣化 | 継続的モニタリング、週次再学習 |
| データ取得失敗 | リトライ処理、アラート通知 |

### 11.3 投資リスク

| リスク | 対策 |
|--------|------|
| 連続損失 | 1日あたり投資上限、Fractional Kelly |
| 過度な賭け | Kelly基準、1レースあたり上限 |
| 予測精度低下 | バックテスト継続、閾値調整 |

---

## 10. モニタリング・改善

### 12.1 KPI

| 指標 | 目標 | 測定頻度 |
|------|------|----------|
| 回収率 | 100%以上 | 週次 |
| 的中率 | 30%以上 | 週次 |
| NDCG@3 | 0.7以上 | 月次 |
| Recall@3 | 0.8以上 | 月次 |
| AUC | 0.7以上 | 月次 |

### 12.2 改善サイクル

1. **日次**: 予測結果と実績の比較
2. **週次**: パフォーマンスレビュー、閾値調整
3. **月次**: モデル再学習、特徴量追加検討
4. **四半期**: システム全体の見直し

---

## 11. 参考資料

### 13.1 プロジェクトドキュメント
- [README.md](./README.md): 実装済み機能の実行手順、APIエンドポイント、テーブル構成
- [ML_FEATURE.md](./ML_FEATURE.md): 特徴量設計詳細、リーク対策
- [SCHEMA.md](./SCHEMA.md): JRDBデータスキーマ詳細
- [infrastructure/README.md](./infrastructure/README.md): インフラセットアップ完全ガイド

### 13.2 外部リソース
- [JRDB公式](http://www.jrdb.com/): JRDBデータ提供元
- [LightGBM Documentation](https://lightgbm.readthedocs.io/): LightGBMドキュメント
- [GCP Documentation](https://cloud.google.com/docs): Google Cloud Platform ドキュメント

---

---
## 実装時の注意事項
### 環境情報
- 開発環境: Apple Silicon Mac (Dockerビルド時は必ず `--platform linux/amd64` を指定)
- 言語/ツール: Python 3.13, pip3 を使用
- シェル設定: 改行コードは LF を使用 (CRLFは避ける)
- プロジェクト概要: JRDB競馬データパイプライン (BigQuery, GCS, Cloud Run)

### Python実行環境（重要）
- システムのpython3は3.9のため、**必ず `.venv` の Python/pytest を使用**すること
- テスト実行: `.venv/bin/pytest tests/`
- Python実行: `.venv/bin/python`
- スクリプト実行: `.venv/bin/python scripts/xxx.py`
- `python3` や `pytest` を直接呼び出さない（システムの3.9が使われてしまう）

### 開発ワークフロー
GitHub Issueの実装時は以下の手順を遵守してください:
1. ブランチ作成 (feature/issue-XX)
  - この手順については全ての実装時に必ず守ること
2. 実装 & テスト作成
3. テスト実行と修正
4. **特徴量・モデル変更を含む場合は必ずモデル精度比較を実施**（PR作成前）
  - `scripts/compare_features.py` を実行して新旧モデルの学習・検証期間と精度を比較
  - 比較結果（学習期間・検証期間・NDCG@3・AUC・Recall@3）をPR説明文に記載
  - コマンド例:
    ```bash
    .venv/bin/python scripts/compare_features.py \
        --project-id <PROJECT_ID> \
        --skip-feature-pipeline \
        --n-trials 50 --timeout 1800
    ```
5. PR作成 & 自己レビュー
6. マージ

### テストと検証
- ファイルの移動または名前変更後は、すべてのテストファイルでモックターゲットパスとインポートパスを必ず更新すること
- BigQueryスキーマまたは列参照を変更した後は、実際のテーブルスキーマに列名が存在することを使用前に確認すること
- パーサーフィールド位置を変更した後は、サンプルデータでテスト解析を実行し、固定長TXTスキーマとの整合性を確認すること

### 投資戦略の重要な設計メモ
- **min_prob_threshold**: 複勝単体買いだけでなく、ワイド・三連複の top_n 候補馬にも適用（PR #245）
  - `select_base_bets` 内で `sorted_df[prob * N/18 >= min_prob_threshold].head(top_n)` で候補馬を絞る
  - これにより低人気馬（低確率馬）がいかなる馬券にも混入しない
- **save_decisions_to_bq**: `target_date` 引数で2つの保存モードを持つ（PR #246）
  - `target_date` 指定（日次全件置換）: `race_date = date` で全削除 → INSERT
  - `target_date` 省略（1レース更新）: `race_id` で削除 → MERGE（`_refresh_investment_decisions_for_race` 向け）
  - 過去日付の再実行時は `--target-date YYYY-MM-DD` を指定すること（ベットなしになったレースの旧データも確実に消える）

### デプロイ時のチェックリスト
Cloud Run または Cloud Function のデプロイメント完了を宣言する前に：
1. 必要なすべての API が有効化されていることを確認する（`gcloud services enable ...`）
2. IAM 権限が付与されていることを確認する
3. リソース名（例：バケットパス）に重複するプロジェクト ID がないか確認する
4. デプロイに使用するスクリプト（deploy.sh）をクリーンな状態でエンドツーエンドでテストする

### レートリミット(Rate limit)への配慮
大規模なタスクに取り組む際は、探索よりも実装を優先してください。
複数のGitHubイシューを扱うタスクでは、順次実装し、各イシュー完了後にコミットして進捗を記録しましょう。これにより、使用制限に達した場合でも作業が失われることを防げます。

---

**End of Document**
