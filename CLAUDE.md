# 競馬予測MLシステム 設計仕様書

このドキュメントは、競馬予測MLシステムの設計思想、目的、および未実装機能の仕様を記載します。
**実装済みの機能の実行手順については [README.md](./README.md) を参照してください。**

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
- Fractional Kelly基準による賭け金計算
- 期待回収率フィルタ（win_place_prob × odds > threshold）
- 評価指標: 回収率・的中率・最大ドローダウン・シャープレシオ
- BigQuery（features.training_data, raw.race_results, raw.payouts）からの期間指定データ取得
- 結果のCSV保存・BigQuery保存・グラフ出力

#### Prediction & Operation Layer 🔧 一部実装済み
- 日次予測パイプライン（Cloud Schedulerからの自動推論・BQ/GCS保存） ✅ Issue #117
  - `POST /api/v1/predict/daily`: `model_path` 未指定時はGCSから最新モデルを自動取得
  - `predictions.daily_predictions` テーブルへのUPSERT保存
  - GCS保存（`gs://{project}-keiba-predictions/{date}/predictions.csv`）
- Webダッシュボード（Streamlit） ⬜ 未実装
- 通知システム（メール/LINE） ⬜ 未実装

詳細なアーキテクチャ図と実行手順は [README.md](./README.md) を参照してください。

### 2.2 GCPリソース構成（設計）

#### 2.2.1 Cloud Storage (GCS)
- `gs://${PROJECT_ID}-keiba-raw-data/`: JRDBダウンロード生データ（実装済み）
- `gs://${PROJECT_ID}-keiba-processed-data/`: 加工済みデータ（未実装）
- `gs://${PROJECT_ID}-keiba-models/`: 学習済みモデル（実装済み）
- `gs://${PROJECT_ID}-keiba-predictions/`: 予測結果（実装済み・Issue #117）

#### 2.2.2 BigQuery
- `raw`: 生データテーブル（実装済み）
- `features`: 特徴量テーブル（実装済み）
- `predictions`: 予測結果テーブル（実装済み・Issue #117）。`predictions.daily_predictions` で日次予測を保存。
- `backtests`: バックテスト結果テーブル（実装済み）

詳細なテーブル構成は [README.md](./README.md#bigqueryテーブル構成) を参照してください。

#### 2.2.3 Cloud Run
- `keiba-pipeline`: データパイプライン・特徴量生成（実装済み）
- `dashboard-service`: Webダッシュボード（未実装）

APIエンドポイントの詳細は [README.md](./README.md#apiエンドポイント) を参照してください。

#### 2.2.4 Cloud Scheduler
- `daily-data-load`: 毎日AM 6:00 データロード（設定手順は実装済み、ジョブ作成は要実施）
- `race-day-predict`: 当日AM 8:00 推論実行（APIエンドポイント実装済み・Issue #117。Schedulerジョブ作成は要実施）
- `race-day-strategy`: 当日AM 8:30 投資戦略策定（未実装）
- `race-day-notify`: 当日AM 9:00 LINE通知（未実装）

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
- `scripts/run_backtest.py`: CLIバックテスト実行スクリプト
- `tests/test_backtest_simulator.py`: シミュレーターテスト（24件）
- `tests/test_backtest_metrics.py`: 指標テスト（24件）

**実行手順は [README.md](./README.md#6-バックテスト) を参照してください。**

### 6.2 投資戦略

#### 6.2.1 Kelly基準ベース
```python
def kelly_criterion(win_prob, odds):
    """
    Kelly基準: 最適賭け金比率
    f* = (p * (odds - 1) - (1 - p)) / (odds - 1)
    """
    if odds <= 1:
        return 0
    kelly = (win_prob * (odds - 1) - (1 - win_prob)) / (odds - 1)
    return max(0, kelly)  # 負の値は賭けない

def fractional_kelly(win_prob, odds, fraction=0.25):
    """
    Fractional Kelly: リスク調整
    """
    return kelly_criterion(win_prob, odds) * fraction
```

#### 6.2.2 投資ルール
1. **閾値設定**: 予測確率 > 閾値 の馬のみ購入
2. **期待値フィルタ**: 期待回収率 = 予測確率 × オッズ > 1.2 の馬のみ
3. **賭け金配分**: Fractional Kelly (25%)
4. **1レースあたり上限**: 総資金の5%まで

---

## 7. 運用設計

### 7.1 実装済み機能

日次データパイプライン、Cloud Runデプロイ、APIエンドポイントの詳細は [README.md](./README.md) および [infrastructure/README.md](./infrastructure/README.md) を参照してください。

### 7.2 予測パイプライン設計（未実装）

#### 7.2.1 実行タイミング
- **前日PM 9:00**: 特徴量生成 → 予測実行 → 通知
- **当日AM 8:00**: オッズ更新 → 予測再実行 → 通知

#### 7.2.2 実装方針
- Cloud Schedulerから Cloud Run HTTPエンドポイントをトリガー
- 予測結果を `predictions` データセットに保存
- 期待回収率上位の馬券を抽出し通知

---

## 8. Webダッシュボード設計（未実装）

### 8.1 機能要件

#### 8.1.1 画面構成
1. **ホーム**: 当日・翌日のおすすめ馬券
2. **レース一覧**: 全レースの予測結果
3. **レース詳細**: 各馬の予測確率、オッズ、期待値
4. **バックテスト**: 過去の成績、回収率推移
5. **モデル情報**: 特徴量重要度、モデル性能

#### 8.1.2 実装 (Streamlit)

```python
# dashboard.py

import streamlit as st
import pandas as pd

def main():
    st.title("競馬予測ダッシュボード")

    # サイドバー
    menu = st.sidebar.selectbox(
        "メニュー",
        ["ホーム", "レース一覧", "バックテスト", "モデル情報"]
    )

    if menu == "ホーム":
        show_home()
    elif menu == "レース一覧":
        show_race_list()
    elif menu == "バックテスト":
        show_backtest()
    elif menu == "モデル情報":
        show_model_info()

def show_home():
    st.header("本日のおすすめ馬券")

    # BigQueryから予測結果取得
    predictions = fetch_today_predictions()

    # TOP3表示
    top_bets = predictions.nlargest(3, 'expected_return')

    for idx, row in top_bets.iterrows():
        with st.expander(f"{row['venue']} {row['race_number']}R - {row['horse_name']}"):
            col1, col2, col3 = st.columns(3)
            col1.metric("予測確率", f"{row['pred_prob']:.1%}")
            col2.metric("オッズ", f"{row['odds']:.1f}")
            col3.metric("期待回収率", f"{row['expected_return']:.2f}")

            st.write(f"推奨投資額: ¥{row['recommended_bet']:,.0f}")

def show_race_list():
    # 実装...
    pass

if __name__ == "__main__":
    main()
```

---

## 9. 通知システム [未実装 - 設計のみ]

### 9.1 メール通知

#### 9.1.1 SendGrid実装
```python
# notification.py

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_email_notification(predictions_df):
    """
    予測結果をメール通知
    """
    top_bets = predictions_df.nlargest(5, 'expected_return')

    html_content = f"""
    <h2>本日のおすすめ馬券</h2>
    <table>
        <tr>
            <th>レース</th>
            <th>馬名</th>
            <th>予測確率</th>
            <th>オッズ</th>
            <th>期待回収率</th>
        </tr>
    """

    for idx, row in top_bets.iterrows():
        html_content += f"""
        <tr>
            <td>{row['venue']} {row['race_number']}R</td>
            <td>{row['horse_name']}</td>
            <td>{row['pred_prob']:.1%}</td>
            <td>{row['odds']:.1f}</td>
            <td>{row['expected_return']:.2f}</td>
        </tr>
        """

    html_content += "</table>"

    message = Mail(
        from_email='noreply@keiba-prediction.com',
        to_emails='user@example.com',
        subject='競馬予測: 本日のおすすめ馬券',
        html_content=html_content
    )

    sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
    response = sg.send(message)
```

### 9.2 LINE通知

#### 9.2.1 LINE Notify実装
```python
import requests

def send_line_notification(predictions_df):
    """
    予測結果をLINE通知
    """
    top_bets = predictions_df.nlargest(3, 'expected_return')

    message = "🏇 本日のおすすめ馬券\n\n"

    for idx, row in top_bets.iterrows():
        message += f"{row['venue']} {row['race_number']}R\n"
        message += f"馬名: {row['horse_name']}\n"
        message += f"予測: {row['pred_prob']:.1%} / オッズ: {row['odds']:.1f}\n"
        message += f"期待回収率: {row['expected_return']:.2f}\n\n"

    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": f"Bearer {os.environ.get('LINE_NOTIFY_TOKEN')}"
    }
    data = {"message": message}

    response = requests.post(url, headers=headers, data=data)
```

---

## 10. 実装状況

実装状況の詳細は [README.md](./README.md#実装状況) を参照してください。

---

## 11. リスク管理

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
| モデル性能劣化 | 継続的モニタリング、月次再学習 |
| データ取得失敗 | リトライ処理、アラート通知 |

### 11.3 投資リスク

| リスク | 対策 |
|--------|------|
| 連続損失 | 1日あたり投資上限、Fractional Kelly |
| 過度な賭け | Kelly基準、1レースあたり上限 |
| 予測精度低下 | バックテスト継続、閾値調整 |

---

## 12. モニタリング・改善

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

## 13. 参考資料

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
- 言語/ツール: Python 3, pip3 を使用
- シェル設定: 改行コードは LF を使用 (CRLFは避ける)
- プロジェクト概要: JRDB競馬データパイプライン (BigQuery, GCS, Cloud Run)

### 開発ワークフロー
GitHub Issueの実装時は以下の手順を遵守してください:
1. ブランチ作成 (feature/issue-XX)
  - この手順については全ての実装時に必ず守ること
2. 実装 & テスト作成
3. テスト実行と修正
4. PR作成 & 自己レビュー
5. マージ

### テストと検証
- ファイルの移動または名前変更後は、すべてのテストファイルでモックターゲットパスとインポートパスを必ず更新すること
- BigQueryスキーマまたは列参照を変更した後は、実際のテーブルスキーマに列名が存在することを使用前に確認すること
- パーサーフィールド位置を変更した後は、サンプルデータでテスト解析を実行し、固定長TXTスキーマとの整合性を確認すること

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

## 変更履歴

| 日付 | バージョン | 変更内容 | 担当者 |
|------|-----------|----------|--------|
| 2026-01-18 | 1.0.0 | 初版作成 | Claude |
| 2026-02-10 | 2.0.0 | 実装状況を反映: データパイプライン・特徴量パイプライン完了、ディレクトリ構成・BigQueryスキーマ・実装計画を現状に合わせて全面更新 | Claude |
| 2026-02-14 | 3.0.0 | README.mdとの重複を除外し、設計仕様書として再構成。実装済み機能の手順詳細はREADME.mdへ移行。Issue #59（特徴量生成API）とIssue #71（デプロイスクリプト整備）の内容を反映 | Claude |
| 2026-02-17 | 3.1.0 | Issue #85（LambdaRankラベル二値化）・Issue #86（Optunaハイパーパラメータチューニング）の実装を反映。Model Training Layer実装済み、評価指標にAUC追加、KPIにAUC追加 | Claude |
| 2026-02-22 | 3.2.0 | Issue #17（バックテストシミュレーター）の実装を反映。Backtest Layer実装済みに更新、セクション6を実装済みに変更、実装ファイル一覧を追記 | Claude |
| 2026-03-01 | 3.3.0 | Issue #116（ロード履歴スキップロジック改善）・Issue #117（日次予測パイプライン完成）の実装を反映。Prediction & Operation Layer一部実装済みに更新、predictionsバケット/データセット実装済みに更新、race-day-predictのAPI実装済みに更新 | Claude |

---

**End of Document**
