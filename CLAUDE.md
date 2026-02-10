# 競馬予測MLシステム 仕様書

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

### 1.4 技術スタック
- **言語**: Python 3.9+
- **機械学習**: LightGBM (Learning to Rank)
- **開発環境**: Jupyter Notebook (EDA), スクリプト実行 (本番)
- **クラウド**: GCP (Cloud Storage, BigQuery, Cloud Run, Cloud Scheduler)
- **通知**: メール (SendGrid/Gmail API), LINE (LINE Notify)
- **可視化**: Streamlit または Dash (Webダッシュボード)

---

## 2. システムアーキテクチャ

### 2.1 全体構成

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Layer [実装済み]                         │
├─────────────────────────────────────────────────────────────────┤
│  [Cloud Scheduler] → [Cloud Run / FastAPI] 日次パイプライン      │
│    1. JRDBDownloader: JRDB公式からHTTPダウンロード + lzh解凍     │
│    2. GCSUploader: GCSへバッチアップロード (MD5重複チェック付き)  │
│    3. BigQueryLoader: MERGE(UPSERT)でBigQueryにロード            │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                Feature Engineering Layer [実装済み]               │
├─────────────────────────────────────────────────────────────────┤
│  [BigQuery] SQL駆動方式 (feature_query_raw.sql)                  │
│    - 5段階CTE: ベース → 過去走 → 集計 → 馬マスター → 差分指標   │
│    - 出力: features.training_data (257カラム)                     │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Model Training Layer [未実装]                  │
├─────────────────────────────────────────────────────────────────┤
│  [Jupyter Notebook] EDA & モデル開発                             │
│  [Python Script] LightGBM ランク学習                             │
│  [GCS] モデルファイル保存                                         │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                Prediction & Operation Layer [未実装]              │
├─────────────────────────────────────────────────────────────────┤
│  [Cloud Run] 予測スクリプト実行                                  │
│  [BigQuery] 予測結果保存                                         │
│  [Streamlit on Cloud Run] Webダッシュボード                      │
│  [メール/LINE] 通知                                              │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 GCPリソース構成

#### 2.2.1 Cloud Storage (GCS)
- **バケット構成**:
  - `gs://${PROJECT_ID}-keiba-raw-data/`: JRDBダウンロード生データ (lzh, txt, csv)
  - `gs://${PROJECT_ID}-keiba-processed-data/`: 加工済みデータ
  - `gs://${PROJECT_ID}-keiba-models/`: 学習済みモデル
  - `gs://${PROJECT_ID}-keiba-predictions/`: 予測結果

  ※ バケット名はグローバルに一意である必要があるため、プロジェクトIDをプレフィックスとして使用します

#### 2.2.2 BigQuery
- **データセット構成**:
  - `raw`: 生データテーブル (JRDB各種データタイプ)
  - `features`: 特徴量テーブル
  - `predictions`: 予測結果テーブル
  - `backtests`: バックテスト結果テーブル

#### 2.2.3 Cloud Run [実装済み]
- **サービス一覧**:
  - `data-pipeline-service`: データ取得パイプライン (FastAPI)
    - `POST /daily-load`: 日次ロード実行
    - `POST /full-load`: 全件ロード実行
    - `GET /health`: ヘルスチェック
    - `GET /status/{job_id}`: ジョブ状態確認
  - `dashboard-service`: Webダッシュボード (Streamlit) [未実装]

#### 2.2.4 Cloud Scheduler [実装済み]
- **ジョブ一覧**:
  - `daily-data-download`: 毎日AM 6:00 データダウンロード (Cloud Run HTTP POST)
  - `pre-race-prediction`: レース前日 PM 9:00 予測実行 [未実装]
  - `race-day-prediction`: レース当日 AM 8:00 予測更新 [未実装]

---

## 3. データパイプライン

### 3.1 データ取得フロー [実装済み]

#### 3.1.1 データパイプライン (Python実装)
```
JRDB公式サイト
    ↓ (JRDBDownloader: HTTP Basic認証 + lzh解凍 + CP932→UTF-8変換)
ローカル: downloaded_files/ (または Cloud Run上の /tmp)
    ↓ (GCSUploader: MD5ハッシュ重複チェック + バッチアップロード)
GCS: gs://{PROJECT_ID}-keiba-raw-data/
    ↓ (BigQueryLoader: JRDBパース + MERGE文によるUPSERT)
BigQuery: raw.{各テーブル}
    ↓ (FeaturePipeline: SQL駆動方式)
BigQuery: features.training_data (257カラム)
```

#### 3.1.2 実行方法

**自動実行 (Cloud Scheduler + Cloud Run)**
- **トリガー**: 毎日AM 6:00 → Cloud Run FastAPI `POST /daily-load`
- **処理**: JRDBDownloader → GCSUploader → BigQueryLoader を順次実行

**手動実行 (ローカル)**
```bash
# 日次: スクリプト直接実行 or API呼び出し
python scripts/reload_gcs_to_bq.py --start-date 2024-01-01 --end-date 2024-01-31

# 全件ロード: full-load パイプライン
# POST /full-load (Cloud Run API)

# 特徴量生成
python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-12-31
```

#### 3.1.3 主要クラス (src/automation/data/)
- `JRDBDownloader`: JRDB公式からHTTP Basic認証でダウンロード、lzh解凍、エンコーディング変換
- `JRDBParser`: 固定長テキスト(JRDB形式)をDict/DataFrameに変換 (15+データタイプ対応)
- `GCSUploader`: GCSへバッチアップロード (MD5重複チェック、リトライ付き)
- `BigQueryLoader`: GCSファイルをパースしBigQueryにMERGE(UPSERT)ロード

### 3.2 BigQueryテーブル設計

#### 3.2.1 rawデータセット [実装済み]

- 備考
  - rawデータのスキーマは別ドキュメントで整理している
  - @SCHEMA.md
  - スキーマ定義ファイル: `config/bq_schema_*.json`

##### 現在のテーブル一覧と状態

| テーブル | データソース | 行数 | 状態 |
|---------|-------------|------|------|
| `raw.race_info` | BAA (番組データ) | ~33,400 | ✅ 稼働中 |
| `raw.horse_results` | KYF (競走馬データ) | ~486,500 | ✅ 稼働中 |
| `raw.race_results` | SEC (成績データ) | ~486,500 | ✅ 稼働中 |
| `raw.horse_extended` | KKA (拡張馬データ) | ~486,500 | ✅ 稼働中 |
| `raw.horse_master` | KSA (馬マスター) | ~21,500 | ✅ 稼働中 |
| `raw.venue_info` | KAB (開催情報) | ~418 | ✅ 稼働中 |
| `raw.load_history` | (管理用) | ~3,350 | ✅ 稼働中 |
| `raw.pedigree` | 血統データ | 0 | ⬚ テーブルのみ作成済み |
| `raw.odds` | OZ (オッズデータ) | 0 | ⬚ テーブルのみ作成済み |

- `race_info`: race_dateでパーティション
- `odds`: odds_timestampでパーティション、(race_id, horse_id, odds_type)でクラスタリング
- `pedigree`: (sire_id, dam_sire_id)でクラスタリング

#### 3.2.2 featuresデータセット [実装済み]

##### テーブル: `features.training_data` (257カラム, 466,265行)

SQL駆動方式（`src/ml/features/feature_query_raw.sql`）により5段階CTEで生成。
`race_date`でパーティション。期間: 2016-01-05〜2026-02-08。

**主要カラムカテゴリ**:
- **基本情報**: race_id, race_date, venue_code, race_number, course_type, distance, direction, num_horses 等
- **馬情報**: horse_id, horse_name, horse_age, bracket_number, horse_number 等
- **人的要素**: jockey_name, jockey_code, trainer_name, trainer_code 等
- **JRDB指数**: idm, jockey_index, info_index, total_index, training_index, stable_index 等
- **過去5走データ**: finish_position_{1-5}, win_odds_{1-5}, idm_{1-5}, improvement_code_{1-5} 等
- **集計メトリクス**: mean/ema/max/min の idm, finish_position, win_popularity 等
- **条件別複勝率**: surface/distance/track/rotation/direction/condition/pace/season/bracket 別 top1/2/3_finish_rate
- **騎手×条件**: jockey_dist, jockey_track_dist, jockey_trainer, jockey_owner 別成績
- **血統指標**: sire_surface_place_rate, broodmare_sire_place_rate 等
- **差分指標**: 各条件別成績と全体成績の差分 (surface_top3_finish_rate_diff 等)
- **総合スコア**: total_diff_sum (全差分の合計 = 激走指標)

※ 全257カラムの詳細は `src/ml/features/feature_query_raw.sql` を参照

#### 3.2.3 その他のデータセット [テーブル未作成]

- `predictions`: 予測結果テーブル（データセットのみ作成済み）
- `backtests`: バックテスト結果テーブル（データセットのみ作成済み）

---

## 4. 特徴量エンジニアリング [実装済み]

### 4.1 実装方式: SQL駆動型パイプライン

特徴量生成はBigQuery SQL（`src/ml/features/feature_query_raw.sql`）により一括処理される。
Pythonオーケストレータ（`src/ml/features/feature_pipeline.py`）がSQLテンプレートにパラメータを埋め込み、
BigQueryジョブとして実行する方式。

#### 実行方法
```bash
# CLIから実行
python scripts/generate_features.py --start-date 2024-01-01 --end-date 2024-12-31

# Python APIから実行
from src.ml.features import FeaturePipeline, FeaturePipelineConfig
pipeline = FeaturePipeline(project_id="keiba-prediction-1768734113")
result = pipeline.run(start_date="2024-01-01", end_date="2024-12-31")
```

#### SQLテンプレートの構造 (5段階CTE)
1. **temp_base_race_entries**: レース基本情報 + 出走馬情報の結合
2. **temp_past_race_features**: 過去5走の詳細データ抽出 (オッズ, 人気, IDM, タイム差等)
3. **temp_past_race_features2**: 過去走の集計指標計算 (mean/EMA/max/min, レート, 差分)
4. **temp_horse_master_feature**: 条件別複勝率 (芝ダ, 距離, コース, ペース, 季節, 枠, 騎手×条件等) + 血統指標
5. **temp_horse_master_feature2**: 条件別成績と全体成績の差分指標

### 4.2 特徴量カテゴリ (257カラム)
**ファイル**: @ML_FEATURE.md

実装済みカテゴリ：
1. **ベース特徴**: レース条件、馬場状態、馬場バイアス (venue_info)
2. **JRDB指数**: IDM、騎手指数、情報指数、総合指数、調教指数、厩舎指数等
3. **過去5走データ**: 着順、オッズ、人気、IDM、改善コード、出遅れ、位置取り不利等
4. **集計メトリクス**: 過去走の平均/EMA/最大/最小 (IDM, 着順, 人気等)
5. **条件別複勝率**: 芝ダ/距離/コース/回り/方向/馬場/ペース/季節/枠 別
6. **騎手×条件**: 騎手×距離、騎手×コース、騎手×調教師、騎手×馬主等
7. **血統指標**: 種牡馬/母父の芝ダ別複勝率、距離適性差分
8. **差分指標**: 条件別成績 - 全体成績の差分 (27項目)
9. **総合指標**: total_diff_sum (全差分合計 = 激走スコア)

### 4.3 リーク対策チェックリスト
- [ ] 発走後に確定する情報を使用していない (確定オッズ、確定馬体重など)
- [ ] 条件別成績は当該レースを除外して計算
- [ ] 同一レース内の情報漏洩がない
- [ ] 時系列分割でバックテスト実施

---

## 5. モデル設計 [未実装 - 設計のみ]

### 5.1 LightGBM ランク学習

#### 5.1.1 モデル概要
- **アルゴリズム**: LightGBM LambdaRank
- **目的関数**: `lambdarank`
- **評価指標**: `ndcg@3`
- **グループ単位**: レースID

#### 5.1.2 実装例
```python
import lightgbm as lgb

# データ準備
train_data = lgb.Dataset(
    X_train,
    label=y_train,  # 着順 (1, 2, 3, ...)
    group=groups_train  # 各レースの馬数 [18, 16, 15, ...]
)

# パラメータ
params = {
    'objective': 'lambdarank',
    'metric': 'ndcg',
    'ndcg_eval_at': [3],
    'boosting_type': 'gbdt',
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': -1,
    'seed': 42
}

# 学習
model = lgb.train(
    params,
    train_data,
    num_boost_round=1000,
    valid_sets=[valid_data],
    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)]
)
```

### 5.2 時系列クロスバリデーション

#### 5.2.1 分割方法
```python
# 例: 月次分割
# 2019/01-2023/06: 学習用
# 2023/07-2023/12: 検証用
# 2024/01-2024/06: テスト用

def time_series_split(df, date_col, n_splits=5):
    """
    時系列を考慮したクロスバリデーション分割
    """
    df = df.sort_values(date_col)
    total_months = (df[date_col].max() - df[date_col].min()).days // 30
    fold_size = total_months // (n_splits + 1)

    for i in range(n_splits):
        train_end = df[date_col].min() + pd.DateOffset(months=fold_size * (i + 1))
        valid_end = train_end + pd.DateOffset(months=fold_size)

        train_idx = df[df[date_col] < train_end].index
        valid_idx = df[(df[date_col] >= train_end) & (df[date_col] < valid_end)].index

        yield train_idx, valid_idx
```

### 5.3 モデル評価

#### 5.3.1 評価指標
1. **NDCG@3**: ランキングの質を評価
2. **Recall@3**: 上位3頭の中に複勝圏内の馬が含まれる割合
3. **回収率**: 実際の投資に基づく評価
4. **的中率**: 3着以内予測の精度

#### 5.3.2 評価実装
```python
from sklearn.metrics import ndcg_score

def evaluate_model(y_true, y_pred, groups):
    """
    モデル評価関数
    """
    results = []
    start = 0

    for group_size in groups:
        end = start + group_size
        race_true = y_true[start:end]
        race_pred = y_pred[start:end]

        # NDCG@3
        ndcg = ndcg_score([race_true], [race_pred], k=3)

        # Recall@3
        top3_pred = np.argsort(race_pred)[-3:]
        top3_true = np.where(race_true <= 3)[0]
        recall = len(set(top3_pred) & set(top3_true)) / min(3, len(top3_true))

        results.append({'ndcg@3': ndcg, 'recall@3': recall})
        start = end

    return pd.DataFrame(results).mean()
```

---

## 6. バックテスト設計 [未実装 - 設計のみ]

### 6.1 バックテスト概要
- **目的**: 過去データで実際の投資をシミュレーション
- **期間**: 最低6ヶ月以上
- **評価**: 回収率、的中率、最大ドローダウン

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

### 6.3 バックテスト実装

```python
def backtest(predictions_df, initial_capital=100000):
    """
    バックテスト実行
    """
    capital = initial_capital
    history = []

    for race_id in predictions_df['race_id'].unique():
        race_data = predictions_df[predictions_df['race_id'] == race_id]

        # 投資対象の馬を選定
        race_data['expected_return'] = race_data['pred_prob'] * race_data['odds']
        bet_horses = race_data[race_data['expected_return'] > 1.2].copy()

        if len(bet_horses) == 0:
            continue

        # 賭け金配分
        for idx, row in bet_horses.iterrows():
            kelly_frac = fractional_kelly(row['pred_prob'], row['odds'])
            bet_amount = min(capital * kelly_frac, capital * 0.05)

            # 結果判定
            if row['finish_position'] <= 3:  # 複勝的中
                payout = bet_amount * row['place_odds']
                capital += (payout - bet_amount)
                result = 'win'
            else:
                capital -= bet_amount
                result = 'lose'

            history.append({
                'race_id': race_id,
                'horse_id': row['horse_id'],
                'bet_amount': bet_amount,
                'result': result,
                'capital': capital
            })

    return pd.DataFrame(history)
```

### 6.4 バックテスト評価指標

```python
def backtest_metrics(history_df, initial_capital):
    """
    バックテスト評価指標計算
    """
    total_bet = history_df['bet_amount'].sum()
    total_return = history_df[history_df['result'] == 'win']['bet_amount'].sum()

    metrics = {
        'recovery_rate': (total_return / total_bet) * 100,
        'hit_rate': (len(history_df[history_df['result'] == 'win']) / len(history_df)) * 100,
        'final_capital': history_df['capital'].iloc[-1],
        'profit': history_df['capital'].iloc[-1] - initial_capital,
        'max_drawdown': calculate_max_drawdown(history_df['capital']),
        'sharpe_ratio': calculate_sharpe_ratio(history_df)
    }

    return metrics
```

---

## 7. 運用フロー

### 7.1 日次データパイプライン [実装済み]

#### 7.1.1 日次データ取得 (AM 6:00)
Cloud Scheduler → Cloud Run FastAPI `POST /daily-load`
1. **JRDBダウンロード**: 指定日のJRDBデータをHTTP Basic認証でダウンロード
2. **GCSアップロード**: MD5チェック付きでGCSにアップロード
3. **BigQueryロード**: JRDBパーサーでパースし、MERGE文でBigQueryにUPSERT

実装: `src/automation/pipeline/daily_pipeline.py` → `DailyPipeline`

#### 7.1.2 予測パイプライン [未実装 - 設計のみ]
- 前日PM 9:00: 特徴量生成 → 予測実行 → 通知
- 当日AM 8:00: オッズ更新 → 予測再実行 → 通知

### 7.2 Cloud Runデプロイ [実装済み]

#### 7.2.1 API (FastAPI)
実装: `src/automation/api/app.py`
- `POST /daily-load`: 日次ロード (BackgroundTasksで非同期実行)
- `POST /full-load`: 全件ロード
- `GET /health`: ヘルスチェック
- `GET /status/{job_id}`: ジョブ状態確認

#### 7.2.2 Dockerfile
```dockerfile
FROM python:3.9-slim
# lzh解凍ツールをインストール
RUN apt-get update && apt-get install -y lhasa p7zip-full
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["uvicorn", "src.automation.api.app:app", "--host", "0.0.0.0", "--port", "8080"]
```

#### 7.2.3 デプロイ
```bash
# infrastructure/scripts/deploy_cloud_run.sh を使用
bash infrastructure/scripts/deploy_cloud_run.sh
```

---

## 8. Webダッシュボード [未実装 - 設計のみ]

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

## 10. 実装計画

### 10.1 Phase 1: データ基盤構築 ✅ 完了

- [x] GCPプロジェクト作成、権限設定
- [x] GCSバケット作成
- [x] BigQueryデータセット・テーブル作成 (config/bq_schema_*.json)
- [x] ローカル→GCS自動アップロードスクリプト (GCSUploader)
- [x] GCS→BigQueryロード (BigQueryLoader / MERGE UPSERT方式)
- [x] 既存データの一括アップロード・ロード (FullLoadPipeline)
- [x] データ品質チェック (src/manual/quality_check.py)
- [x] BigQuery SQLでのデータ集計確認
- [x] 日次パイプライン自動化 (Cloud Scheduler → Cloud Run FastAPI)
- [x] テスト作成 (7テストファイル)

### 10.2 Phase 2: 特徴量エンジニアリング ✅ 完了

- [x] SQL駆動方式で特徴量パイプライン実装 (feature_query_raw.sql, 5段階CTE)
- [x] 過去5走の詳細データ (着順, オッズ, IDM, 改善コード等)
- [x] 集計メトリクス (mean/EMA/max/min)
- [x] 条件別複勝率 (芝ダ/距離/コース/回り/方向/馬場/ペース/季節/枠)
- [x] 騎手×条件別成績 (騎手×距離, ×コース, ×調教師, ×馬主等)
- [x] 血統指標 (種牡馬/母父の芝ダ別複勝率, 距離適性)
- [x] 差分指標 (条件別 - 全体成績) + 総合激走スコア
- [x] Jupyter NotebookでEDA (4ノートブック)
- [x] BigQueryに特徴量テーブル作成 (257カラム, 466,265行)
- [x] 特徴量ドキュメント作成 (ML_FEATURE.md)

### 10.3 Phase 3: モデル開発 ⬚ 未着手

- [ ] LightGBM ランク学習ベースライン構築
- [ ] 時系列CVでの評価
- [ ] ハイパーパラメータチューニング
- [ ] バックテスト実装
- [ ] 投資戦略検証 (Kelly基準)
- [ ] モデル評価レポート作成

### 10.4 Phase 4: 運用システム構築 (一部実装済み)

- [x] データパイプラインCloud Runデプロイ
- [x] Cloud Scheduler設定 (日次データ取得)
- [x] エラーハンドリング・ログ実装
- [ ] 予測パイプライン実装
- [ ] Webダッシュボード実装 (Streamlit)
- [ ] メール/LINE通知実装
- [ ] 結合テスト

### 10.5 Phase 5: 運用開始 ⬚ 未着手

- [ ] 本番運用開始
- [ ] 日次モニタリング
- [ ] 週次パフォーマンスレビュー
- [ ] 月次モデル再学習

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

### 12.2 改善サイクル

1. **日次**: 予測結果と実績の比較
2. **週次**: パフォーマンスレビュー、閾値調整
3. **月次**: モデル再学習、特徴量追加検討
4. **四半期**: システム全体の見直し

---

## 13. 参考資料

### 13.1 ドキュメント
- `ML_FEATURE.md`: 特徴量設計詳細
- `SCHEMA.md`: JRDBデータスキーマ詳細
- `docs/GCP_SETUP.md`: GCPセットアップガイド
- `docs/BIGQUERY_SETUP.md`: BigQueryセットアップガイド
- `infrastructure/README.md`: インフラセットアップ完全ガイド
- `infrastructure/ENV_MANAGEMENT.md`: 環境変数管理

### 13.2 外部リソース
- JRDB公式: http://www.jrdb.com/
- LightGBM Documentation: https://lightgbm.readthedocs.io/
- GCP Documentation: https://cloud.google.com/docs

---

## 付録A: ディレクトリ構成

```
keiba_prediction/
├── src/                            # メインソースコード
│   ├── automation/                 # 自動化パイプライン [実装済み]
│   │   ├── data/
│   │   │   ├── jrdb_downloader.py  # JRDBダウンロード (HTTP + lzh解凍 + エンコーディング変換)
│   │   │   ├── jrdb_parser.py      # JRDBデータパース (固定長テキスト → Dict/DataFrame)
│   │   │   ├── load_to_bq.py       # BigQueryロード (MERGE UPSERT)
│   │   │   └── upload_to_gcs.py    # GCSアップロード (MD5重複チェック)
│   │   ├── pipeline/
│   │   │   ├── daily_pipeline.py   # 日次パイプライン (Cloud Scheduler→Cloud Run)
│   │   │   └── full_load_pipeline.py # 全件ロードパイプライン (初回セットアップ用)
│   │   └── api/
│   │       └── app.py              # FastAPI HTTPエンドポイント (Cloud Run)
│   │
│   ├── ml/                         # 機械学習モジュール
│   │   └── features/               # [実装済み]
│   │       ├── __init__.py
│   │       ├── feature_pipeline.py  # SQL駆動パイプラインオーケストレータ
│   │       └── feature_query_raw.sql # 特徴量生成SQL (5段階CTE, 257カラム出力)
│   │
│   └── manual/                     # 手動実行スクリプト [実装済み]
│       ├── create_tables.py        # BigQueryテーブル作成
│       ├── quality_check.py        # データ品質チェック
│       └── validation_rules.py     # 検証ルール定義
│
├── scripts/                        # スタンドアロンスクリプト [実装済み]
│   ├── generate_features.py        # 特徴量生成CLI
│   ├── reload_gcs_to_bq.py         # GCS→BQ再ロード
│   ├── setup_gcp.sh                # GCP初期セットアップ
│   ├── setup_bigquery.sh           # BigQuery初期化
│   └── sync_to_gcs.sh              # ローカル→GCS同期
│
├── tests/                          # テストコード [実装済み]
│   ├── test_features.py            # 特徴量パイプラインテスト
│   ├── test_jrdb_downloader.py     # ダウンローダーテスト
│   ├── test_load_to_bq.py          # BQロードテスト
│   ├── test_upload_to_gcs.py       # GCSアップロードテスト
│   ├── test_daily_pipeline.py      # 日次パイプラインテスト
│   ├── test_full_load_pipeline.py  # 全件ロードテスト
│   └── test_quality_check.py       # 品質チェックテスト
│
├── config/                         # 設定ファイル (BigQueryスキーマ定義)
│   ├── bq_schema_race_info.json
│   ├── bq_schema_horse_results.json
│   ├── bq_schema_race_results.json
│   ├── bq_schema_horse_master.json
│   ├── bq_schema_horse_extended.json
│   ├── bq_schema_venue_info.json
│   ├── bq_schema_pedigree.json
│   ├── bq_schema_odds.json
│   ├── bq_schema_training_data.json
│   └── bq_schema_load_history.json
│
├── infrastructure/                 # GCPインフラストラクチャ
│   ├── README.md                   # セットアップ完全ガイド
│   ├── ENV_MANAGEMENT.md           # 環境変数管理
│   ├── cloud_run_config.yaml       # Cloud Run設定
│   └── scripts/
│       ├── setup_gcp.sh            # GCP初期セットアップ
│       ├── deploy_cloud_run.sh     # Cloud Runデプロイ
│       └── verify_setup.sh         # セットアップ検証
│
├── docs/                           # ドキュメント
│   ├── GCP_SETUP.md
│   └── BIGQUERY_SETUP.md
│
├── notebooks/                      # Jupyter Notebook (EDA)
│   ├── 01_data_exploration.ipynb
│   ├── 02_race_analysis.ipynb
│   ├── 03_horse_analysis.ipynb
│   └── 04_feature_correlation.ipynb
│
├── legacy/                         # 旧実装 (互換性のため保持)
│   ├── downloader/                 # 旧シェルスクリプトダウンローダー
│   └── cloud_functions/            # 旧Cloud Functions実装
│
├── downloaded_files/               # ローカルダウンロードデータ (gitignore)
├── requirements.txt
├── Dockerfile
├── .env.example
├── .gitignore
├── CLAUDE.md                       # 本仕様書
├── ML_FEATURE.md                   # 特徴量設計
├── SCHEMA.md                       # JRDBデータスキーマ
└── README.md
```

---

## 付録B: 環境変数

```bash
# .env.example

# GCP
GCP_PROJECT_ID=your-project-id
GCP_REGION=asia-northeast1
GCS_BUCKET_RAW=keiba-raw-data
GCS_BUCKET_MODELS=keiba-models

# BigQuery
BQ_DATASET_RAW=raw
BQ_DATASET_FEATURES=features
BQ_DATASET_PREDICTIONS=predictions

# JRDB
JRDB_USER=your-jrdb-user
JRDB_PASSWORD=your-jrdb-password

# Notification
SENDGRID_API_KEY=your-sendgrid-key
LINE_NOTIFY_TOKEN=your-line-token

# Model
MODEL_VERSION=v1.0.0
PREDICTION_THRESHOLD=0.3
```

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
---

## 変更履歴

| 日付 | バージョン | 変更内容 | 担当者 |
|------|-----------|----------|--------|
| 2026-01-18 | 1.0.0 | 初版作成 | Claude |
| 2026-02-10 | 2.0.0 | 実装状況を反映: データパイプライン・特徴量パイプライン完了、ディレクトリ構成・BigQueryスキーマ・実装計画を現状に合わせて全面更新 | Claude |

---

**End of Document**
