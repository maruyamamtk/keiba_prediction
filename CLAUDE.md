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
│                         Data Layer                               │
├─────────────────────────────────────────────────────────────────┤
│  [ローカル] downloader scripts → [GCS] raw data bucket           │
│  [GCS] → [Cloud Functions] → [BigQuery] テーブル化              │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Feature Engineering Layer                   │
├─────────────────────────────────────────────────────────────────┤
│  [BigQuery] SQL/Python → 特徴量テーブル生成                      │
│  - 過去走集計、Target Encoding、相対指標など                     │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                       Model Training Layer                       │
├─────────────────────────────────────────────────────────────────┤
│  [Jupyter Notebook] EDA & モデル開発                             │
│  [Python Script] LightGBM ランク学習                             │
│  [GCS] モデルファイル保存                                         │
└─────────────────────────────────────────────────────────────────┘
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Prediction & Operation Layer                │
├─────────────────────────────────────────────────────────────────┤
│  [Cloud Scheduler] 定期実行トリガー                              │
│  [Cloud Run] 予測スクリプト実行                                  │
│  [BigQuery] 予測結果保存                                         │
│  [Streamlit on Cloud Run] Webダッシュボード                      │
│  [Cloud Functions] メール/LINE通知                               │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 GCPリソース構成

#### 2.2.1 Cloud Storage (GCS)
- **バケット構成**:
  - `gs://keiba-raw-data/`: JRDBダウンロード生データ (lzh, txt, csv)
  - `gs://keiba-processed-data/`: 加工済みデータ
  - `gs://keiba-models/`: 学習済みモデル
  - `gs://keiba-predictions/`: 予測結果

#### 2.2.2 BigQuery
- **データセット構成**:
  - `raw`: 生データテーブル (JRDB各種データタイプ)
  - `features`: 特徴量テーブル
  - `predictions`: 予測結果テーブル
  - `backtests`: バックテスト結果テーブル

#### 2.2.3 Cloud Functions
- **関数一覧**:
  - `gcs_to_bigquery`: GCSにアップロードされたファイルをBigQueryにロード
  - `send_notification`: 予測結果をメール/LINEで通知

#### 2.2.4 Cloud Run
- **サービス一覧**:
  - `prediction-service`: 予測実行サービス
  - `dashboard-service`: Webダッシュボード (Streamlit)

#### 2.2.5 Cloud Scheduler
- **ジョブ一覧**:
  - `daily-data-download`: 毎日AM 6:00 データダウンロード
  - `pre-race-prediction`: レース前日 PM 9:00 予測実行
  - `race-day-prediction`: レース当日 AM 8:00 予測更新

---

## 3. データパイプライン

### 3.1 データ取得フロー

#### 3.1.1 ローカル → GCS
```bash
# 1. ローカルでダウンロード (既存スクリプト使用)
cd downloader
sh download_all_from_date.sh

# 2. GCSにアップロード
gsutil -m rsync -r ../downloaded_files/ gs://keiba-raw-data/
```

#### 3.1.2 自動化 (Cloud Scheduler + Cloud Run)
- **トリガー**: 毎日AM 6:00
- **処理**:
  1. Cloud Runでダウンロードスクリプト実行
  2. GCSにアップロード
  3. Cloud Functionsがトリガーされ、BigQueryにロード

### 3.2 BigQueryテーブル設計

#### 3.2.1 rawデータセット

##### テーブル: `raw.race_info` (BAA: 番組データ)
```sql
CREATE TABLE raw.race_info (
  race_id STRING NOT NULL,          -- レースID (場所コード + 開催回 + 日次 + レース番号)
  race_date DATE NOT NULL,          -- 開催日
  venue_code STRING,                -- 競馬場コード
  race_number INT64,                -- レース番号
  course_type STRING,               -- 芝/ダート
  distance INT64,                   -- 距離
  direction STRING,                 -- 右/左
  race_class STRING,                -- クラス (G1/G2/OP/1600万下等)
  age_condition STRING,             -- 年齢条件
  sex_condition STRING,             -- 性別条件
  weather STRING,                   -- 天候
  track_condition STRING,           -- 馬場状態
  num_horses INT64,                 -- 出走頭数
  -- その他のレース条件...
  PRIMARY KEY (race_id) NOT ENFORCED
) PARTITION BY race_date;
```

##### テーブル: `raw.horse_results` (KYF: 競走馬データ + 過去成績)
```sql
CREATE TABLE raw.horse_results (
  race_id STRING NOT NULL,
  horse_id STRING NOT NULL,         -- 馬ID
  horse_name STRING,                -- 馬名
  bracket_number INT64,             -- 枠番
  horse_number INT64,               -- 馬番
  finish_position INT64,            -- 着順
  finish_time FLOAT64,              -- 走破タイム
  last_3f_time FLOAT64,             -- 上がり3F
  passing_order STRING,             -- 通過順
  odds FLOAT64,                     -- オッズ
  popularity INT64,                 -- 人気
  weight INT64,                     -- 斤量
  jockey_id STRING,                 -- 騎手ID
  jockey_name STRING,               -- 騎手名
  trainer_id STRING,                -- 調教師ID
  trainer_name STRING,              -- 調教師名
  horse_weight INT64,               -- 馬体重
  horse_weight_diff INT64,          -- 馬体重増減
  -- IDM、各種指数...
  idm FLOAT64,
  -- その他のKYFデータ...
  PRIMARY KEY (race_id, horse_id) NOT ENFORCED
) PARTITION BY DATE(race_id);
```

##### テーブル: `raw.pedigree` (血統データ)
```sql
CREATE TABLE raw.pedigree (
  horse_id STRING PRIMARY KEY NOT ENFORCED,
  sire_id STRING,                   -- 種牡馬ID
  sire_name STRING,                 -- 種牡馬名
  dam_sire_id STRING,               -- 母父ID
  dam_sire_name STRING,             -- 母父名
  sire_line STRING                  -- 父系統
);
```

##### テーブル: `raw.odds` (オッズデータ)
```sql
CREATE TABLE raw.odds (
  race_id STRING NOT NULL,
  horse_id STRING NOT NULL,
  odds_type STRING,                 -- 単勝/複勝
  odds_value FLOAT64,               -- オッズ値
  odds_timestamp TIMESTAMP,         -- 取得時刻
  PRIMARY KEY (race_id, horse_id, odds_type, odds_timestamp) NOT ENFORCED
) PARTITION BY DATE(race_id);
```

#### 3.2.2 featuresデータセット

##### テーブル: `features.training_data`
```sql
CREATE TABLE features.training_data (
  race_id STRING NOT NULL,
  horse_id STRING NOT NULL,
  race_date DATE NOT NULL,

  -- 目的変数
  target_place BOOL,                -- 3着以内 (1/0)
  finish_position INT64,            -- 着順 (ランク学習用)

  -- 基本情報
  venue_code STRING,
  race_number INT64,
  course_type STRING,
  distance INT64,
  track_condition STRING,
  num_horses INT64,
  bracket_number INT64,
  horse_number INT64,
  weight INT64,

  -- 過去走集計特徴 (詳細はML_FEATURE.md参照)
  past_3_avg_position FLOAT64,
  past_5_avg_position FLOAT64,
  past_10_avg_position FLOAT64,
  past_3_avg_last3f FLOAT64,
  past_5_avg_last3f FLOAT64,

  -- 条件適性
  turf_win_rate FLOAT64,
  dirt_win_rate FLOAT64,
  distance_category_win_rate FLOAT64,
  venue_win_rate FLOAT64,
  heavy_track_win_rate FLOAT64,

  -- 騎手・調教師
  jockey_id STRING,
  jockey_win_rate FLOAT64,
  jockey_venue_win_rate FLOAT64,
  trainer_id STRING,
  trainer_win_rate FLOAT64,

  -- Target Encoding (時系列OOF)
  jockey_te_place FLOAT64,
  trainer_te_place FLOAT64,
  sire_te_place FLOAT64,
  dam_sire_te_place FLOAT64,

  -- 相対指標 (レース内)
  weight_rank INT64,
  ability_rank INT64,
  last3f_rank INT64,

  -- 展開予測
  front_runner_count INT64,
  expected_pace_score FLOAT64,

  -- オッズ (前日・当日)
  odds_yesterday FLOAT64,
  odds_today FLOAT64,

  -- ... (ML_FEATURE.mdの他の特徴量)

  PRIMARY KEY (race_id, horse_id) NOT ENFORCED
) PARTITION BY race_date;
```

---

## 4. 特徴量エンジニアリング

### 4.1 特徴量一覧
**ファイル**: @doc/ML_FEATURE.md

主要カテゴリ：
1. **ベース特徴**: レース条件、馬場、天候
2. **過去走集計**: N走平均、最大値、トレンド
3. **条件適性**: 距離/コース/馬場/季節適性
4. **近況**: 休養日数、叩き何走目
5. **相対指標**: レース内順位、平均との差
6. **展開予測**: 逃げ候補数、予想ペース
7. **人的要素**: 騎手/調教師/コンビ成績
8. **血統**: 種牡馬/母父適性
9. **調教・馬体**: 調教タイム、馬体重増減
10. **オッズ**: 前日/当日オッズ、変動量
11. **Target Encoding**: 時系列OOF + 平滑化
12. **高次特徴**: 交互作用、差分、ランキング

### 4.2 実装優先順位

#### Phase 1: 基本特徴 (最優先)
1. 過去N走の基本統計 (着順/着差/上がり/通過順)
2. 休養日数、距離変更
3. 条件適性 (芝ダ・距離帯・競馬場・馬場)

#### Phase 2: コア特徴
4. 騎手/調教師/種牡馬のTarget Encoding
5. 展開予測 (先行力集計、逃げ候補数)

#### Phase 3: 高度特徴
6. 自作能力指数 + レース内相対指標
7. 調教/馬体重/オッズ

### 4.3 Target Encoding実装

#### 4.3.1 時系列OOF (Out-of-Fold)
```python
def create_te_oof(df, category_col, target_col, date_col):
    """
    時系列を考慮したTarget Encoding
    各行のTEは、その行より過去のデータのみから計算
    """
    df = df.sort_values(date_col)
    df['te_' + category_col] = np.nan

    for idx in df.index:
        current_date = df.loc[idx, date_col]
        category = df.loc[idx, category_col]

        # 過去データのみ
        past_data = df[df[date_col] < current_date]
        past_category = past_data[past_data[category_col] == category]

        if len(past_category) > 0:
            # 平滑化 (Smoothing)
            global_mean = past_data[target_col].mean()
            category_mean = past_category[target_col].mean()
            count = len(past_category)
            m = 10  # 平滑化パラメータ

            te_value = (count * category_mean + m * global_mean) / (count + m)
            df.loc[idx, 'te_' + category_col] = te_value
        else:
            df.loc[idx, 'te_' + category_col] = df[target_col].mean()

    return df
```

### 4.4 リーク対策チェックリスト
- [ ] 発走後に確定する情報を使用していない (確定オッズ、確定馬体重など)
- [ ] Target Encodingは時系列OOFで作成
- [ ] 同一レース内の情報漏洩がない
- [ ] 時系列分割でバックテスト実施

---

## 5. モデル設計

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

## 6. バックテスト設計

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

### 7.1 日次運用スケジュール

#### 7.1.1 前日処理 (PM 9:00)
1. **データ取得**: 翌日のレース情報、出走馬情報
2. **特徴量生成**: 前日時点で作成可能な特徴量
3. **予測実行**: 翌日全レースの予測
4. **通知**: Webダッシュボード更新 + メール/LINE通知

#### 7.1.2 当日処理 (AM 8:00)
1. **データ更新**: 当日の馬場状態、オッズ情報
2. **特徴量更新**: オッズ関連特徴量
3. **予測更新**: 最新情報で予測を再実行
4. **通知**: 更新をWebダッシュボード + メール/LINE通知

### 7.2 予測処理フロー

```python
# prediction_pipeline.py

def daily_prediction_pipeline(target_date):
    """
    日次予測パイプライン
    """
    # 1. データ取得
    race_info = fetch_race_info(target_date)
    horse_info = fetch_horse_info(target_date)

    # 2. 特徴量生成
    features = generate_features(race_info, horse_info)

    # 3. モデルロード
    model = load_model_from_gcs('gs://keiba-models/latest_model.txt')

    # 4. 予測
    predictions = model.predict(features)

    # 5. 投資判断
    investment_plan = create_investment_plan(predictions, features)

    # 6. 結果保存
    save_predictions_to_bigquery(investment_plan)

    # 7. 通知
    send_notification(investment_plan)

    return investment_plan
```

### 7.3 Cloud Runデプロイ

#### 7.3.1 Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "prediction_pipeline.py"]
```

#### 7.3.2 デプロイコマンド
```bash
# ビルド & デプロイ
gcloud builds submit --tag gcr.io/PROJECT_ID/prediction-service
gcloud run deploy prediction-service \
  --image gcr.io/PROJECT_ID/prediction-service \
  --platform managed \
  --region asia-northeast1 \
  --memory 2Gi \
  --timeout 900
```

---

## 8. Webダッシュボード

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

## 9. 通知システム

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

### 10.1 Phase 1: データ基盤構築 (2週間)

#### Week 1
- [ ] GCPプロジェクト作成、権限設定
- [ ] GCSバケット作成
- [ ] BigQueryデータセット・テーブル作成
- [ ] ローカル→GCS自動アップロードスクリプト
- [ ] GCS→BigQuery自動ロードCloud Functions

#### Week 2
- [ ] 既存データの一括アップロード・ロード
- [ ] データ品質チェック
- [ ] BigQuery SQLでのデータ集計確認

### 10.2 Phase 2: 特徴量エンジニアリング (3-4週間)

#### Week 3-4
- [ ] Phase 1特徴量実装 (過去走集計、条件適性)
- [ ] Jupyter NotebookでEDA
- [ ] 特徴量生成パイプライン実装
- [ ] BigQueryに特徴量テーブル作成

#### Week 5-6
- [ ] Phase 2特徴量実装 (Target Encoding、展開予測)
- [ ] 特徴量検証 (リーク確認)
- [ ] 特徴量ドキュメント作成

### 10.3 Phase 3: モデル開発 (2-3週間)

#### Week 7-8
- [ ] LightGBM ランク学習ベースライン構築
- [ ] 時系列CVでの評価
- [ ] ハイパーパラメータチューニング

#### Week 9
- [ ] バックテスト実装
- [ ] 投資戦略検証
- [ ] モデル評価レポート作成

### 10.4 Phase 4: 運用システム構築 (2-3週間)

#### Week 10-11
- [ ] 予測パイプライン実装
- [ ] Cloud Runデプロイ
- [ ] Cloud Scheduler設定
- [ ] エラーハンドリング・ログ実装

#### Week 12
- [ ] Webダッシュボード実装 (Streamlit)
- [ ] メール/LINE通知実装
- [ ] 結合テスト

### 10.5 Phase 5: 運用開始 (継続)

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
- `downloader/README.md`: データダウンロード手順

### 13.2 外部リソース
- JRDB公式: http://www.jrdb.com/
- LightGBM Documentation: https://lightgbm.readthedocs.io/
- GCP Documentation: https://cloud.google.com/docs

---

## 付録A: ディレクトリ構成

```
keiba_prediction/
├── downloader/              # データダウンロードスクリプト
│   ├── download_from_date.sh
│   ├── download_all_from_date.sh
│   └── ...
├── downloaded_files/        # ローカルダウンロードデータ (gitignore)
├── notebooks/               # Jupyter Notebook (EDA)
│   ├── 01_data_exploration.ipynb
│   ├── 02_feature_engineering.ipynb
│   └── 03_model_training.ipynb
├── src/                     # ソースコード
│   ├── data/
│   │   ├── download.py      # データダウンロード
│   │   ├── upload_to_gcs.py # GCSアップロード
│   │   └── load_to_bq.py    # BigQueryロード
│   ├── features/
│   │   ├── base_features.py
│   │   ├── target_encoding.py
│   │   └── feature_pipeline.py
│   ├── models/
│   │   ├── lgbm_ranker.py
│   │   ├── train.py
│   │   └── predict.py
│   ├── backtest/
│   │   ├── simulator.py
│   │   └── metrics.py
│   ├── api/
│   │   ├── prediction_service.py
│   │   └── dashboard.py
│   └── utils/
│       ├── config.py
│       ├── logger.py
│       └── notification.py
├── cloud_functions/         # Cloud Functions
│   ├── gcs_to_bq/
│   └── notification/
├── tests/                   # テストコード
│   ├── test_features.py
│   ├── test_models.py
│   └── test_backtest.py
├── config/                  # 設定ファイル
│   ├── bq_schema.json
│   └── model_config.yaml
├── requirements.txt
├── Dockerfile
├── .gitignore
├── CLAUDE.md               # 本仕様書
├── ML_FEATURE.md           # 特徴量設計
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

---

## 変更履歴

| 日付 | バージョン | 変更内容 | 担当者 |
|------|-----------|----------|--------|
| 2026-01-18 | 1.0.0 | 初版作成 | Claude |

---

**End of Document**
