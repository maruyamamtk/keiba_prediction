# モデル学習・推論 (src/models/)

LightGBM による競馬着順予測モデルの学習・推論パイプライン。
3つのモデルをアンサンブルして最終ランクスコアと複勝確率を生成する**ハイブリッドアンサンブル方式**を採用。

---

## アーキテクチャ概要

```
BigQuery (features.training_data)
        ↓
┌───────────────────────────────────────────┐
│             3モデル並列学習               │
│                                           │
│  ① ranker_multi   LambdaRank (多値ラベル) │
│  ② regression     着差Zスコア回帰         │
│  ③ classifier     複勝確率 二値分類       │
└───────────────────────────────────────────┘
        ↓ ensemble_rank_scores()
┌───────────────────────────────────────────┐
│  final_rank_score = multi×0.7 + reg×0.3  │
│  win_place_prob   = classifier 確率出力   │
└───────────────────────────────────────────┘
        ↓
  BigQuery (predictions.daily_predictions)
```

---

## ファイル構成

| ファイル | 概要 |
|---------|------|
| `lgbm_base.py` | 基底クラス `LGBMModelBase`（train/predict/save/load の共通実装） |
| `lgbm_ranker.py` | `LGBMRanker` — 二値ラベル LambdaRank（旧モデル・後方互換） |
| `lgbm_ranker_multi.py` | `LGBMRankerMulti` — JRA賞金ウェイト多値ラベル LambdaRank |
| `lgbm_regression.py` | `LGBMRegression` — 着差Zスコア回帰（RMSE最適化） |
| `lgbm_classifier.py` | `LGBMClassifier` — 複勝確率 二値分類（AUC最適化） |
| `ensemble.py` | `ensemble_rank_scores()` — ranker_multi + regression のアンサンブル |
| `train.py` | 学習パイプライン（3モデル対応） |
| `predict.py` | 推論パイプライン（3モデルアンサンブル対応） |
| `tuning.py` | Optuna ハイパーパラメータチューニング（4モデル対応） |
| `../../config/model_config.yaml` | モデル・学習・データ・チューニング設定 |

---

## モデルクラス

### 基底クラス: `LGBMModelBase` (`lgbm_base.py`)

全モデルが継承する基底クラス。以下のメソッドを共通実装。

| メソッド | 説明 |
|---------|------|
| `train(X_train, y_train, X_valid, y_valid, ...)` | モデル学習（groups はランカー系のみ必須） |
| `predict(X)` | 予測スコアを返す |
| `save(path, training_period)` | `.txt` + `.meta.json` に保存 |
| `load(path)` | `.txt` からモデルを復元 |
| `feature_importance()` | 特徴量重要度を DataFrame で返す |

`.meta.json` に保存される情報:
- `feature_names`: 学習時の特徴量名リスト
- `best_iteration`: Early Stopping の停止ラウンド
- `params`: 使用したハイパーパラメータ
- `training_period`: 学習・検証期間（`train_from`/`train_to`/`valid_from`/`valid_to`）

---

### `LGBMRankerMulti` (`lgbm_ranker_multi.py`)

JRA賞金ウェイトをラベルとした多値 LambdaRank モデル。

**ラベル設定（`JRA_PRIZE_WEIGHTS`）:**

| 着順 | ラベル値 |
|------|---------|
| 1着 | 120 |
| 2着 | 90 |
| 3着 | 70 |
| 4着 | 15 |
| 5着 | 10 |
| 6着 | 8 |
| 7着 | 7 |
| 8着 | 6 |
| 9着 | 4 |
| 10着 | 2 |
| 11着以下 | 0 |

`label_gain[i]=i`（0〜120）により、ラベル値がそのまま NDCG ゲインとして機能。

---

### `LGBMRegression` (`lgbm_regression.py`)

1着馬との走破タイム差をレース内σで正規化した **着差Zスコア** を予測する回帰モデル。

```
time_diff = 各馬の finish_time − 1着馬の finish_time  (≥ 0)
zscore    = −time_diff / レース内 std                 (高いほど強い)
```

- 1着馬: ≈ 0（最大値）
- 大敗馬: 大きな負の値
- `finish_time` が NULL の馬・std==0 のレースは学習から除外

---

### `LGBMClassifier` (`lgbm_classifier.py`)

3着以内（1）／それ以外（0）の二値分類で **複勝確率を直接推定**するモデル。  
`predict()` が [0, 1] の確率を返し、期待値計算のキャリブレーション入力として使用する。

---

### アンサンブル: `ensemble_rank_scores()` (`ensemble.py`)

ranker_multi と regression のスコアを正規化してアンサンブルする。

```python
final_rank_score = weight_multi × rank_multi_normalized
                 + (1 − weight_multi) × rank_regression_normalized
```

デフォルト重み: `weight_multi=0.7`（`config/model_config.yaml` の `ensemble.weight_multi`）

片方のみ指定した場合はそのスコアをそのまま使用。

---

## train.py — 学習パイプライン

### データ分割

```
|<------- 学習データ ------->|<-- 検証データ -->|<-- 推論対象 -->|
                              ^                 ^               ^
                         valid_start        saturday         sunday
                         (6ヶ月前)         (今週土曜)       (今週日曜)
```

### ラベル生成関数

| 関数 | モデル | ラベル |
|------|--------|-------|
| `prepare_features()` | ranker（旧） | 二値（3着以内=1） |
| `prepare_features_multi_label()` | ranker_multi | JRA賞金ウェイト（0〜120） |
| `prepare_features_regression()` | regression | 着差Zスコア（float）+ NaN行除去済み df |
| `prepare_features()` | classifier | 二値（3着以内=1） |

### 評価指標

| 指標 | ranker_multi | regression | classifier |
|------|:---:|:---:|:---:|
| NDCG@3 | ✅ | ✅ | ✅ |
| Recall@3 | ✅ | ✅ | ✅ |
| AUC | ✅ | ✅ | ✅ |
| RMSE | — | ✅ | — |

regression の NDCG@3 / Recall@3 は着差Zスコア予測値を降順ソートしてランキング評価。

### CLI 使用方法

#### 基本実行（モデル種別を `--model-type` で指定）

```bash
# 多値ランク学習モデル（メイン）
.venv/bin/python -m src.models.train \
    --model-type multi \
    --project-id keiba-prediction-1768734113

# 着差回帰モデル
.venv/bin/python -m src.models.train \
    --model-type regression \
    --project-id keiba-prediction-1768734113

# 二値分類モデル（複勝確率）
.venv/bin/python -m src.models.train \
    --model-type classifier \
    --project-id keiba-prediction-1768734113
```

#### Optuna チューニングあり（本番推奨）

```bash
.venv/bin/python -m src.models.train \
    --model-type multi \
    --tune \
    --n-trials 100 \
    --tune-timeout 3600 \
    --project-id keiba-prediction-1768734113
```

#### 3モデルを順番に学習する（本番フロー）

```bash
for MODEL in multi regression classifier; do
  .venv/bin/python -m src.models.train \
      --model-type $MODEL \
      --tune \
      --project-id keiba-prediction-1768734113
done
```

#### GCSアップロードせずローカルのみ（精度確認用）

```bash
.venv/bin/python -m src.models.train \
    --model-type multi \
    --tune \
    --n-trials 20 \
    --tune-timeout 900 \
    --skip-gcs-upload \
    --project-id keiba-prediction-1768734113
```

#### 主なオプション

| オプション | 説明 | デフォルト |
|---|---|---|
| `--model-type` | `multi` / `regression` / `classifier` / `ranker` | `ranker` |
| `--project-id` | GCPプロジェクトID | `$GCP_PROJECT_ID` |
| `--tune` | Optuna チューニングを有効化 | なし |
| `--n-trials` | Optuna の trial 数 | config 依存（100） |
| `--tune-timeout` | チューニングタイムアウト（秒） | config 依存（3600） |
| `--output-dir` | ローカル出力ディレクトリ | 一時ディレクトリ |
| `--skip-gcs-upload` | GCS アップロードをスキップ | なし（常にアップロード） |
| `--execution-date` | 実行日（データ分割基準） | 今日 |

### GCS 保存先

| モデル | パス |
|--------|------|
| ranker_multi | `gs://{PROJECT_ID}-keiba-models/lgbm_ranker_multi/{YYYYMMDD}/lgbm_ranker_multi_{YYYYMMDD}.txt` |
| regression | `gs://{PROJECT_ID}-keiba-models/lgbm_regression/{YYYYMMDD}/lgbm_regression_{YYYYMMDD}.txt` |
| classifier | `gs://{PROJECT_ID}-keiba-models/lgbm_classifier/{YYYYMMDD}/lgbm_classifier_{YYYYMMDD}.txt` |

---

## predict.py — 推論パイプライン

### 主要関数

#### `predict_pipeline(project_id, execution_date, config, model_path, model_path_multi, model_path_regression, model_path_classifier, target_dates, ...)`

| 引数 | 説明 |
|------|------|
| `model_path` | 旧 LambdaRank モデルパス（後方互換用） |
| `model_path_multi` | ranker_multi モデルパス（GCS URI またはローカル） |
| `model_path_regression` | regression モデルパス |
| `model_path_classifier` | classifier モデルパス |
| `target_dates` | 推論対象日リスト（未指定時は実行日の週の土日） |

**動作ロジック:**

1. 指定モデルをロード（GCS URI は自動ダウンロード）
2. BigQuery から対象日のデータを取得
3. 特徴量行列を構築
4. 各モデルで予測スコアを算出
5. `ensemble_rank_scores()` で `final_rank_score` を生成（multi + regression が揃う場合）
6. `calibrate_place_prob()` で `win_place_prob`（複勝確率）を生成（classifier 指定時）
7. レース内順位（`pred_rank`）を付与

**出力 DataFrame の主要カラム:**

| カラム | 説明 |
|--------|------|
| `race_id` | レースID |
| `race_date` | レース日 |
| `horse_number` | 馬番 |
| `final_rank_score` | アンサンブルランクスコア（高いほど上位予測） |
| `win_place_prob` | 複勝確率（0〜1、classifier 指定時） |
| `rank_score_multi` | ranker_multi 単体スコア |
| `rank_score_regression` | regression 単体スコア |
| `classifier_prob` | classifier 生確率 |
| `pred_rank` | レース内の予測順位 |

### CLI 使用方法

#### 3モデルアンサンブル推論（本番推奨）

```bash
.venv/bin/python -m src.models.predict \
    --project-id keiba-prediction-1768734113 \
    --model-path-multi \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/20260615/lgbm_ranker_multi_20260615.txt \
    --model-path-regression \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_regression/20260615/lgbm_regression_20260615.txt \
    --model-path-classifier \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_classifier/20260615/lgbm_classifier_20260615.txt \
    --target-dates 2026-06-14 2026-06-15
```

#### 特定日付を指定して過去推論（バックテスト・確認用）

```bash
.venv/bin/python -m src.models.predict \
    --project-id keiba-prediction-1768734113 \
    --model-path-multi \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/20260615/lgbm_ranker_multi_20260615.txt \
    --model-path-regression \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_regression/20260615/lgbm_regression_20260615.txt \
    --model-path-classifier \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_classifier/20260615/lgbm_classifier_20260615.txt \
    --target-dates 2026-06-14
```

#### ranker_multi 単体のみ（regression/classifier なし）

```bash
.venv/bin/python -m src.models.predict \
    --project-id keiba-prediction-1768734113 \
    --model-path-multi \
        gs://keiba-prediction-1768734113-keiba-models/lgbm_ranker_multi/20260615/lgbm_ranker_multi_20260615.txt \
    --target-dates 2026-06-14
```

#### CSV 出力 / BigQuery 保存

```bash
.venv/bin/python -m src.models.predict \
    --project-id keiba-prediction-1768734113 \
    --model-path-multi  gs://.../lgbm_ranker_multi_20260615.txt \
    --model-path-regression  gs://.../lgbm_regression_20260615.txt \
    --model-path-classifier  gs://.../lgbm_classifier_20260615.txt \
    --target-dates 2026-06-14 \
    --output-csv predictions_20260614.csv \
    --save-to-bq
```

#### 主なオプション

| オプション | 説明 | デフォルト |
|---|---|---|
| `--model-path-multi` | ranker_multi モデルパス（GCS URI / ローカル） | なし |
| `--model-path-regression` | regression モデルパス | なし |
| `--model-path-classifier` | classifier モデルパス | なし |
| `--model-path` | 旧 LambdaRank モデルパス（後方互換） | なし |
| `--target-dates` | 推論対象日（複数指定可） | 実行日の週の土日 |
| `--execution-date` | 実行日（target-dates 未指定時の基準） | 今日 |
| `--output-csv` | CSV 出力パス | なし |
| `--save-to-bq` | BigQuery に保存 | なし |
| `--force-sql` | feature SQL を直接実行（BQ キャッシュ不使用） | なし |

> **注意**: `--model-path-multi`, `--model-path-regression`, `--model-path-classifier` のうち少なくとも1つは必須。

---

## tuning.py — Optuna チューニング

### 対応モデル

| `model_type` | 評価関数 | 最適化方向 |
|---|---|---|
| `ranker` | AUC（二値ラベル y_valid） | 最大化 |
| `ranker_multi` | AUC（weight≥70 を1に二値化） | 最大化 |
| `regression` | −RMSE | 最大化（= RMSE 最小化） |
| `classifier` | AUC | 最大化 |

### 探索パラメータ（共通）

| パラメータ | 範囲 |
|---|---|
| `num_leaves` | 15〜127 |
| `learning_rate` | 0.01〜0.3（log スケール） |
| `feature_fraction` | 0.4〜1.0 |
| `bagging_fraction` | 0.4〜1.0 |
| `bagging_freq` | 1〜10 |
| `min_child_samples` | 5〜100 |
| `reg_alpha` | 1e-8〜10.0（log） |
| `reg_lambda` | 1e-8〜10.0（log） |

`model_config.yaml` の `tuning.search_space` で変更可能。

---

## 設定ファイル (`config/model_config.yaml`)

| セクション | 内容 |
|-----------|------|
| `model.params` | LightGBM 共通ハイパーパラメータ |
| `model.training` | num_boost_round / early_stopping_rounds / validation_months |
| `tuning` | n_trials / timeout / search_space（共通） |
| `tuning.ranker_multi` | study_name 上書き |
| `tuning.regression` | study_name 上書き |
| `tuning.classifier` | study_name 上書き |
| `data` | BigQuery 情報・除外カラム・カテゴリカルカラム |
| `gcs` | バケット・プレフィックス設定 |
| `ensemble` | `weight_multi`（ranker_multi の重み、デフォルト 0.7） |

---

## テスト

```bash
# モデル関連の全テストを実行
.venv/bin/pytest tests/test_lgbm_ranker.py \
                 tests/test_train.py \
                 tests/test_predict.py \
                 tests/test_tuning.py -v
```

| テストファイル | 内容 |
|---|---|
| `tests/test_lgbm_ranker.py` | LGBMRanker の単体テスト（学習・予測・保存・読み込み） |
| `tests/test_train.py` | 学習パイプラインのテスト（日付計算・分割・特徴量準備・評価・regression ランキング評価） |
| `tests/test_predict.py` | 推論パイプラインのテスト（正常系・空データ・結果整形） |
| `tests/test_tuning.py` | Optuna チューニングのテスト（目的関数・パラメータ探索・統合テスト） |
