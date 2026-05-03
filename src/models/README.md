# モデル学習・推論 (src/models/)

LightGBM LambdaRankによる競馬着順予測モデルの学習・推論パイプライン。

## ファイル構成

| ファイル | 概要 |
|---------|------|
| `lgbm_ranker.py` | LightGBM LambdaRankモデルクラス |
| `train.py` | 学習パイプライン |
| `predict.py` | 推論パイプライン |
| `tuning.py` | Optunaハイパーパラメータチューニング |
| `../../config/model_config.yaml` | モデル・学習・データ設定 |

---

## lgbm_ranker.py

LightGBM LambdaRankのラッパークラス。レース内の相対的な着順予測を行う。

### クラス構成

#### `LGBMRankerConfig`（データクラス）

モデルのハイパーパラメータと学習設定を保持する。

| フィールド | デフォルト値 | 説明 |
|-----------|-------------|------|
| `params` | lambdarank標準設定 | LightGBMパラメータ辞書 |
| `num_boost_round` | 1000 | 最大ブースティング回数 |
| `early_stopping_rounds` | 50 | 早期停止の待機ラウンド数 |
| `log_evaluation` | 100 | ログ出力間隔 |

デフォルトパラメータ:
- `objective`: lambdarank
- `metric`: ndcg (ndcg_eval_at: [3])
- `num_leaves`: 31
- `learning_rate`: 0.05
- `feature_fraction` / `bagging_fraction`: 0.8

#### `LGBMRanker`（メインクラス）

| メソッド | 引数 | 説明 |
|---------|------|------|
| `train()` | X_train, y_train, groups_train, X_valid, y_valid, groups_valid, categorical_feature | モデルを学習する。y_trainは二値ラベル（3着以内=1, それ以外=0） |
| `predict()` | X | 予測スコアを返す（高いほど上位予測） |
| `save()` | path | モデルを.txtファイル + .meta.jsonに保存 |
| `load()` | path | .txtファイルからモデルを読み込み |
| `feature_importance()` | importance_type | 特徴量重要度をDataFrameで返す |

### 保存形式

`save()` は2つのファイルを生成する:

- `model.txt`: LightGBMモデル本体（Boosterのテキスト形式）
- `model.meta.json`: メタデータ（特徴量名、best_iteration、パラメータ）

---

## train.py

BigQueryから `features.training_data` を取得し、時系列分割で学習・検証を行うパイプライン。

### 主要関数

#### `compute_week_boundaries(execution_date)`

実行日から推論対象の土曜・日曜を計算する。

- 月〜土に実行: 同じ週の土曜・日曜
- 日曜に実行: 前日の土曜・当日の日曜

#### `split_train_valid_predict(df, execution_date, validation_months)`

時系列分割でデータを3つに分ける。

```
|<--- 学習データ --->|<--- 検証データ --->|<--- 推論対象 --->|
                     ^                   ^                  ^
              valid_start           saturday           sunday
              (6ヶ月前)            (今週土曜)         (今週日曜)
```

- **推論対象**: 実行日の週の土曜・日曜
- **検証データ**: 推論対象直前の `validation_months` 分（デフォルト6ヶ月）
- **学習データ**: 検証期間より前の全データ

#### `build_feature_matrix(df, exclude_columns, categorical_columns)`

学習・推論共通の特徴量行列を構築する。`exclude_columns` で指定されたカラムを除外し、`categorical_columns` をcategory型に変換する。

#### `prepare_features(df, exclude_columns, categorical_columns)`

`build_feature_matrix` に加え、ラベル生成とグループサイズ計算を行う（学習用）。

ラベル変換ルール（二値ラベル）:
| 着順 | ラベル |
|------|--------|
| 1着 | 1 |
| 2着 | 1 |
| 3着 | 1 |
| 4着以下 | 0 |

#### `evaluate_predictions(y_true_positions, y_pred, groups)`

検証データでの評価指標を計算する。

- **NDCG@3**: 上位3頭のランキング品質
- **Recall@3**: 実際の3着以内の馬が予測上位3頭に含まれる割合
- **AUC**: 3着以内予測の二値分類性能（ROC-AUC）

#### `upload_model_to_gcs(project_id, local_path, ...)`

ローカルのモデルファイル（.txt + .meta.json）をGCSにアップロードする。
保存先: `gs://{project_id}-keiba-models/lgbm_ranker/{YYYYMMDD}/`

#### `train_pipeline(project_id, execution_date, config, ...)`

学習の全ステップを統合実行する:

1. BigQueryからデータ取得
2. 時系列分割（学習/検証/推論）
3. 特徴量・ラベル準備（二値ラベル: 3着以内=1, それ以外=0）
4. Optunaチューニング（オプション: `--tune`フラグ）
5. LGBMRankerで学習
6. 検証データで評価（NDCG@3, Recall@3, AUC）
7. モデルをローカルに保存
8. GCSにアップロード（オプション）
9. 特徴量重要度の出力

### CLI使用方法

```bash
# 基本実行
python3 -m src.models.train --project-id <PROJECT_ID>

# GCSアップロードなし（ローカルのみ）
python3 -m src.models.train --project-id <PROJECT_ID> --skip-gcs-upload --output-dir ./models

# Optunaハイパーパラメータチューニング
python3 -m src.models.train --project-id <PROJECT_ID> --tune --n-trials 50

# チューニング（タイムアウト付き）
python3 -m src.models.train --project-id <PROJECT_ID> --tune --tune-timeout 3600

# 実行日を指定（OOS評価モデルの作成に使用）
python3 -m src.models.train --project-id <PROJECT_ID> --execution-date 2025-01-06

# カスタム設定ファイル
python3 -m src.models.train --project-id <PROJECT_ID> --config ./my_config.yaml

# 詳細ログ
python3 -m src.models.train --project-id <PROJECT_ID> -v
```

> **`--execution-date` とデータ分割の関係**
>
> `--execution-date YYYY-MM-DD` を指定すると、学習/検証/推論期間が以下のように決まる:
>
> | 期間 | 範囲 |
> |---|---|
> | 推論対象 | 指定日の週の土日 |
> | 検証（Valid） | 推論対象の前日 〜 その6ヶ月前 |
> | 学習（Train） | それ以前の全データ |
>
> **OOS（アウトオブサンプル）評価用モデルを作る場合**: バックテストや戦略最適化で使いたいデータ期間よりも前の日付を `--execution-date` に指定すること。  
> 例: 2025年データで評価したい場合 → `--execution-date 2025-01-06`（検証期間が〜2025-01-06 で終わり、2025-01-11 以降がOOS）

> **`finish_position` ラベルの取得元**
>
> `features.training_data` の `finish_position` 列は常に NULL のため、学習ラベルは `raw.race_results` テーブルから直接 JOIN して取得する。  
> 推論時（未来のレース）はラベルが存在しないが、推論には `finish_position` を使用しないため問題ない。

---

## tuning.py

Optunaベイズ最適化によるLightGBMハイパーパラメータの自動チューニング。

### 主要関数

#### `create_objective(X_train, y_train, groups_train, X_valid, y_valid, groups_valid, categorical_features, base_params)`

Optunaの目的関数を作成する。検証データのNDCG@3を最大化するパラメータを探索する。

探索対象パラメータ:
- `num_leaves`: 決定木の葉の数（範囲: 15〜63）
- `learning_rate`: 学習率（範囲: 0.01〜0.2）
- `feature_fraction`: 各イテレーションで使用する特徴量の割合（範囲: 0.6〜0.95）
- `bagging_fraction`: 各イテレーションで使用するデータの割合（範囲: 0.6〜0.95）
- `min_child_samples`: 葉ノードに必要な最小サンプル数（範囲: 5〜50）

#### `tune_hyperparameters(X_train, y_train, groups_train, X_valid, y_valid, groups_valid, categorical_features, base_params, n_trials, timeout)`

ハイパーパラメータチューニングを実行する。

| 引数 | 説明 |
|------|------|
| `X_train`, `y_train`, `groups_train` | 学習データ |
| `X_valid`, `y_valid`, `groups_valid` | 検証データ |
| `categorical_features` | カテゴリカル特徴量のリスト |
| `base_params` | 固定パラメータ（objective, metric等） |
| `n_trials` | 探索回数（デフォルト: 100） |
| `timeout` | タイムアウト秒数（デフォルト: None） |

返却値:
- `best_params`: 最適パラメータ辞書
- `best_score`: 最高スコア（NDCG@3）
- `study`: Optunaのstudyオブジェクト

### 使用方法

`train.py` から `--tune` フラグで呼び出される:

```bash
# 50回の探索でチューニング
python3 -m src.models.train --project-id <PROJECT_ID> --tune --n-trials 50

# タイムアウト1時間でチューニング
python3 -m src.models.train --project-id <PROJECT_ID> --tune --tune-timeout 3600
```

---

## predict.py

学習済みモデルを使用して、今週末のレースの着順予測を行う。任意の日付を指定することも可能。

### 主要関数

#### `fetch_prediction_data(project_id, dataset, table, target_dates)`

BigQueryから推論対象日のデータを取得する。

#### `load_model_from_gcs(project_id, bucket_suffix, model_prefix, execution_date, local_dir)`

GCSからモデルファイルをダウンロードする。
取得元: `gs://{project_id}-keiba-models/lgbm_ranker/{YYYYMMDD}/`

#### `predict_pipeline(project_id, execution_date, config, model_path, target_dates=None)`

推論の全ステップを実行する。

| 引数 | 型 | デフォルト | 説明 |
|------|----|-----------|------|
| `project_id` | str | - | GCPプロジェクトID |
| `execution_date` | datetime.date | - | 実行日。`target_dates` 未指定時に週の土日を算出する基準日として使用する |
| `config` | dict | - | 設定辞書 |
| `model_path` | str | - | モデルファイルパス（ローカル） |
| `target_dates` | list[datetime.date] \| None | None | 推論対象日のリスト。指定した場合はその日付のみ対象とする。未指定の場合は `execution_date` の週の土曜・日曜を使用する |

実行ステップ:

1. ローカルからモデル読み込み
2. `target_dates` が未指定の場合は `compute_week_boundaries` で推論対象日（土日）を計算
3. BigQueryから対象日のデータを取得
4. `build_feature_matrix`（train.pyの共通関数）で特徴量準備
5. 予測スコアの算出
6. レース内での予測順位（`pred_rank`）を付与
7. 結果を整形して返却

返却DataFrameのカラム:

| カラム | 説明 |
|--------|------|
| `race_id` | レースID |
| `race_date` | レース日 |
| `horse_id` | 馬ID |
| `horse_number` | 馬番 |
| `venue_code` | 開催場コード（存在時） |
| `race_number` | レース番号（存在時） |
| `pred_score` | 予測スコア（高いほど上位予測） |
| `pred_rank` | レース内の予測順位 |
| `finish_position` | 実際の着順（存在時） |

#### `format_predictions(result_df)`

予測結果をテーブル形式の文字列に整形する。各レースごとに予測順位・馬番・スコア・着順を表示する。

### CLI使用方法

```bash
# 基本実行（今週の土日を自動で対象とする）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path ./models/lgbm_ranker_20260215.txt

# 実行日を指定（その週の土日が対象になる）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path <MODEL_PATH> --execution-date 2026-02-15

# 特定の1日を指定して推論（--target-dates）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path <MODEL_PATH> \
  --target-dates 2026-01-10

# 複数の任意日付を指定して推論（スペース区切りで複数指定可）
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path <MODEL_PATH> \
  --target-dates 2026-01-10 2026-01-11 2026-01-12

# CSV出力
python3 -m src.models.predict --project-id <PROJECT_ID> --model-path <MODEL_PATH> --output-csv predictions.csv
```

> **注意**: `--target-dates` を指定した場合、`--execution-date` は無視されます（推論対象日の決定に使用されません）。ただし `--execution-date` 自体は引数として受け付けます。

---

## 設定ファイル (config/model_config.yaml)

### セクション構成

| セクション | 内容 |
|-----------|------|
| `model.params` | LightGBMハイパーパラメータ |
| `model.training` | 学習設定（ブースティング回数、早期停止、検証期間） |
| `tuning` | Optunaチューニング設定（探索範囲、試行回数、タイムアウト） |
| `data` | BigQueryテーブル情報、除外カラム、カテゴリカルカラム |
| `gcs` | GCSバケット・プレフィックス設定 |
| `evaluation` | 評価指標の設定 |

### 除外カラム (exclude_columns)

特徴量として使用しないカラム:
- `race_id`, `horse_id`: 識別子
- `race_date`, `created_at`: 日付メタ情報
- `target_place`, `finish_position`: ラベル・ターゲット
- `venue_code`, `race_number`: メタ情報（推論結果に使用）
- `jockey_id`, `trainer_id`: ID情報（集計済み特徴量を使用）

---

## テスト

```bash
# モデル関連の全テストを実行
python3 -m pytest tests/test_lgbm_ranker.py tests/test_train.py tests/test_predict.py tests/test_tuning.py -v
```

テストファイル:
- `tests/test_lgbm_ranker.py`: LGBMRankerの単体テスト（学習・予測・保存・読み込み・エラー系）
- `tests/test_train.py`: 学習パイプラインのテスト（日付計算・分割・特徴量準備・評価・E2E）
- `tests/test_predict.py`: 推論パイプラインのテスト（正常系・空データ・結果整形）
- `tests/test_tuning.py`: Optunaチューニングのテスト（目的関数・パラメータ探索・統合テスト）
