# バックテストモジュール

このモジュールは、学習済みモデルの予測スコアを使って**複勝馬券の投資戦略**を過去データで検証するシミュレーターです。

---

## 投資戦略の3フェーズ

投資戦略は以下の3フェーズで構成されます。各フェーズで参照するテーブルと実行するコードは以下のとおりです。

---

### フェーズ1: パラメータ最適化

`scripts/run_strategy_optimization.py` を使い、グリッドサーチで最適なハイパーパラメータを探索して `config/strategy_config.yaml` に保存します。

| ステップ | 処理内容 | 参照テーブル |
|---------|---------|------------|
| 1 | 指定期間の特徴量を取得し、MLモデルで複勝率 (`win_place_prob`) を予測 | `features.training_data` |
| 2a | 単複オッズ取得（2段フォールバック） | ① `predictions.daily_odds` → ② `raw.odds` |
| 2b | コンボオッズ取得（3段フォールバック） | ① `predictions.daily_odds_combo` → ② `raw.combo_odds` → ③ `raw.payouts` |
| 3 | グリッドサーチで各パラメータ組み合わせの回収率を集計 | — |
| 4 | 払い戻し金額の算出 | `raw.payouts` |
| 5 | 回収率が最大となるパラメータを `config/strategy_config.yaml` に保存 | — |

**最適化対象パラメータ:**

| パラメータ | 説明 |
|-----------|------|
| `p1` | トップ1馬の突出度閾値（one_dominant パターン判定） |
| `expected_return_threshold` | 期待回収率フィルタ閾値（`win_place_prob × odds > threshold` で馬券選定） |
| `prob_weight_r` | 馬選定スコア係数（スコア = `odds × prob^r`） |

**実行コマンド:**

```bash
python3 scripts/run_strategy_optimization.py \
    --project-id <PROJECT_ID> \
    --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker_multi/20260301/model.txt \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
```

---

### フェーズ2: バックテスト

`scripts/run_backtest.py` を使い、フェーズ1で決定したパラメータを用いて指定期間の馬券購入をシミュレーションします。回収率・的中率・最大ドローダウン等をログ・CSV・BQに記録します。

| ステップ | 処理内容 | 参照テーブル |
|---------|---------|------------|
| 1 | 指定期間の特徴量・着順データを取得 | `features.training_data`, `raw.race_results` |
| 2 | MLモデルで複勝率を予測 | — |
| 3a | 単複オッズ取得（2段フォールバック） | ① `predictions.daily_odds` → ② `raw.odds` |
| 3b | コンボオッズ取得（3段フォールバック） | ① `predictions.daily_odds_combo` → ② `raw.combo_odds` → ③ `raw.payouts` |
| 4 | 全レースに対して馬券購入シミュレーションを実行 | — |
| 5 | 払い戻し金額を算出 | `raw.payouts` |
| 6 | 回収率・的中率・最大ドローダウン・シャープレシオを集計 | — |
| 7 | 結果をCSV・BQ・グラフに保存（任意） | `backtests.backtest_results` |

> フェーズ1と同じテーブル参照・同じオッズ取得ロジックを使用するため、最適化時と同条件でシミュレーションが行われます。

**実行コマンド:**

```bash
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path <MODEL_PATH> \
    --start-date 2023-01-01 \
    --end-date 2023-12-31
```

---

### フェーズ3: 当日投資戦略策定

`scripts/run_strategy.py` を使い、フェーズ1で決定したパラメータを用いて当日レースの推奨馬券を提示します。Cloud Scheduler から `POST /api/v1/strategy/daily` で毎朝 AM 9:00 に自動実行されます。

| ステップ | 処理内容 | 参照テーブル |
|---------|---------|------------|
| 1 | 当日の予測結果（複勝率）を取得 | `predictions.daily_predictions` |
| 2 | 単複オッズ取得 | `predictions.daily_odds`（当日スクレイプ済みデータのみ） |
| 3 | コンボオッズ取得（2段フォールバック） | ① `predictions.daily_odds_combo` → ② `raw.combo_odds` |
| 4 | 推奨馬券を選定し賭け金を配分 | — |
| 5 | 投資判断結果をBQに保存 | `predictions.investment_decisions` |

> **前提条件（Cloud Scheduler の実行順序）:**
> - AM 8:00 `POST /api/v1/predict/daily` → `predictions.daily_predictions` に当日データが格納済み
> - AM 8:30 `POST /api/v1/odds/scrape` → `predictions.daily_odds` / `predictions.daily_odds_combo` に当日データが格納済み
> - AM 9:00 `POST /api/v1/strategy/daily` → 上記2テーブルを参照して投資判断を実行
>
> `predictions.daily_odds` は当日スクレイプしたオッズのみ保持します。フェーズ1・2とは異なり `raw.odds`（JRDB）へのフォールバックはありません。スクレイプが未完了の場合、単複オッズが取得できない馬は馬券選定からスキップされます。

**実行コマンド:**

```bash
# 当日分（Cloud Schedulerと同等の手動実行）
python3 scripts/run_strategy.py --project-id <PROJECT_ID>

# 結果確認のみ（BQ保存をスキップ）
python3 scripts/run_strategy.py --project-id <PROJECT_ID> --dry-run
```

---

### オッズ取得ロジックのフェーズ比較

| フェーズ | 単複オッズ（place_odds） | コンボオッズ（ワイド/三連複/馬連） |
|---------|----------------------|-------------------------------|
| フェーズ1・2（最適化・バックテスト） | ① `predictions.daily_odds` → ② `raw.odds` | ① `predictions.daily_odds_combo` → ② `raw.combo_odds` → ③ `raw.payouts` |
| フェーズ3（当日戦略） | `predictions.daily_odds` のみ（フォールバックなし） | ① `predictions.daily_odds_combo` → ② `raw.combo_odds` |

`predictions.*` テーブルは netkeba からスクレイプしたリアルタイムオッズ、`raw.*` テーブルは JRDB から取得した基準オッズです。詳細は [BigQueryテーブル構成](../../README.md#オッズテーブルの設計意図jrdb系-vs-netkeiba系) を参照してください。

---

## 投資戦略の概要

現在の投資戦略はレースパターン分類（突出型 / 標準型）に基づき、複勝・ワイド・三連複・馬連を組み合わせて購入します。1レースあたりの予算は `budget_per_race`（固定3,000円）で、オッズ逆数比率方式で配分します。

詳細な設計仕様は [STRATEGY.md](../../STRATEGY.md) を参照してください。

---

### 払戻金の計算

| 状況 | 計算方法 |
|------|---------|
| 的中 かつ `raw.payouts` データあり | `賭け金 × (payout_amount / 100)` ← **実際の払戻金額を使用** |
| 的中 かつ `raw.payouts` データなし | `賭け金 × odds` （オッズで代替推定） |
| 不的中 | 払戻なし（0円） |

`raw.payouts` のデータがある場合は実際の払戻金額（JRDBのHJBデータ）を優先します。

---

## 評価指標

| 指標 | 計算式 | 意味 |
|------|--------|------|
| **回収率** | 合計払戻 ÷ 合計賭け金 × 100 (%) | 100%超えが目標 |
| **的中率** | 的中数 ÷ 総賭け数 × 100 (%) | 目安30〜40% |
| **最大ドローダウン** | ピーク資金からの最大下落率 (%) | 小さいほどリスクが低い |
| **シャープレシオ** | 週次超過リターン平均 ÷ 標準偏差 × √52 | 1.0以上が目安 |

---

## パラメータ一覧

`config/strategy_config.yaml` の主要パラメータ:

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `budget_per_race` | 3,000 | 1レースあたり固定予算（円） |
| `expected_return_threshold` | 1.2 | 期待回収率フィルタ（`prob × odds > 閾値` の馬のみ選定） |
| `p1` | 0.5 | 突出型判定の補正ジニ係数閾値 |
| `min_bet_amount` | 100 | 最低賭け金（円） |

---

## ファイル構成

```
src/backtest/
├── __init__.py        # パッケージ初期化・公開API
├── simulator.py       # BacktestSimulator, kelly_criterion, fractional_kelly
├── metrics.py         # recovery_rate, hit_rate, max_drawdown, sharpe_ratio
└── README.md          # このファイル
```

---

## 使用方法

CLIからの実行は `scripts/run_backtest.py` を参照してください。

```bash
python scripts/run_backtest.py \
    --project-id <PROJECT_ID> \
    --model-path <MODEL_PATH> \
    --start-date 2023-01-01 \
    --end-date 2023-12-31
```

投資戦略のパラメータ最適化:

```bash
python3 scripts/run_strategy_optimization.py \
    --project-id <PROJECT_ID> \
    --model-path <MODEL_PATH> \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
```
