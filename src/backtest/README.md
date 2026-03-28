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
    --model-path gs://<PROJECT_ID>-keiba-models/lgbm_ranker/20260301/model.txt \
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

### 対象馬券
**複勝（place bet）** のみ。3着以内に入ると的中となる馬券。

### 戦略の骨子
「モデルが予測した複勝率とオッズを掛け合わせた期待回収率が高い馬だけを買い、賭け金はKelly基準で決める」

---

## 馬券購入の判断フロー

1レース（例: 18頭立て）で以下のフローを全頭に対して実行します。

```
全頭ループ
  ↓
① オッズが有効か？ (NaN・0 でない)
  → No → スキップ
  ↓ Yes
② 期待回収率フィルタを通過するか？
   期待回収率 = win_place_prob × odds > 1.2 (デフォルト)
  → No → スキップ（期待値がマイナスの馬は買わない）
  ↓ Yes
③ Fractional Kelly で賭け金を決定
④ 賭け金が上限・資金を超えていないか確認
  → 超過 → スキップ
  ↓
⑤ 購入 → レース結果を確認 → 資金を更新
```

---

## ステップ詳細

### ① 期待回収率フィルタ

```
期待回収率 = win_place_prob × odds
```

- `win_place_prob`: モデルが予測した複勝率（0〜1）
  - 水充填アルゴリズムで各馬に配分（合計 ≦ 3）
- `odds`: レース前日のオッズ（`odds_yesterday` カラム）
- デフォルト閾値: **1.2**（期待値が20%以上プラスの馬のみ購入）

**例:**
| 馬名 | 複勝率 (予測) | オッズ | 期待回収率 | 購入? |
|------|------------|-------|----------|------|
| A馬  | 0.55       | 2.5   | 1.375    | ✅ 買う |
| B馬  | 0.30       | 3.8   | 1.140    | ❌ 見送り |
| C馬  | 0.20       | 8.0   | 1.600    | ✅ 買う |
| D馬  | 0.10       | 4.0   | 0.400    | ❌ 見送り |

同一レース内で複数頭が閾値を超えた場合は**全頭購入**します（1頭に絞るロジックはありません）。

---

### ② Kelly基準による賭け金計算

**Kelly基準**とは、長期的に資産成長を最大化する最適賭け金比率を求める数式です。

```
f* = (p × (odds - 1) - (1 - p)) / (odds - 1)
```

- `p` = 予測複勝率
- `odds` = 複勝オッズ
- `f*` がプラスのとき＝期待値プラス → 賭ける価値あり
- `f*` がマイナスのとき＝期待値マイナス → 賭けない（0として扱う）

**Fractional Kelly（実際に使用）**

フルKelly値はリスクが高いため、係数（デフォルト `0.25`）を掛けて保守的にします。

```
賭け金比率 = f* × fraction (= f* × 0.25)
賭け金     = 現在の資金 × 賭け金比率
```

**具体例** (資金100,000円、複勝率0.55、オッズ2.5の場合):
```
f*         = (0.55 × 1.5 - 0.45) / 1.5 ≈ 0.25
Frac.Kelly = 0.25 × 0.25 = 0.0625
賭け金     = 100,000 × 0.0625 = 6,250円 → 6,200円（100円単位切捨て）
```

---

### ③ 賭け金の上限・最低額制御

計算された賭け金に以下の制約を適用します。

| 制約 | デフォルト値 | 説明 |
|------|------------|------|
| 1レースあたり上限 | 資金の **5%** | 1頭あたり上限。連続損失時の破産を防ぐ |
| 最低賭け金 | **100円** | 100円未満は100円に切り上げ |
| 100円単位 | - | 小数点以下は切り捨て |
| 資金超過チェック | - | 賭け金が現資金を超える場合はスキップ |

---

### ④ 払戻金の計算

| 状況 | 計算方法 |
|------|---------|
| 3着以内 かつ `raw.payouts` データあり | `賭け金 × (payout_amount / 100)` ← **実際の払戻金額を使用** |
| 3着以内 かつ `raw.payouts` データなし | `賭け金 × odds` （オッズで代替推定） |
| 4着以下 | 払戻なし（0円） |

`raw.payouts` のデータがある場合は実際の払戻金額（JRDBのHJBデータ）を優先します。これにより、同じオッズでも実際の払戻額が異なるケース（オッズ変動など）を正確に再現できます。

---

## 評価指標

バックテスト後、以下の指標で戦略を評価します。

| 指標 | 計算式 | 意味 |
|------|--------|------|
| **回収率** | 合計払戻 ÷ 合計賭け金 × 100 (%) | 100%超えが目標 |
| **的中率** | 的中数 ÷ 総賭け数 × 100 (%) | 複勝なので一般的に30〜40%が目安 |
| **最大ドローダウン** | ピーク資金からの最大下落率 (%) | 小さいほどリスクが低い |
| **シャープレシオ** | 週次超過リターン平均 ÷ 標準偏差 × √52 | リスク調整後リターン。1.0以上が目安 |

---

## パラメータ一覧

`BacktestSimulator` に渡せるパラメータと、その値による挙動の変化です。

| パラメータ | デフォルト | 説明 | 大きくすると | 小さくすると |
|-----------|----------|------|------------|------------|
| `expected_return_threshold` | 1.2 | 期待回収率フィルタ | 購入機会が減り精度重視 | 購入機会が増えリスク増大 |
| `kelly_fraction` | 0.25 | Kelly係数 | 賭け金大・リターン/リスク増 | 賭け金小・安定的 |
| `max_bet_ratio` | 0.05 | 1レース最大賭け金比率 | 大勝負可能・破産リスク増 | 安定的・低リターン |
| `initial_capital` | 100,000 | 初期資金 (円) | - | - |

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

```python
from src.backtest import BacktestSimulator, compute_metrics

# シミュレーター初期化
sim = BacktestSimulator(
    initial_capital=100_000,
    kelly_fraction=0.25,
    expected_return_threshold=1.2,
    max_bet_ratio=0.05,
    odds_column="odds_yesterday",
)

# バックテスト実行
history_df = sim.run(
    predictions_df=predictions_df,  # モデル予測結果
    payouts_df=payouts_df,          # 実際の払戻データ（optional）
)

# 評価指標計算
metrics = compute_metrics(history_df, initial_capital=100_000)
print(f"回収率: {metrics['recovery_rate']:.1f}%")
print(f"的中率: {metrics['hit_rate']:.1f}%")
print(f"最大DD: {metrics['max_drawdown']:.1f}%")
print(f"シャープ: {metrics['sharpe_ratio']:.3f}")
```

CLIからの実行は `scripts/run_backtest.py` を参照してください。
