# 投資戦略 設計仕様書

このドキュメントは `src/backtest/strategy.py` および `src/backtest/strategy_optimizer.py` に実装された投資戦略ロジックを完全に説明する。

> **重要な設計変更（履歴）**
> - **レースパターン分類（突出型/標準型）は廃止**（全レースを統一ロジックで処理）。`top_n`・`prob_weight_r` のパターン別切り替えも廃止。
> - **`prob_weight_r` は 1.0 固定・最適化探索対象外**（アイソトニック校正 Issue #416 / Issue #417）。校正後は `odds × prob` がそのまま真の期待回収率（EV）であり、純EV順（r=1）が原理的に正解。
> - **三連複は本番で除外**（`enabled_bet_types=["place", "wide", "umaren"]`、Issue #411）。コードは三連複も生成可能だが本番設定では無効。
> - **馬連はワイド選定組に一律で自動追加**（旧「突出型のみのパターンA馬券」から変更）。

---

## 目次

1. [全体フロー](#1-全体フロー)
2. [入力データ仕様](#2-入力データ仕様)
3. [ベース馬券選定（複勝・ワイド・馬連・三連複）](#3-ベース馬券選定)
4. [賭け金配分](#4-賭け金配分)
5. [的中判定と払戻計算](#5-的中判定と払戻計算)
6. [パラメータ最適化（Optuna）](#6-パラメータ最適化optuna)
7. [設定パラメータ一覧](#7-設定パラメータ一覧)
8. [数値例](#8-数値例)

---

## 1. 全体フロー

1レースに対して以下の順序で処理する。エントリポイントは `select_bets_for_race()` 関数。
**パターン分類は行わず、全レースを同一ロジックで処理する。**

```
入力:
  race_df        -- 当該レースの予測データ（全馬）
  combo_odds_df  -- コンボ馬券オッズ（ワイド/馬連/三連複）

         ↓
【前処理】
  NaN オッズ・0以下オッズの馬を除外
  有効馬が3頭未満 → ValueError

         ↓
【ベース馬券選定】select_base_bets()
  選定スコア = place_odds × win_place_prob ^ prob_weight_r   （prob_weight_r = 1.0 固定）
  min_prob_threshold フィルタ（prob × N/18 ≥ min_prob_threshold）を全馬券種共通で適用
  複勝:   フィルタ通過馬のうち prob × place_odds > expected_return_threshold
  ワイド: フィルタ通過後スコア上位 top_n 頭の2頭組で
          prob_i × prob_j × wide_odds > expected_return_threshold（max_wide_odds 以下のみ）
  馬連:   ワイドが選定された組み合わせに一律で自動追加（期待値フィルタなし）
  三連複: enabled_bet_types に含まれる場合のみ生成（本番は無効）
  ※ enabled_bet_types で購入対象券種を制御（本番: place / wide / umaren）

         ↓
【重複排除】
  同一 (bet_type, 馬番集合) の馬券を先着優先で1点に集約

         ↓
【賭け金配分】_allocate_bets()
  全選定馬券にオッズ逆数比率で budget_per_race を配分
  100円単位切り捨て後、min_bet_amount 未満の馬券を除外

         ↓
出力: bets（賭け金付き馬券リスト）
```

---

## 2. 入力データ仕様

### race_df（必須カラム）

| カラム名 | 型 | 説明 |
|---|---|---|
| `horse_id` | str | 馬ID |
| `horse_number` | int | 馬番 |
| `win_place_prob` | float | 複勝率（0〜1）。校正済み（アイソトニック→温度）。|
| `odds` | float | 複勝オッズ（`place_odds` から `rename` して使用） |
| `finish_position` | int | 実際の着順（バックテスト時）|
| `win_odds` | float | 単勝オッズ（オプション）|

### combo_odds_df（オプション。None または空の場合は複勝のみ）

| カラム名 | 型 | 説明 |
|---|---|---|
| `race_id` | str | レースID |
| `bet_type` | str | 馬券種（`"wide"` / `"umaren"` / `"sanrenpuku"`）|
| `horse_number_1` | int | 馬番1 |
| `horse_number_2` | int | 馬番2 |
| `horse_number_3` | int | 馬番3（三連複のみ）|
| `odds_value` | float | オッズ |

---

## 3. ベース馬券選定

### 実装: `select_base_bets(race_df, combo_odds_df, expected_return_threshold, top_n, min_prob_threshold, prob_weight_r, max_wide_odds, enabled_bet_types)`

全レース共通の単一ロジック。`enabled_bet_types` で購入対象券種を制御する
（本番設定: `["place", "wide", "umaren"]` → 三連複除外）。

---

### 3-1. 選定スコアの計算

```python
selection_score = place_odds × win_place_prob ^ prob_weight_r   # prob_weight_r = 1.0 固定
```

全馬をこのスコアの**降順**にソートする。`prob_weight_r = 1.0` 固定のため、スコアは
`odds × prob`（＝校正後の期待回収率 EV）と一致する。

#### prob_weight_r を 1.0 固定にした理由（Issue #416 / #417）

アイソトニック校正により `win_place_prob` が実際の複勝圏内率に一致するため、
`odds × prob` がそのまま真の期待回収率になる。したがって純EV順（r=1）が原理的に正しく、
期待値フィルタ（`prob × odds > threshold`）と選定スコアの指数も一致して戦略が解釈しやすくなる。

---

### 3-2. min_prob_threshold による候補馬フィルタ（全馬券種共通）

`min_prob_threshold` は **複勝・ワイド・馬連・三連複すべての馬券種** の候補馬に適用される（PR #245）。
低確率馬はいかなる馬券にも混入しない。

```python
N = len(race_df)   # 出走頭数

# 18頭基準に換算して比較: 少頭数では理論複勝率が高くなるバイアスを補正（Issue #208）
if min_prob_threshold > 0:
    candidates_df = sorted_df[sorted_df["win_place_prob"] * N / 18 >= min_prob_threshold]
else:
    candidates_df = sorted_df

# フィルタ通過馬の上位 top_n 頭がワイド・馬連・三連複の候補となる
top_candidates = candidates_df.head(top_n)
```

#### 出走頭数補正の意味（Issue #208）

`prob × N/18` は「18頭立て換算での理論複勝率」を表す。

| 出走頭数 N | 3頭中3着以内の理論確率 | 補正後（×N/18） |
|---|---|---|
| 6 | 50% | 17% |
| 12 | 25% | 17% |
| 18 | 17% | 17% |

18頭換算後の確率が `min_prob_threshold` を下回る馬は除外され、少頭数レースでの過剰な買いを防ぐ。

---

### 3-3. 複勝（place）の選定

候補はスコア順ではなく**全馬**が対象。ただし `min_prob_threshold` フィルタを通過した馬のみ。

```python
for each horse in sorted_df（スコア降順）:
    if win_place_prob × N/18 < min_prob_threshold:
        continue
    if win_place_prob × place_odds > expected_return_threshold:
        ✓ 複勝を選定
```

---

### 3-4. ワイド（wide）の選定

候補は **`min_prob_threshold` フィルタ通過後のスコア上位 `top_n` 頭** の中の2頭組み合わせのみ。

```python
for each wide bet in combo_odds_df where bet_type == "wide":
    h1, h2 = 馬番
    if h1 not in top_n candidates or h2 not in top_n candidates:
        continue
    if max_wide_odds is not None and wide_odds > max_wide_odds:
        continue   # 高オッズワイドを除外（馬連も連動してスキップ）
    if prob(h1) × prob(h2) × wide_odds > expected_return_threshold:
        ✓ ワイドを選定（"wide" が有効な場合のみ購入。ペアは馬連選定に常に使用）
```

---

### 3-5. 馬連（umaren）の選定: ワイドと同一組み合わせを一律自動追加

ワイドで選定された馬番ペアに対し、馬連を**一律で自動追加**する（旧「突出型のみのパターンA馬券」は廃止）。

```python
# ワイドで選定したペアに対応する馬連を購入
for (h1, h2) in selected_wide_pairs:
    umaren_row = combo_odds_df[bet_type=="umaren" AND horse_numbers==(h1, h2)]
    if found:
        ✓ 馬連を1点自動購入（期待値フィルタなし）
```

**特性**:
- ワイドが0件 → 馬連も0件 / ワイドがn件 → 馬連も最大n件
- ワイドと馬連は**常に同じ馬番ペア**（`max_wide_odds` によるワイドのスキップは馬連にも連動）
- `umaren` が `enabled_bet_types` に含まれる場合のみ生成

---

### 3-6. 三連複（sanrenpuku）の選定 ※本番は無効

候補は **`min_prob_threshold` フィルタ通過後のスコア上位 `top_n` 頭** の中の3頭組み合わせのみ。
`enabled_bet_types` に `"sanrenpuku"` が含まれる場合のみ生成され、**本番設定では除外**（Issue #411）。

```python
for each sanrenpuku bet in combo_odds_df where bet_type == "sanrenpuku":
    h1, h2, h3 = 馬番
    if any(h not in top_n candidates for h in [h1, h2, h3]):
        continue
    if prob(h1) × prob(h2) × prob(h3) × san_odds > expected_return_threshold:
        ✓ 三連複を選定
```

---

## 4. 賭け金配分

### 実装: `_allocate_bets(selected_bets, budget_per_race, min_bet_amount)`

全選定馬券に対し、`budget_per_race` を**オッズ逆数比率**で配分する。

### 数式

```
inv(bet) = 1 / max(odds, 0.01)

ratio(bet) = inv(bet) / Σ inv(all bets)

raw_amount(bet) = budget_per_race × ratio(bet)

bet_amount(bet) = floor(raw_amount / 100) × 100   ← 100円単位切り捨て

if bet_amount < min_bet_amount:
    → この馬券を除外（購入しない）
```

### オッズ逆数比率の意味

オッズの逆数 `1/odds` は「1円の払戻を得るために必要な賭け金」を表す。これを比率にして配分することで、**高オッズ馬券には少額・低オッズ馬券には多額**を賭ける形になる。

### 具体例（budget_per_race=1,000円）

| 馬券 | オッズ | 1/オッズ | 比率 | 配分額 | 100円単位 |
|---|---|---|---|---|---|
| 複勝A | 2.0 | 0.500 | 76.9% | 769円 | 700円 |
| ワイドB | 10.0 | 0.100 | 15.4% | 154円 | 100円 |
| 馬連C | 25.0 | 0.040 | 6.2% | 62円 | → **除外**（100円未満） |
| ワイドD | 100.0 | 0.010 | 1.5% | 15円 | → **除外**（100円未満） |
| **合計** | — | **0.650** | **100%** | — | **800円** |

> 端数処理（切り捨て）・最低賭け金除外により合計が budget_per_race を下回ることがある。

---

## 5. 的中判定と払戻計算

### 的中判定ロジック

```python
if bet_type == "win":
    is_hit = (finish_position(horse) == 1)   # 単勝: 1着のみ
else:
    is_hit = all(1 <= finish_position(h) <= 3 for h in horse_numbers)
    # place/wide/umaren/sanrenpuku: 全馬が3着以内
```

> 取消・除外馬（finish_position == 0）は `skip` として記録から除外する。

### 払戻計算

実際の払戻データ（`raw.payouts`）が存在する場合はそれを使用。存在しない場合はオッズで推定。

```python
payout_key = (race_id, bet_type, tuple(sorted(horse_numbers)))

if is_hit:
    if payout_key in _payout_map:                      # 実払戻データあり
        return_amount = bet_amount × (payout_per_100 / 100.0)
    else:                                               # 推定
        return_amount = bet_amount × odds
else:
    return_amount = 0.0

profit = return_amount - bet_amount
```

### _payout_map のキー形式

```python
# 複勝・単勝（単一馬番）
(race_id, "place", (horse_number,))    → payout_per_100（100円あたり払戻額）
(race_id, "win",   (horse_number,))    → payout_per_100

# ワイド・馬連（2頭、小さい馬番順）
(race_id, "wide",   (h1, h2))  where h1 < h2   → payout_per_100
(race_id, "umaren", (h1, h2))  where h1 < h2   → payout_per_100

# 三連複（3頭、昇順）
(race_id, "sanrenpuku", (h1, h2, h3))  where h1 < h2 < h3   → payout_per_100
```

---

## 6. パラメータ最適化（Optuna）

### 実装: `StrategyOptimizer.run_optuna_search()`

本番の最適化パス。Optunaのベイズ最適化で評価指標（既定: 回収率）を最大化し、
最良パラメータセットを `config/strategy_config.yaml` に保存する。
実行スクリプトは `scripts/optimize_strategy.py`。

> 旧 `run_grid_search()`（`p1`・パターン別 `top_n`/`r` を含むグリッドサーチ）は後方互換のため残るが、本番では使用しない。

### 探索範囲

| パラメータ | 探索範囲 | 説明 |
|---|---|---|
| `expected_return_threshold` | `[1.0, 2.5]`（連続） | 期待回収率フィルタ閾値（リスクマージン） |
| `top_n` | `[1, 6]`（整数） | ワイド/馬連/三連複の候補馬数 |
| `min_prob_threshold` | `[0.0, 0.3]`（連続） | 全馬券種の最低複勝率（18頭換算基準） |
| `max_wide_odds` | `[5.0, 50.0]` または `None`（条件付き） | ワイド購入の上限オッズ |

### 固定パラメータ（探索対象外）

| パラメータ | 値 | 説明 |
|---|---|---|
| `prob_weight_r` | `1.0` | 選定スコア係数。校正後は純EV順が正解のため固定（Issue #416 / #417） |
| `budget_per_race` | `2000` | 1レースあたりの固定予算（円） |
| `min_bet_amount` | `100` | 最低賭け金（円） |
| `enabled_bet_types` | `[place, wide, umaren]` | 購入対象券種（三連複除外、Issue #411） |

### 制約（過学習防止）

回収率 < 下限、最大ドローダウン > 上限、または**賭け数 < 下限**の試行はペナルティスコア（0.0）を与え、
十分な賭け数で安定した戦略のみを採用する。

### 評価指標

| 指標 | 計算式 | 目標 |
|---|---|---|
| `recovery_rate` | 合計払戻 ÷ 合計賭け金 × 100 (%) | 最大化（100%超が目標）|
| `hit_rate` | 的中数 ÷ 総賭け数 × 100 (%) | 最大化 |
| `max_drawdown` | ピーク資金からの最大下落率 (%) | **最小化**（唯一逆順） |
| `sharpe_ratio` | 週次超過リターン平均 ÷ 標準偏差 × √52 | 最大化 |

---

## 7. 設定パラメータ一覧

`config/strategy_config.yaml` に保存。Optuna最適化で更新される。

| パラメータ | 例 | 型 | 探索対象 | 説明 |
|---|---|---|---|---|
| `expected_return_threshold` | `1.94` | float | ✅ | 期待回収率の下限（リスクマージン）|
| `top_n` | `5` | int | ✅ | ワイド/馬連/三連複の候補馬数 |
| `min_prob_threshold` | `0.24` | float | ✅ | 全馬券種の最低複勝率（18頭換算基準）|
| `max_wide_odds` | `44.0` | float \| null | ✅ | ワイド購入の上限オッズ（null で無制限）|
| `prob_weight_r` | `1.0` | float | — | 選定スコア係数（`odds × prob^r`）。**1.0 固定**（Issue #417）|
| `budget_per_race` | `2000` | int | — | 1レースあたりの固定予算（円）|
| `min_bet_amount` | `100` | int | — | 最低賭け金（円）。これ未満は除外 |
| `enabled_bet_types` | `[place, wide, umaren]` | list | — | 購入対象券種。三連複は除外（Issue #411）|

### optimization セクション（最適化結果の記録）

```yaml
optimization:
  last_run: '2026-06-27T16:51:26'  # 最終実行日時
  method: optuna                    # 最適化手法
  metric: recovery_rate             # 最適化した評価指標
  n_trials: 500                     # 試行回数
  start_date: '2025-12-20'          # 最適化に使用した期間
  end_date: '2026-06-27'
  recovery_rate: 216.58             # 最良パラメータの回収率(%)
  hit_rate: 16.2                    # 最良パラメータの的中率(%)
  max_drawdown: 9.17                # 最良パラメータの最大ドローダウン(%)
  total_bets: 605                   # 最良パラメータの総賭け数
```

---

## 8. 数値例

### 例1: 複勝の選定

```
複勝候補A: win_place_prob=0.45, place_odds=2.8, N=16
  18頭換算: 0.45 × 16/18 = 0.40 ≥ min_prob_threshold(0.24) → フィルタ通過
  期待値: 0.45 × 2.8 = 1.26 < expected_return_threshold(1.94) → 複勝は見送り
```

### 例2: ワイド＋馬連の選定

```
top_n 候補内のペア [13, 15]:
  ワイド: prob(13)×prob(15)×wide_odds = 0.42 × 0.35 × 14.0 = 2.06 > 1.94 → ✓ ワイド選定
  max_wide_odds(44.0) 以下 → 購入対象
  馬連: ワイド[13,15]と同じ組み合わせ → 自動購入 ✓（期待値フィルタなし）
```

### 例3: 高オッズワイドの除外

```
ペア [3, 18]: wide_odds = 52.0 > max_wide_odds(44.0) → ワイド・馬連ともにスキップ
```

---

## 関連ファイル

| ファイル | 役割 |
|---|---|
| `src/backtest/strategy.py` | 戦略コアロジック（馬券選定・賭け金配分。全レース統一ロジック）|
| `src/backtest/strategy_optimizer.py` | Optuna最適化（`run_optuna_search`）・最良パラメータ選定 |
| `src/backtest/metrics.py` | 回収率・的中率・最大DD・シャープレシオの計算 |
| `config/strategy_config.yaml` | 最適化済みパラメータの保存先 |
| `scripts/run_backtest.py` | フル戦略バックテストパイプライン |
| `scripts/optimize_strategy.py` | パラメータ最適化（Optuna）の実行スクリプト |
| `scripts/run_strategy.py` | 当日投資戦略の実行スクリプト |
