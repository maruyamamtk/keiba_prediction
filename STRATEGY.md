# 投資戦略 設計仕様書

このドキュメントは `src/backtest/strategy.py` および `src/backtest/strategy_optimizer.py` に実装された投資戦略ロジックを完全に説明する。

---

## 目次

1. [全体フロー](#1-全体フロー)
2. [入力データ仕様](#2-入力データ仕様)
3. [ステップ1: レースパターン分類](#3-ステップ1-レースパターン分類)
4. [ステップ2: ベース馬券選定（複勝・ワイド・三連複）](#4-ステップ2-ベース馬券選定)
5. [ステップ3: パターンA追加馬券選定（単勝・馬連）](#5-ステップ3-パターンa追加馬券選定)
6. [ステップ4: 賭け金配分](#6-ステップ4-賭け金配分)
7. [的中判定と払戻計算](#7-的中判定と払戻計算)
8. [パラメータ最適化（グリッドサーチ）](#8-パラメータ最適化グリッドサーチ)
9. [設定パラメータ一覧](#9-設定パラメータ一覧)
10. [数値例](#10-数値例)

---

## 1. 全体フロー

1レースに対して以下の順序で処理する。エントリポイントは `select_bets_for_race()` 関数。

```
入力:
  race_df        -- 当該レースの予測データ（全馬）
  combo_odds_df  -- コンボ馬券オッズ（ワイド/三連複/馬連）

         ↓
【前処理】
  NaN オッズ・0以下オッズの馬を除外
  有効馬が3頭未満 → ValueError

         ↓
【Step 1】classify_race_pattern()
  複勝率リストを降順ソート
  top1 - top2 > p1  →  one_dominant（突出型）
  それ以外           →  standard（標準型）

         ↓
【Step 2】select_base_bets()         ← 両パターン共通
  選定スコア = place_odds × win_place_prob ^ prob_weight_r
  スコア降順 top_n 頭を候補とする
  複勝: prob × place_odds > threshold の馬を選定
  ワイド:  prob_i × prob_j × wide_odds > threshold の組み合わせを選定
  三連複: prob_i × prob_j × prob_k × san_odds > threshold の組み合わせを選定

         ↓ one_dominant のみ
【Step 3】select_pattern_a_extra_bets()
  単勝: top1_prob × win_odds > threshold → 軸馬の単勝を選定
  馬連: top1_prob × prob_other × umaren_odds > threshold
        （top_n 候補内の相手馬 × 軸馬の組み合わせを選定）

         ↓
【Step 4】_allocate_bets()
  全選定馬券にオッズ逆数比率で budget_per_race を配分
  100円単位切り捨て後、min_bet_amount 未満の馬券を除外

         ↓
出力: bets（賭け金付き馬券リスト）, race_pattern
```

---

## 2. 入力データ仕様

### race_df（必須カラム）

| カラム名 | 型 | 説明 |
|---|---|---|
| `horse_id` | str | 馬ID |
| `horse_number` | int | 馬番 |
| `win_place_prob` | float | 複勝率（0〜1）。水充填アルゴリズムで計算済み |
| `odds` | float | 複勝オッズ（`place_odds` から `rename` して使用） |
| `finish_position` | int | 実際の着順（バックテスト時）|
| `win_odds` | float | 単勝オッズ（**オプション**。存在する場合のみ単勝選定が有効化）|

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

## 3. ステップ1: レースパターン分類

### 実装: `classify_race_pattern(probs, p1)`

```python
sorted_probs = sorted(probs, reverse=True)
top1, top2, top3 = sorted_probs[0], sorted_probs[1], sorted_probs[2]
gap_12 = top1 - top2

if gap_12 > p1:
    pattern = "one_dominant"   # 突出型
else:
    pattern = "standard"       # 標準型
```

### パターン判定表

| 条件 | パターン | 意味 |
|---|---|---|
| `top1の複勝率 − top2の複勝率 > p1` | `one_dominant` | 1頭が突出した本命レース |
| それ以外 | `standard` | 実力伯仲のレース |

### パラメータ p1 の影響

| p1 値 | 影響 |
|---|---|
| 小さい（例: 0.05） | one_dominant と判定されやすい → 馬連・単勝購入が増える |
| 大きい（例: 0.3） | 差が0.3超えないと one_dominant にならない → standard が増える |
| **現在設定値: 0.1** | top1が top2 を10ポイント超えると突出型 |

### 戻り値: RacePattern

```python
@dataclass
class RacePattern:
    pattern: str         # 'one_dominant' | 'standard'
    top1_prob: float     # 1位馬の複勝率
    top2_prob: float     # 2位馬の複勝率
    top3_prob: float     # 3位馬の複勝率
    gap_top1_top2: float # top1 - top2
    gap_top1_top3: float # top1 - top3
```

---

## 4. ステップ2: ベース馬券選定

### 実装: `select_base_bets(race_df, combo_odds_df, expected_return_threshold, top_n, min_prob_threshold, prob_weight_r)`

**両パターン（one_dominant / standard）で共通して実行される。**

---

### 4-1. 選定スコアの計算

```python
selection_score = place_odds × win_place_prob ^ prob_weight_r
```

全馬をこのスコアの**降順**にソートし、上位 `top_n` 頭を「候補馬」とする。

#### prob_weight_r の意味

| r 値 | 式のイメージ | 高スコアになる馬 |
|---|---|---|
| 0.5（現在値） | `odds × √prob` | **高オッズ × 中確率**の馬（穴馬寄り） |
| 1.0 | `odds × prob` | 通常の期待値と同等 |
| 2.0 | `odds × prob²` | 高確率（本命）の馬が有利 |

> **設計上の重要な特性**: prob_weight_r=0.5 では、複勝率が高い（本命）馬は低オッズになるため選定スコアが低くなりがち。逆に低確率・高オッズの穴馬がスコア上位に入る傾向がある。

---

### 4-2. 複勝（place）の選定

候補はスコア順ではなく**全馬**が対象。ただし `min_prob_threshold` でフィルタ。

```python
for each horse in sorted_df（スコア降順）:
    if win_place_prob < min_prob_threshold:
        continue   # 複勝率が低すぎる馬はスキップ
    if win_place_prob × place_odds > expected_return_threshold:
        ✓ 複勝を選定
```

**期待回収率フィルタ式**: `複勝率 × 複勝オッズ > threshold`

---

### 4-3. ワイド（wide）の選定

候補は**スコア上位 top_n 頭の中の2頭組み合わせ**のみ。

```python
for each wide bet in combo_odds_df where bet_type == "wide":
    h1, h2 = 馬番
    if h1 not in top_n candidates or h2 not in top_n candidates:
        continue   # top_n 外はスキップ
    if prob(h1) × prob(h2) × wide_odds > expected_return_threshold:
        ✓ ワイドを選定
```

**期待回収率フィルタ式**: `複勝率(h1) × 複勝率(h2) × ワイドオッズ > threshold`

---

### 4-4. 三連複（sanrenpuku）の選定

候補は**スコア上位 top_n 頭の中の3頭組み合わせ**のみ。

```python
for each sanrenpuku bet in combo_odds_df where bet_type == "sanrenpuku":
    h1, h2, h3 = 馬番
    if any(h not in top_n candidates for h in [h1, h2, h3]):
        continue   # top_n 外はスキップ
    if prob(h1) × prob(h2) × prob(h3) × san_odds > expected_return_threshold:
        ✓ 三連複を選定
```

**期待回収率フィルタ式**: `複勝率(h1) × 複勝率(h2) × 複勝率(h3) × 三連複オッズ > threshold`

---

### 4-5. top_n の役割

`top_n` は**ワイド・三連複・馬連の相手候補馬数**を制限する。

- `top_n=5` → スコア上位5頭の間でのみ組み合わせを探す
- 複勝の選定には top_n は無関係（全馬が対象）
- `min_prob_threshold` は複勝の軸馬選定のみに適用（流し馬券の相手馬には適用されない）

---

## 5. ステップ3: パターンA追加馬券選定

### 実装: `select_pattern_a_extra_bets(race_df, combo_odds_df, expected_return_threshold, top_n)`

**`one_dominant` パターンのときのみ実行**。Step2 のベース馬券に加えて単勝・馬連を追加する。

ここでの候補馬は**選定スコア順ではなく複勝率降順**で選ぶ。

```python
sorted_df = race_df.sort_values("win_place_prob", ascending=False)
top1_horse = sorted_df.iloc[0]   # 複勝率1位 = 軸馬
```

---

### 5-1. 単勝（win）の選定

```python
if "win_odds" in race_df.columns:                          # win_oddsカラムが存在する場合のみ
    win_odds = top1_horse["win_odds"]
    if top1_prob × win_odds > expected_return_threshold:
        ✓ 軸馬の単勝を選定
```

**条件**:
1. `win_odds` カラムが `race_df` に存在すること（`fetch_place_odds()` で取得・マージ済みであること）
2. `軸馬の複勝率 × 単勝オッズ > threshold`

> **注意**: Issue #176 修正前は `win_odds` カラムが `predictions_df` に渡されておらず、単勝が一切購入されていなかった。

---

### 5-2. 馬連（umaren）の選定

```python
# 複勝率上位 top_n 頭 が相手候補
top_candidates = sorted_df.head(top_n)   # 複勝率降順

for each umaren bet in combo_odds_df where bet_type == "umaren":
    h1, h2 = 馬番
    if top1_horse_number not in (h1, h2):
        continue   # 軸馬が含まれていない組み合わせはスキップ
    other = h2 if h1 == top1 else h1
    if other not in top_n candidates:
        continue   # 相手馬が top_n 候補外はスキップ
    if top1_prob × prob(other) × umaren_odds > expected_return_threshold:
        ✓ 馬連を選定
```

**期待回収率フィルタ式**: `軸馬の複勝率 × 相手馬の複勝率 × 馬連オッズ > threshold`

**設計上の特性**:
- 馬連は**必ず軸馬（複勝率1位）が入る**
- 相手は**複勝率順 top_n 頭**から選ぶ（選定スコード順ではない）
- 軸馬は複勝率が高いためこの式が通りやすく、**馬連が最も多く購入される傾向**がある

---

### 5-3. ベース馬券との違い

| 観点 | ベース馬券（Step2） | パターンA馬券（Step3） |
|---|---|---|
| 実行条件 | 両パターン共通 | one_dominant のみ |
| 候補馬の選び方 | **選定スコア順** top_n | **複勝率順** top_n |
| 馬券種 | 複勝 / ワイド / 三連複 | 単勝 / 馬連 |
| 軸馬 | 期待回収率フィルタ次第 | 必ず複勝率1位 |

---

## 6. ステップ4: 賭け金配分

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

### 具体例（budget_per_race=3,000円）

| 馬券 | オッズ | 1/オッズ | 比率 | 配分額 | 100円単位 |
|---|---|---|---|---|---|
| 複勝A | 2.0 | 0.500 | 50.0% | 1,500円 | 1,500円 |
| ワイドB | 10.0 | 0.100 | 10.0% | 300円 | 300円 |
| 馬連C | 25.0 | 0.040 | 4.0% | 120円 | 100円 |
| ワイドD | 100.0 | 0.010 | 1.0% | 30円 | → **除外**（100円未満） |
| **合計** | — | **0.650** | **65.0%** | **1,950円** | **1,900円** |

> 端数処理（切り捨て）により合計が budget_per_race を下回ることがある。

---

## 7. 的中判定と払戻計算

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

## 8. パラメータ最適化（グリッドサーチ）

### 実装: `StrategyOptimizer.run_grid_search()`

以下の3パラメータをグリッドサーチで探索し、最も評価指標が高いパラメータセットを `config/strategy_config.yaml` に保存する。

### デフォルト探索範囲

```python
p1_range        = [0.1, 0.15, 0.2, 0.25, 0.3]    # 5値
threshold_range = [1.0, 1.1, 1.2, 1.3, 1.5]       # 5値
r_range         = [0.5, 1.0, 1.5, 2.0]            # 4値

# 総組み合わせ数: 5 × 5 × 4 = 100通り
```

### 固定パラメータ

グリッドサーチで探索されず、`strategy_config.yaml` から固定値で読み込まれる。

| パラメータ | 説明 |
|---|---|
| `min_prob_threshold` | 複勝軸馬の最低複勝率 |
| `min_bet_amount` | 最低賭け金（100円） |
| `top_n` | 候補馬数（5頭） |
| `budget_per_race` | 1レース予算（3,000円） |

### 評価指標

```python
best_params(results, metric="recovery_rate")  # デフォルト: 回収率最大化
```

| 指標 | 計算式 | 目標 |
|---|---|---|
| `recovery_rate` | 合計払戻 ÷ 合計賭け金 × 100 (%) | 最大化（100%超が目標）|
| `hit_rate` | 的中数 ÷ 総賭け数 × 100 (%) | 最大化 |
| `max_drawdown` | ピーク資金からの最大下落率 (%) | **最小化**（唯一逆順） |
| `sharpe_ratio` | 週次超過リターン平均 ÷ 標準偏差 × √52 | 最大化 |

### パターン別集計

`_run_simulation()` は `one_dominant` / `standard` それぞれの成績を別途集計する。

```python
pattern_stats = {
    "one_dominant": {
        "bets": int,            # 賭け数
        "hits": int,            # 的中数
        "bet_amount": float,    # 合計賭け金
        "return_amount": float, # 合計払戻
        "recovery_rate": float, # 回収率(%)
    },
    "standard": { ... }
}
```

---

## 9. 設定パラメータ一覧

`config/strategy_config.yaml` に保存。パラメータ最適化で更新される。

| パラメータ | 現在値 | 型 | 探索対象 | 説明 |
|---|---|---|---|---|
| `p1` | `0.1` | float | ✅ | 突出型判定閾値。top1−top2 がこれを超えると `one_dominant` |
| `expected_return_threshold` | `1.5` | float | ✅ | 期待回収率の下限。この値を超える馬券のみ購入 |
| `prob_weight_r` | `0.5` | float | ✅ | 選定スコアの複勝率べき乗係数（`odds × prob^r`）|
| `budget_per_race` | `3000` | int | — | 1レースあたりの固定予算（円）|
| `min_bet_amount` | `100` | int | — | 最低賭け金（円）。これ未満は除外 |
| `min_prob_threshold` | `0.1` | float | — | 複勝単体買いの最低複勝率 |
| `top_n` | `5` | int | — | ワイド/三連複/馬連の候補馬数 |

### optimization セクション（最適化結果の記録）

```yaml
optimization:
  last_run: '2026-03-28T15:11:39'  # 最終実行日時
  metric: recovery_rate             # 最適化した評価指標
  start_date: '2023-01-01'          # 最適化に使用した期間
  end_date: '2023-12-31'
  recovery_rate: 198.79             # 最良パラメータの回収率(%)
  hit_rate: 21.25                   # 最良パラメータの的中率(%)
  max_drawdown: 12.0                # 最良パラメータの最大ドローダウン(%)
  total_bets: 11937                 # 最良パラメータの総賭け数
```

---

## 10. 数値例

### 例1: one_dominant パターン（福島10R 2024-04-13）

**入力データ（上位5頭）**

| 馬番 | 複勝率 | 複勝オッズ | 選定スコア（r=0.5） |
|---|---|---|---|
| 15 | 56.8% | 1.7 | 1.7 × √0.568 = **1.28** |
| 13 | 44.7% | 2.3 | 2.3 × √0.447 = **1.54** |
| 11 |  5.3% | 38.6 | 38.6 × √0.053 = **8.91** |
| 6  |  5.0% | 10.5 | 10.5 × √0.050 = **2.35** |

**Step 1: パターン分類**
```
gap_12 = 0.568 - 0.447 = 0.121 > p1(0.1)  →  one_dominant
```

**Step 2: ベース馬券（選定スコア上位5頭：11・他4頭）**
```
複勝率がいずれも5%前後のため:
  0.053 × 38.6 = 2.046 > 1.5  → 馬番11の複勝 ✓
  ワイド: 0.053 × 0.050 × wide_odds → 積が0.003未満 → どのオッズでもフィルタ不通過
```

**Step 3: パターンA馬券（複勝率順：15・13・3・7・14）**
```
馬連 [13, 15]: 0.568 × 0.447 × 19.9 = 5.05 > 1.5  ✓
馬連 [7, 15]:  0.568 × 0.280 × 34.1 = 5.43 > 1.5  ✓
馬連 [3, 15]:  0.568 × 0.423 × 15.1 = 3.62 > 1.5  ✓
馬連 [14, 15]: 0.568 × 0.255 × 31.7 = 4.59 > 1.5  ✓
```

**Step 4: 賭け金配分（budget=3,000円）**

| 馬券 | オッズ | 1/オッズ | 比率 | 配分 |
|---|---|---|---|---|
| 複勝 11 | 38.6 | 0.026 | 5% | ¥100 |
| 馬連 [13,15] | 19.9 | 0.050 | 10% | ¥800 |
| 馬連 [7,15] | 34.1 | 0.029 | 6% | ¥400 |
| 馬連 [3,15] | 15.1 | 0.066 | 13% | ¥1,100 |
| 馬連 [14,15] | 31.7 | 0.032 | 6% | ¥500 |

**結果**: 1着13番・2着15番・3着7番 → 馬連[13,15] ✅（¥800 × 19.9 = ¥15,920）、馬連[7,15] ✅（¥400 × 34.1 = ¥13,640）

---

### 例2: standard パターン（東京6R 2024-06-02）

**Step 1**: gap_12 = 0.000 ≤ p1 → standard

**Step 2（選定スコア上位5頭）**

| 馬番 | 複勝率 | 複勝オッズ | 選定スコア |
|---|---|---|---|
| 7 | 6.0% | 9.3 | 2.29 |
| 6 | 100.0% | 1.1 | 1.10 |
| 3 | 100.0% | 1.1 | 1.10 |
| 9 | 9.1% | 3.0 | 0.90 |
| 5 | 7.8% | 5.7 | 1.59 |

```
ワイド [3, 7]: 1.000 × 0.060 × 27.2 = 1.632 > 1.5  ✓
ワイド [6, 7]: 1.000 × 0.060 × ？倍 → オッズ次第
```

**Step 3**: standard なので実行されない

**結果**: ワイド[3,7] ✅（¥3,000 × 27.2 = ¥81,600）

---

## 関連ファイル

| ファイル | 役割 |
|---|---|
| `src/backtest/strategy.py` | 戦略コアロジック（パターン分類・馬券選定・賭け金配分）|
| `src/backtest/strategy_optimizer.py` | グリッドサーチ・パターン別集計・最良パラメータ選定 |
| `src/backtest/metrics.py` | 回収率・的中率・最大DD・シャープレシオの計算 |
| `config/strategy_config.yaml` | 最適化済みパラメータの保存先 |
| `scripts/run_backtest.py` | フル戦略バックテストパイプライン（`--full-strategy`）|
| `scripts/run_strategy_optimization.py` | パラメータ最適化の実行スクリプト |
| `scripts/run_strategy.py` | 当日投資戦略の実行スクリプト |
