# 投資戦略 設計仕様書

このドキュメントは `src/backtest/strategy.py` および `src/backtest/strategy_optimizer.py` に実装された投資戦略ロジックを完全に説明する。

---

## 目次

1. [全体フロー](#1-全体フロー)
2. [入力データ仕様](#2-入力データ仕様)
3. [ステップ1: レースパターン分類](#3-ステップ1-レースパターン分類)
4. [ステップ2: ベース馬券選定（複勝・ワイド・三連複）](#4-ステップ2-ベース馬券選定)
5. [ステップ3: パターンA追加馬券選定（馬連）](#5-ステップ3-パターンa追加馬券選定)
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
  全馬の複勝率分布のジニ係数を計算し、出走頭数で補正
  補正ジニ係数 > p1  →  one_dominant（突出型）
  それ以外            →  standard（標準型）

         ↓
【Step 2】select_base_bets()         ← 両パターン（パターン別パラメータ適用）
  選定スコア = place_odds × win_place_prob ^ prob_weight_r
  スコア降順 top_n 頭を候補とする  ← パターンによって top_n・prob_weight_r が異なる
  min_prob_threshold フィルタ（prob × N/18 ≥ min_prob_threshold）を通過した馬のみが候補
  複勝: さらに prob × place_odds > expected_return_threshold を満たす馬を選定
  ワイド:  prob_i × prob_j × wide_odds > expected_return_threshold
  三連複: prob_i × prob_j × prob_k × san_odds > expected_return_threshold

         ↓ one_dominant のみ
【Step 3】select_pattern_a_extra_bets()  ← 自動購入方式（期待値フィルタなし）
  馬連: Step2のワイドと同じ組み合わせを自動購入

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

## 3. ステップ1: レースパターン分類

### 実装: `classify_race_pattern(probs, p1)`

#### ジニ係数の計算

```python
def _gini_coefficient(probs: list[float]) -> float:
    """
    複勝率分布のジニ係数を計算する
    G = (2 * Σ(i * x_i)) / (n * Σ x_i) - (n+1)/n
    where x_i は昇順ソート後の値, i は 1-indexed
    """
    n = len(probs)
    sorted_p = sorted(probs)          # 昇順ソート
    total = sum(sorted_p)
    cumsum = sum((i + 1) * p for i, p in enumerate(sorted_p))
    return (2 * cumsum) / (n * total) - (n + 1) / n
```

#### 出走頭数補正（Issue #207）

少頭数レースではジニ係数が過小評価されるバイアスを N/(N-1) 係数で補正する。

```python
N = len(probs)
gini = _gini_coefficient(probs)
gini_adjusted = gini * N / (N - 1)   # N/(N-1) 補正（少頭数ほど係数が大きい）
```

| 出走頭数 N | N/(N-1) 係数 | 意味 |
|---|---|---|
| 3 | 1.500 | 3頭立てはジニ係数を1.5倍に補正 |
| 8 | 1.143 | 一般的なレースでやや補正 |
| 18 | 1.059 | フルゲートではほぼ補正なし |

#### パターン判定

```python
if gini_adjusted > p1:      # 補正後ジニ係数と比較
    pattern = "one_dominant"   # 確率が1頭に集中 = 突出型
else:
    pattern = "standard"       # 分布が均等に近い = 標準型
```

### ジニ係数の意味

| ジニ係数（補正後） | 意味 |
|---|---|
| 0.0 | 全馬の複勝率が均等（例: 全馬 1/8 = 12.5%） |
| 0.2〜0.3 | 本命馬がいる程度の偏り（一般的なレース） |
| 0.4〜0.6 | 1頭が明確に突出したレース |
| 1.0 | 1頭に全確率が集中（極端な本命） |

### パラメータ p1 の影響

| p1 値 | 影響 |
|---|---|
| 小さい（例: 0.2） | 補正ジニ係数が小さくても突出型と判定 → one_dominant が増える |
| 大きい（例: 0.5） | 強い偏りがないと突出型にならない → standard が増える |
| **現在設定値: 0.5** | 強い本命がいるレースでのみ突出型と判定 |

### 戻り値: RacePattern

```python
@dataclass
class RacePattern:
    pattern: str                      # 'one_dominant' | 'standard'
    top1_prob: float                  # 1位馬の複勝率
    top2_prob: float                  # 2位馬の複勝率
    top3_prob: float                  # 3位馬の複勝率
    gap_top1_top2: float              # top1 - top2
    gap_top1_top3: float              # top1 - top3
    gini_coefficient: float           # 補正前ジニ係数
    gini_coefficient_adjusted: float  # 補正後ジニ係数（= gini × N/(N-1)）
```

---

## 4. ステップ2: ベース馬券選定

### 実装: `select_base_bets(race_df, combo_odds_df, expected_return_threshold, top_n, min_prob_threshold, prob_weight_r)`

**両パターン（one_dominant / standard）で実行されるが、パターンに応じて `top_n` と `prob_weight_r` が異なる。**
期待回収率閾値 `expected_return_threshold` は両パターン共通。

---

### 4-1. 選定スコアの計算

```python
selection_score = place_odds × win_place_prob ^ prob_weight_r
```

全馬をこのスコアの**降順**にソートし、上位 `top_n` 頭を「候補馬」とする。

`top_n` と `prob_weight_r` はパターンによって異なる（突出型: `top_n_dominant` / `prob_weight_r_dominant`、標準型: `top_n_standard` / `prob_weight_r_standard`）。

#### prob_weight_r の意味

| r 値 | 式のイメージ | 高スコアになる馬 |
|---|---|---|
| 0.8 | `odds × prob^0.8` | やや高オッズ寄り |
| 1.0 | `odds × prob` | 通常の期待値と同等 |
| 1.2 | `odds × prob^1.2` | やや高確率寄り |
| 1.5 | `odds × prob^1.5` | **高確率（本命）の馬が有利** |

---

### 4-2. min_prob_threshold による候補馬フィルタ（全馬券種共通）

`min_prob_threshold` は **複勝・ワイド・三連複すべての馬券種** の候補馬に適用される。
スコア上位 `top_n` 頭の選定前にフィルタを実施するため、低確率馬はいかなる馬券にも混入しない。

```python
N = len(race_df)   # 出走頭数

# min_prob_threshold フィルタ（PR #245: 全馬券種共通）
# 18頭基準に換算して比較: 少頭数では理論複勝率が高くなるバイアスを補正
if min_prob_threshold > 0:
    candidates_df = sorted_df[sorted_df["win_place_prob"] * N / 18 >= min_prob_threshold]
else:
    candidates_df = sorted_df

# フィルタ通過馬の上位 top_n 頭がワイド・三連複・馬連の候補となる
top_candidates = candidates_df.head(top_n)
```

### 複勝（place）の選定

候補はスコア順ではなく**全馬**が対象。ただし `min_prob_threshold` フィルタを通過した馬のみ。

```python
for each horse in sorted_df（スコア降順）:
    if win_place_prob × N/18 < min_prob_threshold:
        continue
    if win_place_prob × place_odds > expected_return_threshold:
        ✓ 複勝を選定
```

#### 出走頭数補正の意味（Issue #208）

`prob × N/18` は「18頭立て換算での理論複勝率」を表す。

| 出走頭数 N | 3頭中3着以内の理論確率 | 補正後（×N/18） |
|---|---|---|
| 6 | 50% | 17% |
| 12 | 25% | 17% |
| 18 | 17% | 17% |

18頭換算後の確率が `min_prob_threshold` を下回る馬は除外される。これにより少頭数レースで過剰な複勝買いを防ぐ。

---

### 4-3. ワイド（wide）の選定

候補は **`min_prob_threshold` フィルタ通過後のスコア上位 `top_n` 頭** の中の2頭組み合わせのみ（`top_n` はパターン別）。

```python
for each wide bet in combo_odds_df where bet_type == "wide":
    h1, h2 = 馬番
    if h1 not in top_n candidates or h2 not in top_n candidates:
        continue
    if prob(h1) × prob(h2) × wide_odds > expected_return_threshold:
        ✓ ワイドを選定
```

---

### 4-4. 三連複（sanrenpuku）の選定

候補は **`min_prob_threshold` フィルタ通過後のスコア上位 `top_n` 頭** の中の3頭組み合わせのみ。

```python
for each sanrenpuku bet in combo_odds_df where bet_type == "sanrenpuku":
    h1, h2, h3 = 馬番
    if any(h not in top_n candidates for h in [h1, h2, h3]):
        continue
    if prob(h1) × prob(h2) × prob(h3) × san_odds > expected_return_threshold:
        ✓ 三連複を選定
```

---

### 4-5. パターン別パラメータの適用

`select_bets_for_race()` 内でパターンに応じてパラメータを切り替える:

```python
if race_pattern.pattern == "one_dominant":
    active_top_n = top_n_dominant     # 突出型の候補馬数
    active_r = prob_weight_r_dominant # 突出型の選定スコア係数
else:
    active_top_n = top_n_standard     # 標準型の候補馬数
    active_r = prob_weight_r_standard # 標準型の選定スコア係数

# expected_return_threshold は両パターン共通
base_bets = select_base_bets(
    ...,
    expected_return_threshold=expected_return_threshold,
    top_n=active_top_n,
    prob_weight_r=active_r,
)
```

---

## 5. ステップ3: パターンA追加馬券選定

### 実装: `select_pattern_a_extra_bets(race_df, combo_odds_df, base_bets)`

**`one_dominant` パターンのときのみ実行**。Step2 のベース馬券に加えて馬連を**自動購入**する。

> **重要**: Step3 は期待値フィルタを使用しない。自動購入方式。

---

### 5-1. 馬連（umaren）の選定: Step2のワイドと同一組み合わせ

```python
# Step2 で選ばれたワイドの馬番ペアを抽出
wide_pairs = [bet["horse_numbers"] for bet in base_bets if bet["bet_type"] == "wide"]

# ワイドと同じ馬番ペアの馬連を購入
for (h1, h2) in wide_pairs:
    umaren_row = combo_odds_df[bet_type=="umaren" AND horse_numbers==(h1, h2)]
    if found:
        ✓ 馬連を1点自動購入（期待値フィルタなし）
```

**特性**:
- Step2でワイドが0件 → 馬連も0件
- Step2でワイドがn件 → 馬連も最大n件
- ワイドと馬連は**常に同じ馬番ペア** → 戦略の一貫性が保たれる

---

### 5-2. ベース馬券との違い

| 観点 | ベース馬券（Step2） | パターンA馬券（Step3） |
|---|---|---|
| 実行条件 | 両パターン共通 | one_dominant のみ |
| 候補馬の選び方 | **選定スコア順** top_n | — |
| 馬券種 | 複勝 / ワイド / 三連複 | 馬連のみ |
| 購入条件 | 期待回収率フィルタあり | **期待値フィルタなし（自動購入）** |
| 馬連との整合性 | — | **ワイドと同じ組み合わせ** |

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

以下の6パラメータをグリッドサーチで探索し、最も評価指標が高いパラメータセットを `config/strategy_config.yaml` に保存する。

### デフォルト探索範囲

```python
p1_range             = [0.4, 0.45, 0.5]        # 3値（補正ジニ係数の閾値）
threshold_range      = [1.0, 1.2, 1.5]          # 3値（期待回収率閾値・両パターン共通）
top_n_dominant_range = [4, 5, 6]                # 3値（突出型の候補馬数）
top_n_standard_range = [5, 6, 7]                # 3値（標準型の候補馬数）
r_dominant_range     = [0.8, 1.0, 1.2, 1.5]    # 4値（突出型の prob_weight_r）
r_standard_range     = [0.8, 1.0, 1.2, 1.5]    # 4値（標準型の prob_weight_r）

# 総組み合わせ数: 3 × 3 × 3 × 3 × 4 × 4 = 1,296通り
```

### 固定パラメータ

グリッドサーチで探索されず、`strategy_config.yaml` から固定値で読み込まれる。

| パラメータ | 説明 |
|---|---|
| `min_prob_threshold` | 複勝軸馬の最低複勝率（18頭換算基準） |
| `min_bet_amount` | 最低賭け金（100円） |
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
| `p1` | `0.5` | float | ✅ | 突出型判定の補正ジニ係数閾値。超えると `one_dominant` |
| `expected_return_threshold` | `1.2` | float | ✅ | 期待回収率の下限（両パターン共通） |
| `top_n_dominant` | `4` | int | ✅ | 突出型: ワイド/三連複/馬連の候補馬数 |
| `top_n_standard` | `5` | int | ✅ | 標準型: ワイド/三連複/馬連の候補馬数 |
| `prob_weight_r_dominant` | `1.2` | float | ✅ | 突出型の選定スコア係数（`odds × prob^r`）|
| `prob_weight_r_standard` | `1.2` | float | ✅ | 標準型の選定スコア係数（`odds × prob^r`）|
| `budget_per_race` | `3000` | int | — | 1レースあたりの固定予算（円）|
| `min_bet_amount` | `100` | int | — | 最低賭け金（円）。これ未満は除外 |
| `min_prob_threshold` | `0.1` | float | — | 全馬券種の最低複勝率（18頭換算基準）。複勝・ワイド・三連複の候補馬すべてに適用 |

### optimization セクション（最適化結果の記録）

```yaml
optimization:
  last_run: '2026-03-28T21:47:25'  # 最終実行日時
  metric: recovery_rate             # 最適化した評価指標
  start_date: '2023-01-01'          # 最適化に使用した期間
  end_date: '2023-12-31'
  recovery_rate: 156.42             # 最良パラメータの回収率(%)
  hit_rate: 24.72                   # 最良パラメータの的中率(%)
  max_drawdown: 23.58               # 最良パラメータの最大ドローダウン(%)
  total_bets: 13456                 # 最良パラメータの総賭け数
```

---

## 10. 数値例

### 例1: one_dominant パターン（福島10R 2024-04-13）

**Step 1: パターン分類**
```
複勝率: [56.8%, 44.7%, 5.3%, 5.0%, ...] → ジニ係数を計算
gini = 0.42, N=12 → gini_adjusted = 0.42 × 12/11 = 0.46 > p1(0.5)? → standard
（p1=0.5の場合、突出型には0.50超が必要）
```

**Step 2: ベース馬券（expected_return_threshold=1.2, top_n_dominant=4）**
```
複勝率がいずれも5%前後:
  N=12 での補正: prob × 12/18 = prob × 0.667
  0.053 × 0.667 = 0.035 < min_prob_threshold(0.10) → 除外
  ワイド: 0.568 × 0.447 × 5.8 = 1.47 < 1.2? → 確認要
```

---

### 例2: one_dominant パターン（ワイドあり）

**Step 2: ワイドが選定される場合**
```
ワイド[13, 15]: 0.568 × 0.447 × 7.1 = 1.80 > 1.2 → ✓
```

**Step 3: パターンA馬券（自動購入）**
```
馬連 [13, 15]: ワイド[13, 15]と同じ組み合わせ → 自動購入 ✓
```

---

### 例3: standard パターン（東京6R 2024-06-02）

**Step 1**: gini_adjusted = 0.12 ≤ p1(0.5) → standard

**Step 2（expected_return_threshold=1.2, top_n_standard=5）**

```
ワイド [3, 7]: 1.000 × 0.060 × 27.2 = 1.632 > 1.2 ✓
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
