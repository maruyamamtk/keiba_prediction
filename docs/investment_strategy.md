# 投資戦略設計ドキュメント

## 概要

本ドキュメントは、競馬予測MLシステムにおける投資戦略モジュール（`src/backtest/strategy.py` および `src/backtest/strategy_optimizer.py`）の設計思想、パラメータの意味、および使い方を説明します。

---

## 1. 設計思想

### なぜ「レースパターン分類」が必要か

通常のバックテスト（`BacktestSimulator`）では、全レースに対して同一の期待回収率フィルタとKelly基準を適用します。しかし、競馬のレースはその性質によって最適な投資戦略が異なります。

| レースタイプ | 特徴 | 最適戦略 |
|-------------|------|----------|
| 突出型（one_dominant） | 1頭の実力が飛び抜けている | 1頭集中・Kelly最大化 |
| 拮抗型（competitive） | 複数頭が接戦 | 複数頭カバー・穴狙い |
| 標準型（standard） | どちらでもない | 期待回収率による絞り込み |

### 予測複勝率に基づくパターン分類

MLモデルが出力した各馬の複勝率（`win_place_prob`）の分布を分析し、レースパターンを自動判定します。**オッズではなく予測確率**に基づくため、オッズの歪み（過小評価・過大評価）を活用した投資判断が可能になります。

---

## 2. パターン分類の仕様

### 判定ロジック

```python
def classify_race_pattern(probs, p1=0.2, p2=0.15):
    sorted_probs = sorted(probs, reverse=True)
    top1, top2, top3 = sorted_probs[0], sorted_probs[1], sorted_probs[2]

    if top1 - top2 > p1:       # 1位と2位の差が大きい → 突出型
        return "one_dominant"
    elif top1 - top3 < p2:     # 1位と3位の差が小さい → 拮抗型
        return "competitive"
    else:
        return "standard"
```

### パラメータの意味

| パラメータ | デフォルト値 | 意味 |
|-----------|-------------|------|
| `p1` | 0.2 | 突出型の閾値。top1の複勝率がtop2を **p1 超え** ていれば突出型 |
| `p2` | 0.15 | 拮抗型の閾値。top1とtop3の複勝率差が **p2 未満** なら拮抗型 |

**注意事項:**
- 判定は排他的優先順位です（突出型 > 拮抗型 > 標準型）
- 境界値は含まない（`>` と `<` で判定、`>=` や `<=` ではない）
- `p1=0.2` は「top1が top2より20ポイント以上高い場合に突出型」という意味

---

## 3. 各パターンの投資戦略

### 3.1 突出型（one_dominant）: `select_bets_one_dominant`

**戦略:** 複勝率最上位の1頭に集中投資

```python
bets = select_bets_one_dominant(
    race_df,
    capital=100_000.0,
    kelly_fraction=0.25,    # Kelly比率（デフォルト: 25%）
    max_bet_ratio=0.05,     # 最大賭け金比率（資金の5%）
    min_bet_amount=100.0,   # 最低賭け金（100円）
)
```

**投資判断:**
1. 複勝率が最も高い馬を1頭選定
2. `fractional_kelly(win_prob, odds, kelly_fraction)` で賭け比率を計算
3. Kelly値が0以下（期待値マイナス）の場合は賭けない

**使いどころ:** 明らかな本命馬がいるレース。オッズが割に合うか確認した上で集中投資。

---

### 3.2 拮抗型（competitive）: `select_bets_competitive`

**戦略:** 複勝率上位3頭への分散投資（穴狙い）

```python
bets = select_bets_competitive(
    race_df,
    capital=100_000.0,
    top_n=3,                            # 候補頭数
    expected_return_threshold=1.0,      # 期待回収率閾値（低め設定）
    kelly_fraction=0.25,
    max_bet_ratio=0.05,
    min_bet_amount=100.0,
)
```

**投資判断:**
1. 複勝率上位 `top_n` 頭を候補とする
2. 各馬の期待回収率（`win_place_prob × odds`）が `expected_return_threshold` を超えれば賭け対象
3. 拮抗型では閾値を低め（デフォルト1.0）に設定し、複数馬をカバー

**使いどころ:** 多頭接戦レース。高配当が出やすいため、複数馬をカバーして期待値を確保する。

**注意:** `select_bets_for_race` 経由で呼び出すと、`expected_return_threshold` は `max(1.0, threshold - 0.2)` に自動調整されます。

---

### 3.3 標準型（standard）: `select_bets_standard`

**戦略:** 全馬の中から期待回収率フィルタで選定

```python
bets = select_bets_standard(
    race_df,
    capital=100_000.0,
    expected_return_threshold=1.2,  # 期待回収率閾値（デフォルト: 1.2）
    kelly_fraction=0.25,
    max_bet_ratio=0.05,
    min_bet_amount=100.0,
)
```

**投資判断:**
1. 全馬を対象に `win_place_prob × odds > expected_return_threshold` を満たす馬を選定
2. 各馬に Fractional Kelly で賭け金を計算

**使いどころ:** 通常レース。期待値がプラスの馬のみに絞って投資する基本戦略。

---

## 4. 統合関数: `select_bets_for_race`

パターン判定から賭け選定まで一括で処理する統合関数です。

```python
from src.backtest.strategy import select_bets_for_race

bets, pattern = select_bets_for_race(
    race_df,            # horse_id, horse_number, win_place_prob, odds を含む DataFrame
    capital=100_000.0,
    p1=0.2,             # 突出型の判定閾値
    p2=0.15,            # 拮抗型の判定閾値
    expected_return_threshold=1.2,  # 標準型・拮抗型の閾値
    kelly_fraction=0.25,
    max_bet_ratio=0.05,
    min_bet_amount=100.0,
)

print(f"パターン: {pattern.pattern}")  # 'one_dominant' | 'competitive' | 'standard'
for bet in bets:
    print(f"  馬番{bet['horse_number']}: {bet['bet_amount']:.0f}円 (オッズ{bet['odds']:.1f})")
```

### 戻り値の仕様

| フィールド | 型 | 説明 |
|-----------|-----|------|
| `bets` | `list[dict]` | 賭け選定リスト（空リスト = 賭い対象なし） |
| `pattern` | `RacePattern` | パターン分類の詳細情報 |

各 bet の dict キー:

| キー | 型 | 説明 |
|-----|-----|------|
| `horse_id` | `str` | 馬ID |
| `horse_number` | `int` | 馬番 |
| `bet_amount` | `float` | 賭け金（100円単位） |
| `odds` | `float` | 複勝オッズ |

---

## 5. パラメータ最適化: `StrategyOptimizer`

グリッドサーチにより最適なパラメータ設定を探索します。

### 基本的な使い方

```python
from src.backtest.strategy_optimizer import StrategyOptimizer

# 初期化
optimizer = StrategyOptimizer(
    predictions_df=predictions_df,  # 予測結果 DataFrame
    payouts_df=payouts_df,          # 払戻情報 DataFrame（None 可）
    initial_capital=100_000.0,
)

# グリッドサーチ実行（デフォルト範囲）
results = optimizer.run_grid_search()

# 最良パラメータを取得（回収率ベース）
best = optimizer.best_params(results, metric="recovery_rate")
print(f"最良パラメータ: {best.params}")
print(f"回収率: {best.recovery_rate:.1f}%")
print(f"的中率: {best.hit_rate:.1f}%")
print(f"最大ドローダウン: {best.max_drawdown:.1f}%")

# パターン別成績サマリー
summary = optimizer.summary_by_pattern(results)
print(summary)
```

### グリッドサーチのパラメータ範囲

```python
results = optimizer.run_grid_search(
    p1_range=[0.1, 0.15, 0.2, 0.25, 0.3],        # 突出型閾値
    p2_range=[0.1, 0.15, 0.2],                    # 拮抗型閾値
    threshold_range=[1.0, 1.1, 1.2, 1.3, 1.5],   # 期待回収率閾値
    kelly_range=[0.1, 0.25, 0.5],                 # Kelly係数
    max_bet_ratio_range=[0.03, 0.05],             # 最大賭け金比率
)
# デフォルト: 5 × 3 × 5 × 3 × 2 = 450 パラメータ組み合わせ
```

### 最適化指標の選択

```python
# 回収率を最大化（デフォルト）
best_rr = optimizer.best_params(results, metric="recovery_rate")

# シャープレシオを最大化（リスク調整済みリターン）
best_sr = optimizer.best_params(results, metric="sharpe_ratio")

# 最大ドローダウンを最小化
best_dd = optimizer.best_params(results, metric="max_drawdown")

# 的中率を最大化
best_hr = optimizer.best_params(results, metric="hit_rate")
```

---

## 6. 賭け金計算の共通仕様

### Fractional Kelly 基準

```
Kelly値 = (p × (odds - 1) - (1 - p)) / (odds - 1)
賭け比率 = max(0, Kelly値) × kelly_fraction
賭け金 = min(capital × 賭け比率, capital × max_bet_ratio)
賭け金 = floor(賭け金 / 100) × 100   # 100円単位に切り捨て
賭け金 = max(min_bet_amount, 賭け金)
```

| パラメータ | デフォルト | 意味 |
|-----------|-----------|------|
| `kelly_fraction` | 0.25 | Kelly値の何割を使うか（リスク調整） |
| `max_bet_ratio` | 0.05 | 1レースで使える資金の最大比率（5%） |
| `min_bet_amount` | 100.0 | 最低賭け金（100円） |

### 賭け金の安全チェック
- Kelly値が0以下（期待値マイナス） → 賭けない
- 賭け金が現在の資金を超える → 賭けない
- 残高が `min_bet_amount` 未満 → 賭けない

---

## 7. データ仕様

### `race_df` の必須カラム

| カラム名 | 型 | 説明 |
|---------|-----|------|
| `horse_id` | str | 馬の識別子 |
| `horse_number` | int | 馬番 |
| `win_place_prob` | float | 予測複勝率（0〜1） |
| `odds` | float | 複勝オッズ |

### `predictions_df`（オプティマイザー用）の必須カラム

上記に加え:

| カラム名 | 型 | 説明 |
|---------|-----|------|
| `race_id` | str | レースID |
| `race_date` | date | レース日 |
| `finish_position` | int/float | 実際の着順（NaN=不明、0=除外） |
| `place_odds` | float | 複勝オッズ（オプティマイザーでは `place_odds` カラムを使用） |

---

## 8. 設計上の注意点

### データリーク対策
- `classify_race_pattern` はMLモデルの予測複勝率のみを使用し、実際の着順や払戻データは参照しません
- オッズは賭け金計算に使用しますが、意思決定（パターン分類）は複勝率のみで行います

### 境界値の扱い
- パターン判定は **未満（<）** と **超える（>）** で行われます（等号は含まない）
- `p1=0.2` のとき、差がちょうど0.2の場合は突出型 **ではありません**

### エラーハンドリング
- 3頭未満のレース: `ValueError` を raise
- NaNオッズの馬: 自動的にスキップ
- オッズ0以下の馬: 自動的にスキップ

---

## 9. 関連ファイル

| ファイル | 役割 |
|---------|------|
| `src/backtest/strategy.py` | パターン分類・賭け選定の実装 |
| `src/backtest/strategy_optimizer.py` | グリッドサーチによるパラメータ最適化 |
| `src/backtest/simulator.py` | Kelly基準の実装（`kelly_criterion`, `fractional_kelly`） |
| `src/backtest/metrics.py` | 評価指標の実装（`compute_metrics`） |
| `tests/test_backtest_strategy.py` | 投資戦略のユニットテスト |

---

## 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|----------|
| 2026-02-23 | 1.0.0 | Issue #18 実装に伴い初版作成 |
