# 投資戦略設計ドキュメント

> **このドキュメントは廃止されました。**
>
> 現在の投資戦略仕様は [STRATEGY.md](../STRATEGY.md) を参照してください。
>
> 主な変更点（旧→現在）:
> - パターン分類: 3パターン（one_dominant / competitive / standard）→ **2パターン（one_dominant / standard）**
> - パターン判定: `top1 - top2 > p1` 方式 → **ジニ係数ベース（補正ジニ係数 > p1）**
> - `p2`（拮抗型閾値）パラメータ: **廃止**
> - 賭け金計算: `max_bet_ratio × capital` の Kelly 方式 → **`budget_per_race` 固定予算（3,000円）**
> - `kelly_fraction`、`max_bet_ratio` パラメータ: **廃止**
> - 馬券種: 複勝のみ → **複勝・ワイド・三連複・馬連（パターンA）**
