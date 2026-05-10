# 特徴量設計ドキュメント

特徴量の実装状況と今後の追加候補を整理したドキュメントです。
実装は `src/ml/features/feature_query_raw.sql` の BigQuery SQL で行われています。

---

## 前提：目的変数とリーク対策

**目的変数**
- 二値ラベル（3着以内=1, それ以外=0）→ LightGBM LambdaRank によるランク学習

**リークの典型**
- 発走後に確定する情報（確定オッズ、確定馬体重、確定馬場、出走取消/除外の結果反映、レース後指数など）
- 同一レース内の情報を使った集計でターゲットを見てしまう（target encodingのやり方次第で起きる）

**基本分割**
- 必ず時系列分割（例：年単位、月単位、開催単位）
- 同日/同開催を跨いだ情報が混ざらないように（特にTEや平均との差分系）

---

## 1. 実装済みの特徴量

### 1.1 レース基本情報（`raw.race_info` より）

| カラム名 | 説明 |
|---|---|
| `race_id` | レースID |
| `race_date` | 開催日 |
| `venue_code` / `venue_name` | 競馬場コード／名称 |
| `race_number` | レース番号 |
| `race_name` | レース名 |
| `course_type` | コース種別（芝/ダ） |
| `distance` | 距離 |
| `direction` | 回り（右/左） |
| `age_condition` | 年齢条件 |
| `race_class` | クラス |
| `num_horses` | 出走頭数（horse_resultsの馬番最大値で補正済み） |

### 1.2 馬・騎手・厩舎の基本情報（`raw.horse_results` / `raw.horse_master` より）

| カラム名 | 説明 |
|---|---|
| `horse_id` / `horse_name` | 馬ID／名称 |
| `horse_age` | 馬齢（race_date - birth_date） |
| `min_horse_age` | レース内最小馬齢（年齢構成判断用） |
| `bracket_number` | 枠番 |
| `horse_number` | 馬番 |
| `trainer_name` / `trainer_code` | 調教師名／コード |
| `jockey_name` / `jockey_code` | 騎手名／コード |
| `weight_carried` | 斤量 |
| `trainer_affiliation` | 厩舎所属（美浦/栗東） |
| `blinker` | ブリンカー着用フラグ |
| `hoof_code` | 蹄コード |
| `heavy_aptitude_code` | 重適性コード |

### 1.3 JRDBが提供する各種指数（`raw.horse_results` より）

| カラム名 | 説明 |
|---|---|
| `idm` | 能力指数 |
| `jockey_index` | 騎手指数 |
| `info_index` | 情報指数 |
| `total_index` | 総合指数 |
| `popularity_index` | 人気指数 |
| `training_index` | 調教指数 |
| `stable_index` | 厩舎指数 |
| `jockey_expected_win_rate` | 騎手期待連対率 |
| `surge_index` | 激走指数 |
| `ten_index` | テン指数（序盤の速さ） |
| `pace_index` | ペース指数 |
| `agari_index` | 上がり指数 |
| `position_index` | 位置取り指数 |
| `running_style` | 脚質（1逃げ/2先行/3差し/4追込） |
| `distance_aptitude` | 距離適性 |
| `improvement` | 上昇度 |
| `base_odds` / `base_popularity` | 想定オッズ／人気 |
| `pace_forecast` | ペース予想（S/M/H） |
| `mid_gap` | 中間ギャップ |
| `overall_mark` / `idm_mark` / `info_mark` / `jockey_mark` / `stable_mark` | 各種マーク |

### 1.4 展開系特徴量（派生）

| カラム名 | 説明 |
|---|---|
| `early_advantage` | スローペース×逃げ/先行で有利な場合に加点（0-2） |
| `behind_advantage` | ハイペース×差し/追込で有利な場合に加点（0-2） |
| `small_number_early_advantage` | 少頭数（10頭未満）での逃げ有利フラグ |

### 1.5 過去走情報（直近1〜5走、`raw.race_results` より）

各走について以下のカラムを保持（N=1〜5）：

| カラム名 | 説明 |
|---|---|
| `race_name_N` | N走前のレース名 |
| `race_date_diff_N` | N走前からの間隔（週数） |
| `finish_position_N` | N走前の着順（0=失格/取消） |
| `finish_position_rate_N` | 着順 / 出走頭数（率化） |
| `win_odds_N` | N走前の単勝オッズ |
| `win_popularity_N` | N走前の人気 |
| `popularity_rate_N` | 人気 / 出走頭数（率化） |
| `upside_rate_N` | (人気 - 着順) / 出走頭数（上振れ度） |
| `idm_N` | N走前の能力指数 |
| `improvement_code_N` | N走前の上昇度コード |
| `late_start_N` | 出遅れフラグ |
| `position_fault_N` | 位置取り不利フラグ |
| `disadvantage_N` | 不利フラグ |
| `condition_change_flag` | コース種別変更フラグ（1走前との比較） |

### 1.6 直近5走の集計指標（派生）

各指標について mean / EMA（加重平均）/ max / min を算出：

| 対象指標 | カラム prefix |
|---|---|
| 能力指数 (idm) | `mean_idm`, `ema_idm`, `max_idm`, `min_idm` |
| 着順 (finish_position) | `mean_finish_position`, `ema_finish_position`, `max_finish_position`, `min_finish_position` |
| 単勝人気 (win_popularity) | `mean_win_popularity`, `ema_win_popularity`, `max_win_popularity`, `min_win_popularity` |
| 着順率 (finish_position_rate) | `mean_finish_position_rate`, `ema_finish_position_rate`, `max_finish_position_rate`, `min_finish_position_rate` |
| 人気率 (popularity_rate) | `mean_popularity_rate`, `ema_popularity_rate`, `max_popularity_rate`, `min_popularity_rate` |
| 上振れ率 (upside_rate) | `mean_upside_rate`, `ema_upside_rate`, `max_upside_rate`, `min_upside_rate` |

> EMA は 1走前 × 1.5、2走前 × 1.25、3走前 × 1.0、4走前 × 0.75、5走前 × 0.5 の加重平均

### 1.7 レース内相対指標（派生）

| カラム名 | 説明 |
|---|---|
| `horse_age_segment` | 最若馬と同齢かどうかのフラグ（年齢混合戦対応） |
| `age_segment_idm` | 馬齢セグメント内での idm 順位 |
| `age_segment_mean_idm` | 馬齢セグメント内での mean_idm 順位 |
| `age_segment_ema_idm` | 馬齢セグメント内での ema_idm 順位 |
| `age_segment_max_idm` | 馬齢セグメント内での max_idm 順位 |
| `idm_diff` | レース内TOP との idm 差分 |
| `mean_idm_diff` | レース内TOP との mean_idm 差分 |
| `ema_idm_diff` | レース内TOP との ema_idm 差分 |
| `max_idm_diff` | レース内TOP との max_idm 差分 |

### 1.8 馬場情報（`raw.venue_info` より）

| カラム名 | 説明 |
|---|---|
| `turf_condition_code` | 芝馬場状態コード（1良/2稍/3重/4不） |
| `turf_condition_inner` / `turf_condition_outer` | 芝内側/外側の状態 |
| `turf_bias` | 芝の有利枠バイアス |
| `straight_bias_innermost` / `inner` / `outer` / `outermost` | 直線の各ゾーン有利度 |
| `dirt_condition_code` | ダート馬場状態コード |
| `dirt_condition_inner` / `dirt_condition_outer` | ダート内側/外側の状態 |
| `dirt_bias` | ダートの有利枠バイアス |

### 1.9 通算成績（`raw.horse_extended` より）

各条件の通算1着/2着以内/3着以内の率を保持（top1 / top2 / top3）：

| 条件 | カラム prefix |
|---|---|
| 全体 | `top3_finish_rate`, `top2_finish_rate`, `top1_finish_rate` |
| 芝ダ別 | `surface_top3/2/1_finish_rate` |
| 芝ダ×距離別 | `surface_dist_top3/2/1_finish_rate` |
| コース（競馬場×距離）別 | `track_dist_top3/2/1_finish_rate` |
| ローテーション別 | `rotation_top3/2/1_finish_rate` |
| 右/左回り別 | `direction_top3/2/1_finish_rate` |
| 馬場状態別 | `condition_top3/2/1_finish_rate`（コース種別×馬場状態で動的に参照） |
| ペース別 | `pace_top3/2/1_finish_rate`（pace_forecast で動的に参照） |
| 季節別 | `season_top3/2/1_finish_rate` |
| 枠番別 | `bracket_top3/2/1_finish_rate` |

初出走フラグ：`new_surface_flag`, `new_surface_dist_flag`, `new_track_dist_flag`, `new_direction_flag`

### 1.10 騎手・調教師コンビ成績（`raw.horse_extended` より）

| カラム prefix | 説明 |
|---|---|
| `jockey_dist_top3/2/1_finish_rate` | 騎手×距離別 |
| `jockey_track_dist_top3/2/1_finish_rate` | 騎手×コース×距離別 |
| `jockey_trainer_top3/2/1_finish_rate` | 騎手×調教師別 |
| `jockey_owner_top3/2/1_finish_rate` | 騎手×馬主別 |
| `jockey_blinker_top3/2/1_finish_rate` | 騎手×ブリンカー別 |
| `trainer_owner_top3/2/1_finish_rate` | 調教師×馬主別 |

### 1.11 血統情報（`raw.horse_master` より）

| カラム名 | 説明 |
|---|---|
| `sire_surface_place_rate` | 父産駒の今回コース種別複勝率 |
| `sire_surface_place_diff` | 父産駒の芝/ダ複勝率の差 |
| `sire_surface_place_ratio` | 父産駒の芝/ダ複勝率の比 |
| `sire_place_distance_diff` | 今回距離 − 父産駒の平均複勝距離 |
| `broodmare_sire_place_rate` | 母父産駒の今回コース種別複勝率 |
| `broodmare_sire_surface_place_diff` | 母父産駒の芝/ダ複勝率の差 |
| `broodmare_sire_surface_place_ratio` | 母父産駒の芝/ダ複勝率の比 |
| `broodmare_sire_place_distance_diff` | 今回距離 − 母父産駒の平均複勝距離 |

### 1.12 差分・複合特徴量（派生）

各条件別成績と全体成績との差分（`{condition}_top3/2/1_finish_rate_diff`）を全条件で算出。

| カラム名 | 説明 |
|---|---|
| `total_diff_sum` | 全差分の合計（条件適性の総合スコア＝激走フラグの代替） |

---

## 2. 今後追加で実装すると精度向上が期待できる特徴量

優先度の高い順に記載。

### 2.1 走破タイム・上がり3F 系（優先度：高）

現在 race_results テーブルに格納されているが feature SQL では未集計。

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| 過去N走の走破タイム | 生タイムだけでなく同場・同距離・同馬場で標準化した偏差値も有効 | `race_results` から取得し、同条件の平均と比較 |
| 過去N走の上がり3F | 追込・差し馬の末脚評価。上がり順位も重要 | `race_results` から取得 |
| 過去N走の通過順位（4角位置） | 脚質の客観評価。先行率・追込率を算出可能 | `race_results` から取得 |
| 斤量補正タイム | 斤量差を線形補正したタイム | 走破タイム実装後に派生 |

### 2.2 休養日数・ローテーション精緻化（優先度：高）

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| 叩き何走目（連続出走数） | 休み明け1走目か否かで仕上がりが変わる | 休養週数に閾値を設けてフラグ化 |
| 連闘フラグ | race_date_diff_1 <= 1 週で検出可能 | 簡単に実装可能 |
| 直近3走の idm トレンド | 能力の上昇・下降傾向を数値化 | `(idm_1 - idm_3) / 2` 等の線形傾きで近似 |
| 斤量変化 | 前走との斤量差（増減方向も重要） | `weight_carried - (1走前の斤量)` で算出 |

### 2.3 レース内相対指標の拡充（優先度：高）

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| 斤量のレース内順位・平均差 | 斤量ハンデの相対的有利/不利 | Window 関数で算出 |
| 先行力のレース内分布 | 逃げ候補数、先行馬比率（混戦度） | `running_style` 集計で算出 |
| レース内能力指数の分散 | 混戦度。分散が大きいほど実力差がはっきりしている | Window 関数で算出 |
| 馬番の相対位置 | `horse_number / num_horses`（内外の相対位置） | 単純な割り算で算出 |

### 2.4 Target Encoding（優先度：中）

カテゴリ変数の Target Encoding は騎手・調教師・種牡馬で特に効果が高い。リーク防止のため**時系列 OOF（その行より過去のデータだけで集計）＋スムージング** が必須。

| 対象 | ターゲット | 実装方針 |
|---|---|---|
| 騎手 | P(3着以内) | 時系列OOF + `(sum_y + m * global_mean) / (count + m)` |
| 調教師 | P(3着以内) | 同上 |
| 種牡馬 | P(3着以内)、芝ダ別 | 同上 |
| 騎手×競馬場 | P(3着以内) | 疎な場合は騎手単体とブレンド |
| 騎手×距離帯 | P(3着以内) | 同上 |

### 2.5 オッズ系特徴量（優先度：中）

`predictions.daily_odds` でリアルタイムオッズは取得済みだが、training_data への統合が未実装。

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| 当日朝の単勝・複勝オッズ | モデルの予測とオッズの乖離が穴馬発見に有効 | `daily_odds` を発走前固定タイムスタンプで取得してJOIN |
| オッズ変動量（前日→当日朝） | 資金流入の方向性を示す | JRDB の `base_odds` と当日朝オッズの差分 |

> リーク注意：「発走前に公開済みのオッズ」に限定し、確定オッズは使用しない。

### 2.6 距離帯カテゴリ化（優先度：中）

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| 距離帯区分 | 短距離(<1400m) / マイル(1400-1800m) / 中距離(1800-2200m) / 長距離(>2200m) | CASE WHEN で分類 |
| 距離帯ごとの過去成績 | 距離変更時の適性判断 | `horse_extended` の既存成績を距離帯で再集計 |
| 今回距離 − 前走距離 | 距離延長/短縮フラグ | `distance - (1走前の距離)` |

### 2.7 自作能力指数（優先度：低）

JRDB の idm への依存を減らし、自前でタイム補正指数を構築することで汎化性向上が期待できる。

| 特徴量 | 説明 | 実装方針 |
|---|---|---|
| タイム補正指数 | 場×距離×馬場で標準化した走破タイム偏差値 | 走破タイム実装後に構築 |
| 上がり指数 | 上がり3F の同条件偏差値 | 同上 |
| レース格補正指数 | 出走クラスとメンバー質を加味した指数 | 段階的に構築 |

### 2.8 調教タイム（優先度：低）

JRDBのCZAファイル等から取得可能だが、現在未パース。

| 特徴量 | 説明 |
|---|---|
| 最終追い切りタイム（坂路/ウッド） | 調教の仕上がり度を示す最重要指標の一つ |
| 追い切りラップ | 終いの伸びを評価 |
| 調教本数 | 仕上げ過程の充実度 |

---

## 参考：リーク対策チェックリスト

- [ ] 発走後に確定する情報を使用していない（確定オッズ、確定馬体重など）
- [ ] 条件別成績は当該レースを除外して計算
- [ ] 同一レース内の情報漏洩がない
- [ ] 時系列分割でバックテスト実施
- [ ] Target Encoding は時系列OOFで実装

---

*実装は `src/ml/features/feature_query_raw.sql` を参照。*
