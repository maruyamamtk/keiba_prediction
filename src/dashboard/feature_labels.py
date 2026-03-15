"""
特徴量カラム名 → 日本語ラベル マッピング

feature_query_raw.sql のコメントをもとに作成。
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# メインマッピング辞書
# ---------------------------------------------------------------------------

FEATURE_LABELS: dict[str, str] = {
    # ── レース基本情報 ────────────────────────────────────────────────────
    "distance": "距離(m)",
    "course_type": "コース種別(芝/ダート)",
    "direction": "回り(右/左)",
    "age_condition": "年齢条件",
    "race_class": "レースクラス",
    "num_horses": "出走頭数",
    "venue_name": "競馬場名",

    # ── 馬の基本情報 ─────────────────────────────────────────────────────
    "horse_age": "馬齢",
    "min_horse_age": "レース最小馬齢",
    "bracket_number": "枠番",
    "weight_carried": "斤量",
    "horse_age_segment": "馬齢セグメント",
    "trainer_affiliation": "厩舎所属(東西)",
    "blinker": "ブリンカー",
    "hoof_code": "蹄コード",
    "heavy_aptitude_code": "重適性コード",
    "running_style": "脚質(逃/先/差/追)",
    "distance_aptitude": "距離適性",
    "improvement": "上昇度",
    "pace_forecast": "ペース予想",
    "mid_gap": "中間ギャップ",

    # ── JRDBインデックス ──────────────────────────────────────────────────
    "idm": "能力指数(IDM)",
    "jockey_index": "騎手指数",
    "info_index": "情報指数",
    "total_index": "総合指数",
    "training_index": "調教指数",
    "stable_index": "厩舎指数",
    "popularity_index": "人気指数",
    "surge_index": "激走指数",
    "ten_index": "テン指数",
    "pace_index": "ペース指数",
    "agari_index": "上がり指数",
    "position_index": "位置指数",
    "base_odds": "想定単勝オッズ",
    "base_popularity": "想定人気",
    "jockey_expected_win_rate": "騎手期待連対率",

    # ── JRDBマーク ───────────────────────────────────────────────────────
    "overall_mark": "総合マーク",
    "idm_mark": "IDMマーク",
    "info_mark": "情報マーク",
    "jockey_mark": "騎手マーク",
    "stable_mark": "厩舎マーク",

    # ── N走前の結果 ───────────────────────────────────────────────────────
    **{f"finish_position_{n}": f"{n}走前着順" for n in range(1, 6)},
    **{f"finish_position_rate_{n}": f"{n}走前着順率" for n in range(1, 6)},
    **{f"win_popularity_{n}": f"{n}走前人気" for n in range(1, 6)},
    **{f"popularity_rate_{n}": f"{n}走前人気率" for n in range(1, 6)},
    **{f"upside_rate_{n}": f"{n}走前上振れ率" for n in range(1, 6)},
    **{f"win_odds_{n}": f"{n}走前単勝オッズ" for n in range(1, 6)},
    **{f"idm_{n}": f"{n}走前能力指数" for n in range(1, 6)},
    **{f"improvement_code_{n}": f"{n}走前上昇コード" for n in range(1, 6)},
    **{f"race_name_{n}": f"{n}走前レース名" for n in range(1, 6)},
    **{f"position_fault_{n}": f"{n}走前位置取り失敗" for n in range(1, 6)},
    **{f"late_start_{n}": f"{n}走前出遅れ" for n in range(1, 6)},
    **{f"disadvantage_{n}": f"{n}走前不利" for n in range(1, 6)},
    "race_date_diff_1": "1走前からの経過日数",
    "race_date_diff_2": "2走前からの経過日数",

    # ── EMA（指数移動平均）統計 ───────────────────────────────────────────
    "ema_finish_position": "EMA着順",
    "ema_finish_position_rate": "EMA着順率",
    "ema_idm": "EMA能力指数",
    "ema_idm_diff": "EMA能力指数差(馬齢比)",
    "ema_popularity_rate": "EMA人気率",
    "ema_upside_rate": "EMA上振れ率",
    "ema_win_popularity": "EMA人気",

    # ── 直近N走の集計統計 ─────────────────────────────────────────────────
    "mean_finish_position": "平均着順",
    "min_finish_position": "最良着順",
    "max_finish_position": "最悪着順",
    "mean_finish_position_rate": "平均着順率",
    "mean_idm": "平均能力指数",
    "mean_idm_diff": "平均能力指数差(馬齢比)",
    "mean_popularity_rate": "平均人気率",
    "mean_upside_rate": "平均上振れ率",
    "mean_win_popularity": "平均人気",
    "max_idm": "最大能力指数",
    "max_idm_diff": "最大能力指数差(馬齢比)",
    "max_popularity_rate": "最悪人気率",
    "max_upside_rate": "最大上振れ率",
    "max_win_popularity": "最悪人気",
    "min_idm": "最小能力指数",
    "min_popularity_rate": "最良人気率",
    "min_upside_rate": "最小上振れ率",
    "min_win_popularity": "最良人気",

    # ── 馬齢セグメント別指数 ─────────────────────────────────────────────
    "age_segment_idm": "馬齢別IDM",
    "age_segment_max_idm": "馬齢別最大IDM",
    "age_segment_mean_idm": "馬齢別平均IDM",
    "age_segment_ema_idm": "馬齢別EMA IDM",
    "idm_diff": "能力指数差(馬齢比)",
    "max_idm_diff": "最大能力指数差(馬齢比)",

    # ── 全体成績 ─────────────────────────────────────────────────────────
    "top1_finish_rate": "通算1着率",
    "top2_finish_rate": "通算連対率",
    "top3_finish_rate": "通算複勝率",

    # ── 芝/ダート別成績 ───────────────────────────────────────────────────
    "surface_top1_finish_rate": "芝/ダート別1着率",
    "surface_top2_finish_rate": "芝/ダート別連対率",
    "surface_top3_finish_rate": "芝/ダート別複勝率",
    "surface_top1_finish_rate_diff": "芝/ダート別1着率差(全体比)",
    "surface_top2_finish_rate_diff": "芝/ダート別連対率差(全体比)",
    "surface_top3_finish_rate_diff": "芝/ダート別複勝率差(全体比)",
    "new_surface_flag": "芝/ダート初出走フラグ",

    # ── 芝/ダート×距離別成績 ──────────────────────────────────────────────
    "surface_dist_top1_finish_rate": "芝/ダート距離別1着率",
    "surface_dist_top2_finish_rate": "芝/ダート距離別連対率",
    "surface_dist_top3_finish_rate": "芝/ダート距離別複勝率",
    "surface_dist_top1_finish_rate_diff": "芝/ダート距離別1着率差",
    "surface_dist_top2_finish_rate_diff": "芝/ダート距離別連対率差",
    "surface_dist_top3_finish_rate_diff": "芝/ダート距離別複勝率差",
    "new_surface_dist_flag": "芝/ダート距離初出走フラグ",

    # ── コース×距離別成績 ─────────────────────────────────────────────────
    "track_dist_top1_finish_rate": "コース距離別1着率",
    "track_dist_top2_finish_rate": "コース距離別連対率",
    "track_dist_top3_finish_rate": "コース距離別複勝率",
    "track_dist_top1_finish_rate_diff": "コース距離別1着率差",
    "track_dist_top2_finish_rate_diff": "コース距離別連対率差",
    "track_dist_top3_finish_rate_diff": "コース距離別複勝率差",
    "new_track_dist_flag": "コース距離初出走フラグ",

    # ── 回り別成績 ───────────────────────────────────────────────────────
    "direction_top1_finish_rate": "回り別1着率",
    "direction_top2_finish_rate": "回り別連対率",
    "direction_top3_finish_rate": "回り別複勝率",
    "direction_top1_finish_rate_diff": "回り別1着率差(全体比)",
    "direction_top2_finish_rate_diff": "回り別連対率差(全体比)",
    "direction_top3_finish_rate_diff": "回り別複勝率差(全体比)",
    "new_direction_flag": "回り初出走フラグ",

    # ── ローテーション別成績 ──────────────────────────────────────────────
    "rotation_top1_finish_rate": "ローテ別1着率",
    "rotation_top2_finish_rate": "ローテ別連対率",
    "rotation_top3_finish_rate": "ローテ別複勝率",
    "rotation_top1_finish_rate_diff": "ローテ別1着率差(全体比)",
    "rotation_top2_finish_rate_diff": "ローテ別連対率差(全体比)",
    "rotation_top3_finish_rate_diff": "ローテ別複勝率差(全体比)",

    # ── 馬場状態別成績 ───────────────────────────────────────────────────
    "condition_top1_finish_rate": "馬場状態別1着率",
    "condition_top2_finish_rate": "馬場状態別連対率",
    "condition_top3_finish_rate": "馬場状態別複勝率",
    "condition_top1_finish_rate_diff": "馬場状態別1着率差(全体比)",
    "condition_top2_finish_rate_diff": "馬場状態別連対率差(全体比)",
    "condition_top3_finish_rate_diff": "馬場状態別複勝率差(全体比)",
    "condition_change_flag": "馬場状態変化フラグ",

    # ── ペース別成績 ─────────────────────────────────────────────────────
    "pace_top1_finish_rate": "ペース別1着率",
    "pace_top2_finish_rate": "ペース別連対率",
    "pace_top3_finish_rate": "ペース別複勝率",
    "pace_top1_finish_rate_diff": "ペース別1着率差(全体比)",
    "pace_top2_finish_rate_diff": "ペース別連対率差(全体比)",
    "pace_top3_finish_rate_diff": "ペース別複勝率差(全体比)",

    # ── 季節別成績 ───────────────────────────────────────────────────────
    "season_top1_finish_rate": "季節別1着率",
    "season_top2_finish_rate": "季節別連対率",
    "season_top3_finish_rate": "季節別複勝率",
    "season_top1_finish_rate_diff": "季節別1着率差(全体比)",
    "season_top2_finish_rate_diff": "季節別連対率差(全体比)",
    "season_top3_finish_rate_diff": "季節別複勝率差(全体比)",

    # ── 枠順別成績 ───────────────────────────────────────────────────────
    "bracket_top1_finish_rate": "枠順別1着率",
    "bracket_top2_finish_rate": "枠順別連対率",
    "bracket_top3_finish_rate": "枠順別複勝率",
    "bracket_top1_finish_rate_diff": "枠順別1着率差(全体比)",
    "bracket_top2_finish_rate_diff": "枠順別連対率差(全体比)",
    "bracket_top3_finish_rate_diff": "枠順別複勝率差(全体比)",

    # ── 騎手×条件別成績 ───────────────────────────────────────────────────
    "jockey_dist_top1_finish_rate": "騎手×距離別1着率",
    "jockey_dist_top2_finish_rate": "騎手×距離別連対率",
    "jockey_dist_top3_finish_rate": "騎手×距離別複勝率",
    "jockey_track_dist_top1_finish_rate": "騎手×コース距離別1着率",
    "jockey_track_dist_top2_finish_rate": "騎手×コース距離別連対率",
    "jockey_track_dist_top3_finish_rate": "騎手×コース距離別複勝率",
    "jockey_trainer_top1_finish_rate": "騎手×調教師別1着率",
    "jockey_trainer_top2_finish_rate": "騎手×調教師別連対率",
    "jockey_trainer_top3_finish_rate": "騎手×調教師別複勝率",
    "jockey_owner_top1_finish_rate": "騎手×馬主別1着率",
    "jockey_owner_top2_finish_rate": "騎手×馬主別連対率",
    "jockey_owner_top3_finish_rate": "騎手×馬主別複勝率",
    "jockey_blinker_top1_finish_rate": "騎手×ブリンカー別1着率",
    "jockey_blinker_top2_finish_rate": "騎手×ブリンカー別連対率",
    "jockey_blinker_top3_finish_rate": "騎手×ブリンカー別複勝率",

    # ── 調教師×馬主別成績 ─────────────────────────────────────────────────
    "trainer_owner_top1_finish_rate": "調教師×馬主別1着率",
    "trainer_owner_top2_finish_rate": "調教師×馬主別連対率",
    "trainer_owner_top3_finish_rate": "調教師×馬主別複勝率",

    # ── 血統（父/母父産駒成績） ───────────────────────────────────────────
    "sire_surface_place_rate": "父産駒複勝率(芝/ダート)",
    "sire_surface_place_ratio": "父産駒複勝比率(芝/ダート)",
    "sire_place_distance_diff": "父産駒複勝率差(距離別)",
    "sire_surface_place_diff": "父産駒複勝率差(芝/ダート比)",
    "broodmare_sire_place_rate": "母父産駒複勝率",
    "broodmare_sire_surface_place_ratio": "母父産駒複勝比率(芝/ダート)",
    "broodmare_sire_place_distance_diff": "母父産駒複勝率差(距離別)",
    "broodmare_sire_surface_place_diff": "母父産駒複勝率差(芝/ダート比)",

    # ── 展開利 ───────────────────────────────────────────────────────────
    "early_advantage": "前有利指数",
    "behind_advantage": "後有利指数",
    "small_number_early_advantage": "少頭数前有利指数",

    # ── 激走スコア ───────────────────────────────────────────────────────
    "total_diff_sum": "激走スコア合計",
}


def to_label(feature_name: str) -> str:
    """
    カラム名を日本語ラベルに変換する。
    マッピングにない場合はカラム名をそのまま返す。
    """
    return FEATURE_LABELS.get(feature_name, feature_name)
