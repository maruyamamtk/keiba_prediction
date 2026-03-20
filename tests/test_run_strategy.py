"""
run_strategy.py の build_race_df のユニットテスト

Issue #166: win_odds が race_df に渡されないため単勝が常に0件になるバグの修正検証。
"""

from __future__ import annotations

import pandas as pd
import pytest

from scripts.run_strategy import build_race_df


# ---------------------------------------------------------------------------
# テスト用ヘルパー
# ---------------------------------------------------------------------------


def _make_predictions(n_horses: int = 3) -> pd.DataFrame:
    rows = []
    for i in range(1, n_horses + 1):
        rows.append({
            "race_id": "race_001",
            "horse_id": f"horse_{i:03d}",
            "horse_number": i,
            "win_place_prob": 0.3 / i,
            "pred_rank": i,
        })
    return pd.DataFrame(rows)


def _make_odds(n_horses: int = 3, include_win_odds: bool = True) -> pd.DataFrame:
    rows = []
    for i in range(1, n_horses + 1):
        row = {
            "race_id": "race_001",
            "horse_number": i,
            "place_odds_min": 2.0 + i,
        }
        if include_win_odds:
            row["win_odds"] = 5.0 + i * 2
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# build_race_df のテスト
# ---------------------------------------------------------------------------


class TestBuildRaceDf:
    def test_win_odds_included_in_result(self):
        """win_odds が odds_df に存在する場合、merged に win_odds カラムが含まれる（Issue #166）"""
        predictions_df = _make_predictions()
        odds_df = _make_odds(include_win_odds=True)

        result = build_race_df(predictions_df, odds_df)

        assert "win_odds" in result.columns, "win_odds カラムが結果に含まれるべき"

    def test_win_odds_values_correct(self):
        """win_odds の値が正しく JOIN されている"""
        predictions_df = _make_predictions(n_horses=3)
        odds_df = _make_odds(n_horses=3, include_win_odds=True)

        result = build_race_df(predictions_df, odds_df)

        for _, row in result.iterrows():
            expected_win_odds = 5.0 + int(row["horse_number"]) * 2
            assert row["win_odds"] == expected_win_odds

    def test_place_odds_renamed_to_odds(self):
        """place_odds_min が odds にリネームされている"""
        predictions_df = _make_predictions()
        odds_df = _make_odds()

        result = build_race_df(predictions_df, odds_df)

        assert "odds" in result.columns
        assert "place_odds_min" not in result.columns

    def test_win_odds_absent_when_not_in_odds_df(self):
        """odds_df に win_odds が存在しない場合、result にも win_odds は含まれない"""
        predictions_df = _make_predictions()
        odds_df = _make_odds(include_win_odds=False)

        result = build_race_df(predictions_df, odds_df)

        assert "win_odds" not in result.columns

    def test_empty_odds_df_returns_nan_odds(self):
        """odds_df が空の場合、odds=NaN で predictions_df を返す"""
        predictions_df = _make_predictions()
        odds_df = pd.DataFrame()

        result = build_race_df(predictions_df, odds_df)

        assert "odds" in result.columns
        assert result["odds"].isna().all()

    def test_required_columns_present(self):
        """strategy が必要とする必須カラムが全て含まれている"""
        predictions_df = _make_predictions()
        odds_df = _make_odds()

        result = build_race_df(predictions_df, odds_df)

        for col in ["horse_id", "horse_number", "win_place_prob", "odds"]:
            assert col in result.columns, f"{col} カラムが必要"
