"""
run_strategy.py の build_race_df と _build_decision_row のユニットテスト

Issue #166: win_odds が race_df に渡されないため単勝が常に0件になるバグの修正検証。
Issue #194: 単勝の expected_return に複勝率を使っているため誤った値になる問題の修正検証。
Issue #XXX: save_decisions_to_bq が target_date 指定時に日付全体削除→INSERTで保存すること。
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from scripts.run_strategy import build_race_df, _build_decision_row, save_decisions_to_bq


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


# ---------------------------------------------------------------------------
# _build_decision_row のテスト（Issue #194）
# ---------------------------------------------------------------------------


def _make_race_group(horse_number: int = 1, win_place_prob: float = 0.5) -> pd.DataFrame:
    return pd.DataFrame([{
        "race_id": "race_001",
        "horse_id": "horse_001",
        "horse_number": horse_number,
        "horse_name": "テストホース",
        "win_place_prob": win_place_prob,
    }])


def _make_meta_row() -> pd.Series:
    return pd.Series({"venue_code": "09", "race_number": 5})


class TestBuildDecisionRow:
    """_build_decision_row の単勝/複勝 expected_return 計算テスト（Issue #194）"""

    def test_place_bet_has_expected_return(self):
        """複勝は expected_return が計算される"""
        race_group = _make_race_group(horse_number=1, win_place_prob=0.4)
        bet = {"bet_type": "place", "horse_numbers": [1], "horse_id": "horse_001", "odds": 3.0, "bet_amount": 1000.0}
        row = _build_decision_row(
            race_id="race_001",
            target_date=datetime.date(2026, 3, 29),
            meta_row=_make_meta_row(),
            race_group=race_group,
            bet=bet,
            race_pattern_str="standard",
            created_at=datetime.datetime(2026, 3, 29, 9, 0, 0),
        )
        assert row["expected_return"] is not None
        assert abs(row["expected_return"] - 0.4 * 3.0) < 1e-6

    def test_win_bet_expected_return_is_none(self):
        """単勝は expected_return が None（win_prob がないため計算しない）"""
        race_group = _make_race_group(horse_number=1, win_place_prob=0.733)
        bet = {"bet_type": "win", "horse_numbers": [1], "horse_id": "horse_001", "odds": 5.8, "bet_amount": 1000.0}
        row = _build_decision_row(
            race_id="race_001",
            target_date=datetime.date(2026, 3, 29),
            meta_row=_make_meta_row(),
            race_group=race_group,
            bet=bet,
            race_pattern_str="one_dominant",
            created_at=datetime.datetime(2026, 3, 29, 9, 0, 0),
        )
        assert row["expected_return"] is None, "単勝の expected_return は None であるべき"

    def test_win_bet_still_has_win_place_prob(self):
        """単勝でも win_place_prob は保存される（参考表示用）"""
        race_group = _make_race_group(horse_number=1, win_place_prob=0.733)
        bet = {"bet_type": "win", "horse_numbers": [1], "horse_id": "horse_001", "odds": 5.8, "bet_amount": 1000.0}
        row = _build_decision_row(
            race_id="race_001",
            target_date=datetime.date(2026, 3, 29),
            meta_row=_make_meta_row(),
            race_group=race_group,
            bet=bet,
            race_pattern_str="one_dominant",
            created_at=datetime.datetime(2026, 3, 29, 9, 0, 0),
        )
        assert abs(row["win_place_prob"] - 0.733) < 1e-6


# ---------------------------------------------------------------------------
# save_decisions_to_bq の target_date 挙動テスト
# ---------------------------------------------------------------------------


def _make_decisions(n: int = 2) -> list[dict]:
    """テスト用の最小限 decisions リストを生成する"""
    return [
        {
            "race_id": "race_001",
            "race_date": datetime.date(2026, 4, 12),
            "horse_numbers": str(i),
            "horse_names": f"テストホース{i}",
            "venue_code": "09",
            "race_number": 1,
            "race_pattern": "standard",
            "bet_type": "place",
            "bet_amount": 300.0,
            "win_place_prob": 0.3,
            "place_odds": 3.0,
            "expected_return": 0.9,
            "created_at": datetime.datetime(2026, 4, 12, 9, 0, 0, tzinfo=datetime.timezone.utc),
        }
        for i in range(1, n + 1)
    ]


class TestSaveDecisionsToBq:
    """save_decisions_to_bq の保存モード切替テスト"""

    @patch("scripts.run_strategy.bigquery.Client")
    def test_target_date_deletes_by_date_then_inserts(self, mock_bq_cls):
        """target_date 指定時は race_date で全削除してから INSERT する（日次全件置換モード）"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.load_table_from_dataframe.return_value.result.return_value = None
        mock_client.query.return_value.result.return_value = None

        target_date = datetime.date(2026, 4, 12)
        save_decisions_to_bq(_make_decisions(), "test-project", target_date=target_date)

        # query() の呼び出し引数を収集
        query_calls = [c.args[0].strip() for c in mock_client.query.call_args_list]

        # 1回目: race_date による全削除
        assert any("race_date = '2026-04-12'" in q for q in query_calls), (
            "target_date 指定時は race_date で全削除すべき"
        )
        # 2回目: INSERT INTO（MERGE ではない）
        assert any(q.startswith("INSERT INTO") for q in query_calls), (
            "target_date 指定時は MERGE ではなく INSERT INTO を使うべき"
        )
        # MERGE は呼ばれない
        assert not any("MERGE" in q for q in query_calls), (
            "target_date 指定時に MERGE が呼ばれてはいけない"
        )

    @patch("scripts.run_strategy.bigquery.Client")
    def test_no_target_date_uses_race_id_delete_and_merge(self, mock_bq_cls):
        """target_date 未指定時は race_id で削除してから MERGE する（1レース更新モード）"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.load_table_from_dataframe.return_value.result.return_value = None
        mock_client.query.return_value.result.return_value = None

        save_decisions_to_bq(_make_decisions(), "test-project", target_date=None)

        query_calls = [c.args[0].strip() for c in mock_client.query.call_args_list]

        # race_date による全削除は呼ばれない
        assert not any("race_date" in q and "DELETE" in q for q in query_calls), (
            "target_date 未指定時に race_date DELETE が呼ばれてはいけない"
        )
        # race_id による削除が呼ばれる
        assert any("race_id IN" in q for q in query_calls), (
            "target_date 未指定時は race_id で削除すべき"
        )
        # MERGE が呼ばれる
        assert any("MERGE" in q for q in query_calls), (
            "target_date 未指定時は MERGE を使うべき"
        )

    @patch("scripts.run_strategy.bigquery.Client")
    def test_target_date_with_empty_decisions_still_deletes(self, mock_bq_cls):
        """decisions が空でも target_date 指定時は削除だけ実行される（ベットなしレースの旧データ消去）"""
        mock_client = MagicMock()
        mock_bq_cls.return_value = mock_client
        mock_client.query.return_value.result.return_value = None

        target_date = datetime.date(2026, 4, 12)
        result = save_decisions_to_bq([], "test-project", target_date=target_date)

        assert result == 0
        query_calls = [c.args[0].strip() for c in mock_client.query.call_args_list]
        # 削除クエリは実行される
        assert any("race_date = '2026-04-12'" in q for q in query_calls), (
            "decisions が空でも target_date 指定時は race_date で削除すべき"
        )
