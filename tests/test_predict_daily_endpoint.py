"""
predict_daily エンドポイントの target_dates 算出ロジックのテスト

Issue #243 修正: _today_jst() 導入後に日曜日実行で来週土日を参照してしまうバグの回帰テスト

バグの経緯:
  - 旧コード: tomorrow = _today_jst() + timedelta(1) を compute_week_boundaries に渡す
  - 日曜 8:00 JST に実行すると tomorrow = 月曜 → 来週土日 (NG)
  - 修正: today = _today_jst() を直接渡す (土/日/平日いずれでも今週土日が正しく返る)

注意: Cloud Run は UTC で動作するが _today_jst() は ZoneInfo("Asia/Tokyo") で JST 日付を返す。
      このため UTC/JST の日付差 (日曜 AM 8:00 JST = 土曜 PM 11:00 UTC) に依らず正しい曜日で動作する。
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# compute_week_boundaries の単体テスト（基準関数の挙動保証）
# ---------------------------------------------------------------------------

class TestComputeWeekBoundaries:
    """compute_week_boundaries の曜日別動作を確認する"""

    def setup_method(self):
        from src.models.train import compute_week_boundaries
        self.fn = compute_week_boundaries

    def test_saturday_returns_this_weekend(self):
        """土曜基準 → 当週土日を返すこと"""
        sat = datetime.date(2026, 4, 11)  # 土
        s, e = self.fn(sat)
        assert s == datetime.date(2026, 4, 11)
        assert e == datetime.date(2026, 4, 12)

    def test_sunday_returns_this_weekend(self):
        """日曜基準 → 当週土日を返すこと（修正後の期待動作）"""
        sun = datetime.date(2026, 4, 12)  # 日
        s, e = self.fn(sun)
        assert s == datetime.date(2026, 4, 11)
        assert e == datetime.date(2026, 4, 12)

    def test_monday_returns_next_weekend(self):
        """月曜基準 → 翌週土日を返すこと（平日は来週を対象とする設計）"""
        mon = datetime.date(2026, 4, 13)  # 月
        s, e = self.fn(mon)
        assert s == datetime.date(2026, 4, 18)
        assert e == datetime.date(2026, 4, 19)

    def test_friday_returns_next_weekend(self):
        """金曜基準 → 翌週土日（=翌日から1日後）を返すこと"""
        fri = datetime.date(2026, 4, 10)  # 金
        s, e = self.fn(fri)
        assert s == datetime.date(2026, 4, 11)
        assert e == datetime.date(2026, 4, 12)


# ---------------------------------------------------------------------------
# predict_daily エンドポイントの target_dates 検証
# ---------------------------------------------------------------------------

class TestPredictDailyTargetDates:
    """predict_daily が today_jst を基準に正しい土日を target_dates に設定すること"""

    def _make_success_result(self, sat, sun):
        return {
            "num_races": 2,
            "num_horses": 16,
            "saved_to_bq": True,
            "saved_to_gcs": False,
            "gcs_uri": None,
            "model_path": "gs://test/model.txt",
        }

    @patch("src.automation.api.app._run_predict")
    @patch("src.automation.api.app._today_jst")
    @patch.dict("os.environ", {"GCP_PROJECT_ID": "test_project"})
    def test_saturday_targets_this_weekend(self, mock_today, mock_run_predict):
        """土曜 AM 8:00 JST 実行 → [4/11, 4/12] を対象とすること"""
        mock_today.return_value = datetime.date(2026, 4, 11)  # 土
        sat = datetime.date(2026, 4, 11)
        sun = datetime.date(2026, 4, 12)
        mock_run_predict.return_value = self._make_success_result(sat, sun)

        import asyncio
        from src.automation.api.app import predict_daily, PredictDailyRequest

        result = asyncio.run(predict_daily(PredictDailyRequest()))

        assert result.target_dates == ["2026-04-11", "2026-04-12"]
        call_kwargs = mock_run_predict.call_args[1]
        assert call_kwargs["target_dates"] == [sat, sun]

    @patch("src.automation.api.app._run_predict")
    @patch("src.automation.api.app._today_jst")
    @patch.dict("os.environ", {"GCP_PROJECT_ID": "test_project"})
    def test_sunday_targets_this_weekend_not_next(self, mock_today, mock_run_predict):
        """
        日曜 AM 8:00 JST 実行 → [4/11, 4/12] を対象とすること（バグ回帰テスト）

        修正前バグ: tomorrow(月曜)を基準にすると [4/18, 4/19] (来週) になっていた
        """
        mock_today.return_value = datetime.date(2026, 4, 12)  # 日
        sat = datetime.date(2026, 4, 11)
        sun = datetime.date(2026, 4, 12)
        mock_run_predict.return_value = self._make_success_result(sat, sun)

        import asyncio
        from src.automation.api.app import predict_daily, PredictDailyRequest

        result = asyncio.run(predict_daily(PredictDailyRequest()))

        assert result.target_dates == ["2026-04-11", "2026-04-12"], (
            "日曜実行時に来週土日 [4/18, 4/19] を返すバグが再発していないか確認"
        )
        call_kwargs = mock_run_predict.call_args[1]
        assert call_kwargs["target_dates"] == [sat, sun]

    @patch("src.automation.api.app._run_predict")
    @patch("src.automation.api.app._today_jst")
    @patch.dict("os.environ", {"GCP_PROJECT_ID": "test_project"})
    def test_monday_targets_next_weekend(self, mock_today, mock_run_predict):
        """月曜（平日）実行 → [4/18, 4/19] (来週土日) を対象とすること"""
        mock_today.return_value = datetime.date(2026, 4, 13)  # 月
        sat = datetime.date(2026, 4, 18)
        sun = datetime.date(2026, 4, 19)
        mock_run_predict.return_value = self._make_success_result(sat, sun)

        import asyncio
        from src.automation.api.app import predict_daily, PredictDailyRequest

        result = asyncio.run(predict_daily(PredictDailyRequest()))

        assert result.target_dates == ["2026-04-18", "2026-04-19"]
        call_kwargs = mock_run_predict.call_args[1]
        assert call_kwargs["target_dates"] == [sat, sun]
