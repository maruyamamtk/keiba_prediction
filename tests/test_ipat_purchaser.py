"""
IpatPurchaser ユニットテスト

Issue #213: 発走5分前JRA IPAT自動馬券購入パイプラインの実装

テスト対象:
  - fetch_target_races(): 発走時刻ウィンドウによるレース絞り込みロジック
  - IpatPurchaser.login(): ログイン成功・失敗ケース（Playwright モック）
  - IpatPurchaser.purchase_bet(): 購入成功・失敗ケース（Playwright モック）
  - _purchase_pipeline_async(): 予算上限チェック・LINE通知ロジック

Note: pytest-asyncio が未インストールのため、asyncio.run() でラップしてテストする。
"""

from __future__ import annotations

import asyncio
import datetime
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.automation.data.ipat_purchaser import (
    BET_TYPE_MAP,
    DAILY_BUDGET_LIMIT,
    IpatLoginError,
    IpatPurchaseError,
    IpatPurchaser,
    fetch_target_races,
)


def run_async(coro):
    """非同期コルーチンを同期的に実行するヘルパー"""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# fetch_target_races のテスト（純粋関数 — モック不要）
# ---------------------------------------------------------------------------

class TestFetchTargetRaces:
    """発走時刻ウィンドウによるレース絞り込みロジックのテスト"""

    def _make_races(self, start_times: list[str]) -> list[dict]:
        return [
            {"race_id": f"race_{t}", "start_time": t, "venue_name": "東京", "race_number": i + 1}
            for i, t in enumerate(start_times)
        ]

    def test_race_within_window_is_included(self):
        """ウィンドウ内（5〜10分後）のレースが抽出されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1007"])  # 10:07 = now + 7分
        result = fetch_target_races(races, now, window_minutes_before=10, window_minutes_after=5)
        assert len(result) == 1
        assert result[0]["race_id"] == "race_1007"

    def test_race_too_soon_is_excluded(self):
        """ウィンドウより前（3分後）のレースは除外されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1003"])  # 10:03 = now + 3分
        result = fetch_target_races(races, now)
        assert len(result) == 0

    def test_race_too_far_is_excluded(self):
        """ウィンドウより後（15分後）のレースは除外されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1015"])  # 10:15 = now + 15分
        result = fetch_target_races(races, now)
        assert len(result) == 0

    def test_multiple_races_in_window(self):
        """ウィンドウ内に複数レースがある場合すべて抽出されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1006", "1008", "1003", "1020"])
        result = fetch_target_races(races, now)
        race_ids = [r["race_id"] for r in result]
        assert "race_1006" in race_ids
        assert "race_1008" in race_ids
        assert "race_1003" not in race_ids
        assert "race_1020" not in race_ids

    def test_null_start_time_is_skipped(self):
        """start_time が空またはNoneのレースはスキップされること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = [
            {"race_id": "race_empty", "start_time": "", "venue_name": "東京", "race_number": 1},
            {"race_id": "race_none", "start_time": None, "venue_name": "東京", "race_number": 2},
        ]
        result = fetch_target_races(races, now)
        assert len(result) == 0

    def test_empty_race_list(self):
        """レースが0件の場合は空リストを返すこと"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        result = fetch_target_races([], now)
        assert result == []

    def test_boundary_at_five_minutes(self):
        """ウィンドウ境界値（ちょうど5分後）のレースが含まれること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1005"])  # 10:05 = now + 5分
        result = fetch_target_races(races, now)
        assert len(result) == 1

    def test_boundary_at_ten_minutes(self):
        """ウィンドウ境界値（ちょうど10分後）のレースが含まれること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1010"])  # 10:10 = now + 10分
        result = fetch_target_races(races, now)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# IpatPurchaser.login() のテスト（Playwright をモック）
# ---------------------------------------------------------------------------

class TestIpatPurchaserLogin:
    """IpatPurchaser.login() の成功・失敗ケースのテスト"""

    IPAT_BASE_URL = "https://www.ipat.jra.go.jp/sp/index.cgi"
    IPAT_MENU_URL = "https://www.ipat.jra.go.jp/sp/pw_732_i.cgi"

    def _make_purchaser(self) -> IpatPurchaser:
        p = IpatPurchaser("12345678", "1234", "8765")
        p._page = AsyncMock()
        # dialog イベントリスナー登録のモック
        p._page.on = MagicMock()
        return p

    def test_login_success(self):
        """ページが IPAT_BASE_URL 以外に遷移し、エラーなければ True を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_MENU_URL  # ログイン後に遷移
        purchaser._page.text_content = AsyncMock(return_value="馬券購入メニュー")

        result = run_async(purchaser.login())

        assert result is True
        purchaser._page.goto.assert_called_once()
        purchaser._page.fill.assert_called()
        # SP版ログインボタン（li.btnColor a）がクリックされること
        purchaser._page.click.assert_called_once_with('li.btnColor a', timeout=30_000)

    def test_login_failure_url_unchanged(self):
        """ページが IPAT_BASE_URL のまま遷移しなかった場合は False を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_BASE_URL  # ページ遷移なし
        purchaser._page.text_content = AsyncMock(return_value="加入者番号を入力してください")

        result = run_async(purchaser.login())

        assert result is False

    def test_login_failure_error_message_detected(self):
        """ページにエラーキーワードがある場合は False を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_MENU_URL
        purchaser._page.text_content = AsyncMock(return_value="暗証番号が違います。再度ご確認ください。")

        result = run_async(purchaser.login())

        assert result is False

    def test_login_failure_member_id_error(self):
        """加入者番号エラーのメッセージがある場合は False を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_MENU_URL
        purchaser._page.text_content = AsyncMock(return_value="加入者番号または暗証番号が正しくありません")

        result = run_async(purchaser.login())

        assert result is False

    def test_login_raises_on_playwright_error(self):
        """Playwright エラー時は IpatLoginError を送出すること"""
        purchaser = self._make_purchaser()
        purchaser._page.goto = AsyncMock(side_effect=Exception("Network error"))

        with pytest.raises(IpatLoginError):
            run_async(purchaser.login())

    def test_login_without_browser_raises(self):
        """_page が None の場合は IpatLoginError を送出すること"""
        purchaser = IpatPurchaser("12345678", "1234", "8765")
        # _page を None のまま（コンテキストマネージャーを使わない）

        with pytest.raises(IpatLoginError):
            run_async(purchaser.login())


# ---------------------------------------------------------------------------
# IpatPurchaser.purchase_bet() のテスト（Playwright をモック）
# ---------------------------------------------------------------------------

class TestIpatPurchaserPurchaseBet:
    """IpatPurchaser.purchase_bet() の成功・失敗ケースのテスト"""

    def _make_purchaser(self) -> IpatPurchaser:
        p = IpatPurchaser("12345678", "1234", "87654321")
        p._page = AsyncMock()
        return p

    def test_purchase_success(self):
        """購入完了テキストが確認できれば success を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.text_content = AsyncMock(return_value="購入完了しました。受付番号: 12345")

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "success"
        assert result["error_message"] is None

    def test_purchase_failure_insufficient_balance(self):
        """残高不足メッセージがある場合は failed を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.text_content = AsyncMock(return_value="残高不足のため購入できませんでした")

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "failed"
        assert "残高不足" in result["error_message"]

    def test_purchase_invalid_bet_type_raises(self):
        """未対応の馬券種は IpatPurchaseError を送出すること"""
        purchaser = self._make_purchaser()

        with pytest.raises(IpatPurchaseError):
            run_async(purchaser.purchase_bet("invalid_type", [3], 300, "東京(土)", 7))

    def test_purchase_invalid_amount_raises(self):
        """100円単位でない金額は IpatPurchaseError を送出すること"""
        purchaser = self._make_purchaser()

        with pytest.raises(IpatPurchaseError):
            run_async(purchaser.purchase_bet("place", [3], 150, "東京(土)", 7))

    def test_purchase_zero_amount_raises(self):
        """0円は IpatPurchaseError を送出すること"""
        purchaser = self._make_purchaser()

        with pytest.raises(IpatPurchaseError):
            run_async(purchaser.purchase_bet("place", [3], 0, "東京(土)", 7))

    def test_purchase_timeout_returns_failed(self):
        """タイムアウトエラーは status=failed を返すこと"""
        purchaser = self._make_purchaser()
        purchaser._page.click = AsyncMock(side_effect=Exception("Timeout exceeded"))

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "failed"
        assert result["error_message"] is not None


# ---------------------------------------------------------------------------
# dry_run フラグのテスト
# ---------------------------------------------------------------------------

class TestDryRunFlag:
    """PurchaseDailyRequest の dry_run フラグに関するテスト"""

    def test_dry_run_default_is_false(self):
        """dry_run のデフォルト値が False（本番購入モード）であること"""
        import sys
        sys.path.insert(0, str(ROOT_DIR))
        # app.py の PurchaseDailyRequest を直接検査
        from pydantic import BaseModel
        from typing import Optional

        # デフォルト値が False になっていることをフィールド定義で確認
        # （実際のリクエストオブジェクトを生成して確認）
        import importlib
        app_module = importlib.import_module("src.automation.api.app")
        req = app_module.PurchaseDailyRequest()
        assert req.dry_run is False

    def test_dry_run_can_be_set_false(self):
        """dry_run=False を明示的に指定できること"""
        import importlib
        app_module = importlib.import_module("src.automation.api.app")
        req = app_module.PurchaseDailyRequest(dry_run=False)
        assert req.dry_run is False

    def test_dry_run_response_contains_flag(self):
        """レスポンスに dry_run フラグが含まれること"""
        import importlib
        app_module = importlib.import_module("src.automation.api.app")
        resp = app_module.PurchaseDailyResponse(
            status="success",
            execution_date="2026-04-05",
            dry_run=True,
        )
        assert resp.dry_run is True


# ---------------------------------------------------------------------------
# 予算上限チェックロジックのテスト
# ---------------------------------------------------------------------------

class TestBudgetCheck:
    """予算上限（50,000円）チェックロジックのテスト"""

    def test_budget_limit_constant(self):
        """DAILY_BUDGET_LIMIT が 50,000 円に設定されていること"""
        assert DAILY_BUDGET_LIMIT == 50_000

    def test_budget_exceeded_condition(self):
        """累計 + 今回購入額 > 50,000円 で上限超過と判定できること"""
        spent = 48_000
        amount = 3_000
        assert spent + amount > DAILY_BUDGET_LIMIT

    def test_budget_within_limit(self):
        """累計 + 今回購入額 <= 50,000円 なら上限内と判定できること"""
        spent = 47_000
        amount = 3_000
        assert spent + amount <= DAILY_BUDGET_LIMIT

    def test_all_bet_types_in_map(self):
        """全馬券種が BET_TYPE_MAP に定義されていること"""
        expected_types = {"win", "place", "umaren", "wide", "umatan", "sanrenpuku"}
        assert set(BET_TYPE_MAP.keys()) == expected_types

    def test_sanrenpuku_label_is_fullwidth(self):
        """sanrenpuku のIPAT表示ラベルが全角数字「３連複」であること"""
        assert BET_TYPE_MAP["sanrenpuku"] == "３連複"


# ---------------------------------------------------------------------------
# リアルタイムオッズスクレイピングのテスト
# ---------------------------------------------------------------------------

class TestRealtimeScraping:
    """_purchase_pipeline_async() におけるリアルタイムオッズ取得の検証"""

    TARGET_DATE = datetime.date(2026, 4, 12)
    RACE_ID_1 = "06261411"
    RACE_ID_2 = "05261208"

    def _run_pipeline(self, target_races: list[dict], scrape_mock: MagicMock) -> dict:
        """_purchase_pipeline_async() を dry_run=True で実行するヘルパー"""
        import importlib
        app_module = importlib.import_module("src.automation.api.app")

        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[
                    {
                        "race_id": r["race_id"],
                        "start_time": "1000",
                        "venue_name": "東京",
                        "race_number": 1,
                    }
                    for r in target_races
                ],
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_target_races",
                return_value=target_races,
            ),
            patch(
                "src.automation.data.netkeiba_scraper.scrape_odds_for_race",
                side_effect=scrape_mock,
            ),
            patch(
                "src.automation.api.app._refresh_investment_decisions_for_race",
                return_value=True,
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_recommended_bets",
                return_value=[],
            ),
            patch("src.utils.line_notify.push_messages"),
        ):
            return run_async(
                app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="",
                    line_user_id="",
                    dry_run=True,
                )
            )

    def test_scrape_called_for_each_target_race(self):
        """target_races が1件以上ある場合、各レースに対して scrape_odds_for_race が呼ばれること"""
        target_races = [
            {"race_id": self.RACE_ID_1, "venue_name": "東京", "race_number": 11},
            {"race_id": self.RACE_ID_2, "venue_name": "中山", "race_number": 8},
        ]
        call_log: list[str] = []

        def scrape_side_effect(race_id, race_date, project_id, **kwargs):
            call_log.append(race_id)
            return True

        self._run_pipeline(target_races, scrape_side_effect)

        assert call_log == [self.RACE_ID_1, self.RACE_ID_2]

    def test_no_scrape_when_no_target_races(self):
        """target_races が0件の場合、scrape_odds_for_race は呼ばれないこと"""
        import importlib
        app_module = importlib.import_module("src.automation.api.app")

        call_log: list[str] = []

        def scrape_side_effect(race_id, race_date, project_id, **kwargs):
            call_log.append(race_id)
            return True

        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[],
            ),
            patch(
                "src.automation.data.netkeiba_scraper.scrape_odds_for_race",
                side_effect=scrape_side_effect,
            ),
        ):
            result = run_async(
                app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="",
                    line_user_id="",
                    dry_run=True,
                )
            )

        assert result["status"] == "skipped"
        assert call_log == []

    def test_scrape_failure_does_not_abort_pipeline(self):
        """scrape_odds_for_race が例外を投げてもパイプラインが止まらないこと"""
        target_races = [
            {"race_id": self.RACE_ID_1, "venue_name": "東京", "race_number": 11},
        ]

        def scrape_raises(race_id, race_date, project_id, **kwargs):
            raise RuntimeError("ネットワークエラー")

        result = self._run_pipeline(target_races, scrape_raises)

        # スクレイプ失敗でもパイプライン全体は成功
        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# LINE通知が送られないことを保証するテスト（Issue #238）
# ---------------------------------------------------------------------------

class TestNoLineNotificationOnSkip:
    """対象レースなし時に LINE 通知が送られないことを保証するテスト"""

    TARGET_DATE = datetime.date(2026, 4, 12)

    def setup_method(self):
        import importlib
        self.app_module = importlib.import_module("src.automation.api.app")

    def test_no_races_today_does_not_send_line(self):
        """当日レースが0件（all_races が空）の場合、LINE 通知が送られないこと"""
        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[],
            ),
            patch("src.utils.line_notify.push_messages") as mock_push,
        ):
            result = run_async(
                self.app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="dummy-token",
                    line_user_id="dummy-user",
                    dry_run=True,
                )
            )

        assert result["status"] == "skipped"
        mock_push.assert_not_called()

    def test_no_target_races_in_window_does_not_send_line(self):
        """レースは存在するが時間ウィンドウ内の対象レースが0件の場合、LINE 通知が送られないこと"""
        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[
                    {"race_id": "06261411", "start_time": "1000", "venue_name": "東京", "race_number": 1}
                ],
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_target_races",
                return_value=[],
            ),
            patch("src.utils.line_notify.push_messages") as mock_push,
        ):
            result = run_async(
                self.app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="dummy-token",
                    line_user_id="dummy-user",
                    dry_run=True,
                )
            )

        assert result["status"] == "skipped"
        mock_push.assert_not_called()

    def test_dry_run_no_bets_does_not_send_line(self):
        """dry_run=True で全レースの推奨馬券が0件の場合、LINE 通知が送られないこと"""
        target_races = [
            {"race_id": "06261411", "venue_name": "東京", "race_number": 11},
            {"race_id": "05261208", "venue_name": "中山", "race_number": 8},
        ]

        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[
                    {**r, "start_time": "1000"} for r in target_races
                ],
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_target_races",
                return_value=target_races,
            ),
            patch(
                "src.automation.data.netkeiba_scraper.scrape_odds_for_race",
                return_value=True,
            ),
            patch(
                "src.automation.api.app._refresh_investment_decisions_for_race",
                return_value=True,
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_recommended_bets",
                return_value=[],
            ),
            patch("src.utils.line_notify.push_messages") as mock_push,
        ):
            result = run_async(
                self.app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="dummy-token",
                    line_user_id="dummy-user",
                    dry_run=True,
                )
            )

        mock_push.assert_not_called()
        assert result["status"] == "success"
