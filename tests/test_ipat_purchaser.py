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
import contextlib
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
    IN_PROGRESS_STALE_MINUTES,
    PRE_SUBMIT_MAX_ATTEMPTS,
    IpatLoginError,
    IpatPurchaseError,
    IpatPurchaser,
    fetch_target_races,
    has_purchase_attempt_recorded,
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

    def test_already_started_race_is_excluded_by_default(self):
        """
        デフォルト引数（window_minutes_before=5, window_minutes_after=0）では、
        既に発走したレース（-3分）は除外されること。

        購入エンドポイント側（_purchase_pipeline_async）は Issue #433 でこの関数を
        window_minutes_after=-5 で明示的に呼び出しウィンドウを広げているが、
        この関数自体のデフォルト値は変更していない（0〜5分のみ）。
        """
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["0957"])  # 09:57 = now - 3分
        result = fetch_target_races(races, now)
        assert len(result) == 0

    def test_race_too_far_is_excluded(self):
        """ウィンドウより後（15分後）のレースは除外されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1015"])  # 10:15 = now + 15分
        result = fetch_target_races(races, now)
        assert len(result) == 0

    def test_multiple_races_in_window(self):
        """デフォルトウィンドウ（0〜5分後）内の複数レースのみ抽出されること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1002", "1004", "1008", "0955"])
        result = fetch_target_races(races, now)
        race_ids = [r["race_id"] for r in result]
        assert "race_1002" in race_ids
        assert "race_1004" in race_ids
        assert "race_1008" not in race_ids
        assert "race_0955" not in race_ids

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

    def test_boundary_at_zero_minutes(self):
        """ウィンドウ境界値（ちょうど発走時刻＝0分後）のレースが含まれること"""
        now = datetime.datetime(2026, 4, 5, 10, 0, 0)
        races = self._make_races(["1000"])  # 10:00 = now + 0分
        result = fetch_target_races(races, now)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# has_purchase_attempt_recorded() のテスト（BigQuery Client をモック）
#
# /code-reviewで発見: 当初は「ブロック対象のstatusを持つ行が1件でも存在するか」で
# 判定しており、in_progressマーカー行が後から書かれた確定ステータス行（failed等）に
# 論理的に上書きされず、有効期限まで誤ってブロックし続けるバグがあった。
# 「最新行のstatusのみを見る」実装に修正したロジックそのものを検証する。
# ---------------------------------------------------------------------------

class TestHasPurchaseAttemptRecorded:
    """has_purchase_attempt_recorded() の「最新行」判定ロジックのテスト"""

    PROJECT_ID = "test-project"
    TARGET_DATE = datetime.date(2026, 9, 19)
    RACE_ID = "06264507"

    def _run(self, latest_row: dict | None) -> bool:
        with patch("google.cloud.bigquery.Client") as mock_bq_cls:
            mock_client = MagicMock()
            mock_bq_cls.return_value = mock_client
            mock_client.query.return_value.result.return_value = (
                [latest_row] if latest_row is not None else []
            )
            return has_purchase_attempt_recorded(self.PROJECT_ID, self.TARGET_DATE, self.RACE_ID)

    def test_no_history_is_not_blocked(self):
        """履歴が1件もなければブロックしないこと"""
        assert self._run(None) is False

    def test_latest_success_is_blocked(self):
        """最新行が success ならブロックすること"""
        assert self._run({"status": "success", "purchased_at": None}) is True

    def test_latest_need_confirmation_is_blocked(self):
        """最新行が need_confirmation ならブロックすること"""
        assert self._run({"status": "need_confirmation", "purchased_at": None}) is True

    def test_latest_failed_is_not_blocked(self):
        """最新行が failed（投票送信前の失敗）ならブロックしないこと"""
        assert self._run({"status": "failed", "purchased_at": None}) is False

    def test_latest_fresh_in_progress_is_blocked(self):
        """最新行が有効期限内の in_progress ならブロックすること"""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        assert self._run({"status": "in_progress", "purchased_at": now_utc}) is True

    def test_latest_stale_in_progress_is_not_blocked(self):
        """最新行が有効期限切れの in_progress（クラッシュ等で放置）ならブロックしないこと"""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        stale_at = now_utc - datetime.timedelta(minutes=IN_PROGRESS_STALE_MINUTES + 1)
        assert self._run({"status": "in_progress", "purchased_at": stale_at}) is False

    def test_failed_after_in_progress_unblocks(self):
        """
        in_progressマーカーの後に failed 行が追加で書かれた場合、
        「最新行」は failed になるため、古い in_progress が残っていてもブロックされないこと。
        これが今回修正した中核のバグ（/code-review指摘）。
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        # ORDER BY purchased_at DESC LIMIT 1 相当なので、テスト側では
        # 「最新の1行」だけをモックのクエリ結果として渡せば十分
        assert self._run({"status": "failed", "purchased_at": now_utc}) is False


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
        # page.locator() は実際のPlaywrightでは同期メソッドなので MagicMock にする
        # （_capture_failure_state() が失敗時の画面文言取得に使用する）
        locator_mock = MagicMock()
        locator_mock.inner_text = AsyncMock(return_value="")
        p._page.locator = MagicMock(return_value=locator_mock)
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

    def test_login_failure_menu_not_visible(self):
        """URL遷移・エラーメッセージ判定を通過しても「通常投票」が表示されなければ False を返すこと（Issue #433）"""
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_MENU_URL
        purchaser._page.text_content = AsyncMock(return_value="ただいまメンテナンス中です")
        purchaser._page.locator.return_value.inner_text = AsyncMock(
            return_value="ただいまメンテナンス中です"
        )
        purchaser._page.wait_for_selector = AsyncMock(side_effect=Exception("Timeout 5000ms exceeded"))

        result = run_async(purchaser.login())

        assert result is False
        assert purchaser.last_login_debug is not None
        assert purchaser.last_login_debug["url"] == self.IPAT_MENU_URL
        assert "メンテナンス" in purchaser.last_login_debug["text_snippet"]

    def test_capture_failure_state_falls_back_to_body_text_when_locator_raises(self):
        """
        .ui-page-active が見つからず locator().inner_text() が例外を送出する場合でも、
        body 全体へのフォールバックで画面文言を取得できること（/code-review指摘）。
        以前は1つの try で両方を囲っており、前者の例外でフォールバックごと失われていた。
        """
        purchaser = self._make_purchaser()
        purchaser._page.url = self.IPAT_MENU_URL
        purchaser._page.text_content = AsyncMock(return_value="想定外のページ（.ui-page-active無し）")
        purchaser._page.locator.return_value.inner_text = AsyncMock(
            side_effect=Exception("selector not found")
        )
        purchaser._page.wait_for_selector = AsyncMock(side_effect=Exception("Timeout"))

        result = run_async(purchaser.login())

        assert result is False
        assert purchaser.last_login_debug is not None
        assert purchaser.last_login_debug["text_snippet"] == "想定外のページ（.ui-page-active無し）"

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

    def _make_purchaser(self, completion_text: str = "受付番号: 12345") -> IpatPurchaser:
        p = IpatPurchaser("12345678", "1234", "87654321")
        p._page = AsyncMock()

        # locator() は同期メソッドなので MagicMock にし、fill()/inner_text()/count() を
        # AsyncMock で持たせる。count()=1 は「投票ボタンがDOM上に存在する」ことを表す
        # （_submit_and_confirm() の事前チェック用。/code-review指摘）。
        locator_mock = MagicMock()
        locator_mock.fill = AsyncMock()
        locator_mock.inner_text = AsyncMock(return_value=completion_text)
        locator_mock.count = AsyncMock(return_value=1)
        p._page.locator = MagicMock(return_value=locator_mock)

        # expect_navigation() は非同期コンテキストマネージャ
        nav_ctx = MagicMock()
        nav_ctx.__aenter__ = AsyncMock(return_value=nav_ctx)
        nav_ctx.__aexit__ = AsyncMock(return_value=False)
        p._page.expect_navigation = MagicMock(return_value=nav_ctx)

        return p

    def test_purchase_success(self):
        """購入完了テキストが確認できれば success を返すこと"""
        purchaser = self._make_purchaser("購入完了しました。受付番号: 12345")

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "success"
        assert result["error_message"] is None

    def test_purchase_failure_insufficient_balance(self):
        """残高不足メッセージがある場合は failed を返すこと"""
        purchaser = self._make_purchaser("残高不足のため購入できませんでした")

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

    def test_purchase_unrecognized_completion_text_returns_need_confirmation(self):
        """
        「投票」ボタン押下後、既知の成功／失敗パターンのどちらにも一致しない画面文言の場合、
        投票結果が不明なため status=failed ではなく need_confirmation を返すこと（/code-review指摘）。
        status=failed にすると has_purchase_attempt_recorded() の対象外となり、
        実際には投票が成立していた場合に次tickで二重購入してしまう。
        """
        purchaser = self._make_purchaser("予期しないレイアウト変更後の画面テキスト")

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "need_confirmation"

    def test_success_text_with_error_pattern_substring_is_still_success(self):
        """
        完了確認テキストに「受付番号」と、ERROR_PATTERNSに含まれる汎用的な文言
        （例:「ご確認ください」という注意書き）が両方含まれる場合でも、
        successと判定すること（/code-review指摘）。
        判定順が逆（ERROR_PATTERNS優先）だと、実際には成立した投票を誤って
        failedにしてしまい、次tickでの二重購入に直結する。
        """
        purchaser = self._make_purchaser(
            "受付番号: 12345 ご確認ください（投票内容は取消できません）"
        )

        result = run_async(purchaser.purchase_bet("place", [3], 300, "東京(土)", 7))

        assert result["status"] == "success"

    def test_submit_button_not_found_returns_failed_not_need_confirmation(self):
        """
        「投票」ボタンがDOM上に存在しない場合、クリックもサーバへの送信も一切発生
        していないことが確定しているため、need_confirmationではなく安全にリトライ
        可能な failed を返すこと（/code-review指摘）。
        """
        purchaser = self._make_purchaser()
        purchaser._page.locator.return_value.count = AsyncMock(return_value=0)

        # purchase_bet()（後方互換ラッパー）は debug を落とすため、本番コード
        # （app.py）が実際に使う purchase_bets_for_race() を直接呼んで検証する。
        result = run_async(
            purchaser.purchase_bets_for_race(
                [{"bet_type": "place", "horse_numbers": [3], "amount": 300}],
                "東京(土)", 7,
            )
        )

        assert result["status"] == "failed"
        assert "投票ボタン" in result["error_message"]
        # 失敗時の証跡（画面URL等）が記録されていること（/code-review指摘）
        assert result.get("debug") is not None
        # サーバへの送信を試みていないため expect_navigation は呼ばれない
        purchaser._page.expect_navigation.assert_not_called()


# ---------------------------------------------------------------------------
# purchase_bets_for_race() の投票送信前リトライ・送信後の二重購入防止（Issue #433）
# ---------------------------------------------------------------------------

class TestPurchaseBetsRetryAndSafety:
    """
    投票送信前フェーズ（通常投票クリック〜金額セット）の自動リトライと、
    投票送信後は絶対にリトライしない（二重購入防止）ことを検証する。
    """

    BETS = [{"bet_type": "place", "horse_numbers": [3], "amount": 300}]

    def _make_purchaser(self) -> IpatPurchaser:
        p = IpatPurchaser("12345678", "1234", "8765")
        p._page = AsyncMock()
        p._page.on = MagicMock()
        locator_mock = MagicMock()
        locator_mock.inner_text = AsyncMock(return_value="")
        p._page.locator = MagicMock(return_value=locator_mock)
        p._page.text_content = AsyncMock(return_value="")
        p._browser = AsyncMock()
        # リトライ時に同じ（.locator設定済みの）ページを使い回す
        p._browser.new_page = AsyncMock(return_value=p._page)
        return p

    def test_pre_submit_retry_succeeds_after_one_failure(self):
        """投票一覧への追加が1回失敗しても、再ログイン後のリトライで成功すること"""
        purchaser = self._make_purchaser()
        attempts = {"add_bet": 0}

        async def flaky_add_bet_to_list(*args, **kwargs):
            attempts["add_bet"] += 1
            if attempts["add_bet"] == 1:
                raise Exception('Timeout: waiting for locator("a:has-text(\\"通常投票\\")")')

        purchaser._navigate_to_top_menu = AsyncMock(return_value=None)
        purchaser._add_bet_to_list = flaky_add_bet_to_list
        purchaser.login = AsyncMock(return_value=True)
        purchaser._prepare_final_confirmation = AsyncMock(return_value=None)
        purchaser._submit_and_confirm = AsyncMock(
            return_value={"status": "success", "error_message": None}
        )

        result = run_async(
            purchaser.purchase_bets_for_race(self.BETS, "中山(土)", 7)
        )

        assert result["status"] == "success"
        assert attempts["add_bet"] == 2
        purchaser.login.assert_called_once()

    def test_pre_submit_retry_exhausted_returns_failed(self):
        """PRE_SUBMIT_MAX_ATTEMPTS回失敗し続けたら status=failed で諦めること"""
        purchaser = self._make_purchaser()
        attempts = {"add_bet": 0}

        async def always_fail(*args, **kwargs):
            attempts["add_bet"] += 1
            raise Exception("Timeout: locator not found")

        purchaser._navigate_to_top_menu = AsyncMock(return_value=None)
        purchaser._add_bet_to_list = always_fail
        purchaser.login = AsyncMock(return_value=True)
        purchaser._prepare_final_confirmation = AsyncMock(return_value=None)
        purchaser._submit_and_confirm = AsyncMock(
            return_value={"status": "success", "error_message": None}
        )

        result = run_async(
            purchaser.purchase_bets_for_race(self.BETS, "中山(土)", 7)
        )

        assert result["status"] == "failed"
        assert attempts["add_bet"] == PRE_SUBMIT_MAX_ATTEMPTS
        assert purchaser.login.call_count == PRE_SUBMIT_MAX_ATTEMPTS - 1
        purchaser._prepare_final_confirmation.assert_not_called()
        purchaser._submit_and_confirm.assert_not_called()

    def test_pre_submit_retry_stops_near_race_start(self):
        """発走まで残り僅か（MIN_MINUTES_BEFORE_START_FOR_RETRY分未満）ならリトライせず即座に失敗を返すこと"""
        purchaser = self._make_purchaser()
        attempts = {"add_bet": 0}

        async def always_fail(*args, **kwargs):
            attempts["add_bet"] += 1
            raise Exception("Timeout: locator not found")

        purchaser._navigate_to_top_menu = AsyncMock(return_value=None)
        purchaser._add_bet_to_list = always_fail
        purchaser.login = AsyncMock(return_value=True)

        from zoneinfo import ZoneInfo as _ZoneInfo

        now_jst = datetime.datetime.now(_ZoneInfo("Asia/Tokyo"))
        # 発走時刻を「今」に設定 → リトライ猶予（MIN_MINUTES_BEFORE_START_FOR_RETRY分）を
        # 既に過ぎている状態を再現する
        start_time = now_jst.strftime("%H%M")

        result = run_async(
            purchaser.purchase_bets_for_race(self.BETS, "中山(土)", 7, start_time=start_time)
        )

        assert result["status"] == "failed"
        assert attempts["add_bet"] == 1
        purchaser.login.assert_not_called()

    def test_no_retry_after_submit_returns_need_confirmation(self):
        """投票送信（_submit_and_confirm）後の例外は絶対にリトライせず need_confirmation を返すこと"""
        purchaser = self._make_purchaser()
        attempts = {"add_bet": 0}

        async def succeed_once(*args, **kwargs):
            attempts["add_bet"] += 1

        purchaser._navigate_to_top_menu = AsyncMock(return_value=None)
        purchaser._add_bet_to_list = succeed_once
        purchaser.login = AsyncMock(return_value=True)
        purchaser._prepare_final_confirmation = AsyncMock(return_value=None)
        purchaser._submit_and_confirm = AsyncMock(
            side_effect=Exception("Timeout: navigation after submit")
        )

        result = run_async(
            purchaser.purchase_bets_for_race(self.BETS, "中山(土)", 7)
        )

        assert result["status"] == "need_confirmation"
        assert attempts["add_bet"] == 1
        purchaser.login.assert_not_called()
        purchaser._prepare_final_confirmation.assert_called_once()
        purchaser._submit_and_confirm.assert_called_once()

    def test_prepare_final_confirmation_failure_is_retried_not_need_confirmation(self):
        """
        「入力終了」〜合計金額入力（_prepare_final_confirmation）の失敗はまだサーバ未送信のため、
        need_confirmation ではなく通常のリトライ対象として扱われること（/code-review指摘）。
        """
        purchaser = self._make_purchaser()
        attempts = {"prepare": 0}

        async def flaky_prepare(*args, **kwargs):
            attempts["prepare"] += 1
            if attempts["prepare"] == 1:
                raise Exception('Timeout: waiting for locator("a:text-is(\\"入力終了\\")")')

        purchaser._navigate_to_top_menu = AsyncMock(return_value=None)
        purchaser._add_bet_to_list = AsyncMock(return_value=None)
        purchaser.login = AsyncMock(return_value=True)
        purchaser._prepare_final_confirmation = flaky_prepare
        purchaser._submit_and_confirm = AsyncMock(
            return_value={"status": "success", "error_message": None}
        )

        result = run_async(
            purchaser.purchase_bets_for_race(self.BETS, "中山(土)", 7)
        )

        assert result["status"] == "success"
        assert attempts["prepare"] == 2
        purchaser.login.assert_called_once()


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

    def test_dry_run_error_on_one_race_does_not_abort_others(self):
        """
        dry_runモードで1レースの推奨馬券取得（refresh後）が想定外の例外を
        投げても、他のレースのLINE通知は引き続き行われること（/code-review指摘）。
        本番購入ループ側だけでなく、dry_runループ側も同じ問題を抱えていた。
        """
        import importlib

        app_module = importlib.import_module("src.automation.api.app")
        target_races = [
            {"race_id": self.RACE_ID_1, "venue_name": "東京", "race_number": 11},
            {"race_id": self.RACE_ID_2, "venue_name": "中山", "race_number": 8},
        ]

        def fake_fetch_bets(project_id, race_id, target_date):
            if race_id == self.RACE_ID_1:
                raise RuntimeError("不正な投資判断データ")
            return [{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]

        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                return_value=[
                    {"race_id": r["race_id"], "start_time": "1000", "venue_name": "東京", "race_number": 1}
                    for r in target_races
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
                side_effect=fake_fetch_bets,
            ),
            patch("src.utils.line_notify.push_messages") as mock_push,
        ):
            result = run_async(
                app_module._purchase_pipeline_async(
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

        assert result["status"] == "success"
        # RACE_ID_1で例外が起きても、RACE_ID_2の通知は行われる
        assert mock_push.call_count == 1
        notified_race_ids = [r["race_id"] for r in result["results"]]
        assert self.RACE_ID_2 in notified_race_ids
        assert self.RACE_ID_1 not in notified_race_ids


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


# ---------------------------------------------------------------------------
# 本番購入フロー（dry_run=False）: ログイン要否判定・二重購入防止（Issue #433）
# ---------------------------------------------------------------------------

class TestProductionPurchaseFlow:
    """
    投資判断の更新・推奨馬券取得をログインより先に行い、購入対象が0件なら
    IPATへのログイン自体を行わないこと、および既に購入成功済みのレースを
    二重購入しないことを検証する。
    """

    TARGET_DATE = datetime.date(2026, 9, 19)
    RACE_ID = "06264507"

    def _target_race(self) -> dict:
        return {
            "race_id": self.RACE_ID,
            "start_time": "1325",
            "venue_name": "中山",
            "race_number": 7,
        }

    def _run(self, extra_patches: dict):
        import importlib

        app_module = importlib.import_module("src.automation.api.app")
        target_race = self._target_race()
        base_patches = {
            "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time": MagicMock(
                return_value=[target_race]
            ),
            "src.automation.data.ipat_purchaser.fetch_target_races": MagicMock(
                return_value=[target_race]
            ),
            "src.automation.data.netkeiba_scraper.scrape_odds_for_race": MagicMock(return_value=True),
            "src.automation.api.app._refresh_investment_decisions_for_race": MagicMock(return_value=True),
            "src.automation.data.ipat_purchaser.fetch_daily_spent_amount": MagicMock(return_value=0),
            "src.automation.data.ipat_purchaser.save_purchase_record": MagicMock(),
            "src.utils.line_notify.push_messages": MagicMock(),
        }
        base_patches.update(extra_patches)

        with contextlib.ExitStack() as stack:
            for target, new in base_patches.items():
                stack.enter_context(patch(target, new))
            return run_async(
                app_module._purchase_pipeline_async(
                    project_id="test-project",
                    target_date=self.TARGET_DATE,
                    member_id="12345678",
                    pin="1234",
                    pat_number="87654321",
                    channel_access_token="",
                    line_user_id="",
                    dry_run=False,
                )
            )

    def test_skips_login_when_already_purchased(self):
        """対象レースが既に購入成功済みなら IPAT へログインしないこと"""
        mock_ipat_cls = MagicMock()
        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=True),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert result["status"] == "success"
        assert result["purchased_races"] == 0
        mock_ipat_cls.assert_not_called()

    def test_skips_login_when_no_recommended_bets(self):
        """refresh後も推奨馬券が0件なら IPAT へログインしないこと"""
        mock_ipat_cls = MagicMock()
        mock_refresh = MagicMock(return_value=True)
        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.api.app._refresh_investment_decisions_for_race": mock_refresh,
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(return_value=[]),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert result["status"] == "success"
        assert result["purchased_races"] == 0
        mock_ipat_cls.assert_not_called()
        # refresh自体は必ず試みられていること（下のデッドロック回帰テスト参照）
        mock_refresh.assert_called()

    def test_pre_check_refreshes_before_checking_for_bets(self):
        """
        investment_decisions は _refresh_investment_decisions_for_race() でしか
        書き込まれない（race-day-strategyの8:30ジョブはdry_run=trueがデフォルトで
        BQ保存しない）。事前チェックでrefreshを行わず既存データだけを見ると、
        まだ一度もrefreshされていないレースが永久に購入対象と判定されない
        デッドロックになっていた（/code-review指摘・重大な回帰）。
        refreshが実際に呼ばれた後で初めて推奨馬券が見つかり、購入まで
        到達することを検証する。
        """
        state = {"refreshed": False}

        def fake_refresh(project_id, race_id, target_date):
            state["refreshed"] = True
            return True

        def fake_fetch_bets(project_id, race_id, target_date):
            # refresh前は investment_decisions が空のまま（本番の実際の挙動を再現）
            if not state["refreshed"]:
                return []
            return [{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]

        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )
        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.api.app._refresh_investment_decisions_for_race": MagicMock(side_effect=fake_refresh),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(side_effect=fake_fetch_bets),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert state["refreshed"] is True
        assert result["status"] == "success"
        assert result["purchased_races"] == 1
        mock_ipat_cls.assert_called_once()

    def test_logs_in_and_purchases_when_bets_exist(self):
        """購入対象レースが1件でもあれば IPAT へログインし購入を実行すること"""
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )

        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                return_value=[{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]
            ),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert result["status"] == "success"
        assert result["purchased_races"] == 1
        purchaser_instance.login.assert_called_once()
        # start_time がレース発走時刻として渡されること
        _, kwargs = purchaser_instance.purchase_bets_for_race.call_args
        assert kwargs.get("start_time") == "1325"

    def test_marks_in_progress_before_purchasing(self):
        """
        実購入直前に purchase_history へ status='in_progress' のマーカーを記録すること
        （並行tickによる二重購入防止・/code-review指摘）。
        """
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )

        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_save_record = MagicMock()

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                return_value=[{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]
            ),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
            "src.automation.data.ipat_purchaser.save_purchase_record": mock_save_record,
        })

        assert result["status"] == "success"
        in_progress_calls = [
            c for c in mock_save_record.call_args_list if c.args[6] == "in_progress"
        ]
        assert len(in_progress_calls) == 1
        assert in_progress_calls[0].args[2] == self.RACE_ID  # race_id
        assert in_progress_calls[0].args[3] == "_lock"  # bet_type（センチネル）

    def test_resolves_in_progress_marker_when_bets_vanish_after_refresh(self):
        """
        事前チェック時点では推奨馬券があったが、購入直前のrefreshで0件になった場合、
        in_progressマーカーを解消（status='failed'で記録）すること（/code-review指摘）。
        解消しないと、実際には何も購入していないのに次tickでも
        has_purchase_attempt_recorded() がブロックし続け、購入ウィンドウを失う。
        """
        mock_ipat_cls = MagicMock()
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_save_record = MagicMock()

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            # 1回目（事前チェック）は非空、2回目（refresh後の実購入直前）は空
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                side_effect=[
                    [{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}],
                    [],
                ]
            ),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
            "src.automation.data.ipat_purchaser.save_purchase_record": mock_save_record,
        })

        assert result["status"] == "success"
        assert result["purchased_races"] == 0
        purchaser_instance.purchase_bets_for_race.assert_not_called()

        # in_progress で記録された後、failed で解消されていること
        statuses = [c.args[6] for c in mock_save_record.call_args_list if c.args[2] == self.RACE_ID]
        assert statuses == ["in_progress", "failed"]

    def test_skips_purchase_when_concurrent_tick_wins_race(self):
        """
        事前チェック（ログイン前）通過後、実購入直前の再確認で他tickが既に処理済みと
        判明した場合は、ログインはしても実際の購入は行わないこと（/code-review指摘）。
        """
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )

        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = self._run({
            # 1回目（事前チェック）= False（購入対象と判定）、
            # 2回目（購入直前の再確認）= True（他tickが先に処理済み）
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(
                side_effect=[False, True]
            ),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                return_value=[{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]
            ),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert result["status"] == "success"
        assert result["purchased_races"] == 0
        purchaser_instance.purchase_bets_for_race.assert_not_called()

    def test_need_confirmation_status_not_masked_by_budget_skip(self):
        """
        一部の馬券が予算超過でスキップされつつ、残りの馬券がneed_confirmationに
        なった場合、race_results の status が skipped_budget に上書きされず
        need_confirmation のまま表面化すること（/code-review指摘）。
        """
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={
                "status": "need_confirmation",
                "total_amount": 49900,
                "error_message": "投票送信中にエラー",
            }
        )

        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                return_value=[
                    {"bet_type": "place", "horse_numbers": [3], "bet_amount": 49900},
                    {"bet_type": "win", "horse_numbers": [3], "bet_amount": 200},
                ]
            ),
            "src.automation.data.ipat_purchaser.fetch_daily_spent_amount": MagicMock(return_value=0),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert len(result["results"]) == 1
        race_result = result["results"][0]
        assert race_result["bets_skipped_budget"] == 1  # 2件目は予算超過でスキップ
        assert race_result["bets_need_confirmation"] == 1  # 1件目はneed_confirmation
        assert race_result["status"] == "need_confirmation"  # skipped_budgetに上書きされない
        assert race_result["amount"] == 49900

    def test_unexpected_error_in_one_race_does_not_abort_other_races(self):
        """
        1レースの購入処理中に想定外の例外（不正な投資判断データ等）が発生しても、
        tick全体を中断せず、そのレースだけをエラー扱いにして次レースの処理を
        継続すること（/code-review指摘）。in_progressマーカーも解消されること。
        """
        import importlib

        app_module = importlib.import_module("src.automation.api.app")

        race_a = {"race_id": "06264507", "start_time": "1325", "venue_name": "中山", "race_number": 7}
        race_b = {"race_id": "09264507", "start_time": "1330", "venue_name": "阪神", "race_number": 7}
        bets_a = [{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]
        bets_b = [{"bet_type": "place", "horse_numbers": [5], "bet_amount": 300}]

        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )
        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_save_record = MagicMock()

        # 呼び出し順: 事前チェック(A), 事前チェック(B), 実購入直前のrefresh後(A)=例外, 実購入直前(B)=正常
        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                MagicMock(return_value=[race_a, race_b]),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_target_races",
                MagicMock(return_value=[race_a, race_b]),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.scrape_odds_for_race",
                MagicMock(return_value=True),
            ),
            patch(
                "src.automation.api.app._refresh_investment_decisions_for_race",
                MagicMock(return_value=True),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_daily_spent_amount",
                MagicMock(return_value=0),
            ),
            patch(
                "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded",
                MagicMock(return_value=False),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_recommended_bets",
                MagicMock(side_effect=[bets_a, bets_b, ValueError("不正な馬番データ"), bets_b]),
            ),
            patch("src.automation.data.ipat_purchaser.IpatPurchaser", mock_ipat_cls),
            patch("src.automation.data.ipat_purchaser.save_purchase_record", mock_save_record),
            patch("src.utils.line_notify.push_messages", MagicMock()),
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
                    dry_run=False,
                )
            )

        assert result["status"] == "success"
        statuses = {r["race_id"]: r["status"] for r in result["results"]}
        assert statuses["06264507"] == "error"  # 例外発生レース
        assert statuses["09264507"] == "processed"  # 後続レースは正常処理された
        purchaser_instance.purchase_bets_for_race.assert_called_once()  # Bのみ購入実行

        # race_aのin_progressマーカーがfailedで解消されていること
        race_a_statuses = [
            c.args[6] for c in mock_save_record.call_args_list if c.args[2] == "06264507"
        ]
        assert race_a_statuses == ["in_progress", "failed"]

    def test_partial_save_failure_does_not_mask_success_as_error(self):
        """
        購入成功後、複数馬券のうち1件の save_purchase_record 呼び出しが
        BQ一時障害で失敗しても、外側のtry/exceptに伝播して
        status='error'（failed相当・自動再購入OK）で上書きされないこと
        （/code-review指摘）。実際には投票が成功しているため、これを
        failed扱いにすると次tickで二重購入してしまう。
        """
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 600, "error_message": None}
        )
        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        # 1件目の保存は成功、2件目の保存はBQ一時障害で失敗
        mock_save_record = MagicMock(side_effect=[None, RuntimeError("BQ insert失敗")])

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(
                return_value=[
                    {"bet_type": "place", "horse_numbers": [3], "bet_amount": 300},
                    {"bet_type": "win", "horse_numbers": [3], "bet_amount": 300},
                ]
            ),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
            "src.automation.data.ipat_purchaser.save_purchase_record": mock_save_record,
        })

        assert len(result["results"]) == 1
        race_result = result["results"][0]
        # 保存の一部が失敗しても、想定外エラー扱い（error）にはならない
        assert race_result["status"] != "error"
        assert race_result["status"] == "processed"
        assert race_result["bets_purchased"] == 2

    def test_precheck_error_on_one_race_does_not_abort_other_races(self):
        """
        ログイン前の事前チェック（has_purchase_attempt_recorded/refresh/fetch）で
        1レースだけ想定外の例外が発生しても、tick全体を中断せず、他のレースは
        引き続き事前チェック・購入されること（/code-review指摘）。
        以前は本番購入ループ側だけが例外保護されており、事前チェックループが
        同じ問題を抱えたまま残っていた。
        """
        import importlib

        app_module = importlib.import_module("src.automation.api.app")

        race_a = {"race_id": "06264507", "start_time": "1325", "venue_name": "中山", "race_number": 7}
        race_b = {"race_id": "09264507", "start_time": "1330", "venue_name": "阪神", "race_number": 7}
        bets_b = [{"bet_type": "place", "horse_numbers": [5], "bet_amount": 300}]

        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)
        purchaser_instance.purchase_bets_for_race = AsyncMock(
            return_value={"status": "success", "total_amount": 300, "error_message": None}
        )
        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        def fake_has_attempt(project_id, target_date, race_id):
            if race_id == "06264507":
                raise RuntimeError("BQ一時障害")
            return False

        with (
            patch(
                "src.automation.data.ipat_purchaser.fetch_today_races_with_start_time",
                MagicMock(return_value=[race_a, race_b]),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_target_races",
                MagicMock(return_value=[race_a, race_b]),
            ),
            patch(
                "src.automation.data.netkeiba_scraper.scrape_odds_for_race",
                MagicMock(return_value=True),
            ),
            patch(
                "src.automation.api.app._refresh_investment_decisions_for_race",
                MagicMock(return_value=True),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_daily_spent_amount",
                MagicMock(return_value=0),
            ),
            patch(
                "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded",
                MagicMock(side_effect=fake_has_attempt),
            ),
            patch(
                "src.automation.data.ipat_purchaser.fetch_recommended_bets",
                MagicMock(return_value=bets_b),
            ),
            patch("src.automation.data.ipat_purchaser.IpatPurchaser", mock_ipat_cls),
            patch("src.automation.data.ipat_purchaser.save_purchase_record", MagicMock()),
            patch("src.utils.line_notify.push_messages", MagicMock()),
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
                    dry_run=False,
                )
            )

        # tick全体が500で落ちず、正常応答が返ること
        assert result["status"] == "success"
        # race_aは事前チェックで例外→除外されるが、race_bは購入まで到達すること
        assert result["purchased_races"] == 1
        purchaser_instance.purchase_bets_for_race.assert_called_once()

    def test_overall_status_is_error_when_all_races_fail_unexpectedly(self):
        """
        購入対象の全レースが想定外のエラー（status='error'）になった場合、
        個別レース保護は維持しつつ、レスポンス全体のstatusを'error'にして
        Cloud Run監視等でシステム的な問題として検知できるようにすること
        （/code-review指摘: レジリエンス設計により、以前は全レース失敗時でも
        レスポンス全体がstatus='success'のままで異常検知が難しかった）。
        """
        purchaser_instance = AsyncMock()
        purchaser_instance.login = AsyncMock(return_value=True)

        mock_ipat_cls = MagicMock()
        mock_ipat_cls.return_value.__aenter__ = AsyncMock(return_value=purchaser_instance)
        mock_ipat_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        # 事前チェックでは正常にbetsが見つかるが、実購入直前のfetch（refresh後）で例外
        call_count = {"n": 0}

        def fake_fetch_bets(project_id, race_id, target_date):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return [{"bet_type": "place", "horse_numbers": [3], "bet_amount": 300}]
            raise ValueError("不正な投資判断データ")

        result = self._run({
            "src.automation.data.ipat_purchaser.has_purchase_attempt_recorded": MagicMock(return_value=False),
            "src.automation.data.ipat_purchaser.fetch_recommended_bets": MagicMock(side_effect=fake_fetch_bets),
            "src.automation.data.ipat_purchaser.IpatPurchaser": mock_ipat_cls,
        })

        assert result["status"] == "error"
        assert len(result["results"]) == 1
        assert result["results"][0]["status"] == "error"
