"""
JRA IPAT 自動馬券購入モジュール

Playwright を使って JRA インターネット投票（IPAT）に自動ログインし、
推奨馬券を購入する。

環境変数 / Secret Manager から取得する認証情報:
  IPAT_MEMBER_ID  : 加入者番号
  IPAT_PIN        : 暗証番号（4桁）
  IPAT_PAT_NUMBER : PAT番号

Issue #213: 発走5分前JRA IPAT自動馬券購入パイプラインの実装

購入フロー（SP版ウィザード形式）:
  1. トップメニュー(pw_732_i.cgi) → 「通常投票」アイコン
  2. 競馬場選択（例: 「中山(土)」）
  3. レース選択（例: 「7R」）
  4. 式別選択（例: 「複勝」「３連複」）
  5. 馬番選択（1頭ずつクリック）
  6. 金額入力（__00円形式 = 入力値×100円）→「セット」
  7. 投票一覧 → 「入力終了」
  8. 合計金額入力（円単位）→「投票」
"""

import asyncio
import datetime
import logging
import uuid
from zoneinfo import ZoneInfo

from google.cloud import bigquery

logger = logging.getLogger(__name__)

IPAT_LOGIN_URL = "https://www.ipat.jra.go.jp/sp/index.cgi"
IPAT_TOP_MENU_URL = "https://www.ipat.jra.go.jp/sp/pw_732_i.cgi"

# ログイン用URLエイリアス（後方互換）
IPAT_BASE_URL = IPAT_LOGIN_URL

# 購入1件あたりのタイムアウト（ミリ秒）
PURCHASE_TIMEOUT_MS = 30_000

# 馬券種コード → IPAT画面上の選択値（SP版の表示文字列）
BET_TYPE_MAP: dict[str, str] = {
    "win": "単勝",
    "place": "複勝",
    "umaren": "馬連",
    "wide": "ワイド",
    "umatan": "馬単",
    "sanrenpuku": "３連複",  # IPATのSP版は全角数字
}

# 1日あたりの購入上限額（円）
DAILY_BUDGET_LIMIT = 50_000

# 投票送信前フェーズ（通常投票クリック〜合計金額入力）の最大試行回数（初回+リトライ）
PRE_SUBMIT_MAX_ATTEMPTS = 3

# 発走までの残り時間がこれ未満になったらリトライを打ち切る（分）
MIN_MINUTES_BEFORE_START_FOR_RETRY = 2

# 失敗時のデバッグ情報（スクリーンショット等）の保存先バケットサフィックス
DEBUG_BUCKET_SUFFIX = "keiba-predictions"


class IpatLoginError(Exception):
    """IPAT ログイン失敗時の例外"""


class IpatPurchaseError(Exception):
    """IPAT 購入失敗時の例外"""


class IpatBudgetExceededError(Exception):
    """1日の予算上限超過時の例外"""


class IpatPurchaser:
    """
    JRA IPAT 自動馬券購入クラス

    使い方（コンテキストマネージャー）:
        async with IpatPurchaser(member_id, pin, pat_number) as purchaser:
            await purchaser.login()
            result = await purchaser.purchase_bet("place", [3], 300)

    Args:
        project_id: 失敗時のスクリーンショットをGCSに保存する場合のGCPプロジェクトID。
            None の場合はスクリーンショット保存をスキップする（テキスト情報のみ記録）。
    """

    def __init__(
        self,
        member_id: str,
        pin: str,
        pat_number: str,
        project_id: str | None = None,
    ) -> None:
        self.member_id = member_id
        self.pin = pin
        self.pat_number = pat_number
        self.project_id = project_id
        self._playwright = None
        self._browser = None
        self._page = None
        # 直近のログイン失敗時にキャプチャしたデバッグ情報（url/画面文言/スクリーンショットパス）
        self.last_login_debug: dict | None = None

    async def __aenter__(self) -> "IpatPurchaser":
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        self._page = await self._browser.new_page()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.logout()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _capture_failure_state(self, context: str) -> dict:
        """
        失敗時の画面状態（URL・表示テキスト・スクリーンショット）を記録する。

        原因究明用の証跡が一切残らず事後調査が不可能だった問題（Issue #433）への対応。
        この関数自体の失敗が呼び出し元のエラーハンドリングを妨げないよう、
        内部で発生した例外はすべて握りつぶし、取得できた範囲の情報のみを返す。

        Args:
            context: どの処理段階での失敗かを示すラベル（ログ・GCSパスに使用）

        Returns:
            {"context": str, "url": str|None, "text_snippet": str|None, "screenshot_gcs_path": str|None}
        """
        debug: dict = {
            "context": context,
            "url": None,
            "text_snippet": None,
            "screenshot_gcs_path": None,
        }
        if self._page is None:
            return debug

        try:
            debug["url"] = self._page.url
        except Exception:
            pass

        try:
            text = await self._page.locator(".ui-page-active").inner_text()
            if not text:
                text = await self._page.text_content("body") or ""
            debug["text_snippet"] = text.strip()[:500]
        except Exception:
            pass

        if self.project_id:
            try:
                png_bytes = await self._page.screenshot()
                timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                blob_path = f"ipat_debug/{timestamp}_{context}.png"
                from google.cloud import storage as _storage

                gcs_client = _storage.Client(project=self.project_id)
                bucket = gcs_client.bucket(f"{self.project_id}-{DEBUG_BUCKET_SUFFIX}")
                blob = bucket.blob(blob_path)
                blob.upload_from_string(png_bytes, content_type="image/png")
                debug["screenshot_gcs_path"] = f"gs://{self.project_id}-{DEBUG_BUCKET_SUFFIX}/{blob_path}"
            except Exception as e:
                logger.warning(f"失敗時スクリーンショットの保存に失敗（無視します）: {e}")

        return debug

    async def login(self) -> bool:
        """
        JRA IPAT にログインする。

        ログイン後は「通常投票」リンクが実際に表示されるまで確認する。URL遷移と
        エラーキーワード判定だけでは、意図しない中間画面（お知らせ・セッション
        エラー等）が返ってきた場合に誤って成功と判定してしまうため（Issue #433）。

        Returns:
            True: ログイン成功
            False: ログイン失敗（失敗時は self.last_login_debug に画面状態を記録）

        Raises:
            IpatLoginError: ログイン処理中に予期しないエラーが発生した場合
        """
        if self._page is None:
            raise IpatLoginError("ブラウザが初期化されていません。コンテキストマネージャー経由で使用してください。")

        self.last_login_debug = None

        try:
            logger.info("JRA IPAT ログイン開始")

            # alert は accept/dismiss どちらでも閉じる。
            # 購入確認の confirm ダイアログは accept() が必要なので全て accept に統一する。
            self._page.on("dialog", lambda d: asyncio.create_task(d.accept()))

            await self._page.goto(IPAT_LOGIN_URL, timeout=PURCHASE_TIMEOUT_MS, wait_until="domcontentloaded")

            # ログインフォームへの入力（SP版: id属性で識別）
            await self._page.fill('#userid', self.member_id, timeout=PURCHASE_TIMEOUT_MS)
            await self._page.fill('#password', self.pin, timeout=PURCHASE_TIMEOUT_MS)
            await self._page.fill('#pars', self.pat_number, timeout=PURCHASE_TIMEOUT_MS)

            # ログインボタンをクリック
            # SP版のログインボタンは input[type="submit"] ではなく
            # ToSPMenu() を呼び出す <a> タグ（li.btnColor a）
            await self._page.click('li.btnColor a', timeout=PURCHASE_TIMEOUT_MS)
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # ログイン成功の確認
            # SP版にはログアウトリンクがないため、URLの遷移とエラーメッセージで判断する
            current_url = self._page.url
            page_text = await self._page.text_content("body") or ""

            # ログイン失敗を示すエラーキーワード
            error_keywords = [
                "加入者番号または暗証番号",
                "認証エラー",
                "ログインに失敗",
                "入力内容をご確認",
                "暗証番号が違います",
                "PAT番号が違います",
                "加入者番号が違います",
            ]
            has_error = any(kw in page_text for kw in error_keywords)
            if has_error:
                logger.warning(f"IPAT ログイン失敗: エラーメッセージ検出 URL={current_url}")
                self.last_login_debug = await self._capture_failure_state("login_error_keyword")
                return False

            # ログインページのままなら失敗（ToSPMenu のバリデーションで弾かれた等）
            if current_url == IPAT_LOGIN_URL:
                logger.warning(f"IPAT ログイン失敗: ページが遷移していません URL={current_url}")
                self.last_login_debug = await self._capture_failure_state("login_url_unchanged")
                return False

            # 「通常投票」リンクが実際に表示されるまで確認する。
            # ここが確認できないまま先に進むと、購入フェーズで初めて失敗が発覚し
            # 原因（IPAT側の想定外画面）が分からなくなる（2026-09-19 本番障害）。
            try:
                # 他の全画面遷移と同じ PURCHASE_TIMEOUT_MS を使う。ここだけ短いタイムアウトに
                # すると、IPAT側の描画が遅いだけ（5秒はかかるが30秒以内には表示される）の
                # ケースまで「ログイン失敗」と誤判定し、tick全体（他レース分も含む）の
                # 購入を中断させてしまう（/code-review指摘）。
                await self._page.wait_for_selector(
                    'a:has-text("通常投票")', state="visible", timeout=PURCHASE_TIMEOUT_MS
                )
            except Exception:
                logger.warning(f"IPAT ログイン失敗: 「通常投票」が表示されません URL={current_url}")
                self.last_login_debug = await self._capture_failure_state("login_menu_not_visible")
                snippet = (self.last_login_debug or {}).get("text_snippet")
                if snippet:
                    logger.warning(f"IPAT ログイン失敗時の画面文言: {snippet}")
                return False

            logger.info(f"JRA IPAT ログイン成功 URL={current_url}")
            return True

        except Exception as e:
            logger.error(f"JRA IPAT ログインエラー: {e}", exc_info=True)
            raise IpatLoginError(f"ログイン処理中にエラーが発生しました: {e}") from e

    async def _reset_session_for_retry(self) -> bool:
        """
        投票送信前フェーズのリトライ用にブラウザセッションを作り直す。

        新規ページ（＝新規Cookie）で再ログインすることで、前回試行で
        投票一覧に途中まで追加された内容をサーバ側ごと確実に破棄する
        （中途半端な一覧が残ったまま次の試行に進むと合計金額がずれる恐れがあるため）。

        Returns:
            True: 再ログイン成功, False: 再ログイン失敗（これ以上リトライしない）
        """
        try:
            if self._page is not None:
                await self._page.close()
        except Exception as e:
            logger.warning(f"リトライ用ページのクローズに失敗（続行します）: {e}")

        try:
            self._page = await self._browser.new_page()
            return await self.login()
        except Exception as e:
            logger.error(f"リトライ用セッションの再構築に失敗: {e}", exc_info=True)
            return False

    async def purchase_bets_for_race(
        self,
        bets: list[dict],
        venue_name: str,
        race_number: int,
        start_time: str | None = None,
    ) -> dict:
        """
        同一レースの複数馬券を一括購入する。

        投票一覧に全馬券を追加してから1回の「投票」で確定する。
        2件目以降は「場名から続けて入力」で同じウィザードセッションを継続する。

        投票送信（「投票」ボタン押下）**前**のフェーズ（通常投票クリック〜合計金額入力）で
        失敗した場合は、発走まで余裕がある限りセッションを作り直して最大
        PRE_SUBMIT_MAX_ATTEMPTS 回まで自動リトライする。
        投票送信**後**のエラーは二重購入を避けるため絶対にリトライしない
        （status="need_confirmation" を返し、手動確認を促す）。

        Args:
            bets: 馬券リスト [{"bet_type": str, "horse_numbers": list[int], "amount": int}, ...]
            venue_name: 競馬場名（曜日付き、例: "中山(土)"）
            race_number: レース番号（例: 7）
            start_time: レース発走時刻（"HHMM"形式）。指定するとリトライの締切判定に使う。

        Returns:
            {"status": "success"|"failed"|"need_confirmation", "total_amount": int,
             "error_message": str|None, "debug": dict|None}

        Raises:
            IpatPurchaseError: 入力値が不正な場合
        """
        if self._page is None:
            raise IpatPurchaseError("ブラウザが初期化されていません。")
        if not bets:
            raise IpatPurchaseError("馬券リストが空です。")

        for bet in bets:
            if bet.get("bet_type") not in BET_TYPE_MAP:
                raise IpatPurchaseError(f"未対応の馬券種: {bet.get('bet_type')}")
            amount = bet.get("amount", 0)
            if amount <= 0 or amount % 100 != 0:
                raise IpatPurchaseError(f"購入金額は100円単位の正の整数である必要があります: {amount}")

        total_amount = sum(b["amount"] for b in bets)
        summary = ", ".join(
            f"{BET_TYPE_MAP[b['bet_type']]} {'-'.join(str(h) for h in b['horse_numbers'])} {b['amount']}円"
            for b in bets
        )
        logger.info(f"一括購入開始: {venue_name} {race_number}R / {len(bets)}件 合計{total_amount}円 [{summary}]")

        retry_deadline: datetime.datetime | None = None
        if start_time and len(start_time) >= 4:
            try:
                hour, minute = int(start_time[:2]), int(start_time[2:4])
                now_jst = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
                retry_deadline = now_jst.replace(
                    hour=hour, minute=minute, second=0, microsecond=0
                ) - datetime.timedelta(minutes=MIN_MINUTES_BEFORE_START_FOR_RETRY)
            except ValueError:
                retry_deadline = None

        # --- フェーズ1: 投票一覧への追加（投票送信前。失敗時はリトライ可） ---
        last_error_msg: str | None = None
        last_debug: dict | None = None
        for attempt in range(1, PRE_SUBMIT_MAX_ATTEMPTS + 1):
            try:
                await self._navigate_to_top_menu()
                for i, bet in enumerate(bets):
                    await self._add_bet_to_list(
                        bet["bet_type"],
                        bet["horse_numbers"],
                        bet["amount"],
                        venue_name,
                        race_number,
                        is_first_bet=(i == 0),
                    )
                # 「入力終了」〜合計金額入力まではまだサーバに未送信のため、ここまでを
                # リトライ可能フェーズに含める（「投票」ボタン押下のみをフェーズ2に残す）。
                await self._prepare_final_confirmation(total_amount)
                break  # 投票送信の準備が完了 → フェーズ2へ
            except IpatPurchaseError:
                raise
            except Exception as e:
                last_error_msg = str(e)
                last_debug = await self._capture_failure_state(f"pre_submit_attempt{attempt}")
                logger.warning(
                    f"馬券入力に失敗（試行{attempt}/{PRE_SUBMIT_MAX_ATTEMPTS}）: {last_error_msg}"
                )

                if attempt >= PRE_SUBMIT_MAX_ATTEMPTS:
                    return {
                        "status": "failed",
                        "total_amount": total_amount,
                        "error_message": f"購入画面エラー（{attempt}回試行）: {last_error_msg}",
                        "debug": last_debug,
                    }

                if retry_deadline and datetime.datetime.now(ZoneInfo("Asia/Tokyo")) >= retry_deadline:
                    logger.warning(
                        f"発走まで{MIN_MINUTES_BEFORE_START_FOR_RETRY}分未満のためリトライを中止します"
                    )
                    return {
                        "status": "failed",
                        "total_amount": total_amount,
                        "error_message": f"購入画面エラー（発走間近のためリトライ中止）: {last_error_msg}",
                        "debug": last_debug,
                    }

                reset_ok = await self._reset_session_for_retry()
                if not reset_ok:
                    # 再ログイン自体の失敗原因（例: IPAT側の障害・メンテナンス画面）を報告する。
                    # last_debug は「元の（購入画面での）失敗」のスナップショットであり、
                    # ここで実際にブロッキング要因になっているのは再ログイン失敗の方なので、
                    # login() が記録した self.last_login_debug を優先する（/code-review指摘）。
                    return {
                        "status": "failed",
                        "total_amount": total_amount,
                        "error_message": f"リトライ用の再ログインに失敗しました（元エラー: {last_error_msg}）",
                        "debug": self.last_login_debug or last_debug,
                    }
                logger.info(f"再ログイン完了。購入を再試行します（試行{attempt + 1}/{PRE_SUBMIT_MAX_ATTEMPTS}）")

        # --- フェーズ2: 投票送信（ここから先は絶対にリトライしない。二重購入防止） ---
        try:
            result = await self._submit_and_confirm()
            result["total_amount"] = total_amount
            result.setdefault("debug", None)
            logger.info(f"一括購入完了: {venue_name} {race_number}R → {result['status']}")
            return result
        except IpatPurchaseError:
            raise
        except Exception as e:
            debug = await self._capture_failure_state("finalize_submit_error")
            error_msg = (
                f"投票送信中にエラーが発生しました。実際に購入されているか必ずIPATで確認してください: {e}"
            )
            logger.error(error_msg, exc_info=True)
            return {
                "status": "need_confirmation",
                "total_amount": total_amount,
                "error_message": error_msg,
                "debug": debug,
            }

    async def purchase_bet(
        self,
        bet_type: str,
        horse_numbers: list[int],
        amount: int,
        venue_name: str,
        race_number: int,
    ) -> dict:
        """
        馬券を1件購入する（後方互換ラッパー）。

        内部的に purchase_bets_for_race を呼び出す。

        Returns:
            {"status": "success"|"failed", "error_message": str|None}
        """
        bets = [{"bet_type": bet_type, "horse_numbers": horse_numbers, "amount": amount}]
        result = await self.purchase_bets_for_race(bets, venue_name, race_number)
        return {"status": result["status"], "error_message": result.get("error_message")}

    async def _wait_for_jqm_ready(self) -> None:
        """jQuery Mobile のページ遷移が完了するまで待機する。

        IPAT SP版は pw_740_i.cgi 上で全ウィザードステップを jQuery Mobile の
        ページ遷移で処理する。遷移中は <html class="... ui-loading"> となり
        ui-loader-cover が画面を覆うため、その消滅を待つ必要がある。
        wait_for_load_state("domcontentloaded") では不十分（URL が変わらず
        DOMは既にロード済みのため即座に返る）。
        """
        try:
            await self._page.wait_for_function(
                "!document.documentElement.classList.contains('ui-loading')",
                timeout=PURCHASE_TIMEOUT_MS,
            )
        except Exception:
            # ui-loading が存在しないページ（トップメニュー等）では無視
            pass

    async def _navigate_to_top_menu(self) -> None:
        """IPATのトップメニューへ遷移する。

        直接 goto(IPAT_TOP_MENU_URL) を呼ぶとセッションエラー(120)が発生するため使用しない。
        - ログイン直後は URL が pw_732_i.cgi のままなので何もしない。
        - ウィザード途中/完了後は「トップメニュー」ボタンをクリックして戻る。
        """
        if "pw_732_i" in self._page.url:
            return  # すでにトップメニューにいる
        logger.info(f"「トップメニュー」ボタンで戻ります (現在URL: {self._page.url})")
        await self._page.click('a:has-text("トップメニュー")', timeout=PURCHASE_TIMEOUT_MS)
        await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

    async def _add_bet_to_list(
        self,
        bet_type: str,
        horse_numbers: list[int],
        amount: int,
        venue_name: str,
        race_number: int,
        is_first_bet: bool = True,
    ) -> None:
        """
        馬券1件をウィザードで入力し「セット」→投票一覧まで進む。

        is_first_bet=True:  「通常投票」クリックから開始
        is_first_bet=False: 投票一覧の「場名から続けて入力」クリックから開始
        """
        bet_label = BET_TYPE_MAP[bet_type]
        horse_str = "-".join(str(h) for h in horse_numbers)
        logger.info(f"馬券入力({'初回' if is_first_bet else '追加'}): {bet_label} {horse_str} {amount}円")

        if is_first_bet:
            # 「通常投票」アイコンをクリック → 競馬場選択画面
            await self._page.click('a:has-text("通常投票")', timeout=PURCHASE_TIMEOUT_MS)
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)
            await self._wait_for_jqm_ready()
        else:
            # 投票一覧の「場名から続けて入力」で同一セッションを継続
            await self._page.click(
                '.ui-page-active a:text-is("場名から続けて入力")', timeout=PURCHASE_TIMEOUT_MS
            )
            await self._wait_for_jqm_ready()

        # 競馬場選択（例: 「中山(土)」）
        # ul.selectList 内の完全一致で指定。
        # 隠れた JQM ページのパンくず「中山(土)」と混同しないよう .selectList でスコープを絞る。
        await self._page.click(f'.selectList a:text-is("{venue_name}")', timeout=PURCHASE_TIMEOUT_MS)
        await self._wait_for_jqm_ready()

        # レース選択（例: 「7R」「9R 袖ケ浦特別」）
        await self._page.click(
            f'.selectList a:has-text("{race_number}R")', timeout=PURCHASE_TIMEOUT_MS
        )
        await self._wait_for_jqm_ready()

        # 式別選択（例: 「複勝」「３連複」）
        await self._page.click(f'.selectList a:text-is("{bet_label}")', timeout=PURCHASE_TIMEOUT_MS)
        await self._wait_for_jqm_ready()

        # 投票形式選択（複数頭馬券のみ）
        try:
            await self._page.wait_for_selector(
                '.selectList a:text-is("通常")', state="visible", timeout=3000
            )
            await self._page.click('.selectList a:text-is("通常")', timeout=PURCHASE_TIMEOUT_MS)
            await self._wait_for_jqm_ready()
        except Exception as e:
            if "Timeout" not in type(e).__name__:
                logger.warning(f"通常選択スキップ: {e}")

        # 馬番選択
        # JQM tap イベントは Playwright click() では発火しないため jQuery.trigger('tap') を使う。
        for h in horse_numbers:
            await self._page.evaluate(
                """(h) => {
                    window.jQuery('.ui-page-active .selectHorse [data-value="' + h + '"]')
                        .trigger('tap');
                }""",
                h,
            )
            await self._page.wait_for_timeout(200)

        # 「金額入力画面へ」（複数頭馬券のみ）
        # evaluate() 内から trigger('extap') すると JQM Deferred が壊れるため Playwright click() を使う。
        try:
            await self._page.wait_for_selector(
                '.ui-page-active a:text-is("金額入力画面へ")', state="attached", timeout=3000
            )
            await self._page.click(
                '.ui-page-active a:text-is("金額入力画面へ")', timeout=PURCHASE_TIMEOUT_MS
            )
            await self._wait_for_jqm_ready()
        except Exception as e:
            if "Timeout" not in type(e).__name__:
                logger.warning(f"金額入力画面へスキップ: {e}")

        # 金額入力（__00円形式: 500円 → 入力値「5」）
        amount_units = str(amount // 100)
        await self._page.fill(
            '.ui-page-active input[type="tel"]', amount_units, timeout=PURCHASE_TIMEOUT_MS
        )

        # 「セット」→ 投票一覧へ
        await self._page.click('.ui-page-active a:text-is("セット")', timeout=PURCHASE_TIMEOUT_MS)
        await self._wait_for_jqm_ready()
        logger.info(f"投票一覧に追加: {bet_label} {horse_str} {amount}円")

    async def _prepare_final_confirmation(self, total_amount: int) -> None:
        """
        投票一覧の「入力終了」クリック〜合計金額欄への入力までを行う。

        ここまではまだサーバに投票を送信していない（画面遷移とフォーム入力のみ）ため、
        失敗しても安全にリトライできる。「投票」ボタン押下（_submit_and_confirm）と
        意図的に分離している（/code-review指摘: 以前はこの部分の失敗も
        「投票送信後のエラー」として扱われ、安全にリトライできるケースまで
        need_confirmation（要手動確認）になってしまっていた）。
        """
        # 「入力終了」→ 合計金額入力へ
        await self._page.click('.ui-page-active a:text-is("入力終了")', timeout=PURCHASE_TIMEOUT_MS)
        await self._wait_for_jqm_ready()

        # 合計金額確認入力
        await self._page.wait_for_selector('#sum', state='attached', timeout=PURCHASE_TIMEOUT_MS)
        await self._page.locator('#sum').fill(str(total_amount), timeout=PURCHASE_TIMEOUT_MS)

    async def _submit_and_confirm(self) -> dict:
        """
        「投票」ボタン押下〜完了確認。

        #sum は FORM0 の外にある独立した入力欄。JS の投票ハンドラが #sum を読んで
        検証し、confirm ダイアログ後に FORM0.submit() する。

        ここから先はサーバに送信済みの可能性がある（＝投票が成立している可能性がある）
        ため、呼び出し元は絶対にリトライしてはいけない（二重購入防止）。
        """
        async with self._page.expect_navigation(
            wait_until="domcontentloaded", timeout=PURCHASE_TIMEOUT_MS
        ):
            await self._page.evaluate(
                "window.jQuery('.ui-page-active .btnColor a').trigger('tap')"
            )
        await self._wait_for_jqm_ready()

        # 完了確認
        active_text = await self._page.locator('.ui-page-active').inner_text() or ""
        logger.info(f"完了確認 active_text: {active_text[:300]}")

        ERROR_PATTERNS = [
            "残高不足",
            "締め切られました",
            "締め切り",
            "金額が一致しません",
            "合計金額が違います",
            "投票できません",
            "エラーが発生",
            "ご確認ください",
        ]
        for pat in ERROR_PATTERNS:
            if pat in active_text:
                return {"status": "failed", "error_message": pat}

        if "受付番号" in active_text:
            return {"status": "success", "error_message": None}

        snippet = active_text.strip()[:200]
        return {"status": "failed", "error_message": f"完了確認できず: {snippet}"}

    async def logout(self) -> None:
        """IPAT からログアウトする"""
        if self._page is None:
            return
        try:
            logout_link = await self._page.query_selector('a[href*="logout"]')
            if logout_link:
                await self._page.click('a[href*="logout"]', timeout=10_000)
                logger.info("JRA IPAT ログアウト完了")
        except Exception as e:
            logger.warning(f"ログアウト中にエラーが発生しました（無視します）: {e}")


# ---------------------------------------------------------------------------
# BigQuery ユーティリティ
# ---------------------------------------------------------------------------

def fetch_today_races_with_start_time(
    project_id: str,
    target_date: datetime.date,
) -> list[dict]:
    """
    当日の発走時刻付きレース一覧を raw.race_info から取得する。

    start_time が NULL のレースはスキップする（Issue #214 実装前のデータ対策）。

    Returns:
        [{"race_id": str, "start_time": str, "venue_name": str, "race_number": int}, ...]
        start_time の昇順でソートされたリスト
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT race_id, start_time, venue_name, race_number
        FROM `{project}.raw.race_info`
        WHERE race_date = @race_date
          AND start_time IS NOT NULL
        ORDER BY start_time ASC
    """.format(project=project_id)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("race_date", "DATE", target_date.isoformat()),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    result = [dict(r) for r in rows]
    logger.info(f"{target_date}: start_time付きレース {len(result)}件取得")
    return result


def fetch_target_races(
    all_races: list[dict],
    now: datetime.datetime,
    window_minutes_before: int = 5,
    window_minutes_after: int = 0,
) -> list[dict]:
    """
    現在時刻の window_minutes_after〜window_minutes_before 分後に発走するレースを抽出する。

    例: now=10:00, window=(0, 5) → 10:00〜10:05 に発走するレースを返す

    Args:
        all_races: fetch_today_races_with_start_time() の戻り値
        now: 現在時刻（JST。start_time がJSTで格納されているため JST を渡すこと）
        window_minutes_before: ウィンドウ開始（now + この分数後から）
        window_minutes_after:  ウィンドウ終了（now + この分数後まで）

    Returns:
        対象レースの辞書リスト
    """
    target_races = []
    for race in all_races:
        start_time_str = race.get("start_time", "")
        if not start_time_str or len(start_time_str) < 4:
            continue
        try:
            h = int(start_time_str[:2])
            m = int(start_time_str[2:4])
            start_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
            delta_minutes = (start_dt - now).total_seconds() / 60
            if window_minutes_after <= delta_minutes <= window_minutes_before:
                target_races.append(race)
        except (ValueError, AttributeError):
            logger.warning(f"発走時刻のパースに失敗: race_id={race.get('race_id')}, start_time={start_time_str}")
    return target_races


def fetch_recommended_bets(
    project_id: str,
    race_id: str,
    target_date: datetime.date,
) -> list[dict]:
    """
    predictions.investment_decisions から対象レースの推奨馬券を取得する。

    Returns:
        [{"bet_type": str, "horse_numbers": list[int], "bet_amount": int}, ...]
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT bet_type, horse_numbers, bet_amount
        FROM `{project}.predictions.investment_decisions`
        WHERE race_date = @race_date
          AND race_id = @race_id
        ORDER BY bet_amount DESC
    """.format(project=project_id)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("race_date", "DATE", target_date.isoformat()),
            bigquery.ScalarQueryParameter("race_id", "STRING", race_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    result = []
    for row in rows:
        hn_str = str(row["horse_numbers"] or "")
        horse_numbers = [int(h.strip()) for h in hn_str.split(",") if h.strip()]
        result.append({
            "bet_type": row["bet_type"],
            "horse_numbers": horse_numbers,
            "bet_amount": int(row["bet_amount"]),
        })
    logger.info(f"race_id={race_id}: 推奨馬券 {len(result)}件取得")
    return result


def has_purchase_attempt_recorded(
    project_id: str,
    target_date: datetime.date,
    race_id: str,
) -> bool:
    """
    対象レースについて既に purchase_history に status='success' または
    'need_confirmation' の記録があるか確認する。

    購入失敗レースを次回tickでも再試行できるようウィンドウを拡張した際（Issue #433）、
    既に購入成功済み、または投票送信後にエラーが発生し実際の購入有無が不明
    （need_confirmation。サーバに送信済みの可能性がある）なレースを再度自動購入
    してしまう二重購入を防ぐために使用する。status='failed'（投票送信前の失敗。
    サーバには一切送信されていない）のみは対象外とし、次tickでの再挑戦を許可する。

    Returns:
        True: 既に購入成功済み、または結果不明で要確認（自動での再購入は禁止）
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT COUNT(*) AS n
        FROM `{project}.predictions.purchase_history`
        WHERE race_date = @race_date
          AND race_id = @race_id
          AND status IN ('success', 'need_confirmation')
    """.format(project=project_id)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("race_date", "DATE", target_date.isoformat()),
            bigquery.ScalarQueryParameter("race_id", "STRING", race_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return bool(rows and int(rows[0]["n"]) > 0)


def fetch_daily_spent_amount(
    project_id: str,
    target_date: datetime.date,
) -> int:
    """
    当日の累計購入金額を取得する。

    status='success'（購入成功）に加え、status='need_confirmation'（投票送信後に
    エラーが発生し実際に購入されたか不明。サーバに送信済みの可能性がある）も
    「実際に使われた可能性がある金額」として保守的に合算する。need_confirmation
    を除外すると、実際には購入済みの金額が1日の購入上限（DAILY_BUDGET_LIMIT）の
    チェックから漏れ、上限を超過しうる（Issue #433）。

    Returns:
        累計購入金額（円）
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM `{project}.predictions.purchase_history`
        WHERE race_date = @race_date
          AND status IN ('success', 'need_confirmation')
    """.format(project=project_id)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("race_date", "DATE", target_date.isoformat()),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    total = int(rows[0]["total"]) if rows else 0
    logger.info(f"{target_date}: 当日累計購入額 {total:,}円")
    return total


def save_purchase_record(
    project_id: str,
    race_date: datetime.date,
    race_id: str,
    bet_type: str,
    horse_numbers: list[int],
    amount: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    購入結果を predictions.purchase_history に INSERT する。
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.predictions.purchase_history"

    row = {
        "purchase_id": str(uuid.uuid4()),
        "race_date": race_date.isoformat(),
        "race_id": race_id,
        "bet_type": bet_type,
        "horse_numbers": horse_numbers,
        "amount": amount,
        "status": status,
        "error_message": error_message,
        "purchased_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        logger.error(f"purchase_history INSERT エラー: {errors}")
        raise RuntimeError(f"BQ保存失敗: {errors}")
    logger.info(f"purchase_history 保存完了: race_id={race_id}, {bet_type}, {amount}円, status={status}")
