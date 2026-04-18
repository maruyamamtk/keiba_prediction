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

import datetime
import logging
import uuid

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
    """

    def __init__(self, member_id: str, pin: str, pat_number: str) -> None:
        self.member_id = member_id
        self.pin = pin
        self.pat_number = pat_number
        self._playwright = None
        self._browser = None
        self._page = None

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

    async def login(self) -> bool:
        """
        JRA IPAT にログインする。

        Returns:
            True: ログイン成功
            False: ログイン失敗

        Raises:
            IpatLoginError: ログイン処理中に予期しないエラーが発生した場合
        """
        if self._page is None:
            raise IpatLoginError("ブラウザが初期化されていません。コンテキストマネージャー経由で使用してください。")

        try:
            logger.info("JRA IPAT ログイン開始")

            # バリデーション失敗時に出る alert を自動 dismiss
            self._page.on("dialog", lambda d: d.dismiss())

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
                return False

            # ログインページのままなら失敗（ToSPMenu のバリデーションで弾かれた等）
            if current_url == IPAT_LOGIN_URL:
                logger.warning(f"IPAT ログイン失敗: ページが遷移していません URL={current_url}")
                return False

            logger.info(f"JRA IPAT ログイン成功 URL={current_url}")
            return True

        except Exception as e:
            logger.error(f"JRA IPAT ログインエラー: {e}", exc_info=True)
            raise IpatLoginError(f"ログイン処理中にエラーが発生しました: {e}") from e

    async def purchase_bet(
        self,
        bet_type: str,
        horse_numbers: list[int],
        amount: int,
        venue_name: str,
        race_number: int,
    ) -> dict:
        """
        馬券を1件購入する（IPATウィザード形式）。

        Args:
            bet_type: 馬券種 ("win"/"place"/"wide"/"umaren"/"umatan"/"sanrenpuku")
            horse_numbers: 馬番リスト（単勝/複勝は [n]、2頭は [n, m]、3頭は [n, m, k]）
            amount: 購入金額（100円単位）
            venue_name: 競馬場名（曜日付き、例: "中山(土)"）
            race_number: レース番号（例: 7）

        Returns:
            {"status": "success"|"failed", "error_message": str|None}

        Raises:
            IpatPurchaseError: 購入処理中に予期しないエラーが発生した場合
        """
        if self._page is None:
            raise IpatPurchaseError("ブラウザが初期化されていません。")

        if bet_type not in BET_TYPE_MAP:
            raise IpatPurchaseError(f"未対応の馬券種: {bet_type}")

        if amount <= 0 or amount % 100 != 0:
            raise IpatPurchaseError(f"購入金額は100円単位の正の整数である必要があります: {amount}")

        bet_type_label = BET_TYPE_MAP[bet_type]
        horse_str = "-".join(str(h) for h in horse_numbers)
        logger.info(f"馬券購入開始: {bet_type_label} {horse_str} {amount}円")

        try:
            result = await self._execute_purchase(bet_type, horse_numbers, amount, venue_name, race_number)
            logger.info(f"馬券購入完了: {bet_type_label} {horse_str} {amount}円 → {result['status']}")
            return result

        except IpatPurchaseError:
            raise
        except Exception as e:
            logger.error(f"馬券購入エラー: {bet_type_label} {horse_str} - {e}", exc_info=True)
            return {"status": "failed", "error_message": str(e)}

    async def _navigate_to_top_menu(self) -> None:
        """IPATのトップメニュー(pw_732_i.cgi)へ移動する。"""
        if "pw_732_i" not in self._page.url:
            await self._page.goto(
                IPAT_TOP_MENU_URL,
                timeout=PURCHASE_TIMEOUT_MS,
                wait_until="domcontentloaded",
            )

    async def _execute_purchase(
        self,
        bet_type: str,
        horse_numbers: list[int],
        amount: int,
        venue_name: str,
        race_number: int,
    ) -> dict:
        """
        IPAT SP版のウィザード形式で馬券を1件購入する内部メソッド。

        購入フロー:
          1. トップメニューへ遷移
          2. 「通常投票」アイコンをクリック
          3. 競馬場を選択（例: 「中山(土)」）
          4. レースを選択（例: 「7R」）
          5. 式別を選択（例: 「複勝」「３連複」）
          6. 馬番を選択（1頭ずつ、複数頭は繰り返し）
          7. 金額入力（__00円形式 = 入力値×100円）→「セット」
          8. 投票一覧 → 「入力終了」
          9. 合計金額入力（円単位）→「投票」
         10. 完了確認
        """
        try:
            # Step 1: トップメニューへ遷移
            await self._navigate_to_top_menu()

            # Step 2: 「通常投票」アイコンをクリック
            await self._page.click('a:has-text("通常投票")', timeout=PURCHASE_TIMEOUT_MS)
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 3: 競馬場選択（例: 「中山(土)」）
            # has-text は部分一致のため venue_name だけでも機能するが、
            # 同一競馬場が「中山(土)」「中山(日)」両方表示される場合があるので曜日付き名称を優先
            await self._page.click(f'a:has-text("{venue_name}")', timeout=PURCHASE_TIMEOUT_MS)
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 4: レース選択（例: 「7R」）
            await self._page.click(
                f'a:has-text("{race_number}R")',
                timeout=PURCHASE_TIMEOUT_MS,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 5: 式別選択（例: 「複勝」「３連複」）
            bet_label = BET_TYPE_MAP[bet_type]
            await self._page.click(f'a:has-text("{bet_label}")', timeout=PURCHASE_TIMEOUT_MS)
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 6: 馬番選択（複数頭は1頭ずつクリック）
            # IPATでは馬番が2桁ゼロパディングで表示される（例: 03）
            for h in horse_numbers:
                horse_num_str = f"{h:02d}"
                await self._page.click(
                    f'a:has-text("{horse_num_str}")',
                    timeout=PURCHASE_TIMEOUT_MS,
                )
                await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 7: 金額入力（__00円形式: 500円 → 入力値「5」）
            amount_units = str(amount // 100)
            await self._page.fill('input[type="text"]', amount_units, timeout=PURCHASE_TIMEOUT_MS)

            # Step 8: 「セット」ボタンをクリック
            await self._page.click(
                'a:has-text("セット"), button:has-text("セット"), input[value="セット"]',
                timeout=PURCHASE_TIMEOUT_MS,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 9: 「入力終了」をクリック（投票一覧画面）
            await self._page.click(
                'a:has-text("入力終了"), button:has-text("入力終了")',
                timeout=PURCHASE_TIMEOUT_MS,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 10: 合計金額確認入力（円単位）→「投票」ボタン
            await self._page.fill('input[type="text"]', str(amount), timeout=PURCHASE_TIMEOUT_MS)
            await self._page.click(
                'a:has-text("投票"), button:has-text("投票")',
                timeout=PURCHASE_TIMEOUT_MS,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=PURCHASE_TIMEOUT_MS)

            # Step 11: 完了確認
            page_text = await self._page.text_content("body") or ""
            if "残高不足" in page_text:
                return {"status": "failed", "error_message": "残高不足"}

            return {"status": "success", "error_message": None}

        except Exception as e:
            error_msg = str(e)
            if "Timeout" in error_msg:
                return {"status": "failed", "error_message": f"購入画面タイムアウト: {error_msg}"}
            raise IpatPurchaseError(error_msg) from e

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
    window_minutes_before: int = 10,
    window_minutes_after: int = 5,
) -> list[dict]:
    """
    現在時刻の window_minutes_after〜window_minutes_before 分後に発走するレースを抽出する。

    例: now=10:00, window=(5, 10) → 10:05〜10:10 に発走するレースを返す

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


def fetch_daily_spent_amount(
    project_id: str,
    target_date: datetime.date,
) -> int:
    """
    当日の累計購入金額（成功分のみ）を取得する。

    Returns:
        累計購入金額（円）
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM `{project}.predictions.purchase_history`
        WHERE race_date = @race_date
          AND status = 'success'
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
