"""
IPAT SP版 HTML構造診断スクリプト

各画面のHTMLをダンプしてセレクタを確認する。
購入は一切しない（readonlyモード）。

実行:
  IPAT_MEMBER_ID=xxx IPAT_PIN=xxx IPAT_PAT_NUMBER=xxx .venv/bin/python scripts/debug_ipat_html.py
"""

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

IPAT_LOGIN_URL = "https://www.ipat.jra.go.jp/sp/index.cgi"
IPAT_TOP_MENU_URL = "https://www.ipat.jra.go.jp/sp/pw_732_i.cgi"


async def main():
    member_id = os.environ["IPAT_MEMBER_ID"]
    pin = os.environ["IPAT_PIN"]
    pat_number = os.environ["IPAT_PAT_NUMBER"]

    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,  # 画面を見ながらデバッグ
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        page = await browser.new_page()
        page.on("dialog", lambda d: asyncio.create_task(d.dismiss()))

        # ---- Step 1: ログイン ----
        print("=== Step 1: ログイン ===")
        await page.goto(IPAT_LOGIN_URL, wait_until="domcontentloaded")
        await page.fill("#userid", member_id)
        await page.fill("#password", pin)
        await page.fill("#pars", pat_number)
        await page.click("li.btnColor a")
        await page.wait_for_load_state("domcontentloaded")
        print(f"ログイン後URL: {page.url}")

        # ---- Step 2: ログイン後のページ HTML ダンプ（goto なし）----
        print("\n=== Step 2: ログイン後のページ HTML（goto なし）===")
        # ログイン後に自然に着地しているページをそのままキャプチャ
        print(f"現在のURL: {page.url}")
        html = await page.content()
        Path("/tmp/ipat_top_menu.html").write_text(html, encoding="utf-8")
        print("HTML を /tmp/ipat_top_menu.html に保存しました")

        # 「通常投票」を含む要素を検索
        print("\n--- 「通常投票」を含む要素 ---")
        elems = await page.query_selector_all("*:has-text('通常投票')")
        for el in elems:
            tag = await el.evaluate("e => e.tagName")
            cls = await el.evaluate("e => e.className")
            href = await el.evaluate("e => e.href || ''")
            inner = (await el.inner_text()).strip().replace("\n", " ")[:60]
            print(f"  <{tag.lower()} class='{cls}' href='{href}'> {inner!r}")

        # <a>タグをすべてリスト
        print("\n--- 全 <a> タグ (最初の20件) ---")
        anchors = await page.query_selector_all("a")
        for a in anchors[:20]:
            href = await a.evaluate("e => e.href || ''")
            text = (await a.inner_text()).strip().replace("\n", " ")[:60]
            print(f"  href={href!r}  text={text!r}")

        await page.screenshot(path="/tmp/ipat_top_screenshot.png")
        print("\nスクリーンショット: /tmp/ipat_top_screenshot.png")

        # ---- Step 3: 通常投票をクリックして競馬場選択画面のHTML ----
        print("\n=== Step 3: 通常投票クリック試行 ===")
        # 複数の候補を試す
        candidates = [
            "a:has-text('通常投票')",
            "a[href*='pw_']",
            "*:has-text('通常投票') >> nth=0",
        ]
        clicked = False
        for sel in candidates:
            try:
                el = await page.query_selector(sel)
                if el:
                    print(f"  ヒット: {sel}")
                    tag = await el.evaluate("e => e.tagName")
                    href = await el.evaluate("e => e.href || ''")
                    text = (await el.inner_text()).strip()[:40]
                    print(f"    <{tag.lower()} href={href!r}> {text!r}")
                else:
                    print(f"  なし: {sel}")
            except Exception as e:
                print(f"  エラー: {sel} -> {e}")

        # 実際にクリックして次画面のHTMLを取得
        print("\n=== Step 4: 実際にクリックして競馬場選択画面 ===")
        # ログイン後のページからそのままクリックを試みる（goto なし）
        try:
            # li内のimg直後のテキストを含む<a>を探す
            await page.click("a:has-text('通常投票')", timeout=5000)
            await page.wait_for_load_state("domcontentloaded")
            print(f"クリック成功 -> URL: {page.url}")
            html2 = await page.content()
            Path("/tmp/ipat_venue_select.html").write_text(html2, encoding="utf-8")
            print("競馬場選択HTML -> /tmp/ipat_venue_select.html")

            # 競馬場選択画面のリンクを確認
            print("\n--- 競馬場選択画面の全 <a> タグ ---")
            anchors2 = await page.query_selector_all("a")
            for a in anchors2[:20]:
                href = await a.evaluate("e => e.href || ''")
                text = (await a.inner_text()).strip()[:60]
                print(f"  href={href!r}  text={text!r}")

        except Exception as e:
            print(f"クリック失敗: {e}")
            # href から直接推測を試みる
            print("\n--- href に 'pw_' を含むリンク ---")
            links = await page.query_selector_all("a[href*='pw_'], a[href*='vote'], a[href*='ticket']")
            for l in links:
                href = await l.evaluate("e => e.href")
                text = (await l.inner_text()).strip()[:40]
                print(f"  {href!r} -> {text!r}")

        print("\n=== 診断完了 ===")
        input("Enterキーでブラウザを閉じます...")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
