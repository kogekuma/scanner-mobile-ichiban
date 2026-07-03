"""買取商店（kaitorishouten-co.jp）スクレイパー

スクレイピング戦略（5フェーズ）:
  Phase 1: AJAX エンドポイント（keitai・kaden）
    - ベースページでセッション確立 → X-Requested-With ヘッダー付きで AJAX 取得
    - 優先データソース: AJAX で取得した JAN は以降フェーズで上書きしない
    - この AJAX リクエストで取得した Cookie（AWSALB 等）が Phase 5・6 に必要

  Phase 5: kaden カテゴリページ（ゲームソフト・カード等 AJAX 未収録分を補完）
    - /kaden ページナビの a.do-product-list[data-category] から全カテゴリ ID を取得
    - /products/2/list_category/{id} を全ページスキャン
    - Phase 1 の AJAX Cookie がないとアクセス不可のため Phase 1 直後に実行
    - 1並列（レート制限で 403 が頻発するため低並列）

  Phase 6: nitiyouhin カテゴリページ（ウィスキー・日本酒・ワイン等、完全未収録分）
    - /nitiyouhin ページナビの a.do-product-list[data-category] から全カテゴリ ID を取得
    - /products/3/list_category/{id} を全ページスキャン
    - sitemap の /category/3/{id} とは別系統の ID（sitemap 経由では到達不可）
    - Phase 1 の Cookie がないとアクセス不可のため Phase 5 直後に実行
    - 1並列（Phase 5 と同じ理由）

  Phase 3: category/3 全 ID（最大商品ソース、約 2,991 件）
    - sitemap.xml から /category/3/ 配下の全 ID を取得
    - 5並列 ThreadPoolExecutor で処理
    - 約 231 カテゴリ × 平均 14 件

  Phase 4: category/4 keitai サブカテゴリ（約 541 件）
    - sitemap.xml から /category/4/ 配下の全 ID を取得
    - 5並列 ThreadPoolExecutor で処理
    - category/2 (kaden) は AJAX kaden と同一セットのためスキャン不要

HTML 構造（2種類）:
  カードレイアウト（AJAX _new + category/3）:
    span.product-code-default "JAN:" の次の span が JAN 値
    コンテナ: <form data-calc-url>
    商品名: h4.item-title / 価格: div.item-price.plain-price

  テーブルレイアウト（kaden タグページ等）:
    コンテナ: <tr class="price_list_item">
    構造: td[0]=画像 / td[1]=商品名+JAN / td[2]=価格 / form=ボタン群
    ※ form は td の兄弟要素であり form の内側に商品情報はない
    商品名: JAN span の親 td の直接テキスト / 価格: div.item-price.plain-price

スレッド安全性:
  _scan_category は threading.Lock で results への書き込みを保護する。
  Referer ヘッダーの変更は ThreadPoolExecutor ブロックの完了後に行うため安全。
"""

import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from scrapers.common import extract_jan, extract_price, merge_into_results
from scrapers.kaitorishouten.config import (
    SITE_ID, SITE_NAME, BASE_URL, AJAX_CATEGORIES, AJAX_DELAY, CATEGORY_DELAY,
    MAX_WORKERS, PHASE5_WORKERS, PHASE6_WORKERS,
)


def _ajax_sleep():
    """AJAX エンドポイント用のウェイト（固定）。"""
    time.sleep(AJAX_DELAY)


def _cat_sleep():
    """カテゴリページ用のウェイト（±30% ジッター付き）。
    固定間隔パターンによるボット検出を回避する。"""
    time.sleep(random.uniform(CATEGORY_DELAY * 0.7, CATEGORY_DELAY * 1.3))


class KaitorishoutenScraper(BaseScraper):
    site_id   = SITE_ID
    site_name = SITE_NAME

    def _get_sitemap_ids(self) -> dict:
        """sitemap.xml から category/2・/3・/4 の ID 一覧を取得する。

        Returns:
            {"cat2": [int, ...], "cat3": [int, ...], "cat4": [int, ...]}
        """
        try:
            r = self.get(BASE_URL + "/sitemap.xml")
            r.raise_for_status()
        except Exception as e:
            print(f"  [kaitorishouten] sitemap.xml 取得失敗: {e}", flush=True)
            return {"cat2": [], "cat3": [], "cat4": []}
        ids = {}
        for key, pat in [
            ("cat2", "/category/2/"),
            ("cat3", "/category/3/"),
            ("cat4", "/category/4/"),
        ]:
            ids[key] = sorted(
                set(int(x) for x in re.findall(rf"{re.escape(pat)}(\d+)", r.text))
            )
        return ids

    def _extract_jans(self, soup) -> list[tuple[str, str, int | None]]:
        """カード・テーブル両レイアウト対応で (jan, name, price) リストを返す。

        JAN の位置:
          span.product-code-default に "JAN:" というテキストがあり、
          その直後の span.product-code-default が JAN 値を持つ。
          ※ 連続する span の偶数番目がキー、奇数番目が値という構造。

        コンテナ判定:
          祖先を遡り最初に見つかった <tr> か <form> でレイアウトを判定する。
          - <tr>:   テーブルレイアウト → td[0] が商品名
          - <form>: カードレイアウト  → h4.item-title が商品名
        """
        items = []
        spans = soup.select("span.product-code-default")
        for i, s in enumerate(spans):
            if s.get_text(strip=True) != "JAN:" or i + 1 >= len(spans):
                continue
            jan = extract_jan(spans[i + 1].get_text(strip=True))
            if not jan:
                continue

            # 最近傍の <tr> または <form> をコンテナとして特定
            container = None
            for ancestor in spans[i].parents:
                if ancestor.name in ("tr", "form"):
                    container = ancestor
                    break

            name, price, price_el = "", None, None
            if container:
                if container.name == "tr":
                    # テーブルレイアウト: JAN span の親 td から商品名を取得
                    # kaden タグページは td[0]=画像, td[1]=名前+JAN のため
                    # td[0] ではなく JAN span の親 td を使う
                    name_td = None
                    for p in spans[i].parents:
                        if p.name == "td":
                            name_td = p
                            break
                    if name_td:
                        lines = [
                            ln.strip()
                            for ln in name_td.get_text(separator="\n").split("\n")
                            if ln.strip()
                        ]
                        name = " ".join(
                            ln for ln in lines
                            if not ln.startswith("JAN") and not re.fullmatch(r"\d+", ln)
                        )[:100]
                    price_el = container.select_one("div.item-price.plain-price")
                else:
                    # カードレイアウト: h4.item-title に商品名
                    title_el = container.select_one("h4.item-title")
                    if title_el:
                        name = title_el.get_text(strip=True)
                    price_el = container.select_one("div.item-price.plain-price")
                if price_el:
                    price = extract_price(price_el.get_text(strip=True))
            items.append((jan, name, price))
        return items

    def _get_kaden_category_ids(self) -> list[int]:
        """kaden ページのナビゲーションから list_category ID 一覧を取得する。

        a.do-product-list[data-category] の data-category 属性を全て抽出する。
        対象: ゲームソフト・カードゲーム・フィギュア・家電等、kaden 配下の全サブカテゴリ。
        Phase 1 AJAX 後に呼ぶこと（Cookie がないとページが 404 になる）。
        """
        try:
            resp = self.get(BASE_URL + "/kaden")
            soup = BeautifulSoup(resp.text, "html.parser")
            ids: set[int] = set()
            for a in soup.select("a.do-product-list[data-category]"):
                try:
                    ids.add(int(a["data-category"]))
                except (ValueError, KeyError):
                    pass
            return sorted(ids)
        except Exception as e:
            print(f"  [kaitorishouten] kaden category ID 取得失敗: {e}", flush=True)
            return []

    def _get_nitiyouhin_category_ids(self) -> list[int]:
        """nitiyouhin ページのナビゲーションから list_category ID 一覧を取得する。

        a.do-product-list[data-category] の data-category 属性を全て抽出する。
        対象: ウィスキー・日本酒・ワイン・シャンパン・ブランデー・化粧品等。
        URL パターンは /products/3/list_category/{id}。
        sitemap の /category/3/{id} とは別系統の ID（sitemap から到達不可）。
        Phase 1 AJAX 後に呼ぶこと（Cookie がないとページが 404 になる）。
        """
        try:
            resp = self.get(BASE_URL + "/nitiyouhin")
            soup = BeautifulSoup(resp.text, "html.parser")
            ids: set[int] = set()
            for a in soup.select("a.do-product-list[data-category]"):
                try:
                    ids.add(int(a["data-category"]))
                except (ValueError, KeyError):
                    pass
            return sorted(ids)
        except Exception as e:
            print(f"  [kaitorishouten] nitiyouhin category ID 取得失敗: {e}", flush=True)
            return []

    def _scan_ajax(self, base_path: str, ajax_path: str, results: dict):
        """_new AJAX エンドポイントの全ページをスキャンする（優先データソース）。

        1. ベースページ GET でセッション Cookie・Referer を確立
        2. X-Requested-With ヘッダーを付与して AJAX エンドポイントを叩く
        3. form[data-calc-url] がなければ最終ページとみなして終了

        Args:
            base_path: ブラウザが最初に訪問するパス（例: "/keitai"）
            ajax_path: AJAX リストエンドポイント（例: "/products/list_keitai_new/9"）
            results:   JAN をキーとする価格辞書（共有・更新される）
        """
        url_base = BASE_URL + base_path
        url_ajax = BASE_URL + ajax_path
        try:
            self.get(url_base)
        except Exception as e:
            print(f"  [kaitorishouten] {base_path} base failed: {e}")
            _ajax_sleep()
            return
        _ajax_sleep()

        self.session.headers.update({
            "X-Requested-With": "XMLHttpRequest",
            "Referer": url_base,
        })
        page = 1
        while True:
            try:
                resp = self.get(url_ajax, params={"pageno": page})
                resp.raise_for_status()
            except Exception as e:
                print(f"  [kaitorishouten] {ajax_path} page={page} failed: {e}")
                if "404" not in str(e):
                    _ajax_sleep()
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            # form[data-calc-url] が存在しなければ商品ゼロ = 最終ページを超えた
            if not soup.select("form[data-calc-url]"):
                break

            for jan, name, price in self._extract_jans(soup):
                if price:
                    merge_into_results(results, jan, name, price, url_base)
            print(f"  [kaitorishouten] {base_path} ajax page={page}", flush=True)
            page += 1
            _ajax_sleep()

    def _scan_category(self, cat_path: str, results: dict, lock: threading.Lock, force: bool = False):
        """カテゴリページを全ページスキャンして results にマージする。

        複数スレッドから同時に呼ばれるため lock で書き込みを保護する。
        ページ数は 1ページ目のページネーションリンクから動的に取得する。

        Args:
            cat_path: カテゴリパス（例: "/category/3/1234"）
            results:  JAN をキーとする価格辞書（共有・更新される）
            lock:     results への排他アクセス用ロック
            force:    True の場合 AJAX 取得済み価格より低くても上書き（category/3・4 用）
        """
        url = BASE_URL + cat_path
        page, max_page = 1, 1
        while True:
            try:
                resp = self.get(url, params={"pageno": page} if page > 1 else {})
                resp.raise_for_status()
            except Exception as e:
                print(f"  [kaitorishouten] {cat_path} page={page} failed: {e}")
                # 404 はカテゴリ廃止なので sleep 不要。それ以外（403等）は待機する。
                if "404" not in str(e):
                    _cat_sleep()
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            if page == 1:
                # category/3 は a.page-link、list_category は javascript:goto_page('N') を使う
                nums = [
                    int(a.get_text(strip=True))
                    for a in soup.select("a.page-link")
                    if a.get_text(strip=True).isdigit()
                ]
                if not nums:
                    for a in soup.select("a[href*='goto_page']"):
                        m = re.search(r"goto_page\('(\d+)'\)", a.get("href", ""))
                        if m:
                            nums.append(int(m.group(1)))
                max_page = max(nums) if nums else 1

            for jan, name, price in self._extract_jans(soup):
                if price:
                    with lock:
                        merge_into_results(results, jan, name, price, url, force=force)

            if page >= max_page:
                break
            page += 1
            _cat_sleep()

    def scrape(self, mode: str = "full") -> dict:
        """フェーズごとにスキャンして JAN → 価格情報の辞書を返す。

        mode:
          "full" … 全フェーズ（Phase 1/5/6/3/4）。完全スナップショット。3時間ごとに実行。
          "fast" … Phase 1（携帯AJAX＋家電/カメラAJAX）＋ Phase 5（ゲーム機/ソフト/トレカ等の
                    kaden list_category）のみ。変動の速い商品を短時間で取得。15〜30分ごとに実行し、
                    呼び出し側で前回 full の結果に上書き合成する（category/3・4・6 は前回値を保持）。
        """
        results = {}
        lock = threading.Lock()

        # ── Phase 1: AJAX（keitai・kaden、逐次、優先データソース）─────
        # nitiyouhin は category/3 が上位互換なので除外済み
        print("[kaitorishouten] Phase 1: AJAX endpoints", flush=True)
        for base_path, ajax_path in AJAX_CATEGORIES:
            self._scan_ajax(base_path, ajax_path, results)
        print(f"  AJAX total: {len(results)} JANs", flush=True)

        # ── Phase 5: kaden list_category（AJAX 未収録のゲームソフト・カード等を補完）──
        # Phase 1 で取得した Cookie（AWSALB 等）がないとページが 404 になる
        # 5並列だとレート制限で game 系カテゴリが 404 になるため 2並列に抑制
        self.session.headers.pop("X-Requested-With", None)
        cat_ids = self._get_kaden_category_ids()
        print(f"[kaitorishouten] Phase 5: kaden list_category ({len(cat_ids)} categories)", flush=True)
        if cat_ids:
            self.session.headers.update({"Referer": BASE_URL + "/kaden"})
            with ThreadPoolExecutor(max_workers=PHASE5_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self._scan_category,
                        f"/products/2/list_category/{cid}",
                        results,
                        lock,
                    ): cid
                    for cid in cat_ids
                }
                for future in as_completed(futures):
                    if future.exception():
                        print(
                            f"  [kaitorishouten] list_category/{futures[future]} 例外: {future.exception()}",
                            flush=True,
                        )
        print(f"  Phase 5 after: {len(results)} JANs", flush=True)

        # fast モードは Phase 1＋5 のみで終了（携帯・家電/カメラ・ゲーム/トレカの変動分だけ取得）
        if mode == "fast":
            print(f"[kaitorishouten] fast done: {len(results)} JANs", flush=True)
            return results

        # ── Phase 6: nitiyouhin list_category（ウィスキー・日本酒・ワイン等を新規取得）──
        # sitemap の /category/3/{id} とは別系統（sitemap 経由では到達不可）。
        # Phase 1 の Cookie が必要なため Phase 5 直後に実行する。
        niti_ids = self._get_nitiyouhin_category_ids()
        print(f"[kaitorishouten] Phase 6: nitiyouhin list_category ({len(niti_ids)} categories)", flush=True)
        if niti_ids:
            self.session.headers.update({"Referer": BASE_URL + "/nitiyouhin"})
            with ThreadPoolExecutor(max_workers=PHASE6_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self._scan_category,
                        f"/products/3/list_category/{cid}",
                        results,
                        lock,
                    ): cid
                    for cid in niti_ids
                }
                for future in as_completed(futures):
                    if future.exception():
                        print(
                            f"  [kaitorishouten] nitiyouhin list_category/{futures[future]} 例外: {future.exception()}",
                            flush=True,
                        )
        print(f"  Phase 6 after: {len(results)} JANs", flush=True)

        # sitemap.xml からカテゴリ ID を取得
        sm = self._get_sitemap_ids()
        print(
            f"[kaitorishouten] sitemap: "
            f"cat2={len(sm['cat2'])} cat3={len(sm['cat3'])} cat4={len(sm['cat4'])}",
            flush=True,
        )
        _cat_sleep()

        # ── Phase 3: category/3（最大 JAN ソース、5並列）──────────────
        # Referer を /nitiyouhin に設定してからスキャン開始
        print("[kaitorishouten] Phase 3: category/3", flush=True)
        self.session.headers.update({"Referer": BASE_URL + "/nitiyouhin"})
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._scan_category, f"/category/3/{cat_id}", results, lock, True): cat_id
                for cat_id in sm["cat3"]
            }
            for future in as_completed(futures):
                if future.exception():
                    print(
                        f"  [kaitorishouten] cat3/{futures[future]} 例外: {future.exception()}",
                        flush=True,
                    )

        # ── Phase 4: category/4（keitai サブ、5並列）─────────────────
        # with ブロックが完了してから Referer を変更するのでスレッドセーフ
        print("[kaitorishouten] Phase 4: category/4", flush=True)
        self.session.headers.update({"Referer": BASE_URL + "/keitai"})
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._scan_category, f"/category/4/{cat_id}", results, lock, True): cat_id
                for cat_id in sm["cat4"]
            }
            for future in as_completed(futures):
                if future.exception():
                    print(
                        f"  [kaitorishouten] cat4/{futures[future]} 例外: {future.exception()}",
                        flush=True,
                    )

        # category/2 (kaden) は AJAX kaden と同一セットのためスキャン不要
        print(f"[kaitorishouten] done: {len(results)} JANs", flush=True)
        return results
