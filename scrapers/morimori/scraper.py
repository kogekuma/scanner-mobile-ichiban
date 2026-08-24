"""森森買取（morimori-kaitori.jp）スクレイパー（bg-worker 版）

GitHub Actions 上で実行（VPS IP ブロックを回避するため）。

設計の要点（Scanner commit 8f89791 の刷新版を移植）:
  - グローバルレート制限: 全スレッド共有で任意の2リクエストの開始間隔を空ける。
  - 403/429 即中断: ブロック検知時は MorimoriBlockedError を投げて全体を止め、
    部分データで既存 morimori.json を上書きしない。
  - カテゴリ発見: sitemap.xml の 7桁カテゴリ全部 ∪ product URL 由来 ∪ 検証済み
    例外（SITEMAP_MISSING）。product URL の掲載漏れによる取りこぼしを防ぐ。
  - ページ分散走査: _scan_category_pages は page_start/page_step を取り、巨大
    カテゴリ（cat 99 や大型 leaf）をシャード間でページ単位に分散できる。

bg-worker 適合:
  - retry はタイムアウト窓（14分）に合わせ最大1回・12秒に短縮（Codex 助言）。
  - _load_known_nonempty_categories の DATA_PATH は bg-worker に存在しないため
    空集合フォールバックで動作する（優先走査は無効化されるが機能影響なし）。
"""

import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from scrapers.common import extract_jan, extract_price, merge_into_results
from scrapers.morimori.config import (
    BASE_URL,
    GLOBAL_MIN_INTERVAL_MAX,
    GLOBAL_MIN_INTERVAL_MIN,
    MAX_WORKERS,
    PAGE_SIZE,
    SITE_ID,
    SITE_NAME,
)


DATA_PATH = Path(__file__).resolve().parents[2] / "docs" / "data" / "morimori.json"
BLOCK_STATUS_CODES = {403, 429}

# sitemap に載らないことを実ページで検証済みの例外カテゴリ。
SITEMAP_MISSING = [
    "0101002",  # PS5ソフト
    "0104002",  # Switchソフト
    "0104003",  # Switch Lite / Switch関連
    "0108001",  # Xbox Series X/S 本体
    "0108003",  # Xbox Series X/S アクセサリ
    "0109001",  # Xbox One 本体
    "0109003",  # Xbox One アクセサリ
    "0113001",  # Xbox Series S 本体
    "0114",     # Meta Quest / VRヘッドセット
    "0115001",  # Steam Deck 本体
    "0301063",  # iPhone 17
    "0301066",  # iPhone 17 Pro Max
    "0301067",  # iPhone 17e
]

# 全商品集約カテゴリ。leaf 走査では別商品群（家電・その他）を含むため除外せず、
# 専用の cat99 ワークフローでページ単位に分散して回収する。
AGGREGATE_CATEGORY = "99"


class MorimoriBlockedError(RuntimeError):
    """403/429 検知時に部分データで上書きしないための中断例外。"""


class MorimoriScraper(BaseScraper):
    site_id = SITE_ID
    site_name = SITE_NAME

    def __init__(self):
        super().__init__()
        self._rate_lock = threading.Lock()
        self._last_request_at = 0.0
        self._abort_event = threading.Event()
        # 走査予算（time.monotonic() の絶対値）。None なら無制限。
        # 呼び出し側（run_morimori.py 等）が設定すると、ページループが予算超過時点で
        # 打ち切られ deadline_hit が立つ。ハードタイムアウトによる全損を防ぐための安全弁。
        self.deadline = None
        self.deadline_hit = False

    def past_deadline(self) -> bool:
        """走査予算を超過していれば True（超過を記録する）。"""
        if self.deadline is not None and time.monotonic() >= self.deadline:
            self.deadline_hit = True
            return True
        return False

    def _wait_global_rate_limit(self):
        """全スレッド共有で、任意の2リクエストの開始間隔を空ける。"""
        with self._rate_lock:
            now = time.time()
            if self._last_request_at:
                interval = random.uniform(GLOBAL_MIN_INTERVAL_MIN, GLOBAL_MIN_INTERVAL_MAX)
                wait = self._last_request_at + interval - now
                if wait > 0:
                    time.sleep(wait)
                    now = time.time()
            self._last_request_at = now

    def _request_once(self, url: str, **kwargs):
        if self._abort_event.is_set():
            raise MorimoriBlockedError("morimori scrape aborted")

        self._wait_global_rate_limit()
        resp = self.get(url, **kwargs)
        if resp.status_code in BLOCK_STATUS_CODES:
            self._abort_event.set()
            raise MorimoriBlockedError(f"morimori blocked: HTTP {resp.status_code} {url}")
        return resp

    def _get_with_retries(self, url: str, attempts: int = 2, **kwargs):
        """403/429 はリトライせず、接続エラー等は短く（12秒）リトライする。

        各シャードは 14 分のタイムアウト窓で動くため、長い固定待ち（30/60/90秒）は
        budget を食い潰す。Codex 助言に従い通常ページは最大1リトライ・12秒に抑える。
        sitemap のようにシャード全体の前提となるリクエストだけ attempts を増やす。
        """
        last_error = None
        for attempt in range(attempts):
            try:
                resp = self._request_once(url, **kwargs)
                resp.raise_for_status()
                return resp
            except MorimoriBlockedError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt == attempts - 1:
                    break
                print(
                    f"  [morimori] request failed ({attempt + 1}/{attempts}): {exc} -> wait 12s",
                    flush=True,
                )
                time.sleep(12)
        raise last_error

    def _discover_categories(self) -> list[str]:
        """sitemap から走査対象カテゴリを発見する。

        /category/{id} は 7桁カテゴリのみ採用し、短い親カテゴリは除外する。
        /category/{id}/product/{product_id} 由来のカテゴリと検証済み例外はそのまま採用する。

        sitemap の取得に失敗するとこのシャードは1件も取得できずに落ちる（実測: leaf-A の
        失敗 run の大半が ConnectTimeout でここで死んでいた）ため、ここだけリトライを
        4回に増やす。最悪でも +36 秒で 14 分窓を脅かさない。
        """
        resp = self._get_with_retries(BASE_URL + "/sitemap.xml", attempts=4)
        text = resp.text

        category_ids = set(re.findall(r"/category/(\d+)(?:[/?#<\s]|$)", text))
        seven_digit_ids = {cat for cat in category_ids if re.fullmatch(r"\d{7}", cat)}
        product_ids = set(re.findall(r"/category/(\d+)/product/\d+", text))
        missing_ids = set(SITEMAP_MISSING)

        ordered = []
        seen = set()
        for cat in sorted(seven_digit_ids | product_ids | missing_ids):
            if cat not in seen:
                seen.add(cat)
                ordered.append(cat)

        print(
            "  sitemap categories: "
            f"all={len(category_ids)}, seven_digit={len(seven_digit_ids)}, "
            f"product={len(product_ids)}, target={len(ordered)}",
            flush=True,
        )
        return ordered

    def _load_known_nonempty_categories(self) -> set[str]:
        """既存 morimori.json の URL から前回商品ありカテゴリを導出する（bg-worker では空）。"""
        if not DATA_PATH.exists():
            return set()
        try:
            data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  [morimori] existing data read skipped: {exc}", flush=True)
            return set()

        known = set()
        for item in data.get("items", {}).values():
            match = re.search(r"/category/(\d+)(?:/|$)", item.get("url", ""))
            if match:
                known.add(match.group(1))
        return known

    def _sort_categories(self, categories: list[str]) -> list[str]:
        known_nonempty = self._load_known_nonempty_categories()
        if known_nonempty:
            print(f"  known nonempty categories: {len(known_nonempty)}", flush=True)
        return sorted(categories, key=lambda cat: (cat not in known_nonempty, cat))

    def _parse_items(self, resp, cat_id: str, results: dict, lock: threading.Lock) -> int:
        """1ページの HTML から新品商品を抽出して results にマージし、商品数を返す。"""
        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select(".product-item")
        if not items:
            return 0

        for item in items:
            # status-new.svg バッジのある商品のみ採用（中古品は status-old.svg。
            # alt は新品・中古とも "new" のため src で判定する）。
            badge = item.select_one(".status-badge img")
            if not badge or "status-new" not in badge.get("src", ""):
                continue

            # JAN は "JAN:XXXX" 形式の h5（商品名 h5 は .product-details-name）
            jan_el = item.select_one(".product-details a h5:not(.product-details-name)")
            jan = (
                extract_jan(jan_el.get_text(strip=True).replace("JAN:", "").strip())
                if jan_el
                else None
            )
            if not jan:
                continue
            # morimori は EAN-13（先頭 0）を 12桁で表示するため補完する。
            if len(jan) == 12:
                jan = "0" + jan

            # 通常買取価格のみ採用する（特別・キャンペーン価格は含まない）。
            price_el = item.select_one(".price-normal-number h5")
            price = extract_price(price_el.get_text(strip=True)) if price_el else None
            if not price or price <= 0:
                continue

            url_el = item.select_one(".product-details a")
            url = (
                BASE_URL + url_el["href"]
                if url_el and url_el.get("href")
                else f"{BASE_URL}/category/{cat_id}"
            )
            name_el = item.select_one("h5.product-details-name")
            name = " ".join(name_el.get_text(separator=" ", strip=True).split()) if name_el else ""

            with lock:
                merge_into_results(results, jan, name, price, url)
        return len(items)

    def _get_last_page(self, cat_id: str) -> int:
        """カテゴリの最終ページ番号を返す（ページ1のページネーションリンクから）。

        morimori はページ1の HTML に最終ページへの ?page=N リンクを含むため、
        その最大値を最終ページとみなす。リンクが無ければ1ページ（単一ページ）。
        """
        resp = self._get_with_retries(f"{BASE_URL}/category/{cat_id}")
        pages = [int(m) for m in re.findall(r"[?&]page=(\d+)", resp.text)]
        return max(pages) if pages else 1

    def _scan_category_pages(
        self,
        cat_id: str,
        results: dict,
        lock: threading.Lock,
        page_start: int = 1,
        page_step: int = 1,
    ):
        """カテゴリのページを page_start から page_step 間隔で走査して results にマージする。

        page_step == 1（連続走査）: PAGE_SIZE 未満のページで最終ページと判定して止める。
        page_step > 1（シャード間ページ分散）: 先に最終ページ番号を確定し、その既知範囲
            だけを走査する。一時的に0件を返すページがあっても早期終了せず、そのページのみ
            スキップする（取りこぼしは次回以降の増分マージで回収される）。
            ※ 空ページで止めると、深いページが一時的に0件を返したときにカテゴリ後半を
              まるごと取りこぼすため、終端を事前確定する方式にしている。
        """
        if page_step == 1:
            page = page_start
            while not self._abort_event.is_set():
                if self.past_deadline():
                    print(
                        f"  [morimori] {cat_id} page={page} 以降を予算超過で打ち切り",
                        flush=True,
                    )
                    break
                params = {"page": page} if page > 1 else {}
                try:
                    resp = self._get_with_retries(f"{BASE_URL}/category/{cat_id}", params=params)
                except MorimoriBlockedError:
                    raise
                except Exception as exc:
                    print(f"  [morimori] {cat_id} page={page} skipped: {exc}", flush=True)
                    break

                n_items = self._parse_items(resp, cat_id, results, lock)
                if n_items == 0:
                    break
                print(f"  [morimori] {cat_id} page={page} ({n_items} items)", flush=True)
                if n_items < PAGE_SIZE:
                    break
                page += 1
            return

        # ストライド走査: 終端ページを先に確定してから既知範囲のみ走査する
        try:
            last_page = self._get_last_page(cat_id)
        except MorimoriBlockedError:
            raise
        except Exception as exc:
            print(f"  [morimori] {cat_id} last-page detection failed: {exc}", flush=True)
            return

        for page in range(page_start, last_page + 1, page_step):
            if self._abort_event.is_set():
                break
            if self.past_deadline():
                print(
                    f"  [morimori] {cat_id} page={page} 以降を予算超過で打ち切り",
                    flush=True,
                )
                break
            params = {"page": page} if page > 1 else {}
            try:
                resp = self._get_with_retries(f"{BASE_URL}/category/{cat_id}", params=params)
            except MorimoriBlockedError:
                raise
            except Exception as exc:
                print(f"  [morimori] {cat_id} page={page} skipped: {exc}", flush=True)
                continue

            n_items = self._parse_items(resp, cat_id, results, lock)
            print(f"  [morimori] {cat_id} page={page} ({n_items} items)", flush=True)

    def _scan_category(self, cat_id: str, results: dict, lock: threading.Lock):
        """単一カテゴリの全ページを連続走査して results にマージする。"""
        self._scan_category_pages(cat_id, results, lock, page_start=1, page_step=1)

    def scrape(self) -> dict:
        """カテゴリを発見し、既存データで商品ありカテゴリを優先してスキャンする（単体実行用）。"""
        results: dict = {}
        lock = threading.Lock()

        print("[morimori] discovering categories from sitemap", flush=True)
        categories = self._sort_categories(self._discover_categories())
        print(f"  target categories: {len(categories)}", flush=True)

        first_block_error = None
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._scan_category, cat_id, results, lock): cat_id
                for cat_id in categories
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except MorimoriBlockedError as exc:
                    first_block_error = exc
                    self._abort_event.set()
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                except Exception as exc:
                    cat_id = futures[future]
                    print(f"  [morimori] {cat_id} error: {exc}", flush=True)

        if first_block_error:
            raise first_block_error

        print(f"[morimori] done: {len(results)} JANs", flush=True)
        return results
