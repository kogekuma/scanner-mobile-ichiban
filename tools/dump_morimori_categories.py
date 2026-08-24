"""morimori のカテゴリ一覧スナップショットを再生成する。

`scrapers/morimori/categories_fallback.json` は sitemap.xml が取れなかったときの
フォールバック（_discover_categories が使う）。抽出規則は _discover_categories と
同じにしてあるので、規則を変えたらこちらも合わせて更新すること。

使い方:
    python tools/dump_morimori_categories.py
"""

import json
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.morimori import MorimoriScraper                      # noqa: E402
from scrapers.morimori.config import BASE_URL                      # noqa: E402
from scrapers.morimori.scraper import SITEMAP_MISSING              # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "scrapers" / "morimori" / "categories_fallback.json"


def main():
    scraper = MorimoriScraper()
    text = scraper._get_with_retries(BASE_URL + "/sitemap.xml", attempts=4, backoff=20).text

    category_ids = set(re.findall(r"/category/(\d+)(?:[/?#<\s]|$)", text))
    seven_digit_ids = {c for c in category_ids if re.fullmatch(r"\d{7}", c)}
    product_ids = set(re.findall(r"/category/(\d+)/product/\d+", text))
    ordered = sorted(seven_digit_ids | product_ids | set(SITEMAP_MISSING))

    OUT.write_text(
        json.dumps(
            {
                "_comment": "sitemap.xml が取れなかったときのフォールバック用カテゴリ一覧。"
                            "_discover_categories と同じ抽出規則で生成する"
                            "（tools/dump_morimori_categories.py）。",
                "generated_at": date.today().isoformat(),
                "count": len(ordered),
                "categories": ordered,
            },
            ensure_ascii=False,
            indent=1,
        ),
        encoding="utf-8",
    )
    print(f"{OUT} に {len(ordered)} 件を書き出しました")


if __name__ == "__main__":
    main()
