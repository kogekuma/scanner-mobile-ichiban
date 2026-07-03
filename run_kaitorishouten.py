"""買取商店スクレイパー実行スクリプト（fast / full モード対応）。

  --mode full … 全フェーズ（Phase 1/5/6/3/4）。完全スナップショット。3時間ごと。
  --mode fast … Phase 1(携帯AJAX＋家電/カメラAJAX)＋Phase 5(ゲーム機/ソフト/トレカ等)のみ。
                変動の速い商品だけ短時間で取得。15〜30分ごと。
  --base <path> … fast 時に前回 full の JSON を読み、その items に fast の items を上書き合成する
                  （category/3・4・6 等の非fast分は前回値を保持）。

結果を kaitorishouten.json として出力する。GitHub Actions から Scanner の docs/data/ に push。
失敗・0件・大幅縮小時は JSON を生成しない（＝前回データを保持）。
"""

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta

from scrapers.kaitorishouten import KaitorishoutenScraper

JST = timezone(timedelta(hours=9))

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["full", "fast"], default="full")
parser.add_argument("--base", help="fast 時に上書き合成する前回 kaitorishouten.json のパス")
args = parser.parse_args()

# 前回データ（縮小ガード・fast合成のベース）を読み込む
base_items: dict = {}
if args.base:
    try:
        with open(args.base, encoding="utf-8") as f:
            base_items = dict(json.load(f).get("items", {}))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[kaitorishouten] base 読み込み失敗: {e}", flush=True)

try:
    data = KaitorishoutenScraper().scrape(mode=args.mode)
except Exception as e:
    print(f"[kaitorishouten] スクレイプ中断（前回データ保持）: {e}", flush=True)
    sys.exit(0)

if not data:
    print("[kaitorishouten] 0件のため更新しません（前回データ保持）", flush=True)
    sys.exit(0)

# fast: 前回 full の items に fast の items を上書き合成（他カテゴリは前回値を保持）
if args.mode == "fast":
    if not base_items:
        print("[kaitorishouten] fast だが base が無いため出力しません（部分データで上書きしない）", flush=True)
        sys.exit(0)
    merged = dict(base_items)
    merged.update(data)  # fast の JAN で上書き、それ以外は保持（update は縮小しない）
    print(f"[kaitorishouten] fast合成: 前回 {len(base_items)} + fast {len(data)} → {len(merged)}", flush=True)
    data = merged

# 縮小ガード: 前回比 70% 未満なら異常とみなし更新しない（部分取得で全体を痩せさせない）
if base_items and len(data) < len(base_items) * 0.7:
    print(f"[kaitorishouten] 件数が前回の70%未満（{len(data)}/{len(base_items)}）のため更新しません", flush=True)
    sys.exit(0)

output = {
    "updated": datetime.now(JST).strftime("%Y-%m-%d %H:%M JST"),
    "count": len(data),
    "items": data,
}

with open("kaitorishouten.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

print(f"[kaitorishouten] {args.mode} 完了: {len(data)} JANs → kaitorishouten.json", flush=True)
