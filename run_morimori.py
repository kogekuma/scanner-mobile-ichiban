"""森森買取 leaf カテゴリ走査（シャード対応・cat99 除外）。

--shard N --total-shards M で leaf カテゴリを M 分割して担当分のみ取得する。
全商品集約カテゴリ cat99 は別ワークフロー run_morimori_cat99.py が担当するため
ここでは除外する。

シャード設計（Phase0 計測: 商品ありページの応答中央値 ~7.5秒）:
  - 通常カテゴリ: カテゴリ単位で stride 分割（cats[shard::total]）。多くは1ページ。
  - 大カテゴリ（BIG_CATEGORIES, 12ページ超）: 1シャードに偏ると14分窓を超えるため、
    ページ単位で全シャードに分散（shard k は page k+1, k+1+total, ...）。
  - SITEMAP_MISSING: discovery が必ず含めるため通常/大カテゴリ側で自然にカバーされる。

  leaf 全体は約2708ページ（商品はほぼ全て新品・複数カテゴリに重複掲載）ある。
  サーバは高並列に耐えられない（20シャード×並列で過負荷になり全滅する実測あり）ため、
  シャード内は直列（1接続）に保つ。総ページを減らすには重複掲載の集約カテゴリ除外や
  シャード数の調整で対応する。

結果を morimori_shard_{N}.json として出力する（merge_morimori.py が増分マージ）。
"""

import argparse
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta

from scrapers.morimori import MorimoriScraper
from scrapers.morimori.scraper import AGGREGATE_CATEGORY

JST = timezone(timedelta(hours=9))

# leaf から除外するカテゴリ。
#   99 = 全商品集約（別ワークフロー run_morimori_cat99.py が担当）
#   05 = 冗長な集約カテゴリ（196ページ。サンプル新品100%が7桁leafでカバー済み＝除外可）
#   06 / 03 / 24 = 同じ「短縮ID＝親カテゴリ」の集約ページ（2026-08-24 追加）。
#     sitemap の /category/{id}/product/{pid} 由来で短縮IDが混入し、担当シャードが
#     14分窓を丸ごと食い潰していた（実測: 06 は180ページ超で shard 32 が毎回タイムアウト、
#     直近100 run すべて未完＝その担当カテゴリが恒久的に未走査だった）。
#     実測で冗長性を確認済み: 06=50件 / 03=34件 の標本すべてが 7桁leaf 経由で収録済み、
#     24=18件は 7桁leaf または 2403/2404（走査対象として維持）経由で収録済み。
#     ※ 短縮IDでも 0402/0505/0601/1801 等は1ページで固有商品を持つため除外しない。
EXCLUDE_FROM_LEAF = {AGGREGATE_CATEGORY, "05", "06", "03", "24"}

# 1シャードあたりの走査予算（分）。GitHub Actions の timeout-minutes(14) に達すると
# ジョブが強制終了され「そのシャードのデータが1件も出ない」ため、少し手前で自主的に
# 打ち切って部分結果を保存する。ハードタイムアウト（＝全損＋原因不明）を
# 「部分データ＋警告」に変える安全弁。
SHARD_BUDGET_MIN = float(os.environ.get("MORIMORI_SHARD_BUDGET_MIN", "11.5"))

# 12ページ超の大カテゴリ（2026-07-01 実ログ実測ベース。中古込みの実ページ数）。
# 1シャードに偏るとタイムアウトするため、ページ単位で全シャードに分散して負荷を均等化する。
# 新たに大きいカテゴリを見つけたらここに追加する（現行データの items/10 ではなく実ページ数で判断）。
BIG_CATEGORIES = {
    "0602001", "0611001", "0208003", "0605002", "0208005", "0606001",
    "0401006", "1501001", "0801001", "0801", "0610005", "0605003",
    "0206007", "0506010", "1401001", "0607003", "0801008", "0206018",
    "0610001", "0104002", "1401004", "0208012", "1401003", "0208004",
    "0104003", "0611002", "0605006", "0209001", "0203008", "0208015",
    "0207002", "0205", "0801010", "0610004", "0601003", "0206012",
    "0801005", "0607004", "0603001", "0506012", "0303001", "0610006",
    "0609001", "0605005", "0208001", "0206011", "1401002", "0206001",
    "1501002", "0801026", "0801019", "0605007", "0602009", "0505005",
    "0401002", "0204002", "1501004", "1001001", "0609002", "0304007",
    "0303002", "0204001",
}

parser = argparse.ArgumentParser()
parser.add_argument("--shard",        type=int, default=0, help="このジョブのシャード番号（0始まり）")
parser.add_argument("--total-shards", type=int, default=1, help="シャード総数")
args = parser.parse_args()

# 走査予算はプロセス開始から測る。スタガー待ち・sitemap リトライ（最悪2分）を
# 予算の外に置くと 57秒 + 2分 + 11.5分 = 14.4分 で結局ハードタイムアウトするため。
STARTED_AT = time.monotonic()

scraper = MorimoriScraper()

# 起動スタガー: 1ワークフローの20シャードが同時に /sitemap.xml を叩くと
# サーバ側が一瞬で詰まり、何本かが ConnectTimeout で即死する（実測: 失敗 run の
# ほぼ全てがこの sitemap 取得での死。シャードが丸ごと空振りする＝担当カテゴリが
# その回は更新されない）。シャード番号ぶん開始をずらして山を崩す。
# 1ワークフローは20シャードなので %20。既定3秒×19 = 最大57秒で14分窓に影響しない。
STAGGER_SEC = float(os.environ.get("MORIMORI_STAGGER_SEC", "3"))
stagger = (args.shard % 20) * STAGGER_SEC
if stagger:
    print(f"[morimori leaf shard {args.shard}] 起動を {stagger:.0f} 秒ずらします", flush=True)
    time.sleep(stagger)

# sitemap から全カテゴリを発見し、除外カテゴリ（cat99・冗長集約05）を外す。
# _discover_categories は SITEMAP_MISSING を必ず union するため、SITEMAP カテゴリも
# all_cats に含まれる。よって小さい SITEMAP カテゴリは通常 stride で、大きいもの
# （0104002/0104003 等）は BIG のページ分散で自然にカバーされる。
# （旧実装は SITEMAP を全シャードで別途フル走査しており、大 SITEMAP カテゴリを
#   各シャードで全走査＋BIGで二重走査してタイムアウトの主因になっていた）
all_cats   = [c for c in scraper._discover_categories() if c not in EXCLUDE_FROM_LEAF]
big_set    = {c for c in BIG_CATEGORIES if c in all_cats}

# 通常カテゴリ（大カテゴリ以外・小 SITEMAP を含む）を stride 分割
normal_cats = [c for c in all_cats if c not in big_set]
my_normal   = normal_cats[args.shard::args.total_shards]

print(
    f"[morimori leaf shard {args.shard}/{args.total_shards}] "
    f"通常 {len(my_normal)} / 大 {len(big_set)}(ページ分散) / 全カテゴリ {len(all_cats)}",
    flush=True,
)

results: dict = {}
lock = threading.Lock()

# 走査予算（ハードタイムアウトの手前で自主的に打ち切る）を scraper に渡す。
scraper.deadline = STARTED_AT + SHARD_BUDGET_MIN * 60
skipped: list[str] = []

# 直列走査（サーバは高並列に耐えられないため、シャード内は1接続に保つ。
# 並列化すると 20シャード×並列数の同時接続でサーバが過負荷になり全滅する）。
try:
    # 1) 通常カテゴリ（担当分のみ・連続走査。小 SITEMAP カテゴリもここに含まれる）
    for i, cat_id in enumerate(my_normal):
        if scraper.past_deadline():
            skipped.extend(my_normal[i:])
            break
        scraper._scan_category(cat_id, results, lock)

    # 2) 大カテゴリ（ページ単位で全シャードに分散。大 SITEMAP もここで処理される）
    big_sorted = sorted(big_set)
    for i, cat_id in enumerate(big_sorted):
        if scraper.past_deadline():
            skipped.extend(big_sorted[i:])
            break
        scraper._scan_category_pages(
            cat_id, results, lock,
            page_start=args.shard + 1, page_step=args.total_shards,
        )
except Exception as exc:
    # 403/429 ブロックや致命的エラー時は非ゼロ終了し、このシャードを失敗扱いにする。
    # → merge_morimori.py の増分マージが前回データを維持し、部分上書きを防ぐ。
    print(f"[morimori leaf shard {args.shard}] 中断: {exc}", flush=True)
    raise SystemExit(1)

partial = bool(skipped) or scraper.deadline_hit
if partial:
    # GitHub Actions の run サマリに出る形式で警告する（黙って部分データになるのを防ぐ）。
    # 恒久的に出続ける場合はカテゴリが大きくなりすぎている＝BIG_CATEGORIES 追加か
    # EXCLUDE_FROM_LEAF 追加でシャードの担当量を減らすこと。
    print(
        f"::warning::[morimori leaf shard {args.shard}] 走査予算 {SHARD_BUDGET_MIN} 分を超過。"
        f"未走査カテゴリ {len(skipped)} 件: {','.join(skipped[:20])}"
        f"{' ...' if len(skipped) > 20 else ''}",
        flush=True,
    )

print(
    f"[morimori leaf shard {args.shard}] "
    f"{'部分完了' if partial else '完了'}: {len(results)} JANs",
    flush=True,
)

output = {
    "updated": datetime.now(JST).strftime("%Y-%m-%d %H:%M JST"),
    "shard":   args.shard,
    "scope":   "leaf",
    "count":   len(results),
    "partial": partial,
    "items":   results,
}

filename = f"morimori_shard_{args.shard}.json"
with open(filename, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

print(f"→ {filename} に保存", flush=True)
