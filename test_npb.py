#!/usr/bin/env python3
"""Quick test: check yesterday's NPB games for Taiwanese players"""
import re, requests, json

NPB_PLAYERS_INFO = {
    "林安可": {"search": ["林 安可"], "team": "西武"},
    "古林睿煬": {"search": ["古林 睿煬"], "team": "日本ハム"},
    "宋家豪": {"search": ["宋 家豪"], "team": "楽天"},
    "徐若熙": {"search": ["徐 若熙"], "team": "ソフトバンク"},
    "林家正": {"search": ["林 家正"], "team": "日本ハム"},
    "孫易磊": {"search": ["孫 易磊"], "team": "日本ハム"},
    "林冠臣": {"search": ["林 冠臣"], "team": "西武"},
    "張峻瑋": {"search": ["張 峻瑋"], "team": "ソフトバンク"},
    "陽柏翔": {"search": ["陽 柏翔"], "team": "楽天"},
    "徐翔聖": {"search": ["徐 翔聖"], "team": "ヤクルト"},
    "黃錦豪": {"search": ["黃 錦豪", "黄 錦豪"], "team": "巨人"},
    "陳睦衡": {"search": ["陳 睦衡"], "team": "オリックス"},
}

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
test_date = "2026-04-05"  # Yesterday - had games

print(f"=== Testing NPB for {test_date} ===")

# Fetch schedule
url = f"https://baseball.yahoo.co.jp/npb/schedule/first/all?date={test_date}"
r = requests.get(url, headers=headers, timeout=15)
game_ids = list(set(re.findall(r'/npb/game/(\d+)/', r.text)))
print(f"Found {len(game_ids)} game IDs on schedule page")

found_players = []
for gid in game_ids:
    stats_url = f"https://baseball.yahoo.co.jp/npb/game/{gid}/stats"
    sr = requests.get(stats_url, headers=headers, timeout=15)
    if sr.status_code != 200:
        continue
    html = sr.text
    if "打撃成績" not in html:
        continue

    # Check date
    dm = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', html)
    if dm:
        gdate = f"{dm.group(1)}-{int(dm.group(2)):02d}-{int(dm.group(3)):02d}"
        if gdate != test_date:
            continue

    title_m = re.search(r'<title>([^<]+)</title>', html)
    title = title_m.group(1) if title_m else gid
    title = re.sub(r'^\d{4}年\d{1,2}月\d{1,2}日\s*', '', title).replace(" - プロ野球 - スポーツナビ","").replace(" 試合出場成績","")

    for pname, pinfo in NPB_PLAYERS_INFO.items():
        if any(sp in html for sp in pinfo["search"]):
            found_players.append(f"  {pname} ({pinfo['team']}) -> {title}")

print(f"\nPlayers found in yesterday's games:")
for p in found_players:
    print(p)

if not found_players:
    print("  (none found)")
print("\n=== Done ===")
