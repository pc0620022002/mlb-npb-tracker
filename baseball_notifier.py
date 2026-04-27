#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseball Player Notification Bot - MLB / 3A / NPB"""

import re, requests, json, os, sys, signal
from datetime import datetime, date, timedelta, timezone

# 單次 main() 執行最大時間(秒)。launchd 每 5 分鐘觸發一次,
# 設 240s(4 分鐘)留 60 秒緩衝給下一次正常排程。
# 超時會強制 sys.exit(1),已 dedup 的 push 不會丟(寫入 state 是逐筆 append,
# 但因為 main 結尾才 save_state,timeout 會丟掉本次未 save 的 dedup key →
# 下次 run 可能重發某些通知,但比起 launchd 排程卡死整晚是更小的代價)。
MAX_RUNTIME_SECONDS = 240

def _abort_on_timeout(signum, frame):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ main() exceeded {MAX_RUNTIME_SECONDS}s, aborting to keep launchd schedule alive")
    sys.exit(1)

TOKEN = os.environ.get("TG_TOKEN", "")
CHAT_ID = os.environ.get("TG_CHAT_ID", "")

# (name, player_id, mlb_org_team_id, origin)
# origin: "jp"=日本 / "kr"=韓國 / "tw"=台灣 / "us"=美國(自選 MLB 球星)
# 3A 推播規則:只有 origin 在 AAA_PUSH_ORIGINS 集合內的球員會在 3A 比賽被推播;
#   其他國籍球員(jp/kr/us)只推播 MLB,但若日後升上 MLB,MLB 段會自動覆蓋到。
# MLB org IDs: 108=LAA, 109=ARI, 111=BOS, 112=CHC, 114=CLE, 115=COL,
#   116=DET, 117=HOU, 119=LAD, 120=WSH, 121=NYM, 134=PIT,
#   135=SD, 137=SF, 141=TOR, 144=ATL, 145=CHW
MLB_PLAYERS = [
    ("Shohei Ohtani", 660271, 119, "jp"),       # Dodgers
    ("Yoshinobu Yamamoto", 808967, 119, "jp"),   # Dodgers
    ("Roki Sasaki", None, 119, "jp"),            # Dodgers
    ("Yu Darvish", 506433, 135, "jp"),           # Padres
    ("Yuki Matsui", None, 135, "jp"),            # Padres
    ("Yusei Kikuchi", 579328, 108, "jp"),        # Angels
    ("Tatsuya Imai", 837227, 117, "jp"),         # Astros
    ("Seiya Suzuki", 673548, 112, "jp"),         # Cubs
    ("Shota Imanaga", None, 112, "jp"),          # Cubs
    ("Kodai Senga", None, 121, "jp"),            # Mets
    ("Masataka Yoshida", None, 111, "jp"),       # Red Sox
    ("Tomoyuki Sugano", None, 115, "jp"),        # Rockies
    ("Munetaka Murakami", 808959, 145, "jp"),    # White Sox
    ("Kazuma Okamoto", 672960, 141, "jp"),       # Blue Jays
    ("Shinnosuke Ogasawara", None, 120, "jp"),   # Nationals
    ("Jung Hoo Lee", 808982, 137, "kr"),         # Giants
    ("Ha-Seong Kim", 673490, 144, "kr"),         # Braves
    ("Hyeseong Kim", None, 119, "kr"),           # Dodgers
    ("Ji Hwan Bae", 678225, 121, "kr"),          # Mets
    ("Go Woo-Suk", None, 116, "kr"),             # Tigers
    ("Sung-Mun Song", 823550, 135, "kr"),        # Padres
    ("Kai-Wei Teng", None, 117, "tw"),           # Astros
    ("Hao-Yu Lee", 701678, 116, "tw"),           # Tigers
    ("Po-Yu Chen", None, 134, "tw"),             # Pirates
    ("Yu-Min Lin", None, 109, "tw"),             # Diamondbacks
    ("Tsung-Che Cheng", None, 111, "tw"),        # Red Sox
    ("Jonathon Long", None, 112, "us"),          # Cubs
    ("Stuart Fairchild", None, 114, "us"),       # Guardians
]

# 哪些 origin 的球員在 3A 比賽要推播。其他國籍只推播 MLB。
AAA_PUSH_ORIGINS = {"tw"}

# 自動偵測排除名單:即使 birthCountry 符合 jp/kr/tw,也不要納入動態追蹤。
# 用途:有些日裔/韓裔球員是在亞洲出生但生涯都在美國,跟使用者關心的「旅美亞洲球員」概念不同。
# 這個 set 只影響 discover_asian_players(),不影響 hardcoded MLB_PLAYERS。
EXCLUDE_FROM_DISCOVERY = {
    608701,  # Rob Refsnyder(韓裔 MLB,在韓國出生但生涯都在美國體系,使用者不追)
}

# NPB Taiwanese players: Chinese name -> {search patterns on Yahoo Japan, team info}
# Names appear as kanji with space on Yahoo Japan stats pages (e.g. "林 安可")
NPB_PLAYERS_INFO = {
    "林安可": {"search": ["林 安可"], "team": "西武", "team_full": "埼玉西武ライオンズ"},
    "古林睿煬": {"search": ["古林 睿煬"], "team": "日本ハム", "team_full": "北海道日本ハムファイターズ"},
    "宋家豪": {"search": ["宋 家豪"], "team": "楽天", "team_full": "東北楽天ゴールデンイーグルス"},
    "徐若熙": {"search": ["徐 若熙"], "team": "ソフトバンク", "team_full": "福岡ソフトバンクホークス"},
    "林家正": {"search": ["林 家正"], "team": "日本ハム", "team_full": "北海道日本ハムファイターズ"},
    "孫易磊": {"search": ["孫 易磊"], "team": "日本ハム", "team_full": "北海道日本ハムファイターズ"},
    "林冠臣": {"search": ["林 冠臣"], "team": "西武", "team_full": "埼玉西武ライオンズ"},
    "張峻瑋": {"search": ["張 峻瑋"], "team": "ソフトバンク", "team_full": "福岡ソフトバンクホークス"},
    "陽柏翔": {"search": ["陽 柏翔"], "team": "楽天", "team_full": "東北楽天ゴールデンイーグルス"},
    "徐翔聖": {"search": ["徐 翔聖"], "team": "ヤクルト", "team_full": "東京ヤクルトスワローズ"},
    "黃錦豪": {"search": ["黃 錦豪", "黄 錦豪"], "team": "巨人", "team_full": "読売ジャイアンツ"},
    "陳睦衡": {"search": ["陳 睦衡"], "team": "オリックス", "team_full": "オリックス・バファローズ"},
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Use persistent volume path if available (Railway Volume), else fall back to script dir
_state_dir = os.environ.get("STATE_DIR", SCRIPT_DIR)
STATE_FILE = os.path.join(_state_dir, "state.json")
TW = timezone(timedelta(hours=8))

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(state):
    today = date.today()
    cleaned = {k: v for k, v in state.items()
               if any(k.startswith(p) for p in ["mlb_","aaa_","npb_"]) is False
               or _recent(k, today)}
    with open(STATE_FILE, "w") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=2)

def _recent(k, today):
    parts = k.split("_")
    if len(parts) >= 2:
        try:
            return (today - date.fromisoformat(parts[1])).days <= 7
        except Exception:
            pass
    return True

def send_tg(msg):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10)
        if not r.ok:
            log(f"TG err: {r.status_code} {r.text[:80]}")
        return r.ok
    except Exception as e:
        log(f"TG err: {e}")
        return False

def to_tw(iso):
    if not iso: return "未知"
    try:
        dt = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(TW)
        return dt.strftime("%m/%d %H:%M (台灣時間)")
    except Exception:
        return iso

def _day_prefix(iso):
    # 依台灣時間判斷比賽是今天還是明天(以後),用來決定文字寫「今日」還是「明日」
    if not iso: return "今日"
    try:
        dt = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(TW)
        delta_days = (dt.date() - datetime.now(TW).date()).days
        return "明日" if delta_days >= 1 else "今日"
    except Exception:
        return "今日"

def is_match(pid, fname, players):
    # Normalize whitespace (MLB API sometimes returns double spaces e.g. "Hao-Yu  Lee")
    fn = re.sub(r"\s+", " ", (fname or "").lower()).strip()
    for entry in players:
        name, mid = entry[0], entry[1]
        if mid and pid:
            try:
                if int(pid) == int(mid): return name
            except Exception: pass
        nl = re.sub(r"\s+", " ", name.lower()).strip()
        if nl in fn or fn in nl: return name
    return None

# --- MLB/AAA event translation for per-at-bat results ---
_EVENT_TW = {
    "Single": "一安", "Double": "二安", "Triple": "三安", "Home Run": "全壘打",
    "Walk": "四球", "Intent Walk": "故意四球", "Hit By Pitch": "觸身球",
    "Strikeout": "三振", "Strikeout Double Play": "三振雙殺",
    "Groundout": "滾地出局", "Bunt Groundout": "短打出局", "Bunt Pop Out": "短打內飛出局",
    "Flyout": "飛球出局", "Pop Out": "內野飛球", "Lineout": "平飛出局",
    "Forceout": "封殺", "Fielders Choice": "野手選擇", "Fielders Choice Out": "野選出局",
    "Grounded Into DP": "滾地雙殺", "Double Play": "雙殺",
    "Sac Fly": "高飛犧牲打", "Sac Fly Double Play": "高飛犧牲雙殺",
    "Sac Bunt": "犧牲短打", "Sac Bunt Double Play": "犧牲短打雙殺",
    "Field Error": "失誤上壘", "Catcher Interference": "捕手妨礙",
    "Batter Interference": "打者妨礙", "Fan Interference": "觀眾妨礙",
    "Triple Play": "三殺",
}

def _translate_event(event):
    return _EVENT_TW.get(event, event)

def _get_mlb_at_bats(gp, pid):
    """Fetch per-at-bat results for a player via playByPlay. Returns list of (event_zh, rbi) tuples."""
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/game/{gp}/playByPlay", timeout=12)
        if not r.ok:
            return []
        plays = r.json().get("allPlays", [])
        target = int(pid)
        out = []
        for play in plays:
            batter_id = play.get("matchup", {}).get("batter", {}).get("id")
            if batter_id != target:
                continue
            # Only include completed plate appearances (has result.event).
            result = play.get("result", {})
            event = result.get("event")
            if not event:
                continue
            rbi = result.get("rbi", 0)
            out.append((_translate_event(event), rbi))
        return out
    except Exception as e:
        log(f"PBP err {gp}/{pid}: {e}")
        return []

def _fmt_at_bats(ab_list):
    """Format a list of AB results as numbered lines. Each item is (event, rbi) tuple."""
    if not ab_list:
        return ""
    lines = []
    for i, item in enumerate(ab_list):
        if isinstance(item, tuple):
            ev, rbi = item
            if rbi and rbi > 0:
                lines.append(f"  {i+1}. {ev} ({rbi}打點)")
            else:
                lines.append(f"  {i+1}. {ev}")
        else:
            # Backward compat: plain string (NPB)
            lines.append(f"  {i+1}. {item}")
    return "📝 每打席：\n" + "\n".join(lines)

def _get_season_stats(pid):
    """Fetch current season batting avg and pitching ERA via MLB people stats endpoint (fallback).
    Returns dict with 'avg' and 'era' keys (either may be None)."""
    result = {"avg": None, "era": None}
    try:
        year = date.today().year
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{pid}/stats",
            params={"stats": "season", "season": year, "group": "hitting,pitching"},
            timeout=10)
        if r.ok:
            for stat_group in r.json().get("stats", []):
                splits = stat_group.get("splits", [])
                if splits:
                    stat = splits[0].get("stat", {})
                    if "avg" in stat:
                        result["avg"] = stat["avg"]
                    if "era" in stat:
                        result["era"] = stat["era"]
    except Exception as e:
        log(f"Season stats err {pid}: {e}")
    return result

def _fmt_pitcher_line(pit, season_era=None):
    """Detailed pitcher stat line: IP R/ER K BB H HR (ERA)."""
    ip = pit.get("inningsPitched", "")
    if not ip or ip == "0.0":
        return ""
    r = pit.get("runs", 0)
    er = pit.get("earnedRuns", 0)
    k = pit.get("strikeOuts", 0)
    bb = pit.get("baseOnBalls", 0)
    h = pit.get("hits", 0)
    hr = pit.get("homeRuns", 0)
    line = f"\u26be {ip}局 {r}失分({er}自責) {k}K {bb}BB {h}被安 {hr}HR"
    # Prefer season ERA passed in; fall back to game-level era field
    era = season_era or pit.get("era", "")
    if era and era not in ("-.--", "-"):
        line += f" (ERA {era})"
    return line

def _fmt_batter_stats(bat, season_avg=None):
    """Short batter stat body: AB打數H安打 HR RBI BB K ... (打率.xxx) (no emoji prefix)."""
    ab = bat.get("atBats", 0)
    h = bat.get("hits", 0)
    hr = bat.get("homeRuns", 0)
    rbi = bat.get("rbi", 0)
    bb = bat.get("baseOnBalls", 0)
    k = bat.get("strikeOuts", 0)
    r = bat.get("runs", 0)
    sb = bat.get("stolenBases", 0)
    parts = [f"{ab}打數{h}安打"]
    if hr > 0: parts.append(f"{hr}HR")
    if rbi > 0: parts.append(f"{rbi}打點")
    if r > 0: parts.append(f"{r}得分")
    if bb > 0: parts.append(f"{bb}BB")
    if k > 0: parts.append(f"{k}K")
    if sb > 0: parts.append(f"{sb}盜")
    line = " ".join(parts)
    if season_avg and season_avg not in ("-", ".---"):
        line += f" (打率{season_avg})"
    return line

def check_schedule(sport_id, prefix, label, state, players=None):
    if players is None:
        players = MLB_PLAYERS
    notifs = []
    today = date.today()
    yesterday = today - timedelta(days=1)
    dates_to_check = [today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")]
    all_games_data = []
    for check_date in dates_to_check:
        try:
            r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": sport_id, "date": check_date,
                        "hydrate": "lineups,team,probablePitcher,score"}, timeout=15)
            r.raise_for_status()
            data = r.json()
            for de in data.get("dates", []):
                for g in de.get("games", []):
                    all_games_data.append((check_date, g))
        except Exception as e:
            log(f"{label} API error for {check_date}: {e}")

    for game_date_str, g in all_games_data:
            gp = g.get("gamePk", 0)
            abst = g.get("status",{}).get("abstractGameState","")
            home = g.get("teams",{}).get("home",{}).get("team",{}).get("name","")
            away = g.get("teams",{}).get("away",{}).get("team",{}).get("name","")
            hs = g.get("teams",{}).get("home",{}).get("score",0)
            aws = g.get("teams",{}).get("away",{}).get("score",0)
            gt = g.get("gameDate","")
            matchup = f"{away} vs {home}"

            lineups = g.get("lineups",{})
            for side in ["homePlayers","awayPlayers"]:
                for pl in lineups.get(side,[]):
                    pid = pl.get("id"); fn = pl.get("fullName","")
                    m = is_match(pid, fn, players)
                    if m:
                        k = f"{prefix}_{game_date_str}_lineup_{gp}_{pid}"
                        if k not in state:
                            notifs.append(f"\u26be <b>[{label} 先發名單]</b>\n\U0001f3df {matchup}\n\U0001f464 <b>{m}</b> 已列入先發名單！\n\U0001f554 {to_tw(gt)}")
                            state[k] = True

            for side in ["home","away"]:
                p = g.get("teams",{}).get(side,{}).get("probablePitcher",{})
                pid = p.get("id"); fn = p.get("fullName","")
                if pid:
                    m = is_match(pid, fn, players)
                    if m:
                        k = f"{prefix}_{game_date_str}_pitcher_{gp}_{pid}"
                        if k not in state:
                            notifs.append(f"\u26be <b>[{label} 預定先發投手]</b>\n\U0001f3df {matchup}\n\u26be <b>{m}</b> {_day_prefix(gt)}預定先發！\n\U0001f554 {to_tw(gt)}")
                            state[k] = True

            # --- Mid-game appearance AND Final results (combined) ---
            if abst in ("Live", "Final"):
                try:
                    br = requests.get(f"https://statsapi.mlb.com/api/v1/game/{gp}/boxscore", timeout=12).json()
                    for bside in ["home","away"]:
                        pls = br.get("teams",{}).get(bside,{}).get("players",{})
                        for pk, pv in pls.items():
                            pid_str = pk.replace("ID","")
                            fn = pv.get("person",{}).get("fullName","")
                            m = is_match(pid_str, fn, players)
                            if m:
                                stats = pv.get("stats",{})
                                bat = stats.get("batting",{}); pit = stats.get("pitching",{})
                                ab = bat.get("atBats", 0)
                                ip = pit.get("inningsPitched", "")
                                has_bat = ab > 0 or bat.get("runs",0) > 0 or bat.get("baseOnBalls",0) > 0 or bat.get("hitByPitch",0) > 0 or bat.get("sacFlies",0) > 0 or bat.get("sacBunts",0) > 0
                                has_pit = ip and ip != "0.0"
                                # Detect if player actually entered the game (including defensive subs)
                                played = has_bat or has_pit or bool(pv.get("allPositions"))

                                # --- Live update notification (sends on EVERY stat change) ---
                                if played and abst == "Live":
                                    live_key = f"{prefix}_{game_date_str}_live_{gp}_{pid_str}"
                                    # Build current stats snapshot for comparison
                                    snap_parts = []
                                    if has_bat:
                                        snap_parts.append(f"B:{ab}-{bat.get('hits',0)}-{bat.get('homeRuns',0)}-{bat.get('rbi',0)}-{bat.get('baseOnBalls',0)}-{bat.get('strikeOuts',0)}")
                                    if has_pit:
                                        snap_parts.append(f"P:{ip}-{pit.get('runs',0)}-{pit.get('earnedRuns',0)}-{pit.get('strikeOuts',0)}-{pit.get('baseOnBalls',0)}-{pit.get('hits',0)}-{pit.get('homeRuns',0)}")
                                    if not snap_parts:
                                        snap_parts.append("entered")
                                    current_snap = "|".join(snap_parts)
                                    prev_snap = state.get(live_key)

                                    if prev_snap != current_snap:
                                        lines = []
                                        # Get season stats from boxscore; fallback to API
                                        season_avg = pv.get("seasonStats", {}).get("batting", {}).get("avg")
                                        season_era = pv.get("seasonStats", {}).get("pitching", {}).get("era")
                                        if (has_bat and (not season_avg or season_avg in ("-", ".---"))) or \
                                           (has_pit and (not season_era or season_era in ("-", "-.--"))):
                                            fb = _get_season_stats(pid_str)
                                            if not season_avg or season_avg in ("-", ".---"):
                                                season_avg = fb.get("avg")
                                            if not season_era or season_era in ("-", "-.--"):
                                                season_era = fb.get("era")
                                        if has_bat:
                                            lines.append("\U0001f3cf 目前：" + _fmt_batter_stats(bat, season_avg))
                                            ab_list = _get_mlb_at_bats(gp, pid_str)
                                            ab_block = _fmt_at_bats(ab_list)
                                            if ab_block:
                                                lines.append(ab_block)
                                        if has_pit:
                                            pl = _fmt_pitcher_line(pit, season_era)
                                            if pl:
                                                lines.append(pl)
                                        score_line = f"{away} {aws} - {hs} {home}"
                                        stat_str = "\n".join(lines) if lines else "已進入比賽"
                                        tag = "賽中更新" if prev_snap else "賽中出場"
                                        notifs.append(f"\u26be <b>[{label} {tag}]</b>\n\U0001f3df {matchup}\n\U0001f4ca {score_line}\n\U0001f464 <b>{m}</b> 已上場！\n{stat_str}")
                                        state[live_key] = current_snap

                                # --- First appearance for games already Final (no live updates sent) ---
                                if played and abst == "Final":
                                    live_key = f"{prefix}_{game_date_str}_live_{gp}_{pid_str}"
                                    if live_key not in state:
                                        state[live_key] = "final_only"

                                # --- Final result notification (only when game is finished) ---
                                if abst == "Final" and played:
                                    final_key = f"{prefix}_{game_date_str}_final_{gp}_{pid_str}"
                                    if final_key not in state:
                                        lines = []
                                        # Get season stats from boxscore; fallback to API
                                        season_avg = pv.get("seasonStats", {}).get("batting", {}).get("avg")
                                        season_era = pv.get("seasonStats", {}).get("pitching", {}).get("era")
                                        if (has_bat and (not season_avg or season_avg in ("-", ".---"))) or \
                                           (has_pit and (not season_era or season_era in ("-", "-.--"))):
                                            fb = _get_season_stats(pid_str)
                                            if not season_avg or season_avg in ("-", ".---"):
                                                season_avg = fb.get("avg")
                                            if not season_era or season_era in ("-", "-.--"):
                                                season_era = fb.get("era")
                                        if has_bat:
                                            if ab > 0:
                                                lines.append("\U0001f3cf " + _fmt_batter_stats(bat, season_avg))
                                            else:
                                                # 0 AB but appeared (walks, HBP, sac, pinch-run)
                                                parts = []
                                                if bat.get("baseOnBalls",0) > 0: parts.append(f"{bat['baseOnBalls']}BB")
                                                if bat.get("hitByPitch",0) > 0: parts.append(f"{bat['hitByPitch']}觸身")
                                                if bat.get("runs",0) > 0: parts.append(f"{bat['runs']}得分")
                                                if bat.get("stolenBases",0) > 0: parts.append(f"{bat['stolenBases']}盜")
                                                if bat.get("sacFlies",0) > 0: parts.append(f"{bat['sacFlies']}高飛犧牲打")
                                                if bat.get("sacBunts",0) > 0: parts.append(f"{bat['sacBunts']}犧牲短打")
                                                if parts:
                                                    zero_ab_line = "\U0001f3cf " + " ".join(parts)
                                                    if season_avg and season_avg not in ("-", ".---"):
                                                        zero_ab_line += f" (打率{season_avg})"
                                                    lines.append(zero_ab_line)
                                            ab_list = _get_mlb_at_bats(gp, pid_str)
                                            ab_block = _fmt_at_bats(ab_list)
                                            if ab_block:
                                                lines.append(ab_block)
                                        if ip and ip != "0.0":
                                            pl = _fmt_pitcher_line(pit, season_era)
                                            if pl:
                                                lines.append(pl)
                                            if pit.get("wins"): lines.append("\u2705 勝投")
                                            if pit.get("losses"): lines.append("\u274c 敗投")
                                            if pit.get("saves"): lines.append("\U0001f512 救援成功")
                                        if not lines:
                                            lines.append("出場（無打擊/投球紀錄）")
                                        stat_str = "\n".join(lines)
                                        notifs.append(f"\u26be <b>[{label} 比賽結果]</b>\n\U0001f3df {matchup}\n\U0001f4ca {away} <b>{aws}</b> - <b>{hs}</b> {home}\n\U0001f464 <b>{m}</b>：\n{stat_str}")
                                        state[final_key] = True
                except Exception as e:
                    log(f"Boxscore err {gp}: {e}")
    return notifs

def _fetch_yahoo(url, timeout=8):
    """Fetch Yahoo Japan page with proper headers.
    timeout 預設 8 秒(2026-04-28 從 15 秒降下,避免 schedule 頁列出的未來一週 game 一個個 fetch
    時遇到 Yahoo Japan 慢就累積拖垮整個 main()。配合 NPB_BUDGET_SECONDS 用)。"""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        log(f"Fetch err {url}: {e}")
    return None

def _extract_npb_batting(html, search_patterns):
    """Extract batting stats for a player from Yahoo Japan stats HTML"""
    for pattern in search_patterns:
        escaped = re.escape(pattern)
        # Match: name link -> batting avg -> at_bats -> runs -> hits -> rbi -> strikeouts
        # Yahoo Japan HTML has whitespace between </a> and </td>
        bat_re = escaped + r'</a>\s*</td>\s*<td[^>]*>([.\d-]+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>'
        m = re.search(bat_re, html, re.DOTALL)
        if m:
            avg, ab, runs, hits, rbi, so = m.groups()
            if int(ab) > 0:
                return f"\U0001f3cf {ab}打數{hits}安打 {runs}得分 {rbi}打點 {so}三振 (打率{avg})"
            elif int(runs) > 0 or int(hits) > 0 or int(rbi) > 0 or int(so) > 0:
                # 0 AB but has other non-zero stats → actually appeared (walk/HBP)
                return "\U0001f3cf 0打數 (四球/觸身上壘)"
            # else: all stats are zero → player listed in lineup but hasn't batted yet
    return ""

def _extract_npb_rbi_total(html, search_patterns):
    """從打擊 row 取出該打者的總打點數,用來推算每打席打點分配。"""
    for pattern in search_patterns:
        escaped = re.escape(pattern)
        bat_re = escaped + r'</a>\s*</td>\s*<td[^>]*>([.\d-]+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>\s*<td[^>]*>(\d+)</td>'
        m = re.search(bat_re, html, re.DOTALL)
        if m:
            try:
                return int(m.group(5))
            except (ValueError, IndexError):
                return 0
    return 0

def _extract_npb_pitching(html, search_patterns):
    """Extract pitching stats for a player from Yahoo Japan stats HTML.
    Columns after name: 防御率 投球回 投球数 打者 被安打 被本塁打 奪三振 与四球 与死球 ボーク 失点 自責点
    Returns a detailed stat line with IP, runs, ER, K, BB, hits, HR, and season ERA.
    """
    _v = r'<td[^>]*>\s*(?:<p[^>]*>)?\s*([.\d-]+)\s*(?:</p>)?\s*</td>'
    for pattern in search_patterns:
        escaped = re.escape(pattern)
        pit_re = escaped + r'</a>\s*</td>\s*' + r'\s*'.join([_v] * 12)
        m = re.search(pit_re, html, re.DOTALL)
        if m:
            era, ip, pitches, batters, bh, bhr, k, bb, hbp, balk, runs, er = m.groups()
            if ip != "0" and ip != "0.0":
                line = f"\u26be {ip}局 {runs}失分({er}自責) {k}K {bb}BB {bh}被安 {bhr}HR"
                if era and era not in ("-", "-.--"):
                    line += f" (ERA {era})"
                return line
    return ""

def _extract_npb_at_bats(html, search_patterns):
    """Extract per-inning at-bat results for a batter from Yahoo Japan stats HTML.
    Each batter row has 12 stat cells + 9 inning cells (innings 1-9), where inning cells
    may contain <div class="bb-statsTable__dataDetail">result</div> text like:
        右安 (right single), 左2 (left double), 中本 (center HR),
        空三振 (swinging K), 見三振 (called K), 四球, 投ゴロ, ... etc.
    打點打席會額外加上 class bb-statsTable__dataDetail--point。
    Returns a list of (result_str, has_point: bool) tuples in order.
    """
    for pattern in search_patterns:
        idx = html.find(pattern)
        if idx < 0:
            continue
        row_end = html.find("</tr>", idx)
        if row_end < 0:
            continue
        row = html[idx:row_end]
        tds = re.findall(r'<td[^>]*>([\s\S]*?)</td>', row)
        # After name link cell, 12 stat cells + 9 inning cells. Take last 9.
        inning_cells = tds[-9:] if len(tds) >= 21 else tds[12:]
        results = []
        for cell in inning_cells:
            m = re.search(r'<div class="(bb-statsTable__dataDetail[^"]*)">([^<]+)</div>', cell)
            if m:
                classes, text = m.group(1), m.group(2).strip()
                has_point = "--point" in classes
                results.append((text, has_point))
        if results:
            return results
    return []

def _extract_npb_lineup(html, search_patterns):
    """
    Extract starting lineup info from Yahoo Japan /top page HTML.
    The lineup table uses <td class="bb-splitsTable__data"> cells like:
      <td>3</td><td>指</td><td>...<a>林 安可</a>...</td>       (batter)
      <td>先発</td><td>投</td><td>...<a>徐 若熙</a>...</td>    (pitcher)
    Returns "pitcher" / "batter" if player is in the lineup, else None.
    """
    for pattern in search_patterns:
        if pattern not in html:
            continue
        escaped = re.escape(pattern)
        # Grab the two preceding <td> cells (batting order/role indicator).
        # Values are inside <td class="bb-splitsTable__data">.
        row_re = (
            r'<td[^>]*bb-splitsTable__data[^>]*>\s*([^<]+?)\s*</td>\s*'
            r'<td[^>]*bb-splitsTable__data[^>]*>\s*([^<]+?)\s*</td>\s*'
            r'<td[^>]*bb-splitsTable__data[^>]*>\s*(?:<[^>]+>\s*)*<a[^>]*>'
            + escaped + r'</a>'
        )
        m = re.search(row_re, html, re.DOTALL)
        if not m:
            continue
        order, pos = m.group(1).strip(), m.group(2).strip()
        if "先発" in order or pos == "投":
            return "pitcher"
        if order.isdigit():
            return "batter"
        # Fallback: present in lineup table but unclassified → treat as batter
        return "batter"
    return None

def _check_npb_league(state, league, league_label):
    """Check NPB games (1軍 or 2軍) for Taiwanese player lineups and appearances"""
    notifs = []
    # NPB 段最多花 90 秒(2026-04-28 加,避免 Yahoo Japan 半夜慢時拖垮整個 main)。
    # schedule 頁面會列出未來一週的 game,每個 game 都要 fetch /top 才知道日期,
    # 34 場 × 8s timeout 最壞 270s,所以這裡用 budget bail 而非 timeout 累積。
    NPB_BUDGET_SECONDS = 90
    npb_start_ts = datetime.now(timezone.utc).timestamp()
    # Use JST (UTC+9) for Japan date
    jst = timezone(timedelta(hours=9))
    now_jst = datetime.now(jst)
    date_str = now_jst.strftime("%Y-%m-%d")

    # Fetch schedule page
    schedule_url = f"https://baseball.yahoo.co.jp/npb/schedule/{league}/all?date={date_str}"
    schedule_html = _fetch_yahoo(schedule_url)
    if not schedule_html:
        log(f"NPB {league_label}: failed to fetch schedule")
        return notifs

    # Extract game IDs from links like /npb/game/2021038671/
    game_ids = list(set(re.findall(r'/npb/game/(\d+)/', schedule_html)))
    if not game_ids:
        log(f"NPB {league_label}: no games found for {date_str}")
        return notifs

    log(f"NPB {league_label}: found {len(game_ids)} games for {date_str}")

    # Debug: show existing NPB state keys
    npb_keys = [k for k in state if k.startswith("npb_")]
    if npb_keys:
        log(f"NPB state keys: {npb_keys}")

    games_with_stats = 0
    games_today = 0
    games_with_lineup_checked = 0
    players_found_total = 0
    bailed_due_to_budget = False

    for game_id in game_ids:
        # 預算保護:NPB 段最多花 NPB_BUDGET_SECONDS 秒,超過就停止處理剩下的 game,
        # 讓 MLB / 3A 段已經做完的推播能正常 save_state,且不會撞到 main 的 240s SIGALRM
        elapsed_npb = datetime.now(timezone.utc).timestamp() - npb_start_ts
        if elapsed_npb > NPB_BUDGET_SECONDS:
            log(f"NPB {league_label}: budget {NPB_BUDGET_SECONDS}s exceeded after {games_today} today/{len(game_ids)} total games — bailing rest")
            bailed_due_to_budget = True
            break
        # Fetch /top page for reliable game status (score/finished/lineup)
        top_url = f"https://baseball.yahoo.co.jp/npb/game/{game_id}/top"
        top_html = _fetch_yahoo(top_url)
        if not top_html:
            continue

        # Extract game title from <title> tag and verify date
        title_match = re.search(r'<title>([^<]+)</title>', top_html)
        if not title_match:
            log(f"  game {game_id}: no <title> tag found, skipping")
            continue
        raw_title = title_match.group(1)
        # Verify this game is from today (mandatory check - skip if date not found)
        date_in_title = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', raw_title)
        if not date_in_title:
            log(f"  game {game_id}: no date in title, skipping")
            continue
        game_date = f"{date_in_title.group(1)}-{int(date_in_title.group(2)):02d}-{int(date_in_title.group(3)):02d}"
        if game_date != date_str:
            log(f"  game {game_id}: date {game_date} != today {date_str}, skipping")
            continue
        game_title = raw_title.replace(" - プロ野球 - スポーツナビ", "").strip()
        game_title = re.sub(r'\s*試合(出場成績|速報)\s*', '', game_title)
        game_title = re.sub(r'^\d{4}年\d{1,2}月\d{1,2}日\s*', '', game_title)

        games_today += 1

        # RELIABLE status detection from /top page:
        # - "<span>試合終了</span>" only appears when game is actually finished
        # - "スターティングメンバー" <h2> appears once the lineup is published
        # NOTE: do NOT look for "試合終了" or "打撃成績" substring alone - they appear in
        # JS comments / layout headers regardless of game state.
        is_finished = "<span>試合終了</span>" in top_html
        has_lineup = '<h2 class="bb-head01__title">スターティングメンバー</h2>' in top_html

        log(f"  game {game_id}: {game_title} finished={is_finished} has_lineup={has_lineup}")

        # Fetch stats page (contains per-player batting/pitching tables)
        stats_url = f"https://baseball.yahoo.co.jp/npb/game/{game_id}/stats"
        stats_html = _fetch_yahoo(stats_url)
        if not stats_html:
            continue

        # Iterate over tracked players
        for player_name, pinfo in NPB_PLAYERS_INFO.items():
            # Extract actual stats (empty if all-zero pre-populated rows or absent)
            bat_stats = _extract_npb_batting(stats_html, pinfo["search"])
            pit_stats = _extract_npb_pitching(stats_html, pinfo["search"])
            stat_line = bat_stats or pit_stats or ""

            if stat_line:
                # Player has actual non-zero stats — mid-game or final
                players_found_total += 1
                log(f"    FOUND with stats: {player_name} in game {game_id} -> {stat_line}")

                # For batters, append per-AB results extracted from the same page
                full_stat = stat_line
                if bat_stats:
                    ab_results = _extract_npb_at_bats(stats_html, pinfo["search"])
                    if ab_results:
                        # Yahoo Japan 只在 cell 標 boolean「該打席有打點」,沒提供具體數字。
                        # 用「總 RBI + 有打點打席數」推算每打席打點數。
                        rbi_total = _extract_npb_rbi_total(stats_html, pinfo["search"])
                        point_count = sum(1 for _, has_pt in ab_results if has_pt)
                        ab_lines = []
                        for i, (ev, has_pt) in enumerate(ab_results):
                            if has_pt and rbi_total > 0:
                                if point_count == 1:
                                    suffix = f" ({rbi_total}打點)"
                                elif point_count == rbi_total:
                                    suffix = " (1打點)"
                                else:
                                    suffix = " (有打點)"
                            else:
                                suffix = ""
                            ab_lines.append(f"  {i+1}. {ev}{suffix}")
                        full_stat = f"{stat_line}\n\U0001f4dd 每打席：\n" + "\n".join(ab_lines)

                if not is_finished:
                    # --- MID-GAME: live update whenever stat line changes ---
                    live_key = f"npb_{date_str}_live_{game_id}_{player_name}"
                    prev_snap = state.get(live_key)
                    if prev_snap != full_stat:
                        tag = "賽中更新" if prev_snap else "賽中出場"
                        msg = f"\u26be <b>[NPB {league_label} {tag}]</b>\n"
                        if game_title:
                            msg += f"\U0001f3df {game_title}\n"
                        msg += f"\U0001f464 <b>{player_name}</b> ({pinfo['team']}) 已上場！\n"
                        msg += f"{full_stat}\n"
                        msg += f"\U0001f517 https://baseball.yahoo.co.jp/npb/game/{game_id}/stats"
                        notifs.append(msg)
                        state[live_key] = full_stat
                        log(f"NPB {league_label}: {player_name} {'UPDATE' if prev_snap else 'APPEAR'} in game {game_id}")
                    else:
                        log(f"    SKIP (stats unchanged): {live_key}")
                else:
                    # --- FINAL: post-game result, once per player per game ---
                    final_key = f"npb_{date_str}_final_{game_id}_{player_name}"
                    if final_key not in state:
                        msg = f"\U0001f4ca <b>[NPB {league_label} 比賽結果]</b>\n"
                        if game_title:
                            msg += f"\U0001f3df {game_title}\n"
                        msg += f"\U0001f464 <b>{player_name}</b> ({pinfo['team']}) 出場！\n"
                        msg += f"{full_stat}\n"
                        msg += f"\U0001f517 https://baseball.yahoo.co.jp/npb/game/{game_id}/stats"
                        notifs.append(msg)
                        state[final_key] = True
                        log(f"NPB {league_label}: {player_name} FINAL in game {game_id}")
                    else:
                        log(f"    SKIP (final key exists): {final_key}")
            elif not is_finished and has_lineup:
                # --- PRE-GAME: player listed in starting lineup ---
                games_with_lineup_checked += 1
                lineup_type = _extract_npb_lineup(top_html, pinfo["search"])
                if lineup_type:
                    lineup_key = f"npb_{date_str}_lineup_{game_id}_{player_name}"
                    if lineup_key not in state:
                        msg = f"\u26be <b>[NPB {league_label} 先發名單]</b>\n"
                        if game_title:
                            msg += f"\U0001f3df {game_title}\n"
                        msg += f"\U0001f464 <b>{player_name}</b> ({pinfo['team']}) 列入先發！\n"
                        msg += f"\U0001f517 https://baseball.yahoo.co.jp/npb/game/{game_id}/top"
                        notifs.append(msg)
                        state[lineup_key] = True
                        log(f"NPB {league_label}: {player_name} in LINEUP for game {game_id}")
                    else:
                        log(f"    SKIP (lineup key exists): {lineup_key}")

    npb_total_secs = int(datetime.now(timezone.utc).timestamp() - npb_start_ts)
    bail_note = " [⚠️ BAILED ON BUDGET]" if bailed_due_to_budget else ""
    log(f"NPB {league_label} summary: {games_with_stats} with stats, {games_today} today, {games_with_lineup_checked} checked for lineup, {players_found_total} player appearances, {npb_total_secs}s elapsed{bail_note}")

    return notifs

# --- 自動偵測亞洲球員 ---
# 規則:每天跑一次,掃 MLB(sport=1)所有 jp/kr/tw 球員 + 3A(sport=11)所有 tw 球員,
# 自動加進 state["_dynamic_players"]。Hardcoded MLB_PLAYERS 同 pid 不會被覆蓋。
_BIRTH_COUNTRY_TO_ORIGIN = {"Japan": "jp", "Republic of Korea": "kr", "Taiwan": "tw"}
_DISCOVERY_BY_SPORT = {1: {"jp", "kr", "tw"}, 11: {"tw"}}
_DISCOVERY_INTERVAL_SECONDS = 86400  # 24 小時

def _norm_name(s):
    """跟 is_match() 一樣的 normalize:小寫 + 多空白合併 + 前後 trim。"""
    return re.sub(r"\s+", " ", (s or "").lower()).strip()

def discover_asian_players(state):
    """每天首次執行時跑一次,把符合條件的球員加進 state['_dynamic_players']。
    結構:{ pid_str: {"name": str, "org": int(MLB org id), "origin": "jp|kr|tw"}, ... }"""
    now_ts = datetime.now(timezone.utc).timestamp()
    if now_ts - state.get("_last_discovery_ts", 0) < _DISCOVERY_INTERVAL_SECONDS:
        return
    log("Running discovery scan for new Asian players...")
    discovered = state.get("_dynamic_players", {})
    # 用 pid 跟 normalize 過的名字兩條路擋,因為 hardcoded MLB_PLAYERS 的 pid 可能是 None(用 fuzzy name match)
    hardcoded_pids = {p[1] for p in MLB_PLAYERS if p[1]}
    hardcoded_names = {_norm_name(p[0]) for p in MLB_PLAYERS}
    new_count = 0

    # 預抓 AAA 隊伍 → 母 MLB org id 對照(3A 球員 currentTeam.id 是 3A 隊 id,要轉成母球團)
    aaa_to_parent = {}
    try:
        rt = requests.get("https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 11, "season": date.today().year}, timeout=10)
        if rt.ok:
            for t in rt.json().get("teams", []):
                p = t.get("parentOrgId")
                if p:
                    aaa_to_parent[t["id"]] = p
    except Exception as e:
        log(f"discovery: AAA team mapping err: {e}")

    for sport_id, allowed_origins in _DISCOVERY_BY_SPORT.items():
        try:
            r = requests.get(f"https://statsapi.mlb.com/api/v1/sports/{sport_id}/players",
                params={"season": date.today().year}, timeout=20)
            if not r.ok:
                continue
            for p in r.json().get("people", []):
                origin = _BIRTH_COUNTRY_TO_ORIGIN.get(p.get("birthCountry", ""))
                if not origin or origin not in allowed_origins:
                    continue
                pid = p.get("id")
                if not pid or pid in hardcoded_pids:
                    continue
                if pid in EXCLUDE_FROM_DISCOVERY:
                    continue  # 永久排除名單
                if _norm_name(p.get("fullName", "")) in hardcoded_names:
                    continue  # hardcoded 用 fuzzy name 比對的(pid 為 None),也擋掉
                pid_str = str(pid)
                if pid_str in discovered:
                    continue  # 已 discover 過,不重複加(team 可能換,但 origin 不變)
                team_id = p.get("currentTeam", {}).get("id")
                org = aaa_to_parent.get(team_id, team_id) if sport_id == 11 else team_id
                if not org:
                    continue
                discovered[pid_str] = {
                    "name": p.get("fullName", ""),
                    "org": org,
                    "origin": origin,
                }
                new_count += 1
                log(f"  discovered: {p.get('fullName')} (id={pid}, origin={origin}, org={org}, sport={sport_id})")
        except Exception as e:
            log(f"discovery err sport={sport_id}: {e}")

    state["_dynamic_players"] = discovered
    state["_last_discovery_ts"] = now_ts
    log(f"discovery done. dynamic pool size: {len(discovered)} (added {new_count} new)")


def get_all_tracked_players(state):
    """合併 hardcoded MLB_PLAYERS + 動態名單。Hardcoded 優先(同 pid 不會重複)。"""
    hardcoded_pids = {p[1] for p in MLB_PLAYERS if p[1]}
    out = list(MLB_PLAYERS)
    for pid_str, info in state.get("_dynamic_players", {}).items():
        try:
            pid = int(pid_str)
        except (ValueError, TypeError):
            continue
        if pid in hardcoded_pids:
            continue
        out.append((info.get("name", ""), pid, info.get("org"), info.get("origin")))
    return out


def _tracked_teams_have_games(state):
    """Check if any tracked player's TEAM has a game scheduled today.
    This ensures we detect substitute players who haven't appeared yet."""
    today_str = date.today().strftime("%Y-%m-%d")
    all_players = get_all_tracked_players(state)
    tracked_mlb_orgs = set(p[2] for p in all_players if p[2])
    # 3A 只看會被推播的國籍(預設台灣);其他國籍球員的 3A 比賽不再列入追蹤
    tracked_aaa_orgs = set(p[2] for p in all_players if p[3] in AAA_PUSH_ORIGINS and p[2])

    # --- MLB (sportId=1): team IDs match org IDs directly ---
    try:
        r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": today_str}, timeout=10)
        if r.ok:
            for de in r.json().get("dates", []):
                for g in de.get("games", []):
                    for side in ["home", "away"]:
                        tid = g.get("teams", {}).get(side, {}).get("team", {}).get("id")
                        if tid in tracked_mlb_orgs:
                            log(f"MLB team playing today: ID {tid}")
                            return True
    except Exception as e:
        log(f"MLB schedule check err: {e}")

    # --- AAA (sportId=11): resolve parent org via teams API ---
    try:
        aaa_to_parent = {}
        rt = requests.get("https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 11, "season": today_str[:4]}, timeout=10)
        if rt.ok:
            for t in rt.json().get("teams", []):
                p = t.get("parentOrgId")
                if p:
                    aaa_to_parent[t["id"]] = p

        r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 11, "date": today_str}, timeout=10)
        if r.ok:
            for de in r.json().get("dates", []):
                for g in de.get("games", []):
                    for side in ["home", "away"]:
                        tid = g.get("teams", {}).get(side, {}).get("team", {}).get("id")
                        parent = aaa_to_parent.get(tid)
                        if parent and parent in tracked_aaa_orgs:
                            log(f"AAA team playing today: ID {tid} (parent org {parent})")
                            return True
    except Exception as e:
        log(f"AAA schedule check err: {e}")

    # --- NPB: check Yahoo Japan schedule for tracked team names ---
    jst = timezone(timedelta(hours=9))
    jst_date_str = datetime.now(jst).strftime("%Y-%m-%d")
    npb_tracked_teams = set(info["team"] for info in NPB_PLAYERS_INFO.values())
    try:
        html = _fetch_yahoo(f"https://baseball.yahoo.co.jp/npb/schedule/first/all?date={jst_date_str}")
        if html:
            for team in npb_tracked_teams:
                if team in html:
                    log(f"NPB team playing today: {team}")
                    return True
    except Exception as e:
        log(f"NPB schedule check err: {e}")

    return False

def check_npb(state):
    """Check NPB 1軍 games for Taiwanese player appearances"""
    return _check_npb_league(state, "first", "1軍")

def check_npb_farm(state):
    """Check NPB 2軍 (farm) games for Taiwanese player appearances"""
    return _check_npb_league(state, "farm", "2軍")

def main():
    log("=" * 40)
    log("Baseball Notifier start")
    log(f"State file: {STATE_FILE}")
    # 設定整個 main() 最大執行時間,避免 NPB Yahoo Japan timeout 累積拖垮 launchd 排程
    signal.signal(signal.SIGALRM, _abort_on_timeout)
    signal.alarm(MAX_RUNTIME_SECONDS)
    state = load_state()

    # One-time cleanup: clear stale NPB keys from old broken detection logic.
    # Bumps: _npb_fix_v2 (all-zero stats fix), _npb_fix_v3 (real status detection from /top)
    if not state.get("_npb_fix_v3"):
        jst = timezone(timedelta(hours=9))
        today_jst = datetime.now(jst).strftime("%Y-%m-%d")
        stale_keys = [k for k in list(state.keys()) if k.startswith(f"npb_{today_jst}")]
        for k in stale_keys:
            del state[k]
        state["_npb_fix_v2"] = True
        state["_npb_fix_v3"] = True
        save_state(state)
        if stale_keys:
            log(f"Cleared {len(stale_keys)} stale NPB state keys for {today_jst} (fix v3)")

    # 自動偵測亞洲球員(每天跑一次,結果寫進 state["_dynamic_players"])
    # 必須在 _tracked_teams_have_games 之前,因為動態名單會影響「今天哪些隊在打」的判斷
    discover_asian_players(state)

    last_run = state.get("_last_run_ts", 0)
    now_ts = datetime.now(timezone.utc).timestamp()
    elapsed = now_ts - last_run

    # Dynamic frequency: 1min when tracked players' TEAMS have games, 10min otherwise
    teams_playing = _tracked_teams_have_games(state)
    if teams_playing:
        # Tracked teams have games today - run every time (1min with cron)
        log(f"Tracked team(s) playing today, running (elapsed: {int(elapsed)}s)")
    elif elapsed < 600:
        # No tracked team games - only check every 10 minutes
        log(f"No tracked team games today, last run {int(elapsed)}s ago, skipping (10min interval)")
        save_state(state)  # 即使早返也要存,免得 discovery 結果丟失
        log("=" * 40)
        return
    else:
        log(f"No tracked team games today, {int(elapsed)}s elapsed, running discovery check")

    state["_last_run_ts"] = now_ts

    # 取合併名單(hardcoded MLB_PLAYERS + 動態 discovery 加的)
    all_tracked = get_all_tracked_players(state)
    log(f"Tracked player pool: {len(all_tracked)} ({len(MLB_PLAYERS)} hardcoded + {len(all_tracked) - len(MLB_PLAYERS)} dynamic)")

    all_notifs = []

    log("Checking MLB...")
    all_notifs += check_schedule(1, "mlb", "MLB", state, players=all_tracked)
    log(f"  -> {len(all_notifs)} MLB notifications")

    log("Checking Triple-A...")
    n = len(all_notifs)
    aaa_players = [p for p in all_tracked if p[3] in AAA_PUSH_ORIGINS]
    all_notifs += check_schedule(11, "aaa", "AAA 3A", state, players=aaa_players)
    log(f"  -> {len(all_notifs)-n} 3A notifications")

    log("Checking NPB 1軍...")
    n = len(all_notifs)
    all_notifs += check_npb(state)
    log(f"  -> {len(all_notifs)-n} NPB 1軍 notifications")

    sent = 0
    for msg in all_notifs:
        if send_tg(msg):
            sent += 1
            log(f"Sent: {msg[:50].replace(chr(10),' ')}...")
        else:
            log("Send failed")

    save_state(state)
    signal.alarm(0)  # 正常結束前關掉 timeout
    log(f"Done! Sent {sent}/{len(all_notifs)} notifications")
    log("=" * 40)

if __name__ == "__main__":
    if "--loop" in sys.argv:
        import time
        log("Starting in LOOP mode (60s interval)")
        while True:
            try:
                main()
            except Exception as e:
                log(f"Loop error: {e}")
            time.sleep(60)
    else:
        main()
