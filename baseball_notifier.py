#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Baseball Player Notification Bot - MLB / 3A / NPB"""

import re, requests, json, os, sys, signal, threading
from datetime import datetime, date, timedelta, timezone

# 單次 main() 執行最大時間(秒)。launchd 每 5 分鐘觸發一次,
# 設 240s(4 分鐘)留 60 秒緩衝給下一次正常排程。
# 雙保險(2026-04-28):
# (1) signal.SIGALRM + os._exit:Python signal handler 在「卡在 C 層 socket I/O」時可能不會觸發
# (2) threading.Timer daemon watchdog:不依賴 Python 解譯器,時間到直接 os._exit 強殺
# 兩道機制同時設,只要其中一道生效就能讓 launchd 準時排下一輪。
# 代價:超時會丟掉本次未 save_state 的 dedup key → 下次 run 可能重發少數通知,
# 比起 launchd 排程卡死整晚是更小的代價。
MAX_RUNTIME_SECONDS = 240
WATCHDOG_SECONDS = 250  # daemon watchdog,比 SIGALRM 晚 10 秒當作後備

def _abort_on_timeout(signum, frame):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ main() exceeded {MAX_RUNTIME_SECONDS}s (SIGALRM), os._exit(1)")
    sys.stdout.flush()
    os._exit(1)

def _watchdog_kill():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ watchdog {WATCHDOG_SECONDS}s exceeded, os._exit(2) — SIGALRM was swallowed (likely C-level socket hang)")
    sys.stdout.flush()
    os._exit(2)

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
AAA_PUSH_ORIGINS = {"tw", "us"}

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
    """Atomic write:先寫 .tmp 再 os.replace 到目標檔。
    避免 cancel/timeout 期間寫到一半 → state.json 被 corrupt → 下次 load 失敗。
    os.replace() 在 POSIX 上是原子操作,不會看到中間狀態。"""
    today = date.today()
    cleaned = {k: v for k, v in state.items()
               if any(k.startswith(p) for p in ["mlb_","aaa_","npb_"]) is False
               or _recent(k, today)}
    tmp_path = STATE_FILE + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATE_FILE)
    except Exception as e:
        log(f"save_state err: {e}")
        # 清掉殘留 tmp 檔避免下次混淆
        try:
            os.remove(tmp_path)
        except Exception:
            pass

def _recent(k, today):
    parts = k.split("_")
    if len(parts) >= 2:
        try:
            return (today - date.fromisoformat(parts[1])).days <= 7
        except Exception:
            pass
    return True

def send_tg(msg):
    """推送 Telegram 訊息。connection / timeout / 5xx / 429 自動 backoff retry 2 次(0.5s, 2s)。
    4xx 不重試(訊息格式錯 / token 失效之類,重試也沒用)。
    避免 Telegram API 偶發抖動造成「明明事件對但訊息丟掉」的隱性漏推。"""
    import time as _time
    last_err = None
    for attempt in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=10)
            if r.ok:
                return True
            if r.status_code >= 500 or r.status_code == 429:
                last_err = f"HTTP {r.status_code}"
            else:
                # 4xx(400 訊息格式 / 401 token / 403 chat 等)重試無用
                log(f"TG err (no retry): {r.status_code} {r.text[:80]}")
                return False
        except Exception as e:
            last_err = type(e).__name__ + ": " + str(e)[:120]
        if attempt < 2:
            sleep_s = (0.5, 2.0)[attempt]
            log(f"  TG retry {attempt+1}/2 in {sleep_s}s ({last_err})")
            _time.sleep(sleep_s)
    log(f"TG send FAILED after 3 attempts: {last_err}")
    return False

def _send_alert(msg, state=None):
    """系統層級警告(失聯/漏推/心跳)。**失敗時若有 state 就 enqueue 到 _pending_send**,
    確保警告跟一般推播訊息一樣不會被 Telegram 暫時壞掉吃掉(F2 同類保險原來只 cover 球員推播)。
    state 為 None 時退回原本「失敗就丟」行為(極早期 bootstrap 場景)。"""
    if send_tg(msg):
        return True
    if state is not None:
        state.setdefault("_pending_send", []).append({
            "msg": msg,
            "ts": datetime.now(timezone.utc).timestamp(),
            "attempts": 1,
        })
        log("Alert send failed → enqueued to _pending_send for next-run retry")
    else:
        log("Alert send failed (no state for enqueue, message lost)")
    return False

def _maybe_send_daily_heartbeat(state):
    """每天台灣時間 9:00 後若還沒推今天心跳 → 推一條「✅ 系統運作正常」。
    用途:讓 user 主動知道系統活著。**沒收到心跳 = 系統壞掉**,從被動發現變主動偵測。
    這是整個系統最後一道 meta 保險,所有其他保險都靠 GHA log 偵錯,只有心跳會主動告知 user。"""
    tw = timezone(timedelta(hours=8))
    now_tw = datetime.now(tw)
    today_str = now_tw.strftime("%Y-%m-%d")
    if now_tw.hour < 9:
        return
    if state.get("_heartbeat_last_date") == today_str:
        return
    last_ok = state.get("_last_ok_ts", 0)
    last_ok_min = int((datetime.now(timezone.utc).timestamp() - last_ok) / 60) if last_ok else -1
    pending_count = len(state.get("_pending_send", []))
    msg = (
        f"✅ <b>Baseball Notifier 心跳</b> {now_tw.strftime('%Y-%m-%d %H:%M')}\n"
        f"系統運作正常,上次成功跑 {last_ok_min} 分鐘前。\n"
        f"待重發訊息: {pending_count} 條。\n"
        f"<i>每天此時間推一條。某天沒收到 = 系統可能壞掉,請查 GitHub Actions。</i>"
    )
    if _send_alert(msg, state):
        state["_heartbeat_last_date"] = today_str
        log(f"Daily heartbeat sent for {today_str}")
    # heartbeat 失敗:state["_heartbeat_last_date"] 不更新 → 下輪會再試;訊息也已 enqueue

def _robust_get(url, retries=2, backoff_seq=(0.5, 2.0), **kwargs):
    """requests.get 包裝,connection / timeout / 5xx 自動 backoff retry。
    4xx 不重試(client 端錯誤,重試也沒用)。
    返回 Response 或 None(用盡 retries)。"""
    import time as _time
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, **kwargs)
            if r.status_code == 200:
                return r
            if r.status_code >= 500:
                last_err = f"HTTP {r.status_code}"
            else:
                # 4xx → 不重試
                return r
        except Exception as e:
            last_err = type(e).__name__ + ": " + str(e)[:120]
        if attempt < retries:
            sleep_s = backoff_seq[min(attempt, len(backoff_seq)-1)]
            log(f"  retry {attempt+1}/{retries} in {sleep_s}s ({last_err}): {url[:80]}")
            _time.sleep(sleep_s)
    log(f"  FAILED after {retries+1} attempts ({last_err}): {url[:80]}")
    return None

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
        r = _robust_get(f"https://statsapi.mlb.com/api/v1/game/{gp}/playByPlay", timeout=12)
        if r is None or not r.ok:
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
        r = _robust_get(
            f"https://statsapi.mlb.com/api/v1/people/{pid}/stats",
            params={"stats": "season", "season": year, "group": "hitting,pitching"},
            timeout=10)
        if r and r.ok:
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
            r = _robust_get("https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": sport_id, "date": check_date,
                        "hydrate": "lineups,team,probablePitcher,linescore"}, timeout=15)
            if r is None or not r.ok:
                continue  # _robust_get 已經 retry 過,放棄這個 date 進下一輪
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
            # 比分優先從 linescore.teams.{home,away}.runs 拿(canonical 即時比分),
            # fallback 到 teams.{home,away}.score,兩個都拿不到顯示 "?"。
            # 原因:hydrate=score 對 Live 狀態 AAA game 會 missing(實測 Reno Aces 04-29 場
            # team.score=null) → 推播訊息顯示 0-0 比分錯。f91e39c 用 linescore 修了主路徑,
            # 但 fallback 仍 `.get("score", 0)` — Python `.get(key, default)` 只在 key 缺失時用
            # default,**key 存在但 value=None 時仍回 None**;若兩個來源都 None 訊息會印 "None - None"。
            # 2026-04-30 audit:統一兩段都用 "value is None → 繼續 fallback" 處理,最後 None 顯示 "?"。
            ls_teams = (g.get("linescore") or {}).get("teams", {})
            hs = ls_teams.get("home", {}).get("runs")
            if hs is None:
                hs = g.get("teams", {}).get("home", {}).get("score")
            if hs is None:
                hs = "?"
            aws = ls_teams.get("away", {}).get("runs")
            if aws is None:
                aws = g.get("teams", {}).get("away", {}).get("score")
            if aws is None:
                aws = "?"
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
                    _br_r = _robust_get(f"https://statsapi.mlb.com/api/v1/game/{gp}/boxscore", timeout=12)
                    br = _br_r.json() if _br_r and _br_r.ok else {}
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
                                # 「final_only」標記代表中段沒推過 live → 比賽結果訊息會額外加漏推警示
                                live_key = f"{prefix}_{game_date_str}_live_{gp}_{pid_str}"
                                if played and abst == "Final":
                                    if live_key not in state:
                                        state[live_key] = "final_only"

                                # --- Final result notification (only when game is finished) ---
                                if abst == "Final" and played:
                                    final_key = f"{prefix}_{game_date_str}_final_{gp}_{pid_str}"
                                    # 漏推偵測:中段從未推 live → 補一條警示前綴
                                    is_missed_live = state.get(live_key) == "final_only"
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
                                        if is_missed_live:
                                            header = (f"\u26a0\ufe0f <b>[{label} 比賽結果 — 系統補推]</b>\n"
                                                      f"<i>此球員從未在 live 階段被推播,中段過程可能因系統空白漏掉</i>\n"
                                                      f"\u26be 完整本場紀錄:")
                                        else:
                                            header = f"\u26be <b>[{label} 比賽結果]</b>"
                                        notifs.append(f"{header}\n\U0001f3df {matchup}\n\U0001f4ca {away} <b>{aws}</b> - <b>{hs}</b> {home}\n\U0001f464 <b>{m}</b>：\n{stat_str}")
                                        state[final_key] = True
                except Exception as e:
                    log(f"Boxscore err {gp}: {e}")
    return notifs

def _fetch_yahoo(url, timeout=(5, 8)):
    """Fetch Yahoo Japan page with proper headers + backoff retry.
    timeout 用 tuple `(connect, read)`,connect 5s + read 8s。
    transient 失敗(connection error / timeout / 5xx)會 retry 2 次(0.5s, 2s)。
    最終卡死的最後保險是 main() 的 watchdog daemon thread(os._exit)。"""
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    r = _robust_get(url, headers=headers, timeout=timeout)
    if r is not None and r.status_code == 200:
        return r.text
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
        log(f"NPB {league_label}: failed to fetch schedule (整段抓不到)")
        # 推 TG 警告(每 6 小時最多推 1 條,避免 NPB 持續壞時 spam)
        last_alert_ts = state.get(f"_npb_schedule_alert_ts_{league}", 0)
        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts - last_alert_ts > 21600:  # 6 小時
            _send_alert(f"\u26a0\ufe0f <b>[NPB schedule 整段抓不到]</b>\n"
                        f"{league_label} schedule_url 連續 retry 後仍失敗。\n"
                        f"<i>NPB 監控可能整段失效,請檢查 Yahoo Japan 是否被 ban / 改版</i>", state)
            state[f"_npb_schedule_alert_ts_{league}"] = now_ts
        return notifs

    # Extract game IDs from links like /npb/game/2021038671/
    game_ids = list(set(re.findall(r'/npb/game/(\d+)/', schedule_html)))
    if not game_ids:
        log(f"NPB {league_label}: no games found for {date_str} (schedule 抓到但 0 game,可能 Yahoo 改版)")
        # 跟 schedule 抓不到一樣推警告(可能是 HTML 結構改了)
        last_alert_ts = state.get(f"_npb_zero_games_alert_ts_{league}", 0)
        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts - last_alert_ts > 21600:
            _send_alert(f"\u26a0\ufe0f <b>[NPB schedule 解析 0 game]</b>\n"
                        f"{league_label} schedule HTML 抓到但 regex 找不到 game ID。\n"
                        f"<i>可能 Yahoo Japan 改版 HTML 結構,regex `/npb/game/(\\d+)/` 需更新</i>", state)
            state[f"_npb_zero_games_alert_ts_{league}"] = now_ts
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

    def _budget_exceeded(stage):
        """Check 累計耗時是否超過 NPB_BUDGET_SECONDS。在每次 fetch 前後都呼叫一次,
        避免單一 fetch 卡很久時 budget 永遠檢查不到。"""
        elapsed = datetime.now(timezone.utc).timestamp() - npb_start_ts
        if elapsed > NPB_BUDGET_SECONDS:
            log(f"NPB {league_label}: budget {NPB_BUDGET_SECONDS}s exceeded ({stage}) after {games_today} today/{len(game_ids)} total games — bailing rest")
            return True
        return False

    for game_id in game_ids:
        # 預算保護:每場 game 處理前 + 兩次 fetch 之間都檢查,單一 fetch 卡住時也能 bail。
        if _budget_exceeded("loop-head"):
            bailed_due_to_budget = True
            break
        # Fetch /top page for reliable game status (score/finished/lineup)
        top_url = f"https://baseball.yahoo.co.jp/npb/game/{game_id}/top"
        top_html = _fetch_yahoo(top_url)
        if _budget_exceeded("after-top-fetch"):
            bailed_due_to_budget = True
            break
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
        if _budget_exceeded("after-stats-fetch"):
            bailed_due_to_budget = True
            break
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
                    live_key = f"npb_{date_str}_live_{game_id}_{player_name}"
                    # 漏推偵測:final 時若沒推過 live → 標記並補警示
                    is_missed_live = live_key not in state
                    if is_missed_live:
                        state[live_key] = "final_only"
                    if final_key not in state:
                        if is_missed_live:
                            msg = (f"\u26a0\ufe0f <b>[NPB {league_label} 比賽結果 — 系統補推]</b>\n"
                                   f"<i>此球員從未在 live 階段被推播,中段過程可能因系統空白漏掉</i>\n"
                                   f"\U0001f4ca 完整本場紀錄:\n")
                        else:
                            msg = f"\U0001f4ca <b>[NPB {league_label} 比賽結果]</b>\n"
                        if game_title:
                            msg += f"\U0001f3df {game_title}\n"
                        msg += f"\U0001f464 <b>{player_name}</b> ({pinfo['team']}) 出場！\n"
                        msg += f"{full_stat}\n"
                        msg += f"\U0001f517 https://baseball.yahoo.co.jp/npb/game/{game_id}/stats"
                        notifs.append(msg)
                        state[final_key] = True
                        tag = "FINAL (missed-live)" if is_missed_live else "FINAL"
                        log(f"NPB {league_label}: {player_name} {tag} in game {game_id}")
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

    # 如果 NPB 段被 budget 強制截斷 → 推 TG 警告(每 6 小時最多 1 條,避免 NPB 持續慢時 spam)
    if bailed_due_to_budget:
        last_alert_ts = state.get(f"_npb_budget_bail_alert_ts_{league}", 0)
        now_ts_alert = datetime.now(timezone.utc).timestamp()
        if now_ts_alert - last_alert_ts > 21600:
            _send_alert(f"⚠️ <b>[NPB {league_label} 處理被 90s budget 強制截斷]</b>\n"
                        f"已處理 {games_today}/{len(game_ids)} 場,總耗時 {npb_total_secs}s 超過上限。\n"
                        f"<i>剩餘場次本輪沒檢查,可能漏掉部分 NPB 推播。Yahoo Japan 慢時會發生,持續發生請查 GHA log</i>", state)
            state[f"_npb_budget_bail_alert_ts_{league}"] = now_ts_alert

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
    disc_fail_count = 0

    # 預抓 AAA 隊伍 → 母 MLB org id 對照(3A 球員 currentTeam.id 是 3A 隊 id,要轉成母球團)
    aaa_to_parent = {}
    try:
        rt = _robust_get("https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 11, "season": date.today().year}, timeout=10)
        if rt and rt.ok:
            for t in rt.json().get("teams", []):
                p = t.get("parentOrgId")
                if p:
                    aaa_to_parent[t["id"]] = p
    except Exception as e:
        log(f"discovery: AAA team mapping err: {e}")

    for sport_id, allowed_origins in _DISCOVERY_BY_SPORT.items():
        try:
            r = _robust_get(f"https://statsapi.mlb.com/api/v1/sports/{sport_id}/players",
                params={"season": date.today().year}, timeout=20)
            if r is None or not r.ok:
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
            disc_fail_count += 1

    if disc_fail_count >= 2:
        last_alert = state.get("_discovery_alert_ts", 0)
        if now_ts - last_alert > 21600:
            _send_alert("⚠️ <b>[Discovery 整段失敗]</b>\nMLB + 3A 兩個 sport API 都 fail,動態名單可能過時。\n<i>球員交易 / 新球員上來可能無法即時偵測</i>", state)
            state["_discovery_alert_ts"] = now_ts

    state["_dynamic_players"] = discovered
    state["_last_discovery_ts"] = now_ts
    log(f"discovery done. dynamic pool size: {len(discovered)} (added {new_count} new)")
    # hardcoded refresh 抽出獨立呼叫(每 6h),不再依附 discovery 24h


_HARDCODED_REFRESH_INTERVAL = 21600  # 6 小時

def _maybe_refresh_hardcoded_teams(state):
    """獨立的 hardcoded teams refresh,6h 一次(比 discovery 的 24h 更快偵測球員交易)。"""
    now_ts = datetime.now(timezone.utc).timestamp()
    if now_ts - state.get("_last_hardcoded_refresh_ts", 0) < _HARDCODED_REFRESH_INTERVAL:
        return
    log("Refreshing hardcoded MLB_PLAYERS team_ids...")
    aaa_to_parent = {}
    try:
        rt = _robust_get("https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 11, "season": date.today().year}, timeout=10)
        if rt and rt.ok:
            for t in rt.json().get("teams", []):
                p = t.get("parentOrgId")
                if p:
                    aaa_to_parent[t["id"]] = p
    except Exception as e:
        log(f"  hardcoded refresh AAA mapping err: {e}")
    _refresh_hardcoded_team_ids(state, aaa_to_parent)
    state["_last_hardcoded_refresh_ts"] = now_ts


def _refresh_hardcoded_team_ids(state, aaa_to_parent):
    """每天 discovery 時順便用 batch API 抓 hardcoded MLB_PLAYERS 的最新 currentTeam,
    寫進 state["_hardcoded_team_overrides"]。get_all_tracked_players() 會用這個 override 過時的 hardcoded org。

    觸發場景:球員交易換 MLB 球隊(例如從 Cubs 交易到 Padres),hardcoded org 過時 →
    `_tracked_teams_have_games` 用舊 org 判斷 polling 頻率會有 false negative。

    註:升降 3A 不需 override(parent org 不變);NPB 球員換隊不在這裡處理(沒可靠 player API)。
    """
    pids = [str(p[1]) for p in MLB_PLAYERS if p[1]]
    if not pids:
        state["_hardcoded_team_overrides"] = {}
        return
    overrides = {}
    try:
        r = _robust_get("https://statsapi.mlb.com/api/v1/people",
            params={"personIds": ",".join(pids), "hydrate": "currentTeam"}, timeout=10)
        if not r or r.status_code != 200:
            log(f"  hardcoded team refresh: API not OK")
            return
        # build pid → hardcoded entry map
        hp_by_pid = {p[1]: p for p in MLB_PLAYERS if p[1]}
        for p_data in r.json().get("people", []):
            pid = p_data.get("id")
            if not pid or pid not in hp_by_pid:
                continue
            current_team_id = p_data.get("currentTeam", {}).get("id")
            if not current_team_id:
                continue
            current_org_id = aaa_to_parent.get(current_team_id, current_team_id)
            hp = hp_by_pid[pid]  # (name, pid, org, origin)
            if current_org_id != hp[2]:
                overrides[str(pid)] = current_org_id
                log(f"  Hardcoded team override: {hp[0]}({pid}) {hp[2]} -> {current_org_id}")
    except Exception as e:
        log(f"  hardcoded team refresh err: {e}")
    state["_hardcoded_team_overrides"] = overrides
    if overrides:
        log(f"  hardcoded team refresh: {len(overrides)} player(s) had team changes")


def get_all_tracked_players(state):
    """合併 hardcoded MLB_PLAYERS + 動態名單。Hardcoded 優先(同 pid 不會重複)。
    若 _hardcoded_team_overrides 有值,則 hardcoded org 會用 override 替換(處理球員交易)。"""
    overrides = state.get("_hardcoded_team_overrides", {})
    hardcoded_pids = {p[1] for p in MLB_PLAYERS if p[1]}
    out = []
    for entry in MLB_PLAYERS:
        name, pid, hp_org, origin = entry
        org = overrides.get(str(pid), hp_org) if pid else hp_org
        out.append((name, pid, org, origin))
    for pid_str, info in state.get("_dynamic_players", {}).items():
        try:
            pid = int(pid_str)
        except (ValueError, TypeError):
            continue
        if pid in hardcoded_pids:
            continue
        out.append((info.get("name", ""), pid, info.get("org"), info.get("origin")))
    return out


def _parse_iso_utc(s):
    """Parse an ISO 8601 string (with optional Z) into a UTC-aware datetime, or None."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _tracked_teams_have_games(state):
    """檢查「現在這個時間點」是否在任何追蹤球隊的比賽活動視窗內。
    視窗:開賽前 90 分鐘(覆蓋 lineup 公佈)~ 開賽後 4.5 小時(覆蓋 9 局 + 延長 + 結束後 buffer)。
    回傳 True → polling 該頻繁(2 分鐘);False → 沒比賽附近,polling 改 10 分鐘。

    過往是「今天整天有沒有比賽」(粒度太粗),改成 wall-clock 視窗判斷(2026-04-29)。"""
    now = datetime.now(timezone.utc)
    window_before = timedelta(minutes=90)
    window_after = timedelta(hours=4, minutes=30)

    all_players = get_all_tracked_players(state)
    tracked_mlb_orgs = set(p[2] for p in all_players if p[2])
    tracked_aaa_orgs = set(p[2] for p in all_players if p[3] in AAA_PUSH_ORIGINS and p[2])

    # 抓今天 + 昨天 schedule(跨日 / 昨晚比賽延長 / buffer)
    today = date.today()
    dates_to_check = [(today - timedelta(days=1)).strftime("%Y-%m-%d"),
                      today.strftime("%Y-%m-%d")]

    def _scan_mlb_schedule(sport_id, tracked_orgs, aaa_to_parent=None):
        """Scan a sport schedule, return True if any game is active right now."""
        for d in dates_to_check:
            try:
                r = _robust_get("https://statsapi.mlb.com/api/v1/schedule",
                    params={"sportId": sport_id, "date": d}, timeout=10)
                if r is None or not r.ok:
                    continue
                for de in r.json().get("dates", []):
                    for g in de.get("games", []):
                        game_dt = _parse_iso_utc(g.get("gameDate"))
                        if not game_dt:
                            continue
                        # H 修正:status==Live 時強制視為 active(處理延長賽超過 wall-clock 4.5h),
                        # Final / Preview 時才用 wall-clock 視窗判斷
                        status = g.get("status",{}).get("abstractGameState","")
                        if status == "Live":
                            in_window = True  # Live = 一定 active,不論 wall-clock
                        else:
                            in_window = (game_dt - window_before <= now <= game_dt + window_after)
                        if not in_window:
                            continue
                        for side in ["home", "away"]:
                            tid = g.get("teams", {}).get(side, {}).get("team", {}).get("id")
                            org = aaa_to_parent.get(tid, tid) if aaa_to_parent else tid
                            if org in tracked_orgs:
                                gp = g.get("gamePk")
                                log(f"  ACTIVE {('MLB' if sport_id==1 else 'AAA')}: game {gp} status={status} starts {game_dt.isoformat()}, org {org}")
                                return True
            except Exception as e:
                log(f"  schedule err sport={sport_id} date={d}: {e}")
        return False

    # --- MLB ---
    if _scan_mlb_schedule(1, tracked_mlb_orgs):
        return True

    # --- 3A:resolve parent org first ---
    aaa_to_parent = {}
    try:
        rt = _robust_get("https://statsapi.mlb.com/api/v1/teams",
            params={"sportId": 11, "season": str(today.year)}, timeout=10)
        if rt and rt.ok:
            for t in rt.json().get("teams", []):
                p = t.get("parentOrgId")
                if p:
                    aaa_to_parent[t["id"]] = p
    except Exception as e:
        log(f"  AAA teams resolve err: {e}")
    if _scan_mlb_schedule(11, tracked_aaa_orgs, aaa_to_parent):
        return True

    # --- NPB:從 Yahoo Japan schedule 抓追蹤球隊的場次狀態 ---
    # 兩道判斷(對應 MLB/AAA 的 status==Live 強制 + wall-clock 視窗):
    #   1. <li class="bb-score__item--live"> → 比賽進行中,直接 ACTIVE(這條是 2026-04-30 修補)
    #      理由:Yahoo 比賽開始後會把 <time bb-score__status>HH:MM</time> 整段拿掉,
    #      只用 wall-clock 抓不到開賽時間 → 永遠 false → polling 卡 600s。
    #      跟 commit 7896e94 在 MLB/AAA 加的 `status==Live → in_window=True` 是同類保險。
    #   2. preview(賽前)仍有 <time bb-score__status>HH:MM</time> → 用 wall-clock 視窗
    #      (開賽前 90min ~ 開賽後 4h30m,跟 MLB/AAA 一致)
    jst = timezone(timedelta(hours=9))
    now_jst = now.astimezone(jst)
    npb_tracked_teams = set(info["team"] for info in NPB_PLAYERS_INFO.values())
    try:
        jst_date_str = now_jst.strftime("%Y-%m-%d")
        html = _fetch_yahoo(f"https://baseball.yahoo.co.jp/npb/schedule/first/all?date={jst_date_str}")
        if html:
            items = re.findall(r'<li class="bb-score__item.*?</li>', html, re.DOTALL)
            for item in items:
                team_names = re.findall(r'<p class="bb-score__(?:home|away)Logo[^"]*">([^<]+)</p>', item)
                if not any(t in team_names for t in npb_tracked_teams):
                    continue
                # 1. Live 強制 ACTIVE — 不靠 wall-clock(該 tag 在 live 期間消失)
                header = item.split(">", 1)[0]
                if "bb-score__item--live" in header:
                    log(f"  ACTIVE NPB: tracked team game LIVE ({'/'.join(team_names)})")
                    return True
                # 2. preview / scheduled — 仍走 wall-clock 視窗
                time_m = re.search(r'<time[^>]*bb-score__status[^>]*>([0-9]{1,2}:[0-9]{2})', item)
                if not time_m:
                    continue
                try:
                    game_dt_jst = datetime.strptime(
                        f"{jst_date_str} {time_m.group(1)}", "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=jst)
                    game_dt_utc = game_dt_jst.astimezone(timezone.utc)
                    if game_dt_utc - window_before <= now <= game_dt_utc + window_after:
                        log(f"  ACTIVE NPB: tracked team game at JST {time_m.group(1)} ({'/'.join(team_names)})")
                        return True
                except Exception as e:
                    log(f"  NPB time parse err: {e}")
    except Exception as e:
        log(f"  NPB schedule err: {e}")

    log(f"  no tracked game in active window now (UTC {now.strftime('%H:%M')}, JST {now_jst.strftime('%H:%M')})")
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
    # 啟動驗證:TG_TOKEN / TG_CHAT_ID 缺失時 log 明顯警告(不 exit,讓 state 仍可更新)
    if not TOKEN or not CHAT_ID:
        log(f"⚠️ MISSING ENV: TG_TOKEN={'set' if TOKEN else 'EMPTY'} TG_CHAT_ID={'set' if CHAT_ID else 'EMPTY'}")
        log(f"   程式仍會跑(state / discovery 有意義),但所有 send_tg 會失敗")
        log(f"   請至 GitHub Secrets 設定")
    # 防呆雙保險(2026-04-28):
    # (1) signal.SIGALRM:240s 後送信號,handler 用 os._exit(1)
    # (2) threading.Timer daemon watchdog:250s 後直接 os._exit(2),不依賴 Python signal
    # 後者是為了應付 SIGALRM 被 C 層 socket I/O 吞掉的情境
    signal.signal(signal.SIGALRM, _abort_on_timeout)
    signal.alarm(MAX_RUNTIME_SECONDS)
    watchdog = threading.Timer(WATCHDOG_SECONDS, _watchdog_kill)
    watchdog.daemon = True
    watchdog.start()
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
    # hardcoded teams refresh(獨立 6h interval,比 discovery 24h 更快偵測球員交易)
    _maybe_refresh_hardcoded_teams(state)

    # ⭐ Cold-start 偵測:state 為空(沒 _last_ok_ts 也沒 _last_run_ts) → 本輪 suppress 所有 send_tg
    # 用途:第一次部署 / cache 遺失 / state corrupt 救回 — 避免把「今天所有舊事件」當新事件 spam
    cold_start = ("_last_ok_ts" not in state) and ("_last_run_ts" not in state)
    if cold_start:
        log("⚠️ COLD START detected (no _last_ok_ts and no _last_run_ts in state)")

    # ⭐ 自我健檢 + 動態 polling interval(2026-04-29 重寫)
    # 雲端 long-running run 透過 yaml bash loop 每 N 秒呼叫一次,
    # N 由本程式寫進 state["_next_poll_interval"](有比賽 120s,沒比賽 600s)。
    # 移除舊的「沒比賽 10 分鐘 elapsed early return」邏輯,改由 polling 間隔本身控制頻率。
    teams_playing = _tracked_teams_have_games(state)
    expected_interval = 120 if teams_playing else 600
    # R 修正:健檢用「上輪寫的 _next_poll_interval」當基準,避免「上輪沒比賽 600s / 這輪有比賽 120s」邊界場景誤警告
    prev_expected_interval = state.get("_next_poll_interval", expected_interval)

    last_ok_ts = state.get("_last_ok_ts", 0)
    now_ts = datetime.now(timezone.utc).timestamp()
    gap_s = int(now_ts - last_ok_ts) if last_ok_ts else 0

    # Gap 超過上輪預期間隔 3 倍 = 系統真的失聯,推一條警告讓使用者知道剛才有空白
    if last_ok_ts > 0 and gap_s > prev_expected_interval * 3:
        gap_min = gap_s // 60
        alert_msg = (
            f"\u26a0\ufe0f <b>[系統健檢]</b>\n"
            f"上次成功執行距現在 <b>{gap_min} 分鐘</b>,超過預期 {prev_expected_interval//60} 分鐘間隔。\n"
            f"<i>這段時間可能漏推賽中事件,以下若有累積資料會在後續訊息送出。</i>\n"
            f"\U0001f50d 詳情看 GitHub Actions log"
        )
        _send_alert(alert_msg, state)
        log(f"⚠️ Gap {gap_s}s > {expected_interval*3}s, sent system-gap alert")
    elif last_ok_ts > 0:
        log(f"Last ok run {gap_s}s ago (prev_expected={prev_expected_interval}s, this_round_expected={expected_interval}s, teams_playing={teams_playing})")
    else:
        log(f"First run with new health-check schema (no _last_ok_ts yet)")

    log(f"Tracked teams playing today: {teams_playing}")

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
    if cold_start:
        # state 為空(初次部署 / cache 遺失 / corrupt 救回) → 所有 dedup key 已寫進 state,
        # 但訊息不送出。避免「補推今天所有舊事件」spam(user 過去因 cache miss 一次收 16+ 條的問題)
        # 真正中段漏掉的事件靠 final 推播時的 "[系統補推]" 標籤抓回(那條會送)
        log(f"COLD START: state was empty, suppressing {len(all_notifs)} backfill notif(s) (only marking dedup keys)")
        for msg in all_notifs:
            log(f"  suppressed: {msg[:60].replace(chr(10),' ')}...")
    else:
        # F2 保險:先重試上輪失敗暫存的訊息,避免 Telegram 持續壞時訊息丟失
        pending = state.get("_pending_send", [])
        if pending:
            log(f"Retrying {len(pending)} pending message(s) from previous run")
            still_pending = []
            for entry in pending:
                msg = entry.get("msg", "")
                if not msg:
                    continue
                if send_tg(msg):
                    sent += 1
                    log(f"Sent (retry): {msg[:50].replace(chr(10),' ')}...")
                else:
                    # 加重試次數;超過 5 次 / 超過 24 小時就丟棄
                    entry["attempts"] = entry.get("attempts", 0) + 1
                    enqueued_at = entry.get("ts", now_ts)
                    age_hours = (now_ts - enqueued_at) / 3600
                    if entry["attempts"] < 8 and age_hours < 48:
                        still_pending.append(entry)
                    else:
                        log(f"  GIVE UP after {entry['attempts']} attempts / {age_hours:.1f}h: {msg[:50]}")
            state["_pending_send"] = still_pending
        # 本輪新訊息
        for msg in all_notifs:
            if send_tg(msg):
                sent += 1
                log(f"Sent: {msg[:50].replace(chr(10),' ')}...")
            else:
                log("Send failed → enqueue to _pending_send for next-run retry")
                state.setdefault("_pending_send", []).append({
                    "msg": msg, "ts": now_ts, "attempts": 1
                })

    # ⭐ 跑成功 → 寫 _last_ok_ts + _next_poll_interval(yaml bash loop 會讀後者決定下次 sleep)
    state["_last_ok_ts"] = now_ts
    state["_next_poll_interval"] = expected_interval

    # ⭐ 每日心跳(只在 cold_start 之外的正常 run 才推)
    if not cold_start:
        _maybe_send_daily_heartbeat(state)

    save_state(state)
    signal.alarm(0)  # 正常結束前關掉 SIGALRM
    watchdog.cancel()  # 正常結束前關掉 watchdog daemon
    log(f"Done! Sent {sent}/{len(all_notifs)} notifications")
    log(f"Next poll interval: {expected_interval}s ({'2min' if expected_interval==120 else '10min'})")
    log("=" * 40)

if __name__ == "__main__":
    main()
