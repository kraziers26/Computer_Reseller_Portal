"""
services/alerts.py

Telegram alerts for the Deal Scanner (send-only, round 1).

The portal posts straight to the Telegram Bot API with requests — no bot
library and no webhook. Buttons are URL buttons, so this works with any bot,
including one that another service is already polling.

Railway variables:
  TELEGRAM_BOT_TOKEN   token from BotFather
  TELEGRAM_CHAT_ID     the private group's id (-100…)
  PORTAL_BASE_URL      optional; defaults to https://$RAILWAY_PUBLIC_DOMAIN
                       (used for "Open in portal", "Snooze", "Stop" buttons)

What gets sent (all configurable in Deal Scanner → Alert settings):
  • Watchlist alerts — one message per watched product that changed
  • Price Drops batch — one message per sync listing new deals ≥ min score
  • Deal ended — the original watchlist alert is edited in place
  • System — Best Buy sync failed 3 times in a row / recovered
  • Daily summary
Quiet hours either send silently or hold everything for one morning message.
"""

import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from psycopg.types.json import Json

from .price_engine import score_event, in_stock, now_utc, PRICE_EPS

logger = logging.getLogger(__name__)

TG_API = "https://api.telegram.org"

DEFAULTS = {
    "enabled": True,
    "use_topics": False,
    "topics": {"watchlist": None, "drops": None, "system": None},
    "watch_on": True,
    "watch_sound": "loud",
    "deal_ended_on": True,
    "deal_ended_mode": "edit",          # edit | new
    "drops_on": True,
    "drops_min_score": 50,
    "drops_delivery": "batch",          # batch | summary
    "drops_sound": "loud_if_80",        # loud_if_80 | loud | silent
    "system_on": True,
    "system_sound": "silent",
    "summary_on": True,
    "summary_time": "08:00",
    "quiet_on": True,
    "quiet_tz": "Asia/Dubai",
    "quiet_start": "00:00",
    "quiet_end": "07:00",
    "quiet_mode": "silent",             # silent | hold
    "photo": True,
    "show_held": True,
    "show_limit": True,
    "same_sku_hours": 6,
    "max_per_sync": 10,
}

TIMEZONES = [
    ("Asia/Dubai", "Dubai (GMT+4)"),
    ("America/Guayaquil", "Ecuador (GMT−5)"),
    ("America/New_York", "US Eastern"),
    ("America/Chicago", "US Central"),
    ("America/Los_Angeles", "US Pacific"),
]


class TelegramError(Exception):
    pass


# ── Settings ──────────────────────────────────────────────────────────────────

def get_settings(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT settings FROM alert_settings WHERE id = 1")
        row = cur.fetchone()
    saved = (row or {}).get("settings") or {}
    s = {**DEFAULTS, **{k: v for k, v in saved.items() if k in DEFAULTS}}
    s["topics"] = {**DEFAULTS["topics"], **(saved.get("topics") or {})}
    return s


def save_settings(conn, data: dict) -> dict:
    current = get_settings(conn)
    clean = dict(current)
    for k, default in DEFAULTS.items():
        if k not in data:
            continue
        v = data[k]
        if k == "topics":
            clean["topics"] = {t: (_int_or_none((v or {}).get(t))) for t in DEFAULTS["topics"]}
        elif isinstance(default, bool):
            clean[k] = bool(v)
        elif isinstance(default, int):
            try:
                clean[k] = max(0, int(v))
            except Exception:
                pass
        else:
            clean[k] = str(v)[:40]
    if clean["quiet_tz"] not in {tz for tz, _ in TIMEZONES}:
        clean["quiet_tz"] = DEFAULTS["quiet_tz"]
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO alert_settings (id, settings, updated_at) VALUES (1, %s, NOW())
                       ON CONFLICT (id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = NOW()""",
                    (Json(clean),))
    conn.commit()
    return clean


def _int_or_none(v):
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None


TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")


def bot_token() -> str:
    """TELEGRAM_BOT_TOKEN, cleaned of the usual paste mistakes:
    spaces/newlines, surrounding quotes, a leading 'bot', or a whole API URL."""
    t = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip().strip('"').strip("'").strip()
    m = re.search(r"bot(\d{5,}:[A-Za-z0-9_-]+)", t)
    if m and (t.lower().startswith("bot") or "api.telegram.org" in t):
        t = m.group(1)
    return t


def chat_id() -> str:
    return (os.environ.get("TELEGRAM_CHAT_ID") or "").strip().strip('"').strip("'").strip()


def connection_info() -> dict:
    token = bot_token()
    chat = chat_id()
    return {"token_set": bool(token), "token_looks_valid": bool(TOKEN_RE.match(token)),
            "chat_id": chat or None, "chat_looks_valid": bool(re.match(r"^-?\d+$", chat)),
            "portal_base_url": portal_base_url()}


def portal_base_url():
    url = os.environ.get("PORTAL_BASE_URL", "").rstrip("/")
    if url:
        return url
    dom = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    return f"https://{dom}" if dom else None


def portal_link(path):
    base = portal_base_url()
    return f"{base}{path}" if base else None


# ── Telegram API ──────────────────────────────────────────────────────────────

FRIENDLY_ERRORS = {
    404: "Telegram didn't recognize the bot token. Check TELEGRAM_BOT_TOKEN in Railway — "
         "it should look like 123456789:AAH… with nothing before or after it.",
    401: "Telegram rejected the bot token (revoked or wrong). Get the current token from BotFather.",
}


def tg_call(method: str, payload: dict) -> dict:
    token = bot_token()
    if not token:
        raise TelegramError("TELEGRAM_BOT_TOKEN is not set")
    for attempt in range(2):
        try:
            r = requests.post(f"{TG_API}/bot{token}/{method}", json=payload, timeout=15)
            data = r.json()
        except Exception as e:
            raise TelegramError(f"request failed: {e}")
        if data.get("ok"):
            return data.get("result") or {}
        retry = (data.get("parameters") or {}).get("retry_after")
        if data.get("error_code") == 429 and retry and attempt == 0:
            import time
            time.sleep(min(int(retry), 5))
            continue
        desc = data.get("description") or f"HTTP {r.status_code}"
        code = data.get("error_code") or r.status_code
        if code in FRIENDLY_ERRORS and desc in ("Not Found", "Unauthorized"):
            desc = FRIENDLY_ERRORS[code]
        elif "chat not found" in desc.lower():
            desc = ("Telegram can't find that chat. Check TELEGRAM_CHAT_ID (starts with -100) "
                    "and that the bot has been added to the group.")
        elif "thread not found" in desc.lower():
            desc = "That topic ID doesn't exist in the group. Check the topic IDs below."
        raise TelegramError(desc)
    raise TelegramError("rate limited")


def _keyboard(buttons):
    rows = []
    for row in buttons or []:
        r = [{"text": b[0], "url": b[1]} for b in row if b and b[1]]
        if r:
            rows.append(r)
    return {"inline_keyboard": rows} if rows else None


def is_quiet(s, now=None) -> bool:
    if not s.get("quiet_on"):
        return False
    now = now or now_utc()
    local = now.astimezone(ZoneInfo(s["quiet_tz"])).strftime("%H:%M")
    start, end = s["quiet_start"], s["quiet_end"]
    if start == end:
        return False
    return (start <= local < end) if start < end else (local >= start or local < end)


def local_time(s, dt=None, fmt="%-I:%M %p"):
    dt = dt or now_utc()
    return dt.astimezone(ZoneInfo(s["quiet_tz"])).strftime(fmt)


def send(conn, s, *, kind, text, topic, alert_key, silent=False, force_loud=False,
         buttons=None, photo=None, sku=None, product_id=None, price=None, bypass_quiet=False):
    """Send one Telegram message, idempotently (alert_key). Returns message_id or None."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM alert_log WHERE alert_key = %s", (alert_key,))
        if cur.fetchone():
            return None

    chat = chat_id()
    thread = s["topics"].get(topic) if s.get("use_topics") else None
    quiet = is_quiet(s) and not bypass_quiet

    if quiet and s["quiet_mode"] == "hold" and not force_loud:
        _log(conn, alert_key, kind, "held", sku, product_id, price, chat, thread, None, False, text)
        return None

    if quiet and not force_loud:
        silent = True

    payload = {"chat_id": chat, "parse_mode": "HTML", "disable_notification": bool(silent)}
    if thread:
        payload["message_thread_id"] = thread
    kb = _keyboard(buttons)
    if kb:
        payload["reply_markup"] = kb

    try:
        if not chat:
            raise TelegramError("TELEGRAM_CHAT_ID is not set")
        if photo and s.get("photo") and len(text) <= 1024:
            res = tg_call("sendPhoto", {**payload, "photo": photo, "caption": text})
            has_photo = True
        else:
            res = tg_call("sendMessage", {**payload, "text": text, "link_preview_options": {"is_disabled": True}})
            has_photo = False
    except TelegramError as e:
        logger.error(f"[Alerts] send failed ({kind}): {e}")
        _log(conn, alert_key, kind, "failed", sku, product_id, price, chat, thread, None, False, text, str(e))
        return None

    mid = res.get("message_id")
    _log(conn, alert_key, kind, "sent", sku, product_id, price, chat, thread, mid, has_photo, text)
    return mid


def _log(conn, key, kind, status, sku, product_id, price, chat, thread, mid, has_photo, text, err=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO alert_log (alert_key, kind, sku, product_id, price, status, chat_id, thread_id,
                                   message_id, has_photo, text, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (alert_key) DO NOTHING
        """, (key, kind, sku, product_id, price, status, chat or None, thread, mid, has_photo, text, err))
    conn.commit()


# ── Formatting ────────────────────────────────────────────────────────────────

def esc(s):
    return html.escape(str(s or ""), quote=False)


def money(v):
    return f"${v:,.2f}"


def _drop_line(old, new):
    if old and old > new:
        diff = old - new
        return f"{money(old)} → {money(new)}\n−{money(diff)} (−{round(diff / old * 100)}%)"
    return money(new)


def _duration(td):
    mins = int(td.total_seconds() // 60)
    if mins < 60:
        return f"{mins}m"
    h, m = divmod(mins, 60)
    if h < 48:
        return f"{h}h {m}m"
    return f"{h // 24} days"


# ── After each sync ───────────────────────────────────────────────────────────

def after_sync(conn, events: list, run_ok: bool = True):
    s = get_settings(conn)
    if not s["enabled"] or not bot_token():
        return
    if run_ok:
        if events:
            sent = watch_alerts(conn, s, events)
            deal_ended(conn, s, events)
            if s["drops_on"] and s["drops_delivery"] == "batch":
                drops_batch(conn, s, events, sent_budget=max(0, s["max_per_sync"] - sent))
        flush_held(conn, s)
        daily_summary(conn, s)
    system_health(conn, s, run_ok)


def _product(cur, sku):
    cur.execute("SELECT * FROM bb_products WHERE sku = %s", (sku,))
    return cur.fetchone()


def watch_alerts(conn, s, events) -> int:
    if not s["watch_on"]:
        return 0
    by_sku = {}
    for e in events:
        by_sku.setdefault(e["sku"], []).append(e)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT w.*, l.retailer_ref AS sku
            FROM watch_products w JOIN watch_listings l ON l.product_id = w.id
            WHERE w.is_active AND l.retailer = 'bestbuy' AND l.status = 'tracked'
              AND l.retailer_ref = ANY(%s)
        """, (list(by_sku.keys()),))
        watches = cur.fetchall()

    now = now_utc()
    messages = []
    for w in watches:
        if w["snoozed_until"] and w["snoozed_until"] > now:
            continue
        with conn.cursor() as cur:
            p = _product(cur, w["sku"])
        if not p:
            continue
        evs = by_sku[w["sku"]]
        target = float(w["target_price"]) if w["target_price"] is not None else None
        reasons, header, force_loud, main = [], None, False, None

        for e in evs:
            t = e["event_type"]
            new = float(e["new_price"]) if e["new_price"] is not None else None
            old = float(e["old_price"]) if e["old_price"] is not None else None
            if t == "drop":
                main = e
                crossed = target is not None and new <= target + PRICE_EPS and (old is None or old > target)
                if crossed and w["alert_below_target"]:
                    reasons.append(f"Below your {money(target)} target")
                    header, force_loud = "BELOW TARGET · WATCHLIST", True
                if (e.get("flags") or {}).get("low_status") == "new_low" and w["alert_all_time_low"]:
                    reasons.append("New all-time low")
                    header = header or "ALL-TIME LOW · WATCHLIST"
                if w["alert_drop"]:
                    header = header or "PRICE DROP · WATCHLIST"
                elif not reasons:
                    main = None
            elif t == "back_in_stock" and w["alert_back_in_stock"]:
                main = main or e
                reasons.append("Back in stock")
                header = header or "BACK IN STOCK · WATCHLIST"
            elif t == "rise" and w["alert_price_up"]:
                main = main or e
                reasons.append("Price went up")
                header = header or "PRICE UP · WATCHLIST"
        if not main or not header:
            continue

        new = float(main["new_price"])
        old = float(main["old_price"]) if main["old_price"] is not None else None
        lines = [f"<b>{esc(header)}</b>", f"<b>{esc(p['name'])}</b>",
                 f"<code>{esc(_drop_line(old, new) if main['event_type'] in ('drop','rise') else money(new))}</code>"]
        detail = list(reasons)
        held = (main.get("flags") or {}).get("held_days")
        if s["show_held"] and held is not None and main["event_type"] == "drop" and old:
            detail.append(f"Old price held {held} day{'s' if held != 1 else ''}")
        if detail:
            lines.append("\n".join(esc(d) for d in dict.fromkeys(detail)))
        if s["show_limit"]:
            extra = "New condition"
            if p["qty_limit"]:
                extra += f" · limit {p['qty_limit']} per order"
            lines.append(esc(extra))
        text = "\n".join(lines)
        token = w["action_token"]
        buttons = [[("Buy at Best Buy", p["url"]), ("Open in portal", portal_link(f"/deals/sku/{p['sku']}"))],
                   [("Snooze 24h", portal_link(f"/deals/w/{token}/snooze")),
                    ("Stop watching", portal_link(f"/deals/w/{token}/stop"))]]
        messages.append(dict(kind="watch", text=text, topic="watchlist", buttons=buttons,
                             alert_key=f"watch:{w['id']}:{main['id']}", photo=p["image_url"],
                             sku=p["sku"], product_id=w["id"], price=new, force_loud=force_loud,
                             silent=(s["watch_sound"] == "silent" and not force_loud)))

    cap = max(1, s["max_per_sync"])
    for m in messages[:cap]:
        send(conn, s, **m)
    rest = messages[cap:]
    if rest:
        body = "\n\n".join(m["text"] for m in rest)[:3800]
        send(conn, s, kind="watch", topic="watchlist",
             alert_key="watchrest:" + ",".join(m["alert_key"] for m in rest)[:180],
             text=f"<b>{len(rest)} MORE WATCHLIST ALERTS</b>\n\n{body}",
             force_loud=any(m["force_loud"] for m in rest))
    return min(len(messages), cap) + (1 if rest else 0)


def deal_ended(conn, s, events):
    if not s["deal_ended_on"]:
        return
    now = now_utc()
    for e in events:
        if e["event_type"] not in ("rise", "sold_out"):
            continue
        new = float(e["new_price"]) if e["new_price"] is not None else None
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM alert_log
                WHERE kind = 'watch' AND status = 'sent' AND sku = %s AND message_id IS NOT NULL
                  AND created_at > NOW() - INTERVAL '7 days'
                  AND (%s OR price < %s)
                ORDER BY created_at DESC
            """, (e["sku"], e["event_type"] == "sold_out", (new or 0) - PRICE_EPS))
            sent = cur.fetchall()
            p = _product(cur, e["sku"])
        for a in sent:
            lasted = _duration(now - a["created_at"])
            what = "Sold out" if e["event_type"] == "sold_out" else f"<s>{money(float(a['price']))}</s> → back to {money(new)}"
            text = (f"<b>DEAL ENDED · {esc(local_time(s, now))}</b>\n<b>{esc(p['name'] if p else e['sku'])}</b>\n"
                    f"{what}\nLasted {lasted}")
            if s["deal_ended_mode"] == "edit":
                try:
                    payload = {"chat_id": a["chat_id"], "message_id": a["message_id"], "parse_mode": "HTML",
                               "reply_markup": {"inline_keyboard": []}}
                    if a["has_photo"]:
                        tg_call("editMessageCaption", {**payload, "caption": text})
                    else:
                        tg_call("editMessageText", {**payload, "text": text})
                except TelegramError as err:
                    logger.warning(f"[Alerts] edit failed, sending new message instead: {err}")
                    send(conn, s, kind="deal_ended", text=text, topic="watchlist", silent=True,
                         alert_key=f"ended:{a['id']}:{e['id']}", sku=e["sku"], price=new)
            else:
                send(conn, s, kind="deal_ended", text=text, topic="watchlist", silent=True,
                     alert_key=f"ended:{a['id']}:{e['id']}", sku=e["sku"], price=new)
            with conn.cursor() as cur:
                cur.execute("UPDATE alert_log SET status = 'ended' WHERE id = %s", (a["id"],))
            conn.commit()


def _drop_candidates(conn, s, events):
    now = now_utc()
    out = []
    with conn.cursor() as cur:
        for e in events:
            if e["event_type"] not in ("drop", "back_in_stock"):
                continue
            p = _product(cur, e["sku"])
            if not p or not in_stock(p["orderable"], p["online_available"]):
                continue
            sc = score_event(e, p, now)
            if sc["total"] < s["drops_min_score"]:
                continue
            new = float(e["new_price"])
            cur.execute("""
                SELECT 1 FROM alert_log WHERE kind = 'drops_item' AND sku = %s
                  AND created_at > NOW() - make_interval(hours => %s) AND price <= %s
            """, (e["sku"], s["same_sku_hours"], new + PRICE_EPS))
            if cur.fetchone():
                continue
            out.append((sc["total"], e, p))
    out.sort(key=lambda x: -x[0])
    return out


def _drop_item_text(score, e, p):
    f = e.get("flags") or {}
    tag = {"new_low": " · all-time low", "ties_low": " · ties low"}.get(f.get("low_status"), "")
    if e["event_type"] == "back_in_stock":
        tag += " · back in stock"
    depth = f.get("depth_pct") or 0
    real = f" · −{depth:g}% real" if depth > 0 else ""
    name = f'<a href="{esc(p["url"])}">{esc(p["name"][:70])}</a>' if p["url"] else esc(p["name"][:70])
    return f"<b>{score}</b> · {name}{esc(tag)}\n<code>{money(float(e['new_price']))}</code>{esc(real)}"


def drops_batch(conn, s, events, sent_budget=10):
    cands = _drop_candidates(conn, s, events)
    if not cands:
        return
    shown = cands[:max(1, sent_budget or 1)]
    now = now_utc()
    lines = [f"<b>{len(cands)} NEW DEAL{'S' if len(cands) != 1 else ''} · {esc(local_time(s, now))} SYNC</b>"]
    lines += [_drop_item_text(sc, e, p) for sc, e, p in shown]
    if len(cands) > len(shown):
        lines.append(f"+{len(cands) - len(shown)} more in the portal")
    text = "\n\n".join(lines)[:4000]
    loud = s["drops_sound"] == "loud" or (s["drops_sound"] == "loud_if_80" and cands[0][0] >= 80)
    key = f"batch:{now.strftime('%Y%m%d%H%M')}:{cands[0][1]['id']}"
    mid = send(conn, s, kind="drops_batch", text=text, topic="drops", alert_key=key, silent=not loud,
               buttons=[[("Open Price Drops", portal_link("/deals/drops"))]])
    for sc, e, p in cands:
        _log(conn, f"drops_item:{e['sku']}:{e['id']}", "drops_item", "sent", e["sku"], None,
             float(e["new_price"]), None, None, mid, False, None)


def flush_held(conn, s):
    if is_quiet(s):
        return
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM alert_log WHERE status = 'held' ORDER BY created_at")
        held = cur.fetchall()
    if not held:
        return
    body = "\n\n— — —\n\n".join(h["text"] for h in held if h["text"])
    text = f"<b>WHILE YOU WERE AWAY · {len(held)} ALERT{'S' if len(held) != 1 else ''}</b>\n\n{body}"
    if len(text) > 4000:
        text = text[:3950] + "\n\n… more in the portal"
    mid = send(conn, s, kind="held_digest", text=text, topic="watchlist",
               alert_key=f"held:{held[0]['id']}-{held[-1]['id']}",
               buttons=[[("Open Price Drops", portal_link("/deals/drops"))]])
    with conn.cursor() as cur:
        cur.execute("UPDATE alert_log SET status = 'sent', message_id = %s WHERE id = ANY(%s)",
                    (mid, [h["id"] for h in held]))
    conn.commit()


def system_health(conn, s, run_ok):
    if not s["system_on"]:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT id, status, started_at, error_message FROM price_sync_runs "
                    "WHERE status <> 'running' ORDER BY started_at DESC LIMIT 4")
        runs = cur.fetchall()
        cur.execute("SELECT started_at FROM price_sync_runs WHERE status = 'ok' ORDER BY started_at DESC LIMIT 1")
        last_ok = cur.fetchone()
    silent = s["system_sound"] == "silent"
    if not run_ok:
        if len(runs) >= 3 and all(r["status"] == "error" for r in runs[:3]):
            # One alert per failure streak: keyed on the first failed run of the streak
            last_good = esc(local_time(s, last_ok["started_at"])) if last_ok else "never"
            send(conn, s, kind="system", topic="system", silent=silent,
                 alert_key=f"system:fail:{_streak_start(conn)}",
                 text=(f"<b>SYNC PROBLEM</b>\nBest Buy sync failed 3 times in a row.\n"
                       f"{esc((runs[0]['error_message'] or '')[:200])}\n"
                       f"Last good sync {last_good}. Retrying every 15 min."))
    else:
        if len(runs) >= 2 and runs[1]["status"] == "error":
            start = _streak_start(conn, before_id=runs[0]["id"])
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM alert_log WHERE alert_key = %s", (f"system:fail:{start}",))
                had_alert = cur.fetchone()
            if had_alert:
                send(conn, s, kind="system", topic="system", silent=silent,
                     alert_key=f"system:recover:{runs[0]['id']}",
                     text="<b>SYNC RECOVERED</b>\nBest Buy sync is working again.")


def _streak_start(conn, before_id=None):
    """id of the first failed run in the current failure streak."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, status FROM price_sync_runs
            WHERE status <> 'running' AND (%s::int IS NULL OR id < %s)
            ORDER BY id DESC LIMIT 200
        """, (before_id, before_id))
        start = None
        for r in cur.fetchall():
            if r["status"] != "error":
                break
            start = r["id"]
    return start


def daily_summary(conn, s):
    if not s["summary_on"]:
        return
    tz = ZoneInfo(s["quiet_tz"])
    local = now_utc().astimezone(tz)
    hh, mm = (s["summary_time"] + ":00").split(":")[:2]
    try:
        due = local.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except Exception:
        return
    if not (due <= local < due + timedelta(hours=6)):
        return
    key = f"summary:{local.strftime('%Y-%m-%d')}"
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM alert_log WHERE alert_key = %s", (key,))
        if cur.fetchone():
            return
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE event_type = 'drop' AND (flags->>'depth_pct')::numeric >= 10) AS real_drops,
              COUNT(*) FILTER (WHERE event_type = 'drop' AND flags->>'low_status' = 'new_low') AS new_lows,
              COUNT(*) FILTER (WHERE event_type IN ('rise','sold_out')) AS ups
            FROM bb_price_events WHERE detected_at > NOW() - INTERVAL '24 hours'
        """)
        ev = cur.fetchone()
        cur.execute("""SELECT COUNT(*) AS n FROM alert_log WHERE kind = 'watch'
                       AND created_at > NOW() - INTERVAL '24 hours' AND status IN ('sent','ended')""")
        watch_n = cur.fetchone()["n"]
        cur.execute("""SELECT COUNT(*) FILTER (WHERE status = 'ok') AS ok,
                              COUNT(*) FILTER (WHERE status = 'error') AS err,
                              COALESCE(SUM(api_calls),0) AS calls
                       FROM price_sync_runs WHERE started_at > NOW() - INTERVAL '24 hours'""")
        runs = cur.fetchone()
    failed = f" · {runs['err']} failed" if runs["err"] else ""
    lines = ["<b>DAILY SUMMARY</b>",
             f"Last 24h: {ev['real_drops']} real drops (≥10%) · {ev['new_lows']} all-time lows · "
             f"{watch_n} watchlist alerts · {ev['ups']} price rises / sell-outs",
             f"{runs['ok']} syncs OK{failed} · API calls {int(runs['calls']):,}"]
    if s["drops_on"] and s["drops_delivery"] == "summary":
        from .price_engine import list_drops
        top = [i for i in list_drops(conn, window_hours=24)["items"]
               if not i["ended"] and i["score"]["total"] >= s["drops_min_score"]][:8]
        if top:
            lines.append("")
            lines += [f"<b>{i['score']['total']}</b> · {esc(i['name'][:60])} · <code>{money(i['price'])}</code>"
                      for i in top]
    send(conn, s, kind="summary", text="\n".join(lines), topic="system", silent=True, alert_key=key,
         buttons=[[("Open Price Drops", portal_link("/deals/drops"))]])


def send_test(conn) -> dict:
    """Sends a sample to each configured topic. Ignores quiet hours."""
    s = get_settings(conn)
    stamp = now_utc().strftime("%Y%m%d%H%M%S")
    results = {}
    topics = ["watchlist", "drops", "system"] if s["use_topics"] else ["watchlist"]
    for t in topics:
        label = {"watchlist": "Watchlist", "drops": "Price drops", "system": "System"}[t]
        mid = send(conn, s, kind="test", topic=t, alert_key=f"test:{stamp}:{t}", bypass_quiet=True,
                   text=(f"<b>TEST · {label.upper()}</b>\nDeal Scanner alerts are connected."
                         f"\n{esc(local_time(s))} ({esc(s['quiet_tz'])})"),
                   buttons=[[("Open Deal Scanner", portal_link("/deals/drops"))]])
        with conn.cursor() as cur:
            cur.execute("SELECT status, error_message FROM alert_log WHERE alert_key = %s",
                        (f"test:{stamp}:{t}",))
            row = cur.fetchone()
        results[t] = {"ok": bool(mid), "error": (row or {}).get("error_message")}
    return results
