import os
import time
import json
import asyncio
import logging
import sqlite3
import zoneinfo
import datetime as dt
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import requests
import genshin
from genshin import errors as genshin_errors


# ──────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ──────────────────────────────────────────────────────────────────────────────

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ──────────────────────────────────────────────────────────────────────────────
# Config + env loading
# ──────────────────────────────────────────────────────────────────────────────

def load_env_files() -> None:
    """
    Optionally load .env or stack.env if they exist in the working directory.
    This is just a convenience for local dev; in Docker you usually rely on
    environment variables or docker-compose env_file.
    """
    cwd = Path(".").resolve()
    env_files = [cwd / ".env", cwd / "stack.env"]
    loaded_any = False

    for env_path in env_files:
        if env_path.exists():
            loaded_any = True
            try:
                with env_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" not in line:
                            continue
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip('"').strip("'")
                        # Do not override existing environment
                        if key not in os.environ:
                            os.environ[key] = val
                logging.info("%s | Loaded env file: %s", utc_iso(), env_path)
            except Exception as e:
                logging.warning(
                    "%s | Failed to load env file %s: %s",
                    utc_iso(),
                    env_path,
                    e,
                )

    if not loaded_any:
        logging.info(
            "%s | No .env or stack.env found. Using Docker environment variables only.",
            utc_iso(),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Account:
    """
    One Genshin account.

    - name:          Friendly display name for Slack (no UID leak).
    - uid:           Genshin UID as a string (never log it directly).
    - ltuid_v2:      ltuid_v2 cookie value.
    - ltoken_v2:     ltoken_v2 cookie value.
    - db_name:       SQLite DB file name, eg: "genshin_XXXXXXXXX.sqlite3".
    - slack_mention: Optional Slack mention like "<@U0123ABCD>" to ping that user.
    - tz:            IANA timezone string, e.g. "Asia/Tokyo", "America/Los_Angeles".
    """
    name: str
    uid: str
    ltuid_v2: str
    ltoken_v2: str
    db_name: str
    slack_mention: str | None = None
    tz: str = "UTC"


# ──────────────────────────────────────────────────────────────────────────────
# Accounts loading
# ──────────────────────────────────────────────────────────────────────────────

def load_accounts_from_json(path: Path) -> List[Account]:
    """
    Loads accounts from a JSON file.

    Expected format (array of objects):

    [
      {
        "name": "example",
        "uid": "123456789",
        "ltuid_v2": "123456789",
        "ltoken_v2": "v2_XXXXXXXXXXXXXXXXXXXX",
        "db_name": "genshin_123456789.sqlite3",
        "slack_mention": "<@U0123ABCD>",
        "tz": "Asia/Tokyo"
      },
      ...
    ]
    """
    if not path.exists():
        raise FileNotFoundError(
            f"accounts.json not found. Expected at {path}"
        )

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    accounts: List[Account] = []

    if isinstance(raw, dict):
        # allow a single-object file too
        raw_list = [raw]
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raise ValueError("accounts.json must be a JSON object or an array")

    for item in raw_list:
        if not isinstance(item, dict):
            logging.warning(
                "%s | Skipping non-dict entry in accounts.json: %r",
                utc_iso(),
                item,
            )
            continue

        name = item.get("name")
        uid = item.get("uid")
        ltuid_v2 = item.get("ltuid_v2")
        ltoken_v2 = item.get("ltoken_v2")
        db_name = item.get("db_name")
        slack_mention = item.get("slack_mention")
        tz = item.get("tz", "UTC")

        missing = []
        if not name:
            missing.append("name")
        if not uid:
            missing.append("uid")
        if not ltuid_v2:
            missing.append("ltuid_v2")
        if not ltoken_v2:
            missing.append("ltoken_v2")
        if not db_name:
            missing.append("db_name")

        if missing:
            logging.warning(
                "%s | Skipping account due to missing field(s) %s: %r",
                utc_iso(),
                ", ".join(missing),
                item,
            )
            continue

        uid_str = str(uid).strip()

        accounts.append(
            Account(
                name=str(name).strip(),
                uid=uid_str,
                ltuid_v2=str(ltuid_v2).strip(),
                ltoken_v2=str(ltoken_v2).strip(),
                db_name=str(db_name).strip(),
                slack_mention=(
                    str(slack_mention).strip()
                    if slack_mention is not None
                    else None
                ),
                tz=str(tz).strip() if tz else "UTC",
            )
        )

    if not accounts:
        raise ValueError("No valid accounts loaded from accounts.json.")

    return accounts


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────

def create_db_if_needed(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Daily notes table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_notes (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            taken_at_utc                TEXT NOT NULL,
            account_name                TEXT NOT NULL,
            resin_now                   INTEGER,
            resin_max                   INTEGER,
            resin_timer                 TEXT,
            expeditions_finished        INTEGER,
            expeditions_total           INTEGER,
            teapot_now                  INTEGER,
            teapot_max                  INTEGER,
            teapot_timer                TEXT,
            commissions_completed       INTEGER,
            commissions_total           INTEGER,
            commissions_claimed_reward  INTEGER
        )
        """
    )

    # Character stat snapshots
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS character_stats_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            taken_at_utc TEXT NOT NULL,
            account_name TEXT NOT NULL,

            character_id INTEGER NOT NULL,
            character_name TEXT NOT NULL,

            element TEXT,
            rarity INTEGER,

            level INTEGER,
            friendship INTEGER,
            constellation INTEGER,

            base_hp REAL,
            base_atk REAL,
            base_def REAL,

            total_hp REAL,
            total_atk REAL,
            total_def REAL,

            crit_rate REAL,
            crit_dmg REAL,
            elemental_mastery REAL,
            energy_recharge REAL,

            pyro_dmg REAL,
            hydro_dmg REAL,
            electro_dmg REAL,
            cryo_dmg REAL,
            anemo_dmg REAL,
            geo_dmg REAL,
            dendro_dmg REAL,
            physical_dmg REAL,

            weapon_id INTEGER,
            weapon_name TEXT,
            weapon_rarity INTEGER,
            weapon_level INTEGER,
            weapon_type INTEGER,
            weapon_refinement INTEGER
        )
        """
    )

    conn.commit()
    return conn


def _load_character_batch(cur, account_name: str, taken_at_utc: str):
    """
    Load one snapshot batch (all chars for a given taken_at_utc) into a dict:
    { character_name: { level, friendship, constellation, weapon_name, weapon_level, weapon_refinement } }
    """
    rows = cur.execute(
        """
        SELECT
            character_name,
            level,
            friendship,
            constellation,
            weapon_name,
            weapon_level,
            weapon_refinement
        FROM character_stats_snapshots
        WHERE account_name = ? AND taken_at_utc = ?
        """,
        (account_name, taken_at_utc),
    ).fetchall()

    batch: dict[str, dict] = {}

    for (
        name,
        level,
        friendship,
        constellation,
        weapon_name,
        weapon_level,
        weapon_refinement,
    ) in rows:
        batch[name] = {
            "level": level or 0,
            "friendship": friendship or 0,
            "constellation": constellation or 0,
            "weapon_name": weapon_name or "",
            "weapon_level": weapon_level or 0,
            "weapon_refinement": weapon_refinement or 0,
        }

    return batch


def _format_duration_short(seconds: int | None) -> str:
    """Return things like '3h 12m', '45m', '2h', or 'ready'."""
    if seconds is None:
        return "?"
    if seconds <= 0:
        return "ready"

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")

    return " ".join(parts) if parts else "less than 1m"


def _parse_recovery_seconds(value):
    """
    Try to turn whatever genshin gives us into 'seconds until full'.
    Handles int, str('1234'), or datetime.
    """
    if value is None:
        return None

    import datetime as _dt

    # int or float → assume 'seconds remaining'
    if isinstance(value, (int, float)):
        return int(value)

    # string from API like "12345"
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # simple int string
        try:
            return int(s)
        except Exception:
            pass
        # maybe ISO datetime string
        try:
            iso = s.replace("Z", "+00:00")
            target_dt = _dt.datetime.fromisoformat(iso)
            now = _dt.datetime.now(target_dt.tzinfo or _dt.timezone.utc)
            return int((target_dt - now).total_seconds())
        except Exception:
            return None

    # datetime → time difference from now
    if isinstance(value, _dt.datetime):
        now = _dt.datetime.now(value.tzinfo or _dt.timezone.utc)
        delta = (value - now).total_seconds()
        return int(delta)

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Daily notes → DB + Slack
# ──────────────────────────────────────────────────────────────────────────────

def store_daily_notes(
    conn: sqlite3.Connection,
    account: Account,
    notes,
    reason_parts=None,
    slack_webhook_url: str | None = None,
):
    """
    Store the current daily notes in the SQLite DB and optionally
    post a summary to Slack (with resin/teapot timers).
    """
    cur = conn.cursor()
    now_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # --- ensure the daily_notes table exists, with names matching INSERT ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_notes (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            taken_at_utc                TEXT NOT NULL,
            account_name                TEXT NOT NULL,
            resin_now                   INTEGER,
            resin_max                   INTEGER,
            resin_timer                 TEXT,
            expeditions_finished        INTEGER,
            expeditions_total           INTEGER,
            teapot_now                  INTEGER,
            teapot_max                  INTEGER,
            teapot_timer                TEXT,
            commissions_completed       INTEGER,
            commissions_total           INTEGER,
            commissions_claimed_reward  INTEGER
        )
        """
    )

    # ----------------- field extraction from notes --------------------------
    resin_now = getattr(notes, "current_resin", None)
    resin_max = getattr(notes, "max_resin", None)

    expeditions = getattr(notes, "expeditions", None) or []
    exp_finished = sum(1 for e in expeditions if getattr(e, "finished", False))
    exp_total = len(expeditions)

    realm_currency = getattr(notes, "current_realm_currency", None)
    realm_max = getattr(notes, "max_realm_currency", None)

    commissions_done = getattr(notes, "finished_commissions", 0) or 0
    commissions_total = getattr(notes, "max_commissions", 4) or 4
    commissions_claimed = bool(getattr(notes, "claimed_commission_reward", False))

    # Recovery info from API
    resin_recovery_raw = (
        getattr(notes, "resin_recovery_time", None)
        or getattr(notes, "resin_recovery_seconds", None)
    )
    teapot_recovery_raw = (
        getattr(notes, "realm_currency_recovery_time", None)
        or getattr(notes, "home_coin_recovery_time", None)
        or getattr(notes, "realm_recovery_time", None)
    )

    # Convert to seconds (helper)
    resin_full_secs = _parse_recovery_seconds(resin_recovery_raw)
    teapot_full_secs = _parse_recovery_seconds(teapot_recovery_raw)

    def _format_timer(seconds: int | None, current, maximum) -> str:
        try:
            if maximum is not None and current is not None and current >= maximum:
                return "Full"
            if seconds is None:
                return "Unknown"
            if seconds <= 0:
                return "Full"

            hours = seconds // 3600
            minutes = (seconds % 3600) // 60

            parts = []
            if hours:
                parts.append(f"{hours}h")
            if minutes or not parts:
                parts.append(f"{minutes}m")

            return "in " + "".join(parts)
        except Exception:
            return "Unknown"

    resin_timer_str = _format_timer(resin_full_secs, resin_now, resin_max)
    teapot_timer_str = _format_timer(teapot_full_secs, realm_currency, realm_max)

    # Optional: exact clock time when resin will be full, using account.tz
    resin_full_eta_str = None
    try:
        if resin_full_secs is not None and resin_full_secs > 0:
            now_utc_dt = dt.datetime.now(dt.timezone.utc)
            full_utc = now_utc_dt + dt.timedelta(seconds=resin_full_secs)

            try:
                user_tz = zoneinfo.ZoneInfo(account.tz)
            except Exception:
                user_tz = dt.timezone.utc

            full_local = full_utc.astimezone(user_tz)
            resin_full_eta_str = full_local.strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        resin_full_eta_str = None

    # ----------------- INSERT row into daily_notes --------------------------
    cur.execute(
        """
        INSERT INTO daily_notes (
            taken_at_utc,
            account_name,
            resin_now,
            resin_max,
            resin_timer,
            expeditions_finished,
            expeditions_total,
            teapot_now,
            teapot_max,
            teapot_timer,
            commissions_completed,
            commissions_total,
            commissions_claimed_reward
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now_utc,
            account.name,
            resin_now,
            resin_max,
            resin_timer_str,
            exp_finished,
            exp_total,
            realm_currency,
            realm_max,
            teapot_timer_str,
            commissions_done,
            commissions_total,
            int(commissions_claimed),
        ),
    )

    conn.commit()

    # If no Slack webhook configured, we're done
    if not slack_webhook_url:
        return

    if reason_parts is None:
        reason_parts = []

    # ── Build Slack message ───────────────────────────────────────────────────
    lines: list[str] = []

    # Header with optional @mention
    if getattr(account, "slack_mention", None):
        lines.append(f"{account.slack_mention} Genshin Daily Notes — {account.name}")
    else:
        lines.append(f"Genshin Daily Notes — {account.name}")

    # Resin with timer
    if resin_now is not None and resin_max is not None:
        lines.append(f"Resin: {resin_now}/{resin_max} ({resin_timer_str})")
    else:
        lines.append(f"Resin: unknown ({resin_timer_str})")

    # Exact resin full time (if we could compute it)
    if resin_full_eta_str:
        lines.append(f"Resin full at: {resin_full_eta_str}")

    # Expeditions
    lines.append(f"Expeditions finished: {exp_finished}/{exp_total}")

    # Teapot with timer
    if realm_currency is not None and realm_max is not None:
        lines.append(
            f"Teapot currency: {realm_currency}/{realm_max} ({teapot_timer_str})"
        )
    else:
        lines.append(f"Teapot currency: unknown ({teapot_timer_str})")

    # Commissions
    comm_line = f"Commissions: {commissions_done}/{commissions_total}"
    if commissions_claimed:
        comm_line += " (reward claimed)"
    else:
        comm_line += " (reward NOT claimed)"
    lines.append(comm_line)

    # Change reasons (first-run or diff vs last snapshot)
    if reason_parts:
        lines.append("")
        lines.append("_Changes_: " + ", ".join(reason_parts))

    payload = {"text": "\n".join(lines)}

    try:
        resp = requests.post(slack_webhook_url, json=payload, timeout=10)
        if not resp.ok:
            logging.warning(
                "%s | [%s] Slack webhook responded with %s: %s",
                utc_iso(),
                account.name,
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        logging.warning(
            "%s | [%s] Failed to post to Slack: %s",
            utc_iso(),
            account.name,
            e,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Character diff → Slack (only changes)
# ──────────────────────────────────────────────────────────────────────────────

def maybe_post_character_diff(
    conn: sqlite3.Connection,
    account: Account,
    snapshot_time_utc: str,
    slack_webhook_url: str,
):
    """
    Compare the snapshot at `snapshot_time_utc` with the previous snapshot
    for this account. If changes are found, send a Slack message that ONLY
    shows changes.
    """
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT DISTINCT taken_at_utc
        FROM character_stats_snapshots
        WHERE account_name = ? AND taken_at_utc < ?
        ORDER BY taken_at_utc DESC
        LIMIT 1
        """,
        (account.name, snapshot_time_utc),
    ).fetchone()

    if not row:
        # No previous snapshot to diff against
        return

    prev_time_utc = row[0]

    prev_batch = _load_character_batch(cur, account.name, prev_time_utc)
    curr_batch = _load_character_batch(cur, account.name, snapshot_time_utc)

    new_chars: list[str] = []
    level_ups: list[str] = []
    friendship_ups: list[str] = []
    constellation_changes: list[str] = []
    weapon_changes: list[str] = []
    weapon_level_changes: list[str] = []
    weapon_refinement_changes: list[str] = []

    all_names = sorted(set(prev_batch.keys()) | set(curr_batch.keys()))

    for name in all_names:
        old = prev_batch.get(name)
        new = curr_batch.get(name)

        # New character
        if old is None and new is not None:
            new_chars.append(
                f"• {name}: Lv{new['level']} C{new['constellation']} F{new['friendship']} — "
                f"{new['weapon_name'] or 'No weapon'} (Lv{new['weapon_level']} R{new['weapon_refinement']})"
            )
            continue

        if new is None:
            continue

        if new["level"] > (old["level"] or 0):
            level_ups.append(f"• {name}: Lv{old['level']} → Lv{new['level']}")

        if new["friendship"] > (old["friendship"] or 0):
            friendship_ups.append(
                f"• {name}: F{old['friendship']} → F{new['friendship']}"
            )

        if new["constellation"] != (old["constellation"] or 0):
            constellation_changes.append(
                f"• {name}: C{old['constellation']} → C{new['constellation']}"
            )

        old_w = old["weapon_name"] or ""
        new_w = new["weapon_name"] or ""

        if new_w != old_w:
            weapon_changes.append(
                f"• {name}: {old_w or 'No weapon'} → {new_w or 'No weapon'} "
                f"(Lv{new['weapon_level']} R{new['weapon_refinement']})"
            )
        else:
            if new["weapon_level"] > (old["weapon_level"] or 0):
                weapon_level_changes.append(
                    f"• {name} ({new_w or 'No weapon'}): "
                    f"Lv{old['weapon_level']} → Lv{new['weapon_level']}"
                )
            if new["weapon_refinement"] != (old["weapon_refinement"] or 0):
                weapon_refinement_changes.append(
                    f"• {name} ({new_w or 'No weapon'}): "
                    f"R{old['weapon_refinement']} → R{new['weapon_refinement']}"
                )

    prev_total = len(prev_batch)
    curr_total = len(curr_batch)
    total_line = None
    if prev_total != curr_total:
        delta = curr_total - prev_total
        sign = "+" if delta > 0 else ""
        total_line = f"• Characters: {prev_total} → {curr_total} ({sign}{delta})"

    if (
        not new_chars
        and not level_ups
        and not friendship_ups
        and not constellation_changes
        and not weapon_changes
        and not weapon_level_changes
        and not weapon_refinement_changes
        and not total_line
    ):
        return

    lines: list[str] = []
    mention_prefix = f"{account.slack_mention} " if getattr(account, "slack_mention", None) else ""
    lines.append(f"{mention_prefix}*Genshin Character Updates — {account.name}*")
    lines.append(f"_Snapshot: {prev_time_utc} → {snapshot_time_utc}_")
    lines.append("")

    if new_chars:
        lines.append(f"*New Characters ({len(new_chars)})*")
        lines.extend(new_chars)
        lines.append("")

    if level_ups:
        lines.append(f"*Level Ups ({len(level_ups)})*")
        lines.extend(level_ups)
        lines.append("")

    if friendship_ups:
        lines.append(f"*Friendship Gains ({len(friendship_ups)})*")
        lines.extend(friendship_ups)
        lines.append("")

    if constellation_changes:
        lines.append(f"*Constellation Changes ({len(constellation_changes)})*")
        lines.extend(constellation_changes)
        lines.append("")

    if weapon_changes:
        lines.append(f"*Weapon Changes ({len(weapon_changes)})*")
        lines.extend(weapon_changes)
        lines.append("")

    if weapon_level_changes:
        lines.append(f"*Weapon Level Ups ({len(weapon_level_changes)})*")
        lines.extend(weapon_level_changes)
        lines.append("")

    if weapon_refinement_changes:
        lines.append(f"*Refinement Changes ({len(weapon_refinement_changes)})*")
        lines.extend(weapon_refinement_changes)
        lines.append("")

    if total_line:
        lines.append("*Totals*")
        lines.append(total_line)

    payload = {"text": "\n".join(lines).strip()}

    try:
        resp = requests.post(slack_webhook_url, json=payload, timeout=10)
        if not resp.ok:
            logging.warning(
                "%s | [%s] Slack webhook (char diff) responded with %s: %s",
                utc_iso(),
                account.name,
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        logging.warning(
            "%s | [%s] Failed to post character diff to Slack: %s",
            utc_iso(),
            account.name,
            e,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Character Summary 7,30,90,365 days
# ──────────────────────────────────────────────────────────────────────────────

def post_period_summary(
    conn: sqlite3.Connection,
    account: Account,
    slack_webhook_url: str,
    label: str,
    days_back: int,
):
    """
    Build a 'changes over this period' summary Slack message.
    """
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS character_summary_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_name TEXT NOT NULL,
            period_label TEXT NOT NULL,
            last_run_utc TEXT NOT NULL
        )
        """
    )

    now_utc = dt.datetime.now(dt.timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S")

    row = cur.execute(
        """
        SELECT last_run_utc
        FROM character_summary_runs
        WHERE account_name = ? AND period_label = ?
        ORDER BY id DESC
        LIMIT 1;
        """,
        (account.name, label),
    ).fetchone()

    if row:
        try:
            last_run_dt = dt.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone.utc
            )
            delta_days = (now_utc - last_run_dt).total_seconds() / 86400.0
            if delta_days < (days_back - 1):
                return
        except Exception:
            pass

    window_start_dt = now_utc - dt.timedelta(days=days_back)
    window_start_str = window_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    window_end_str = now_str

    snap_rows = cur.execute(
        """
        SELECT DISTINCT taken_at_utc
        FROM character_stats_snapshots
        WHERE account_name = ?
          AND taken_at_utc >= ?
          AND taken_at_utc <= ?
        ORDER BY taken_at_utc ASC;
        """,
        (account.name, window_start_str, window_end_str),
    ).fetchall()

    if len(snap_rows) < 2:
        cur.execute(
            """
            INSERT INTO character_summary_runs (account_name, period_label, last_run_utc)
            VALUES (?, ?, ?)
            """,
            (account.name, label, now_str),
        )
        conn.commit()
        return

    start_time = snap_rows[0][0]
    end_time = snap_rows[-1][0]

    prev_batch = _load_character_batch(cur, account.name, start_time)
    curr_batch = _load_character_batch(cur, account.name, end_time)

    existed_before = {
        name
        for (name,) in cur.execute(
            """
            SELECT DISTINCT character_name
            FROM character_stats_snapshots
            WHERE account_name = ?
              AND taken_at_utc < ?;
            """,
            (account.name, start_time),
        ).fetchall()
    }

    new_chars: list[str] = []
    level_ups: list[str] = []
    friendship_ups: list[str] = []
    constellation_changes: list[str] = []
    weapon_changes: list[str] = []
    weapon_level_changes: list[str] = []
    weapon_refinement_changes: list[str] = []

    all_names = sorted(set(prev_batch.keys()) | set(curr_batch.keys()))

    for name in all_names:
        old = prev_batch.get(name)
        new = curr_batch.get(name)

        if new is None:
            continue

        if old is None:
            if name not in existed_before:
                new_chars.append(
                    f"• {name}: Lv{new['level']} C{new['constellation']} F{new['friendship']} — "
                    f"{new['weapon_name'] or 'No weapon'} (Lv{new['weapon_level']} R{new['weapon_refinement']})"
                )
            continue

        if new["level"] > (old["level"] or 0):
            level_ups.append(f"• {name}: Lv{old['level']} → Lv{new['level']}")

        if new["friendship"] > (old["friendship"] or 0):
            friendship_ups.append(
                f"• {name}: F{old['friendship']} → F{new['friendship']}"
            )

        if new["constellation"] != (old["constellation"] or 0):
            constellation_changes.append(
                f"• {name}: C{old['constellation']} → C{new['constellation']}"
            )

        old_w = old["weapon_name"] or ""
        new_w = new["weapon_name"] or ""

        if new_w != old_w:
            weapon_changes.append(
                f"• {name}: {old_w or 'No weapon'} → {new_w or 'No weapon'} "
                f"(Lv{new['weapon_level']} R{new['weapon_refinement']})"
            )
        else:
            if new["weapon_level"] > (old["weapon_level"] or 0):
                weapon_level_changes.append(
                    f"• {name} ({new_w or 'No weapon'}): "
                    f"Lv{old['weapon_level']} → Lv{new['weapon_level']}"
                )
            if new["weapon_refinement"] != (old["weapon_refinement"] or 0):
                weapon_refinement_changes.append(
                    f"• {name} ({new_w or 'No weapon'}): "
                    f"R{old['weapon_refinement']} → R{new['weapon_refinement']}"
                )

    prev_total = len(prev_batch)
    curr_total = len(curr_batch)
    total_line = None
    if prev_total != curr_total:
        delta = curr_total - prev_total
        sign = "+" if delta > 0 else ""
        total_line = f"• Characters: {prev_total} → {curr_total} ({sign}{delta})"

    if (
        not new_chars
        and not level_ups
        and not friendship_ups
        and not constellation_changes
        and not weapon_changes
        and not weapon_level_changes
        and not weapon_refinement_changes
        and not total_line
    ):
        cur.execute(
            """
            INSERT INTO character_summary_runs (account_name, period_label, last_run_utc)
            VALUES (?, ?, ?)
            """,
            (account.name, label, now_str),
        )
        conn.commit()
        return

    lines: list[str] = []

    mention_prefix = f"{account.slack_mention} " if getattr(account, "slack_mention", None) else ""
    lines.append(f"{mention_prefix}*Genshin Character Summary — {account.name}*")
    lines.append(f"_Period: {label} ({start_time} → {end_time})_")
    lines.append("")

    if new_chars:
        lines.append(f"*New Characters ({len(new_chars)})*")
        lines.extend(new_chars)
        lines.append("")

    if level_ups:
        lines.append(f"*Level Ups ({len(level_ups)})*")
        lines.extend(level_ups)
        lines.append("")

    if friendship_ups:
        lines.append(f"*Friendship Gains ({len(friendship_ups)})*")
        lines.extend(friendship_ups)
        lines.append("")

    if constellation_changes:
        lines.append(f"*Constellation Changes ({len(constellation_changes)})*")
        lines.extend(constellation_changes)
        lines.append("")

    if weapon_changes:
        lines.append(f"*Weapon Changes ({len(weapon_changes)})*")
        lines.extend(weapon_changes)
        lines.append("")

    if weapon_level_changes:
        lines.append(f"*Weapon Level Ups ({len(weapon_level_changes)})*")
        lines.extend(weapon_level_changes)
        lines.append("")

    if weapon_refinement_changes:
        lines.append(f"*Refinement Changes ({len(weapon_refinement_changes)})*")
        lines.extend(weapon_refinement_changes)
        lines.append("")

    if total_line:
        lines.append("*Totals*")
        lines.append(total_line)

    payload = {"text": "\n".join(lines).strip()}

    try:
        resp = requests.post(slack_webhook_url, json=payload, timeout=10)
        if not resp.ok:
            logging.warning(
                "%s | [%s] Slack webhook (period summary %s) responded with %s: %s",
                utc_iso(),
                account.name,
                label,
                resp.status_code,
                resp.text,
            )
    except Exception as e:
        logging.warning(
            "%s | [%s] Failed to post period summary (%s) to Slack: %s",
            utc_iso(),
            account.name,
            label,
            e,
        )

    cur.execute(
        """
        INSERT INTO character_summary_runs (account_name, period_label, last_run_utc)
        VALUES (?, ?, ?)
        """,
        (account.name, label, now_str),
    )
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Store character snapshots
# ──────────────────────────────────────────────────────────────────────────────

def store_character_stats_snapshots(conn, account, characters):
    cur = conn.cursor()
    now_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for c in characters:
        c_dict = c.model_dump()
        weapon = c_dict.get("weapon") or {}

        cur.execute(
            """
            INSERT INTO character_stats_snapshots (
                taken_at_utc,
                account_name,

                character_id,
                character_name,

                element,
                rarity,

                level,
                friendship,
                constellation,

                base_hp,
                base_atk,
                base_def,

                total_hp,
                total_atk,
                total_def,

                crit_rate,
                crit_dmg,
                elemental_mastery,
                energy_recharge,

                pyro_dmg,
                hydro_dmg,
                electro_dmg,
                cryo_dmg,
                anemo_dmg,
                geo_dmg,
                dendro_dmg,
                physical_dmg,

                weapon_id,
                weapon_name,
                weapon_rarity,
                weapon_level,
                weapon_type,
                weapon_refinement
            ) VALUES (
                ?, ?,  ?, ?,  ?, ?,  ?, ?, ?,  ?, ?, ?,  ?, ?, ?,  ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?
            )
            """,
            (
                now_utc,
                account.name,

                c_dict.get("id"),
                c_dict.get("name"),

                c_dict.get("element"),
                c_dict.get("rarity"),

                c_dict.get("level"),
                c_dict.get("friendship"),
                c_dict.get("constellation"),

                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,

                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,

                weapon.get("id"),
                weapon.get("name"),
                weapon.get("rarity"),
                weapon.get("level"),
                weapon.get("type"),
                weapon.get("refinement"),
            ),
        )

    conn.commit()
    return now_utc


# ──────────────────────────────────────────────────────────────────────────────
# Genshin client + fetch helpers
# ──────────────────────────────────────────────────────────────────────────────

def make_client_for_account(account: Account) -> genshin.Client:
    cookies = {
        "ltuid_v2": account.ltuid_v2,
        "ltoken_v2": account.ltoken_v2,
    }
    client = genshin.Client(cookies=cookies)
    return client


async def fetch_daily_notes_for_account(account: Account):
    client = make_client_for_account(account)
    try:
        notes = await client.get_genshin_notes(account.uid)
        return notes
    except genshin_errors.InvalidCookies as e:
        logging.error(
            "%s | [%s] Invalid cookies (please login again / refresh ltuid_v2 + ltoken_v2): %s",
            utc_iso(),
            account.name,
            e,
        )
        return None
    except genshin_errors.GenshinException as e:
        logging.error(
            "%s | [%s] Genshin API error while fetching notes: %s",
            utc_iso(),
            account.name,
            e,
        )
        return None
    except Exception as e:
        logging.error(
            "%s | [%s] Unexpected error while fetching notes: %s",
            utc_iso(),
            account.name,
            e,
        )
        return None


async def fetch_characters_for_account(account: Account):
    client = make_client_for_account(account)
    try:
        characters = await client.get_genshin_characters(account.uid)
        return characters
    except genshin_errors.GenshinException as e:
        logging.error(
            "%s | [%s] Genshin API error while fetching characters: %s",
            utc_iso(),
            account.name,
            e,
        )
        return None
    except Exception as e:
        logging.error(
            "%s | [%s] Unexpected error while fetching characters: %s",
            utc_iso(),
            account.name,
            e,
        )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-account + all-accounts runners
# ──────────────────────────────────────────────────────────────────────────────

async def run_once_for_account(
    account: Account,
    data_dir: Path,
    slack_webhook_url: Optional[str],
):
    logging.info("%s | Processing account: %s (UID hidden)", utc_iso(), account.name)

    db_path = data_dir / account.db_name
    conn = create_db_if_needed(db_path)

    try:
        # 1) Daily notes → DB + Slack
        notes = await fetch_daily_notes_for_account(account)
        if notes is None:
            logging.info(
                "%s | [%s] Failed to fetch daily notes, skipping store.",
                utc_iso(),
                account.name,
            )
        else:
            store_daily_notes(conn, account, notes, slack_webhook_url=slack_webhook_url)

        # 2) Character stats snapshot
        characters = await fetch_characters_for_account(account)
        snapshot_time_utc = None
        if characters:
            snapshot_time_utc = store_character_stats_snapshots(conn, account, characters)
        else:
            logging.info(
                "%s | [%s] Could not fetch characters for snapshot (see earlier logs).",
                utc_iso(),
                account.name,
            )

        # 3) Character diff Slack message
        if slack_webhook_url and snapshot_time_utc:
            maybe_post_character_diff(
                conn=conn,
                account=account,
                snapshot_time_utc=snapshot_time_utc,
                slack_webhook_url=slack_webhook_url,
            )

        # 4) Period summaries (7/30/90/365 days)
        if slack_webhook_url:
            post_period_summary(
                conn=conn,
                account=account,
                slack_webhook_url=slack_webhook_url,
                label="Last 7 days",
                days_back=7,
            )
            post_period_summary(
                conn=conn,
                account=account,
                slack_webhook_url=slack_webhook_url,
                label="Last 30 days",
                days_back=30,
            )
            post_period_summary(
                conn=conn,
                account=account,
                slack_webhook_url=slack_webhook_url,
                label="Last 90 days",
                days_back=90,
            )
            post_period_summary(
                conn=conn,
                account=account,
                slack_webhook_url=slack_webhook_url,
                label="Last 365 days",
                days_back=365,
            )

    finally:
        conn.close()


async def run_once_all_accounts(
    accounts: List[Account],
    data_dir: Path,
    slack_webhook_url: Optional[str],
):
    for account in accounts:
        await run_once_for_account(account, data_dir, slack_webhook_url)


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

def main_loop() -> None:
    load_env_files()

    errors: List[str] = []

    # Required env
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        errors.append("SLACK_WEBHOOK_URL is not set.")

    # Optional env
    data_dir = Path(os.getenv("DATA_DIR", "/data"))
    accounts_file_env = os.getenv("ACCOUNTS_FILE", "")
    if accounts_file_env:
        accounts_path = Path(accounts_file_env)
        if not accounts_path.is_absolute():
            accounts_path = data_dir / accounts_file_env
    else:
        accounts_path = data_dir / "accounts.json"

    loop_seconds_str = os.getenv("LOOP_INTERVAL_SECONDS", "3600")
    try:
        loop_seconds = int(loop_seconds_str)
        if loop_seconds <= 0:
            raise ValueError()
    except ValueError:
        errors.append(
            f"LOOP_INTERVAL_SECONDS must be a positive integer, got: {loop_seconds_str}"
        )
        loop_seconds = 3600  # fallback

    accounts: List[Account] = []
    try:
        accounts = load_accounts_from_json(accounts_path)
    except Exception as e:
        errors.append(f"Failed to load accounts.json: {e}")

    if errors:
        raise RuntimeError("Config error:\n - " + "\n - ".join(errors))

    logging.info(
        "%s | Starting hoyo-slack 2.0 (multi-account, local).",
        utc_iso(),
    )
    logging.info(
        "%s | Loaded %d account(s).",
        utc_iso(),
        len(accounts),
    )
    logging.info(
        "%s | Data dir: %s | Accounts file: %s | Interval: %d seconds",
        utc_iso(),
        data_dir,
        accounts_path,
        loop_seconds,
    )

    while True:
        asyncio.run(
            run_once_all_accounts(
                accounts=accounts,
                data_dir=data_dir,
                slack_webhook_url=slack_webhook_url,
            )
        )
        logging.info(
            "%s | Entering loop: running again in %d seconds.",
            utc_iso(),
            loop_seconds,
        )
        time.sleep(loop_seconds)


if __name__ == "__main__":
    main_loop()