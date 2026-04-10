import os
import json
import logging
import asyncio
import random
import re
import difflib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List, Set, Tuple, Literal, Awaitable, Callable

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
import aiosqlite
import sqlite3

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = DATA_DIR / "config.json"
ALT_CONFIG_PATH = BASE_DIR / "config.json"  # optional fallback
DB_PATH = DATA_DIR / "trader.db"
ITEMS_PATH = BASE_DIR / "items.json"  # item catalog for /trade autocomplete

# Payment rails (NOT trade). USD is still the baseline currency.
ALLOWED_PAYMENT_METHODS: Set[str] = {"paypal", "btc", "ltc", "eth"}
ALLOWED_SELL_PAYOUTS: Set[str] = {"paypal", "btc", "ltc", "eth"}

# Ticket retention
TICKET_DELETE_AFTER_DAYS = 7

# Max autocomplete results Discord allows
AC_MAX = 25

# Static IDs (server-specific)
MIDDLEMAN_ROLE_ID = 1468393696109007049
ADMIN_ROLE_ID = 1468393977777754334
SELLER_ROLE_ID = 1467939794142232748
REP_CHANNEL_ID = 1468394241733689478
REP_REMINDER_CHANNEL_ID = 1467939025192091793  # channel to remind users where to /rep
TRADE_BOARD_CHANNEL_ID = 1468394341255876658
TRADE_HISTORY_CHANNEL_ID = 1468394523209109534

# /update changelog target
CHANGELOG_CHANNEL_ID = 1471006568123465919
CHANGELOG_ROLE_ID = 1471007068533031077

TRADE_OFFER_EXPIRE_DAYS = 7

TRADE_OFFER_REMOVE_COOLDOWN_SECONDS = 180

# in-memory cooldowns (user_id -> last_remove_monotonic)
_TRADE_OFFER_REMOVE_LAST: Dict[int, float] = {}

def load_config() -> Dict[str, Any]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Prefer data/config.json, but allow a fallback config.json next to bot.py
    if not CONFIG_PATH.exists() and ALT_CONFIG_PATH.exists():
        try:
            cfg = json.loads(ALT_CONFIG_PATH.read_text(encoding="utf-8"))
            # Persist into data/ so the bot has a single canonical location
            save_config(cfg)
        except Exception:
            pass
    if not CONFIG_PATH.exists():
        default = {
            "listings_channel_id": None,
            "tickets_category_id": None,
            "archived_category_id": None,
            "seller_role_id": None,
            "listings_message_id": None,
            "sync_on_startup": False,

            # Rep embed color cycle state
            "rep_embed_color_index": 0,

            # Channel to announce rep milestones (falls back to system/#general if null)
            "general_announce_channel_id": None,

            # /update changelog settings
            "bot_version": "dev",
            "changelog_channel_id": None,
            "changelog_role_id": None,

            # Static website community data publisher. Disabled until the GitHub Pages repo is cloned
            # onto the bot host and site_repo_path is set to that checkout.
            "site_publish_enabled": False,
            "site_repo_path": None,
            "site_publish_interval_minutes": 30,
            "site_git_push": False,
            "site_invite_url": "https://discord.gg/raidertrader",

            # Admin gating is ROLE-ONLY (no permission-based checks).
            "admin_role_id": ADMIN_ROLE_ID,

            # Cosmetic rep milestone roles (stacking). Only trade reps ("trade_partner") earn these roles.
            # Fill role_id values with your cosmetic role IDs.
            "rep_roles": {
                "trade": [
                    {"min_reps": 1, "role_id": 0},
                    {"min_reps": 5, "role_id": 0},
                    {"min_reps": 10, "role_id": 0},
                    {"min_reps": 25, "role_id": 0},
                    {"min_reps": 50, "role_id": 0},
                    {"min_reps": 100, "role_id": 0},
                    {"min_reps": 250, "role_id": 0},
                    {"min_reps": 500, "role_id": 0},
                    {"min_reps": 1000, "role_id": 0},
                ]
            },
        }
        CONFIG_PATH.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default

    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if "sync_on_startup" not in cfg:
        cfg["sync_on_startup"] = False
        save_config(cfg)

    # Backfill newer config keys
    if "admin_role_id" not in cfg:
        cfg["admin_role_id"] = ADMIN_ROLE_ID
        save_config(cfg)
    if "rep_embed_color_index" not in cfg:
        cfg["rep_embed_color_index"] = 0
        save_config(cfg)
    if "general_announce_channel_id" not in cfg:
        cfg["general_announce_channel_id"] = None
        save_config(cfg)
    if "bot_version" not in cfg:
        cfg["bot_version"] = "dev"
        save_config(cfg)
    if "changelog_channel_id" not in cfg:
        cfg["changelog_channel_id"] = None
        save_config(cfg)
    if "changelog_role_id" not in cfg:
        cfg["changelog_role_id"] = None
        save_config(cfg)
    if "site_publish_enabled" not in cfg:
        cfg["site_publish_enabled"] = False
        save_config(cfg)
    if "site_repo_path" not in cfg:
        cfg["site_repo_path"] = None
        save_config(cfg)
    if "site_publish_interval_minutes" not in cfg:
        cfg["site_publish_interval_minutes"] = 30
        save_config(cfg)
    if "site_git_push" not in cfg:
        cfg["site_git_push"] = False
        save_config(cfg)
    if "site_invite_url" not in cfg:
        cfg["site_invite_url"] = "https://discord.gg/raidertrader"
        save_config(cfg)
    if "rep_roles" not in cfg or not isinstance(cfg.get("rep_roles"), dict):
        cfg["rep_roles"] = {"trade": []}
        save_config(cfg)
    return cfg


def save_config(cfg: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def require_env_int(name: str) -> int:
    v = os.getenv(name)
    if not v or not v.strip().isdigit():
        raise RuntimeError(f"Missing or invalid {name} in .env")
    return int(v.strip())


def get_admin_role_id(obj: Any = None) -> int:
    """Resolve the configured admin role id. Role-only gating everywhere."""
    try:
        if obj is None:
            cfg = load_config()
        elif isinstance(obj, dict):
            cfg = obj
        else:
            cfg = getattr(obj, "cfg", None) or load_config()
        rid = int(cfg.get("admin_role_id") or 0)
        return rid or ADMIN_ROLE_ID
    except Exception:
        return ADMIN_ROLE_ID


def is_admin(interaction: discord.Interaction) -> bool:
    if not interaction.user or not isinstance(interaction.user, discord.Member):
        return False
    bot = getattr(interaction, "client", None)
    role_id = get_admin_role_id(bot)
    return has_role(interaction.user, role_id)


def has_role(member: discord.Member, role_id: int) -> bool:
    return any(r.id == role_id for r in member.roles)


def is_admin_member(member: discord.Member) -> bool:
    role_id = get_admin_role_id(getattr(member, "guild", None))
    # get_admin_role_id can accept a dict/bot; guild doesn't have cfg, so it falls back to file.
    # Prefer constant if config not present.
    role_id = role_id or ADMIN_ROLE_ID
    return has_role(member, role_id)


def is_seller_member(member: discord.Member) -> bool:
    return has_role(member, SELLER_ROLE_ID)


def user_search_tag(user: discord.abc.User) -> str:
    # Non-mention, searchable
    name = getattr(user, "display_name", None) or getattr(user, "name", "user")
    return f"{name} ({user.id})"


async def safe_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    m = guild.get_member(user_id)
    if m:
        return m
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None


def parse_payment_methods(raw: str, allowed: Set[str] = ALLOWED_PAYMENT_METHODS) -> Optional[List[str]]:
    """
    Accepts comma-separated string like: "btc, ltc"
    Returns normalized unique list like: ["btc", "ltc"] or None if invalid/empty.
    """
    if not raw:
        return None
    parts = [p.strip().lower() for p in raw.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return None

    unique: List[str] = []
    seen = set()
    for p in parts:
        if p not in allowed:
            return None
        if p not in seen:
            unique.append(p)
            seen.add(p)
    return unique if unique else None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def safe_int(s: Any, default: int = 0) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default


def safe_float(s: str, default: float = 0.0) -> float:
    try:
        return float(str(s).strip())
    except Exception:
        return default


def load_items_cache() -> List[str]:
    """
    Optional list of all in-game items for autocomplete (used by /sell and /trade offering).
    Put a JSON array of strings into data/items.json:
      ["Deadline Blueprint", "Anvil Splitter", ...]
    """
    try:
        if ITEMS_PATH.exists():
            data = json.loads(ITEMS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                out = []
                for x in data:
                    if isinstance(x, str) and x.strip():
                        out.append(x.strip())
                seen = set()
                uniq = []
                for x in out:
                    xl = x.lower()
                    if xl not in seen:
                        uniq.append(x)
                        seen.add(xl)
                return uniq
    except Exception:
        pass
    return []


def normalize_item_name(name: str) -> str:
    # Canonical form for comparisons (case-insensitive, whitespace-normalized).
    s = str(name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def resolve_item_from_cache(items: List[str], query: str) -> Optional[str]:
    """Return the canonical item string from cache matching query (case/space-insensitive)."""
    qn = normalize_item_name(query)
    if not qn:
        return None
    for it in items or []:
        if normalize_item_name(it) == qn:
            return it
    return None


def save_items_cache(items: List[str]) -> None:
    """Persist items to data/items.json as a JSON array of strings."""
    try:
        ITEMS_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Deduplicate case-insensitively while preserving first-seen casing.
        out: List[str] = []
        seen: set[str] = set()
        for x in items or []:
            if not isinstance(x, str):
                continue
            s = x.strip()
            if not s:
                continue
            key = normalize_item_name(s)
            if key in seen:
                continue
            out.append(s)
            seen.add(key)
        ITEMS_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception:
        pass




# ---------- Blueprint tracking ----------

BLUEPRINT_SUFFIX = "Blueprint"
BP_WIPE_CONFIRM_SECONDS = 10
BP_MAX_DM_NOTIFICATIONS_PER_POST = 50  # safety cap

def is_blueprint_item(name: str) -> bool:
    return str(name or "").strip().lower().endswith(BLUEPRINT_SUFFIX.lower())

def get_all_blueprints_from_items_cache() -> List[str]:
    items = load_items_cache()
    bps = [x for x in items if is_blueprint_item(x)]
    # stable sort, case-insensitive
    bps.sort(key=lambda s: normalize_item_name(s))
    return bps

def blueprint_key(name: str) -> str:
    # normalized key for DB storage and comparisons
    return normalize_item_name(name)

async def db_ensure_bp_user(bot: "ArcTraderBot", user_id: int) -> None:
    if bot.db is None:
        return
    await bot.db.execute(
        "INSERT OR IGNORE INTO bp_users(user_id, dms_enabled, created_at) VALUES(?, 1, ?);",
        (int(user_id), dt_to_iso(now_utc())),
    )

async def db_bp_owned_keys(bot: "ArcTraderBot", user_id: int) -> Set[str]:
    if bot.db is None:
        return set()
    rows = await bot.db.execute_fetchall(
        "SELECT blueprint_key FROM bp_owned WHERE user_id=?;",
        (int(user_id),),
    )
    return {str(r[0]) for r in rows if r and r[0]}

async def db_bp_add_many(bot: "ArcTraderBot", user_id: int, blueprints: List[str]) -> int:
    if bot.db is None:
        return 0
    await db_ensure_bp_user(bot, user_id)
    n = 0
    for bp in blueprints:
        bp_name = str(bp).strip()
        if not bp_name:
            continue
        await bot.db.execute(
            "INSERT OR IGNORE INTO bp_owned(user_id, blueprint_key, blueprint_name, added_at) VALUES(?, ?, ?, ?);",
            (int(user_id), blueprint_key(bp_name), bp_name[:200], dt_to_iso(now_utc())),
        )
        n += 1
    await bot.db.commit()
    return n

async def db_bp_remove_many(bot: "ArcTraderBot", user_id: int, blueprints: List[str]) -> int:
    if bot.db is None:
        return 0
    await db_ensure_bp_user(bot, user_id)
    n = 0
    for bp in blueprints:
        k = blueprint_key(bp)
        if not k:
            continue
        cur = await bot.db.execute("DELETE FROM bp_owned WHERE user_id=? AND blueprint_key=?;", (int(user_id), k))
        n += cur.rowcount if cur else 0
    await bot.db.commit()
    return n

async def db_bp_wipe(bot: "ArcTraderBot", user_id: int) -> int:
    if bot.db is None:
        return 0
    await db_ensure_bp_user(bot, user_id)
    cur = await bot.db.execute("DELETE FROM bp_owned WHERE user_id=?;", (int(user_id),))
    await bot.db.commit()
    return int(cur.rowcount or 0)

def parse_bp_list(raw: str) -> List[str]:
    # Accept commas or newlines
    if not raw:
        return []
    parts = re.split(r"[,\n]+", str(raw))
    out: List[str] = []
    for p in parts:
        s = p.strip()
        if s:
            out.append(s)
    return out

def resolve_blueprints(inputs: List[str], all_bps: List[str]) -> Tuple[List[str], List[str]]:
    """
    Resolve user inputs to canonical blueprint names.

    - Exact match is done on a normalized "key" (casefold + stripped punctuation/whitespace).
    - If no exact match, tries a conservative fuzzy match against keys (to tolerate minor typos).

    Returns (resolved, unknown_inputs).
    """
    cache = all_bps or []
    resolved: List[str] = []
    unknown: List[str] = []

    # Map key -> canonical
    key_to_name = {blueprint_key(x): x for x in cache}
    keys = list(key_to_name.keys())

    for x in inputs:
        k = blueprint_key(x)
        if not k:
            continue
        if k in key_to_name:
            resolved.append(key_to_name[k])
            continue

        # Conservative fuzzy match (minor typos only)
        match = difflib.get_close_matches(k, keys, n=1, cutoff=0.86)
        if match:
            resolved.append(key_to_name[match[0]])
        else:
            unknown.append(x)

    # Dedup resolved
    seen: Set[str] = set()
    uniq: List[str] = []
    for x in resolved:
        k = blueprint_key(x)
        if k in seen:
            continue
        uniq.append(x)
        seen.add(k)
    return uniq, unknown


TRADE_ITEM_LINE_RE = re.compile(r"^\s*(.+?)(?:\s*[x×]\s*(\d+))?\s*$")

def parse_trade_item_list(raw: str) -> List[Tuple[str, int]]:
    """Parse newline/comma separated trade items like 'Railgun Blueprint x2'."""
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[\n,]+", str(raw)) if p.strip()]
    out: List[Tuple[str, int]] = []
    for part in parts:
        m = TRADE_ITEM_LINE_RE.match(part)
        if not m:
            continue
        name = str(m.group(1) or "").strip()
        qty = safe_int(m.group(2) or "1", 1)
        if name and qty > 0:
            out.append((name, qty))
    return out

def resolve_trade_items(inputs: List[Tuple[str, int]], all_items: List[str]) -> Tuple[List[Tuple[str, int]], List[str]]:
    """Resolve parsed trade items against the item catalog using normalized exact match + conservative fuzzy fallback."""
    cache = all_items or []
    key_to_name = {normalize_item_name(x): x for x in cache}
    keys = list(key_to_name.keys())
    resolved: List[Tuple[str, int]] = []
    unknown: List[str] = []

    for raw_name, qty in inputs:
        k = normalize_item_name(raw_name)
        if not k or qty <= 0:
            continue
        canon = key_to_name.get(k)
        if canon is None:
            match = difflib.get_close_matches(k, keys, n=1, cutoff=0.86)
            canon = key_to_name.get(match[0]) if match else None
        if canon is None:
            unknown.append(raw_name)
            continue
        resolved.append((canon, int(qty)))

    merged: Dict[str, Tuple[str, int]] = {}
    for canon, qty in resolved:
        key = normalize_item_name(canon)
        if key in merged:
            prev_name, prev_qty = merged[key]
            merged[key] = (prev_name, prev_qty + int(qty))
        else:
            merged[key] = (canon, int(qty))
    return list(merged.values()), unknown

def format_trade_items(items: List[Tuple[str, int]], *, limit: int = 8) -> Tuple[str, int]:
    """Return a bullet list plus remaining count for embed display."""
    items = items or []
    if not items:
        return "(none)", 0
    head = items[:limit]
    lines = [f"• {name} ×{int(qty)}" for name, qty in head]
    rem = max(0, len(items) - len(head))
    return "\n".join(lines), rem


def format_trade_items_inline(items: List[Tuple[str, int]], *, limit: int = 12) -> Tuple[str, int]:
    """Return a comma-separated trade item list plus remaining count for compact display."""
    items = items or []
    if not items:
        return "(none)", 0
    head = items[:limit]
    parts = [f"{name} x{int(qty)}" for name, qty in head]
    rem = max(0, len(items) - len(head))
    return ", ".join(parts), rem

async def db_set_trade_offer_items(bot: "ArcTraderBot", offer_id: int, have_items: List[Tuple[str, int]], want_items: List[Tuple[str, int]]) -> None:
    if bot.db is None:
        return
    await bot.db.execute("DELETE FROM trade_offer_items WHERE offer_id=?;", (int(offer_id),))
    sort_order = 0
    for side, items in (("have", have_items), ("want", want_items)):
        for name, qty in items:
            await bot.db.execute(
                "INSERT INTO trade_offer_items(offer_id, side, item_name, qty, sort_order) VALUES(?,?,?,?,?);",
                (int(offer_id), side, str(name)[:200], int(qty), int(sort_order)),
            )
            sort_order += 1
    await bot.db.commit()

async def db_get_trade_offer_items(bot: "ArcTraderBot", offer_id: int) -> Dict[str, List[Tuple[str, int]]]:
    out: Dict[str, List[Tuple[str, int]]] = {"have": [], "want": []}
    if bot.db is None:
        return out
    rows = await bot.db.execute_fetchall(
        "SELECT side, item_name, qty FROM trade_offer_items WHERE offer_id=? ORDER BY sort_order ASC, id ASC;",
        (int(offer_id),),
    )
    for side, name, qty in rows:
        side_s = str(side or "")
        if side_s not in out:
            out[side_s] = []
        out[side_s].append((str(name), int(qty or 1)))
    return out

async def db_trade_offer_looks_like_bp_need(bot: "ArcTraderBot", offer_id: int) -> bool:
    bundle = await db_get_trade_offer_items(bot, int(offer_id))
    have_items = bundle.get("have") or []
    want_items = bundle.get("want") or []
    if have_items or len(want_items) <= 1:
        return False
    return all(is_blueprint_item(str(name)) for name, _qty in want_items)

async def db_set_ticket_trade_items(
    bot: "ArcTraderBot",
    ticket_id: int,
    creator_items: List[Tuple[str, int]],
    joiner_items: Optional[List[Tuple[str, int]]] = None,
) -> None:
    if bot.db is None:
        return
    await bot.db.execute("DELETE FROM ticket_trade_items WHERE ticket_id=?;", (int(ticket_id),))
    sort_order = 0
    for owner_role, items in (("creator", creator_items), ("joiner", joiner_items or [])):
        for name, qty in items:
            await bot.db.execute(
                "INSERT INTO ticket_trade_items(ticket_id, owner_role, item_name, qty, sort_order) VALUES(?,?,?,?,?);",
                (int(ticket_id), owner_role, str(name)[:200], int(qty), int(sort_order)),
            )
            sort_order += 1
    await bot.db.commit()

async def db_get_ticket_trade_items(bot: "ArcTraderBot", ticket_id: int) -> Dict[str, List[Tuple[str, int]]]:
    out: Dict[str, List[Tuple[str, int]]] = {"creator": [], "joiner": []}
    if bot.db is None:
        return out
    rows = await bot.db.execute_fetchall(
        "SELECT owner_role, item_name, qty FROM ticket_trade_items WHERE ticket_id=? ORDER BY sort_order ASC, id ASC;",
        (int(ticket_id),),
    )
    for owner_role, name, qty in rows:
        role_s = str(owner_role or "")
        if role_s not in out:
            out[role_s] = []
        out[role_s].append((str(name), int(qty or 1)))
    return out


async def maybe_congrats_all_blueprints(bot: "ArcTraderBot", guild: discord.Guild, member: discord.Member) -> None:
    if bot.db is None:
        return
    all_bps = get_all_blueprints_from_items_cache()
    total = len(all_bps)
    if total <= 0:
        return
    owned = await bot.db.execute_fetchall("SELECT COUNT(*) FROM bp_owned WHERE user_id=?;", (int(member.id),))
    have_count = int(owned[0][0]) if owned else 0
    if have_count < total:
        return
    # Gate announcement so it's only sent once
    cur = await bot.db.execute(
        "INSERT OR IGNORE INTO bp_milestones(user_id, milestone, announced_at) VALUES(?, ?, ?);",
        (int(member.id), int(total), dt_to_iso(now_utc())),
    )
    await bot.db.commit()
    if int(getattr(cur, 'rowcount', 0) or 0) == 0:
        return

    # Find announce channel
    ch_id = safe_int(getattr(bot, "cfg", {}).get("general_announce_channel_id") or 0, 0)
    ch: Optional[discord.TextChannel] = None
    if ch_id:
        c = guild.get_channel(int(ch_id))
        if isinstance(c, discord.TextChannel):
            ch = c
    if ch is None and isinstance(guild.system_channel, discord.TextChannel):
        ch = guild.system_channel
    if ch is None:
        # best-effort: channel named "general"
        for c in guild.text_channels:
            if c.name.lower() == "general":
                ch = c
                break
    if ch is None:
        return

    try:
        await ch.send(f"🎉 <@{member.id}> just completed **every Blueprint** in the game ({total}/{total})!")
    except Exception:
        pass

async def notify_blueprint_needed_from_trade_post(
    bot: "ArcTraderBot",
    guild: discord.Guild,
    *,
    blueprint_name: str,
    offer_id: int,
    board_channel_id: int,
    board_message_id: Optional[int],
    creator_id: int,
) -> None:
    """
    DM users who have blueprint tracking enabled and do NOT own this blueprint yet.
    """
    if bot.db is None:
        return
    bp_name = str(blueprint_name).strip()
    if not bp_name:
        return

    # Safety: do not spam on huge servers
    k = blueprint_key(bp_name)

    rows = await bot.db.execute_fetchall(
        """
        SELECT u.user_id
        FROM bp_users u
        WHERE u.dms_enabled=1
          AND u.user_id != ?
          AND u.user_id NOT IN (
            SELECT user_id FROM bp_owned WHERE blueprint_key=?
          )
        LIMIT ?;
        """,
        (int(creator_id), k, int(BP_MAX_DM_NOTIFICATIONS_PER_POST)),
    )
    user_ids = [int(r[0]) for r in rows if r and r[0]]
    if not user_ids:
        return

    link_txt = f"offer #{offer_id} in <#{int(board_channel_id)}>"
    if board_message_id:
        link_txt = f"https://discord.com/channels/{guild.id}/{int(board_channel_id)}/{int(board_message_id)}"

    for uid in user_ids:
        try:
            u = guild.get_member(uid) or await guild.fetch_member(uid)  # type: ignore
            if not u:
                continue
            dm = await u.create_dm()
            await dm.send(
                f"Blueprint you need just got posted: **{bp_name}**\n"
                f"Posted by <@{creator_id}> • {link_txt}"
            )
        except Exception:
            continue


def make_trade_open_view(*, offer_id: int, intent: str = "trade") -> discord.ui.View:
    """Create the trade-board view. Button uses persistent DynamicItem handler."""
    v = discord.ui.View(timeout=None)
    # Label kept as "Send offer" to match your UI.
    v.add_item(
        discord.ui.Button(
            label="Send offer",
            style=discord.ButtonStyle.primary,
            custom_id=f"rt:trade_open:{int(offer_id)}",
        )
    )
    return v


class ArcTraderBot(commands.Bot):
    def __init__(self, guild_id: int):
        self._bg_cleanup_task = None
        self._last_site_publish_ts = 0.0
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        super().__init__(command_prefix="!", intents=intents)

        self.guild_id = guild_id
        self.cfg: Dict[str, Any] = load_config()
        self.db: Optional[aiosqlite.Connection] = None

        self.items_cache: List[str] = load_items_cache()

    async def setup_hook(self) -> None:
        if bool(self.cfg.get("sync_on_startup")):
            guild_obj = discord.Object(id=self.guild_id)
            self.tree.clear_commands(guild=guild_obj)
            self.tree.copy_global_to(guild=guild_obj)
            synced = await self.tree.sync(guild=guild_obj)
            logging.info("Synced %d app commands to guild %s", len(synced), self.guild_id)
        else:
            logging.info("Skipping command sync on startup (sync_on_startup=false). Use /sync when needed.")

        await self.init_db()

        # Persistent trade-board buttons (survive restarts)
        try:
            # Persistent trade-board buttons (survive restarts)
            self.add_view(TradeBoardPersistentView(self))

            # Persistent ticket buttons (survive restarts)
            self.add_view(TradeTicketView(self))
            self.add_view(TicketActionView(self))  # optional
        except Exception as e:
            logging.warning("Failed to add persistent trade board view: %s", e)

        # hourly rep-role sweep (failsafe)
        try:
            if not self.rep_role_sweep_loop.is_running():
                self.rep_role_sweep_loop.start()
        except Exception as e:
            logging.warning("rep_role_sweep_loop start failed: %s", e)

        # background cleanup (trade offer expiry, optional archived ticket cleanup)
        if not hasattr(self, "_bg_cleanup_task") or self._bg_cleanup_task is None:
            self._bg_cleanup_task = asyncio.create_task(self._background_cleanup())

    async def on_message(self, message: discord.Message) -> None:
        if (
                self.db is not None
                and message.guild is not None
                and int(message.guild.id) == int(self.guild_id)
                and not message.author.bot
        ):
            await self.db.execute(
                """
                INSERT INTO counters(key, value)
                VALUES('total_messages', 1)
                ON CONFLICT(key) DO UPDATE SET value = value + 1;
                """
            )
            await self.db.commit()

        await self.process_commands(message)

    async def _background_cleanup(self) -> None:
        await self.wait_until_ready()
        # Run immediately once, then align future passes to wall-clock 5-minute boundaries.
        while not self.is_closed():
            try:
                await self._refresh_bp_need_trade_offers()
            except Exception as e:
                logging.warning("bp-need trade offer refresh loop error: %s", e)
            try:
                await self._expire_trade_offers()
            except Exception as e:
                logging.warning("trade offer expiry loop error: %s", e)
            # Optional: if you have archived-ticket cleanup implemented, call it
            try:
                if hasattr(self, "cleanup_old_archived_tickets"):
                    await self.cleanup_old_archived_tickets()  # type: ignore
            except Exception as e:
                logging.warning("ticket cleanup loop error: %s", e)
            try:
                await self.maybe_publish_site_community_data()
            except Exception as e:
                logging.warning("site community publish loop error: %s", e)

            now_ts = datetime.now(timezone.utc).timestamp()
            next_run_ts = ((int(now_ts) // 300) + 1) * 300
            await asyncio.sleep(max(1.0, next_run_ts - now_ts))

    async def maybe_publish_site_community_data(self, *, force: bool = False) -> bool:
        if self.db is None:
            return False
        if not bool(self.cfg.get("site_publish_enabled")) and not force:
            return False

        repo_raw = str(self.cfg.get("site_repo_path") or "").strip()
        if not repo_raw:
            return False

        repo_path = Path(repo_raw).expanduser()
        assets_dir = repo_path / "assets"
        if not assets_dir.exists():
            logging.warning("site publish skipped: assets dir not found at %s", assets_dir)
            return False

        interval_min = safe_int(self.cfg.get("site_publish_interval_minutes"), 30)
        interval_sec = max(15 * 60, int(interval_min) * 60)
        now_m = asyncio.get_running_loop().time()
        if not force and (now_m - float(self._last_site_publish_ts)) < interval_sec:
            return False

        if not await self._sync_site_repo(repo_path):
            return False

        payload = await self.build_site_community_payload()
        out_path = assets_dir / "community.json"
        new_text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        old_text = ""
        try:
            if out_path.exists():
                old_text = out_path.read_text(encoding="utf-8")
        except Exception:
            old_text = ""

        self._last_site_publish_ts = now_m
        if old_text == new_text:
            return False

        out_path.write_text(new_text, encoding="utf-8")

        if bool(self.cfg.get("site_git_push")):
            await self._commit_and_push_site_data(repo_path)
        return True

    async def _sync_site_repo(self, repo_path: Path) -> bool:
        async def run_git(*args: str) -> Tuple[int, str, str]:
            proc = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=str(repo_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_b, stderr_b = await proc.communicate()
            return int(proc.returncode or 0), stdout_b.decode("utf-8", "replace"), stderr_b.decode("utf-8", "replace")

        # community.json is generated on the bot host, so discard any stale local copy
        # before syncing site edits that may have been pushed from another machine.
        for args in (
            ("restore", "--staged", "assets/community.json"),
            ("restore", "assets/community.json"),
        ):
            code, _out, _err = await run_git(*args)
            if code not in (0, 1):
                logging.warning("site sync restore failed for %s", " ".join(args))

        code, out, err = await run_git("pull", "--rebase", "origin", "main")
        if code != 0:
            logging.warning("site sync git pull --rebase failed: %s%s", out.strip(), err.strip())
            return False
        return True

    async def _commit_and_push_site_data(self, repo_path: Path) -> None:
        async def run_git(*args: str) -> Tuple[int, str, str]:
            proc = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=str(repo_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_b, stderr_b = await proc.communicate()
            return int(proc.returncode or 0), stdout_b.decode("utf-8", "replace"), stderr_b.decode("utf-8", "replace")

        code, _out, err = await run_git("add", "assets/community.json")
        if code != 0:
            logging.warning("site publish git add failed: %s", err.strip())
            return

        code, out, err = await run_git("diff", "--cached", "--quiet", "--", "assets/community.json")
        if code == 0:
            return
        if code not in (0, 1):
            logging.warning("site publish git diff failed: %s%s", out.strip(), err.strip())
            return

        code, out, err = await run_git("commit", "-m", "Update community data")
        if code != 0:
            logging.warning("site publish git commit failed: %s%s", out.strip(), err.strip())
            return

        code, out, err = await run_git("push")
        if code != 0:
            logging.warning("site publish git push failed: %s%s", out.strip(), err.strip())

    async def build_site_community_payload(self) -> Dict[str, Any]:
        assert self.db is not None
        now_iso = now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        invite_url = str(self.cfg.get("site_invite_url") or "https://discord.gg/raidertrader")

        open_rows = await self.db.execute_fetchall(
            """
            SELECT id, creator_id, intent, have_item, have_qty, want_item, want_qty, created_ts
            FROM trade_offers
            WHERE status='open' AND (ticket_channel_id IS NULL OR ticket_channel_id <= 0)
            ORDER BY id DESC
            LIMIT 12;
            """
        )
        stats_rows = await self.db.execute_fetchall(
            """
            SELECT
                COUNT(*) AS open_trades,
                SUM(CASE WHEN intent='request' THEN 1 ELSE 0 END) AS requests,
                COUNT(DISTINCT creator_id) AS active_traders
            FROM trade_offers
            WHERE status='open' AND (ticket_channel_id IS NULL OR ticket_channel_id <= 0);
            """
        )
        rep_rows = await self.db.execute_fetchall(
            "SELECT COUNT(*) FROM reps WHERE rep_type='trade_partner';"
        )
        completed_trade_rows = await self.db.execute_fetchall(
            """
            SELECT COUNT(*)
            FROM tickets
            WHERE status='closed'
              AND outcome='finalized'
              AND (ticket_type='trade' OR trade_offer_id IS NOT NULL);
            """
        )
        rep_trade_ticket_rows = await self.db.execute_fetchall(
            """
            SELECT COUNT(DISTINCT ticket_id)
            FROM reps
            WHERE rep_type='trade_partner';
            """
        )
        bp_rows = await self.db.execute_fetchall(
            """
            SELECT COUNT(*)
            FROM trade_offers o
            WHERE o.status='open'
              AND o.intent='request'
              AND (o.ticket_channel_id IS NULL OR o.ticket_channel_id <= 0)
              AND (
                    o.want_item LIKE '%Blueprint%'
                    OR EXISTS (
                        SELECT 1
                        FROM trade_offer_items toi
                        WHERE toi.offer_id=o.id
                          AND toi.side='want'
                          AND toi.item_name LIKE '%Blueprint%'
                    )
              );
            """
        )
        total_message_rows = await self.db.execute_fetchall(
            "SELECT value FROM counters WHERE key='total_messages' LIMIT 1;"
        )

        stats_row = stats_rows[0] if stats_rows else (0, 0, 0)
        finalized_trade_tickets = int(completed_trade_rows[0][0] or 0) if completed_trade_rows else 0
        repped_trade_tickets = int(rep_trade_ticket_rows[0][0] or 0) if rep_trade_ticket_rows else 0
        guild = self.get_guild(self.guild_id)
        stats = {
            "open_trades": int(stats_row[0] or 0),
            "requests": int(stats_row[1] or 0),
            "active_traders": int(stats_row[2] or 0),
            "blueprint_requests": int(bp_rows[0][0] or 0) if bp_rows else 0,
            "completed_trades": max(finalized_trade_tickets, repped_trade_tickets),
            "completed_trade_reps": int(rep_rows[0][0] or 0) if rep_rows else 0,
            "member_count": int(guild.member_count or 0) if guild else 0,
            "total_messages": int(total_message_rows[0][0] or 0) if total_message_rows else 0,
        }

        recent_trades: List[Dict[str, Any]] = []
        for offer_id, creator_id, intent, have_item, have_qty, want_item, want_qty, created_ts in open_rows[:8]:
            bundle = await db_get_trade_offer_items(self, int(offer_id))
            have_items = bundle.get("have") or ([(str(have_item), int(have_qty or 1))] if have_item else [])
            want_items = bundle.get("want") or ([(str(want_item), int(want_qty or 1))] if want_item and str(want_item) != "Offers" else [])
            _total_reps, trade_reps = await _count_reps(self, int(creator_id))

            intent_s = str(intent or "trade").lower()
            recent_trades.append({
                "intent": intent_s,
                "label": "Looking for" if intent_s == "request" else ("Taking offers on" if intent_s == "offers" else "Trading"),
                "items": self._site_item_preview(want_items if intent_s == "request" else have_items),
                "wants": ([] if intent_s == "request" else (["Offers"] if intent_s == "offers" else self._site_item_preview(want_items))),
                "trade_rep": int(trade_reps),
                "created_ts": int(created_ts or 0),
            })

        hot_blueprints: List[str] = []
        bp_item_rows = await self.db.execute_fetchall(
            """
            SELECT item_name, COUNT(*) AS n
            FROM (
                SELECT want_item AS item_name
                FROM trade_offers o
                WHERE o.status='open'
                  AND o.intent='request'
                  AND (o.ticket_channel_id IS NULL OR o.ticket_channel_id <= 0)
                  AND o.want_item LIKE '%Blueprint%'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM trade_offer_items toi
                      WHERE toi.offer_id=o.id AND toi.side='want'
                  )
                UNION ALL
                SELECT toi.item_name AS item_name
                FROM trade_offer_items toi
                JOIN trade_offers o ON o.id = toi.offer_id
                WHERE o.status='open'
                  AND o.intent='request'
                  AND (o.ticket_channel_id IS NULL OR o.ticket_channel_id <= 0)
                  AND toi.side='want'
                  AND toi.item_name LIKE '%Blueprint%'
            )
            WHERE item_name IS NOT NULL AND item_name <> ''
            GROUP BY item_name
            ORDER BY n DESC, item_name COLLATE NOCASE ASC
            LIMIT 8;
            """
        )
        hot_blueprints = [str(r[0]) for r in bp_item_rows if r and r[0]]

        return {
            "updated_at": now_iso,
            "invite_url": invite_url,
            "stats": stats,
            "recent_trades": recent_trades,
            "hot_blueprints": hot_blueprints,
        }

    def _site_item_preview(self, items: List[Tuple[str, int]], *, limit: int = 4) -> List[str]:
        out: List[str] = []
        for name, qty in (items or [])[:limit]:
            qty_i = int(qty or 1)
            label = str(name)
            if qty_i > 1:
                label = f"{label} x{qty_i}"
            out.append(label[:80])
        return out

    async def _expire_trade_offers(self) -> None:
        if self.db is None:
            return
        # Select open offers whose expiry has passed
        now_ts = int(datetime.now(timezone.utc).timestamp())
        async with self.db.execute(
                "SELECT id, created_at, created_ts, expires_ts, board_message_id, ticket_channel_id, creator_id, have_item, want_item "
                "FROM trade_offers WHERE status='open' AND expires_ts IS NOT NULL AND expires_ts > 0 AND expires_ts <= ? AND (ticket_channel_id IS NULL OR ticket_channel_id <= 0);",
                (now_ts,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            return

        guild = self.get_guild(self.guild_id)
        board_ch = None
        if guild:
            board_ch = guild.get_channel(TRADE_BOARD_CHANNEL_ID)

        for (oid, created_at, created_ts, expires_ts, board_mid, ticket_chid, creator_id, have_item, want_item) in rows:
            # delete board post if present
            if board_ch and board_mid:
                try:
                    msg = await board_ch.fetch_message(int(board_mid))
                    await msg.delete()
                except Exception:
                    pass

            # mark offer expired
            await self.db.execute(
                "UPDATE trade_offers SET status='expired', board_message_id=NULL WHERE id=?",
                (int(oid),),
            )

        await self.db.commit()

    async def _refresh_bp_need_trade_offers(self) -> None:
        if self.db is None:
            return

        rows = await self.db.execute_fetchall(
            """
            SELECT id, creator_id, intent, have_item, have_qty, want_item, want_qty, notes, created_ts, expires_ts, board_message_id, ticket_channel_id, bp_need_auto
            FROM trade_offers
            WHERE status='open' AND intent='request' AND (ticket_channel_id IS NULL OR ticket_channel_id <= 0);
            """
        )
        if not rows:
            return

        guild = self.get_guild(self.guild_id)
        board_ch = None
        if guild:
            board_ch = guild.get_channel(TRADE_BOARD_CHANNEL_ID)
            if board_ch is None:
                try:
                    board_ch = await guild.fetch_channel(TRADE_BOARD_CHANNEL_ID)  # type: ignore
                except Exception:
                    board_ch = None

        all_bps = get_all_blueprints_from_items_cache()
        for row in rows:
            (
                offer_id,
                creator_id,
                intent,
                have_item,
                have_qty,
                want_item,
                want_qty,
                notes,
                created_ts,
                expires_ts,
                board_mid,
                _ticket_chid,
                bp_need_auto,
            ) = row

            auto_enabled = bool(int(bp_need_auto or 0))
            if not auto_enabled:
                # One-time compatibility for active posts created before bp_need_auto existed.
                auto_enabled = await db_trade_offer_looks_like_bp_need(self, int(offer_id))
                if auto_enabled:
                    await self.db.execute(
                        "UPDATE trade_offers SET bp_need_auto=1 WHERE id=?;",
                        (int(offer_id),),
                    )

            if not auto_enabled:
                continue

            owned_keys = await db_bp_owned_keys(self, int(creator_id))
            want_items = [(bp, 1) for bp in all_bps if blueprint_key(bp) not in owned_keys]
            if not want_items:
                if board_ch and board_mid and isinstance(board_ch, discord.TextChannel):
                    try:
                        msg = await board_ch.fetch_message(int(board_mid))
                        await msg.delete()
                    except Exception:
                        pass
                await self.db.execute(
                    "UPDATE trade_offers SET status='closed', board_message_id=NULL WHERE id=?;",
                    (int(offer_id),),
                )
                continue

            first_want = want_items[0]
            current_bundle = await db_get_trade_offer_items(self, int(offer_id))
            current_wants = current_bundle.get("want") or []
            if current_wants == want_items:
                continue

            await self.db.execute(
                "UPDATE trade_offers SET want_item=?, want_qty=? WHERE id=?;",
                (str(first_want[0])[:200], int(first_want[1]), int(offer_id)),
            )
            await db_set_trade_offer_items(self, int(offer_id), [], want_items)

            if board_ch and board_mid and isinstance(board_ch, discord.TextChannel):
                try:
                    msg = await board_ch.fetch_message(int(board_mid))
                    embed = await self._build_trade_offer_embed(
                        offer_id=int(offer_id),
                        creator_id=int(creator_id),
                        intent=str(intent or "request"),
                        have_item=str(have_item or ""),
                        have_qty=int(have_qty or 1),
                        want_item=str(first_want[0]),
                        want_qty=int(first_want[1]),
                        notes=(str(notes) if notes else None),
                        created_ts=(int(created_ts) if created_ts else None),
                        expires_ts=(int(expires_ts) if expires_ts else None),
                    )
                    await msg.edit(embed=embed, view=make_trade_open_view(offer_id=int(offer_id), intent="request"))
                except Exception:
                    pass

        await self.db.commit()

    async def init_db(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH.as_posix())

        # Legacy base table (older DBs might already have it)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT NOT NULL,
                quantity INTEGER NOT NULL, -- total owned
                price_type TEXT,           -- legacy
                price_value TEXT,          -- legacy
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self.db.commit()

        # ---- Migrations ----
        cur = await self.db.execute("PRAGMA table_info(listings);")
        cols = await cur.fetchall()
        await cur.close()
        col_names = {c[1] for c in cols}

        # seller_id (listing owner)
        if "seller_id" not in col_names:
            logging.info("Migrating DB: adding listings.seller_id")
            await self.db.execute("ALTER TABLE listings ADD COLUMN seller_id INTEGER;")
            await self.db.commit()

        # payment_methods (rails only)
        if "payment_methods" not in col_names:
            logging.info("Migrating DB: adding listings.payment_methods")
            await self.db.execute("ALTER TABLE listings ADD COLUMN payment_methods TEXT;")
            await self.db.execute("UPDATE listings SET payment_methods='[\"paypal\"]' WHERE payment_methods IS NULL;")
            await self.db.commit()

        # reservations
        if "quantity_reserved" not in col_names:
            logging.info("Migrating DB: adding listings.quantity_reserved")
            await self.db.execute("ALTER TABLE listings ADD COLUMN quantity_reserved INTEGER NOT NULL DEFAULT 0;")
            await self.db.commit()

        # new pricing model
        if "price_usd" not in col_names:
            logging.info("Migrating DB: adding listings.price_usd")
            await self.db.execute("ALTER TABLE listings ADD COLUMN price_usd REAL;")
            await self.db.commit()

        if "trade_allowed" not in col_names:
            logging.info("Migrating DB: adding listings.trade_allowed")
            await self.db.execute("ALTER TABLE listings ADD COLUMN trade_allowed INTEGER NOT NULL DEFAULT 0;")
            await self.db.commit()

        if "trade_only" not in col_names:
            logging.info("Migrating DB: adding listings.trade_only")
            await self.db.execute("ALTER TABLE listings ADD COLUMN trade_only INTEGER NOT NULL DEFAULT 0;")
            await self.db.commit()

        if "trade_notes" not in col_names:
            logging.info("Migrating DB: adding listings.trade_notes")
            await self.db.execute("ALTER TABLE listings ADD COLUMN trade_notes TEXT;")
            await self.db.commit()

        # Backfill from legacy columns if present and new fields are NULL/default
        # - legacy usd: price_usd = price_value
        # - legacy trade: trade_allowed=1, trade_only=1, trade_notes = price_value
        try:
            await self.db.execute("""
                UPDATE listings
                SET price_usd = CASE
                    WHEN price_usd IS NULL AND (price_type = 'usd' OR price_type IS NULL) THEN CAST(price_value AS REAL)
                    ELSE price_usd
                END
                WHERE price_usd IS NULL AND price_value IS NOT NULL;
            """)
            await self.db.execute("""
                UPDATE listings
                SET trade_allowed = 1,
                    trade_only = 1,
                    trade_notes = COALESCE(trade_notes, price_value)
                WHERE price_type = 'trade' AND trade_allowed = 0;
            """)
            # Make sure trade-only listings don't advertise payment rails unless explicitly set
            await self.db.execute("""
                UPDATE listings
                SET payment_methods = '[]'
                WHERE trade_only = 1;
            """)
            await self.db.commit()
        except Exception:
            pass

        # Helpful index
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_listings_item ON listings(item_name);")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_listings_seller ON listings(seller_id);")
        await self.db.commit()

        # Prevent identical offer duplicates (same item + legacy price key)
        await self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_offer_unique ON listings(seller_id, item_name, price_type, price_value);"
        )
        await self.db.commit()

        # Counters + tickets
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
        """)
        for k in ("buy", "sell", "trade"):
            await self.db.execute("INSERT OR IGNORE INTO counters(key, value) VALUES(?, 0);", (k,))
        await self.db.execute("INSERT OR IGNORE INTO counters(key, value) VALUES('total_messages', 0);")

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_type TEXT NOT NULL CHECK (ticket_type IN ('buy','sell','trade')),
                ticket_number INTEGER NOT NULL,
                channel_id INTEGER NOT NULL UNIQUE,
                requester_id INTEGER NOT NULL,
                seller_id INTEGER,
                status TEXT NOT NULL CHECK (status IN ('open','closed')),
                outcome TEXT, -- 'finalized' | 'released' | 'closed'
                created_at TEXT NOT NULL,
                closed_at TEXT,

                -- listing linkage (optional)
                listing_id INTEGER,
                item_name TEXT,
                reserved_qty INTEGER DEFAULT 0,

                -- snapshot pricing / rails / trade
                price_usd REAL,
                trade_offer_id INTEGER,
                middleman_requested INTEGER NOT NULL DEFAULT 0,
                payment_methods TEXT,
                trade_allowed INTEGER,
                trade_only INTEGER,
                trade_notes TEXT,

                -- buy
                payment_method TEXT,
                buyer_notes TEXT,

                -- trade
                offer_item TEXT,
                want_item TEXT,

-- trade quantities (for board offers)
offer_qty INTEGER NOT NULL DEFAULT 1,
want_qty INTEGER NOT NULL DEFAULT 1,

-- two-party confirmations
finalize_req_ok INTEGER NOT NULL DEFAULT 0,
finalize_seller_ok INTEGER NOT NULL DEFAULT 0,
cancel_req_ok INTEGER NOT NULL DEFAULT 0,
cancel_seller_ok INTEGER NOT NULL DEFAULT 0,
middleman_req_ok INTEGER NOT NULL DEFAULT 0,
middleman_seller_ok INTEGER NOT NULL DEFAULT 0,

                -- sell
                sell_item TEXT,
                payout_method TEXT,
                ask_price TEXT
            );
        """)
        await self.db.commit()

        # ---- Migrate tickets table (older DBs may be missing snapshot columns) ----
        cur = await self.db.execute("PRAGMA table_info(tickets);")
        tcols = await cur.fetchall()
        await cur.close()
        tnames = {c[1] for c in tcols}

        # snapshot pricing / rails / trade (added in newer versions)
        if "price_usd" not in tnames:
            logging.info("Migrating DB: adding tickets.price_usd")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN price_usd REAL;")
            await self.db.commit()
        if "payment_methods" not in tnames:
            logging.info("Migrating DB: adding tickets.payment_methods")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN payment_methods TEXT;")
            await self.db.commit()
        if "trade_allowed" not in tnames:
            logging.info("Migrating DB: adding tickets.trade_allowed")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN trade_allowed INTEGER;")
            await self.db.commit()
        if "trade_only" not in tnames:
            logging.info("Migrating DB: adding tickets.trade_only")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN trade_only INTEGER;")
            await self.db.commit()
        if "trade_notes" not in tnames:
            logging.info("Migrating DB: adding tickets.trade_notes")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN trade_notes TEXT;")
            await self.db.commit()

        if "trade_offer_id" not in tnames:
            logging.info("Migrating DB: adding tickets.trade_offer_id")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN trade_offer_id INTEGER;")
            await self.db.commit()

        if "middleman_requested" not in tnames:
            logging.info("Migrating DB: adding tickets.middleman_requested")
            await self.db.execute("ALTER TABLE tickets ADD COLUMN middleman_requested INTEGER NOT NULL DEFAULT 0;")
            await self.db.commit()
        # two-party confirmations + trade quantities
        for col, ddl in [
            ("finalize_req_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("finalize_seller_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("cancel_req_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("cancel_seller_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("middleman_req_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("middleman_seller_ok", "INTEGER NOT NULL DEFAULT 0"),
            ("offer_qty", "INTEGER NOT NULL DEFAULT 1"),
            ("want_qty", "INTEGER NOT NULL DEFAULT 1"),
        ]:
            if col not in tnames:
                logging.info("Migrating DB: adding tickets.%s", col)
                await self.db.execute(f"ALTER TABLE tickets ADD COLUMN {col} {ddl};")
                await self.db.commit()

        # Reps table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS reps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                ticket_id INTEGER NOT NULL,
                repper_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                rep_type TEXT NOT NULL CHECK (rep_type IN ('seller','trade_partner','middleman')),
                item_name TEXT,
                review TEXT,
                is_anonymous INTEGER NOT NULL DEFAULT 0
            );
        """)
        await self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_reps_unique ON reps(ticket_id, repper_id, target_id, rep_type);")
        await self.db.commit()
        # Rep overrides (admin-set absolute counts)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS rep_overrides (
                user_id INTEGER PRIMARY KEY,
                total_override INTEGER,
                trade_override INTEGER,
                updated_at TEXT NOT NULL,
                updated_by INTEGER NOT NULL
            );
        """)
        await self.db.commit()

        # Rep milestone announcements (one-time gates)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS rep_milestones (
                user_id INTEGER NOT NULL,
                milestone INTEGER NOT NULL,
                announced_at TEXT NOT NULL,
                PRIMARY KEY(user_id, milestone)
            );
        """)
        await self.db.commit()

        # Ticket button one-time use gate (per-user, per-ticket, per-button)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS ticket_button_uses (
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                custom_id TEXT NOT NULL,
                used_at TEXT NOT NULL,
                PRIMARY KEY(channel_id, user_id, custom_id)
            );
        """)
        await self.db.commit()

        # Trade offers table (board posts)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS trade_offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- ISO timestamp (legacy/diagnostic)
                created_at TEXT NOT NULL,

                -- Unix timestamps (used for <t:...:R> formatting and expiry enforcement)
                created_ts INTEGER,
                expires_ts INTEGER,

                status TEXT NOT NULL CHECK (status IN ('open','closed','expired')),
                creator_id INTEGER NOT NULL,

                -- intent: 'trade' (have->want), 'offers' (have->offers), 'request' (want->offering)
                intent TEXT NOT NULL DEFAULT 'trade',

                have_item TEXT NOT NULL,
                have_qty INTEGER NOT NULL DEFAULT 1,
                want_item TEXT NOT NULL,
                want_qty INTEGER NOT NULL DEFAULT 1,

                notes TEXT,

                bp_need_auto INTEGER NOT NULL DEFAULT 0,
                board_message_id INTEGER,
                ticket_channel_id INTEGER
            );
        """)
        # Ensure required columns exist
        cols = await self.db.execute_fetchall("PRAGMA table_info(trade_offers)")
        col_names = {c[1] for c in cols}

        if "created_ts" not in col_names:
            await self.db.execute("ALTER TABLE trade_offers ADD COLUMN created_ts INTEGER;")

        if "expires_ts" not in col_names:
            await self.db.execute("ALTER TABLE trade_offers ADD COLUMN expires_ts INTEGER;")

        if "bp_need_auto" not in col_names:
            await self.db.execute("ALTER TABLE trade_offers ADD COLUMN bp_need_auto INTEGER NOT NULL DEFAULT 0;")

        await self.db.commit()

        # Create indexes safely
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_trade_offers_status ON trade_offers(status);")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_trade_offers_expires ON trade_offers(expires_ts);")
        await self.db.commit()

        # ---- Migrate trade_offers table (older DBs may be missing qty columns) ----
        cur = await self.db.execute("PRAGMA table_info(trade_offers);")
        ocols = await cur.fetchall()
        await cur.close()
        onames = {c[1] for c in ocols}
        for col, ddl in [
            ("have_qty", "INTEGER NOT NULL DEFAULT 1"),
            ("want_qty", "INTEGER NOT NULL DEFAULT 1"),
            ("intent", "TEXT NOT NULL DEFAULT 'trade'"),
            ("notes", "TEXT"),
            ("created_ts", "INTEGER"),
            ("expires_ts", "INTEGER"),
            ("bp_need_auto", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if col not in onames:
                logging.info("Migrating DB: adding trade_offers.%s", col)
                await self.db.execute(f"ALTER TABLE trade_offers ADD COLUMN {col} {ddl};")
                await self.db.commit()

        # ---- Allow request posts with no "have"/offering ----
        # Older DBs created trade_offers.have_item as NOT NULL. Option B requires have_item/have_qty to be nullable.
        try:
            info = await self.db.execute_fetchall("PRAGMA table_info(trade_offers);")
            # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
            have_col = next((c for c in info if c[1] == "have_item"), None)
            have_qty_col = next((c for c in info if c[1] == "have_qty"), None)
            have_notnull = int(have_col[3]) if have_col else 0
            have_qty_notnull = int(have_qty_col[3]) if have_qty_col else 0
            if have_notnull == 1 or have_qty_notnull == 1:
                logging.info("Migrating DB: making trade_offers.have_* nullable (request posts)")
                await self.db.execute("BEGIN;")
                await self.db.execute("""
                    CREATE TABLE IF NOT EXISTS trade_offers__v2 (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at TEXT NOT NULL,
                        created_ts INTEGER,
                        expires_ts INTEGER,
                        status TEXT NOT NULL CHECK (status IN ('open','closed','expired')),
                        creator_id INTEGER NOT NULL,
                        intent TEXT NOT NULL DEFAULT 'trade',
                        have_item TEXT,
                        have_qty INTEGER,
                        want_item TEXT NOT NULL,
                        want_qty INTEGER NOT NULL DEFAULT 1,
                        notes TEXT,
                        bp_need_auto INTEGER NOT NULL DEFAULT 0,
                        board_message_id INTEGER,
                        ticket_channel_id INTEGER
                    );
                """)
                # Copy rows (coerce empty strings to NULL for have_item)
                await self.db.execute("""
                    INSERT INTO trade_offers__v2(
                        id, created_at, created_ts, expires_ts, status, creator_id, intent,
                        have_item, have_qty, want_item, want_qty, notes, bp_need_auto, board_message_id, ticket_channel_id
                    )
                    SELECT
                        id, created_at, created_ts, expires_ts, status, creator_id, COALESCE(intent,'trade'),
                        NULLIF(have_item,''), have_qty, want_item, want_qty, notes,
                        CASE WHEN COALESCE(bp_need_auto, 0) THEN 1 ELSE 0 END,
                        board_message_id, ticket_channel_id
                    FROM trade_offers;
                """)
                await self.db.execute("DROP TABLE trade_offers;")
                await self.db.execute("ALTER TABLE trade_offers__v2 RENAME TO trade_offers;")
                # Recreate indexes
                await self.db.execute("CREATE INDEX IF NOT EXISTS idx_trade_offers_status ON trade_offers(status);")
                await self.db.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trade_offers_expires ON trade_offers(expires_ts);")
                await self.db.execute("COMMIT;")
        except Exception as e:
            try:
                await self.db.execute("ROLLBACK;")
            except Exception:
                pass
            logging.warning("trade_offers nullable-have migration failed (continuing): %s", e)

        # Backfill created_ts/expires_ts for older rows
        cur = await self.db.execute(
            "SELECT id, created_at FROM trade_offers WHERE created_ts IS NULL OR expires_ts IS NULL;")
        legacy = await cur.fetchall()
        await cur.close()
        for oid, created_at in legacy:
            try:
                dt = iso_to_dt(str(created_at))
                cts = int(dt.replace(tzinfo=timezone.utc).timestamp())
                ets = cts + (TRADE_OFFER_EXPIRE_DAYS * 86400)
                await self.db.execute(
                    "UPDATE trade_offers SET created_ts=COALESCE(created_ts, ?), expires_ts=COALESCE(expires_ts, ?) WHERE id=?;",
                    (cts, ets, oid))
            except Exception:
                pass
        await self.db.commit()

        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS trade_offer_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_id INTEGER NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('have','want')),
                item_name TEXT NOT NULL,
                qty INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
        """)
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_trade_offer_items_offer ON trade_offer_items(offer_id, sort_order);")
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS ticket_trade_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                owner_role TEXT NOT NULL CHECK (owner_role IN ('creator','joiner')),
                item_name TEXT NOT NULL,
                qty INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
        """)
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_ticket_trade_items_ticket ON ticket_trade_items(ticket_id, sort_order);")
        await self.db.commit()

        # Blueprint tracking tables
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS bp_users (
                user_id INTEGER PRIMARY KEY,
                dms_enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
        """)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS bp_owned (
                user_id INTEGER NOT NULL,
                blueprint_key TEXT NOT NULL,
                blueprint_name TEXT NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY(user_id, blueprint_key)
            );
        """)
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_bp_owned_key ON bp_owned(blueprint_key);")
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS bp_wipe_confirms (
                user_id INTEGER PRIMARY KEY,
                expires_ts INTEGER NOT NULL
            );
        """)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS bp_milestones (
                user_id INTEGER NOT NULL,
                milestone INTEGER NOT NULL,
                announced_at TEXT NOT NULL,
                PRIMARY KEY(user_id, milestone)
            );
        """)
        await self.db.commit()

        # Cleanup any truly-dead listings (0 qty and 0 reserved)
        await self.cleanup_dead_listings()

        logging.info("DB ready: %s", DB_PATH)

    async def close(self) -> None:
        try:
            if self._bg_cleanup_task is not None:
                self._bg_cleanup_task.cancel()
                try:
                    await self._bg_cleanup_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            if self.db is not None:
                await self.db.close()
        finally:
            await super().close()

    async def on_ready(self) -> None:
        logging.info("Logged in as %s (id=%s)", self.user, self.user.id)

    # ---------- Cosmetic helpers ----------

    def next_rep_embed_color(self) -> int:
        """Return the next embed color (cycles through Arc Raiders colors) and persist the index."""
        colors = [0x00FFFF, 0x00FF00, 0xFFFF00, 0xFF0000]
        try:
            idx = int(self.cfg.get("rep_embed_color_index") or 0)
        except Exception:
            idx = 0
        color = colors[idx % len(colors)]
        self.cfg["rep_embed_color_index"] = (idx + 1) % len(colors)
        try:
            save_config(self.cfg)
        except Exception:
            pass
        return int(color)

    async def maybe_announce_trade_rep_milestone(self, guild: discord.Guild, member: discord.Member,
                                                 trade_reps: int) -> None:
        """Announce once when someone hits 100+ trade reps."""
        if self.db is None:
            return
        milestone = 100
        if trade_reps < milestone:
            return

        # One-time gate in DB
        try:
            async with self.db.execute(
                    "SELECT 1 FROM rep_milestones WHERE user_id=? AND milestone=? LIMIT 1;",
                    (int(member.id), int(milestone)),
            ) as cur:
                row = await cur.fetchone()
            if row:
                return
            await self.db.execute(
                "INSERT OR IGNORE INTO rep_milestones(user_id, milestone, announced_at) VALUES(?,?,?);",
                (int(member.id), int(milestone), dt_to_iso(now_utc())),
            )
            await self.db.commit()
        except Exception:
            return

        # Pick the highest role they qualify for (trade rules only)
        role_mention = f"**{milestone}+ rep**"
        try:
            rules = (self.cfg.get("rep_roles") or {}).get("trade") or []
            best_min = -1
            best_role_id = 0
            for r in rules:
                if not isinstance(r, dict):
                    continue
                mr = int(r.get("min_reps") or 0)
                rid = int(r.get("role_id") or 0)
                if rid and mr <= trade_reps and mr > best_min:
                    best_min = mr
                    best_role_id = rid
            if best_role_id:
                role = guild.get_role(int(best_role_id))
                if role is not None:
                    role_mention = role.mention
        except Exception:
            pass

        # Resolve announce channel: config -> system -> #general
        ch = None
        try:
            cid = self.cfg.get("general_announce_channel_id")
            if isinstance(cid, int) and cid:
                ch = guild.get_channel(int(cid))
        except Exception:
            ch = None

        if not isinstance(ch, discord.TextChannel):
            if isinstance(getattr(guild, "system_channel", None), discord.TextChannel):
                ch = guild.system_channel
            else:
                for c in guild.text_channels:
                    if c.name.lower() == "general":
                        ch = c
                        break

        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(f"{member.mention} has reached {role_mention}! GG")
            except Exception:
                pass

    async def guard_ticket_button_once(
            self,
            interaction: discord.Interaction,
            custom_id: str,
    ) -> bool:
        """Return True if this user already used this button in this ticket (and notify ephemerally).

        Admins are exempt.
        """
        if self.db is None:
            return False
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        if is_admin_member(interaction.user):
            return False
        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            return False

        try:
            async with self.db.execute(
                    "SELECT 1 FROM ticket_button_uses WHERE channel_id=? AND user_id=? AND custom_id=? LIMIT 1;",
                    (int(ch.id), int(interaction.user.id), str(custom_id)),
            ) as cur:
                row = await cur.fetchone()
            if row:
                try:
                    await interaction.response.send_message("You already used that button in this ticket.",
                                                            ephemeral=True)
                except discord.InteractionResponded:
                    await interaction.followup.send("You already used that button in this ticket.", ephemeral=True)
                return True

            await self.db.execute(
                "INSERT OR IGNORE INTO ticket_button_uses(channel_id, user_id, custom_id, used_at) VALUES(?,?,?,?);",
                (int(ch.id), int(interaction.user.id), str(custom_id), dt_to_iso(now_utc())),
            )
            await self.db.commit()
        except Exception:
            # If anything goes wrong, don't block the action.
            return False

        return False

    # ---------- DB helpers ----------

    async def next_counter(self, key: str) -> int:
        assert self.db is not None
        async with self.db.execute("SELECT value FROM counters WHERE key = ?;", (key,)) as cur:
            row = await cur.fetchone()
        cur_val = int(row[0]) if row else 0
        new_val = cur_val + 1
        await self.db.execute("UPDATE counters SET value = ? WHERE key = ?;", (new_val, key))
        await self.db.commit()
        return new_val

    async def get_listing_by_id(self, listing_id: int) -> Optional[Dict[str, Any]]:
        assert self.db is not None
        async with self.db.execute(
                """
                SELECT id, item_name, quantity, quantity_reserved,
                       price_usd, payment_methods, trade_allowed, trade_only, trade_notes, seller_id
                FROM listings WHERE id = ?;
                """,
                (listing_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None

        pm_json = row[5] or "[]"
        try:
            pms = json.loads(pm_json)
            if not isinstance(pms, list):
                pms = []
        except Exception:
            pms = []

        return {
            "id": int(row[0]),
            "item_name": str(row[1]),
            "quantity": int(row[2]),
            "quantity_reserved": int(row[3]),
            "price_usd": float(row[4] or 0.0),
            "payment_methods": [str(x).lower() for x in pms if isinstance(x, str)],
            "trade_allowed": bool(int(row[6] or 0)),
            "trade_only": bool(int(row[7] or 0)),
            "trade_notes": (row[8] or "").strip(),
            "seller_id": int(row[9]) if row[9] is not None else None,
        }

    async def reserve_listing(self, listing_id: int, qty: int) -> Tuple[bool, str]:
        assert self.db is not None
        if qty <= 0:
            return False, "Quantity must be > 0."

        listing = await self.get_listing_by_id(listing_id)
        if not listing:
            return False, "Listing not found."

        available = listing["quantity"] - listing["quantity_reserved"]
        if available < qty:
            return False, f"Not enough stock available. Available: {available}."

        await self.db.execute(
            "UPDATE listings SET quantity_reserved = quantity_reserved + ? WHERE id = ?;",
            (qty, listing_id),
        )
        await self.db.commit()
        return True, "Reserved."

    async def release_reservation(self, listing_id: int, qty: int) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            UPDATE listings
            SET quantity_reserved = CASE
                WHEN quantity_reserved - ? < 0 THEN 0
                ELSE quantity_reserved - ?
            END
            WHERE id = ?;
            """,
            (qty, qty, listing_id),
        )
        await self.db.commit()

    async def finalize_sale(self, listing_id: int, qty: int) -> Tuple[bool, str]:
        assert self.db is not None
        listing = await self.get_listing_by_id(listing_id)
        if not listing:
            return False, "Listing not found."

        if listing["quantity"] < qty:
            return False, f"Listing total quantity is too low (total={listing['quantity']})."

        await self.db.execute(
            "UPDATE listings SET quantity = quantity - ? WHERE id = ?;",
            (qty, listing_id),
        )
        await self.release_reservation(listing_id, qty)

        listing2 = await self.get_listing_by_id(listing_id)
        if listing2 and listing2["quantity"] <= 0 and listing2["quantity_reserved"] <= 0:
            await self.db.execute("DELETE FROM listings WHERE id = ?;", (listing_id,))
        await self.db.commit()
        return True, "Finalized."

    async def cleanup_dead_listings(self) -> None:
        """Delete listing rows that are truly dead (0 qty and 0 reserved)."""
        if self.db is None:
            return
        try:
            await self.db.execute(
                "DELETE FROM listings WHERE COALESCE(quantity,0) <= 0 AND COALESCE(quantity_reserved,0) <= 0;"
            )
            await self.db.commit()
        except Exception as e:
            logging.warning("cleanup_dead_listings failed: %s", e)

    async def find_open_ticket_by_channel(self, channel_id: int) -> Optional[Dict[str, Any]]:
        assert self.db is not None
        async with self.db.execute(
                """
                SELECT id, ticket_type, ticket_number, requester_id, seller_id, listing_id, item_name, reserved_qty, trade_offer_id, middleman_requested
                FROM tickets
                WHERE channel_id = ? AND status = 'open'
                LIMIT 1;
                """,
                (channel_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "ticket_type": str(row[1]),
            "ticket_number": int(row[2]),
            "requester_id": int(row[3]),
            "seller_id": int(row[4]) if row[4] is not None else None,
            "listing_id": int(row[5]) if row[5] is not None else None,
            "item_name": row[6],
            "reserved_qty": int(row[7] or 0),
            "trade_offer_id": int(row[8]) if row[8] is not None else None,
            "middleman_requested": bool(int(row[9] or 0)),
        }

    async def close_ticket_record(self, ticket_id: int, outcome: str) -> None:
        assert self.db is not None
        await self.db.execute(
            """
            UPDATE tickets
            SET status='closed', outcome=?, closed_at=?
            WHERE id = ?;
            """,
            (outcome, dt_to_iso(now_utc()), ticket_id),
        )
        await self.db.commit()

    async def _set_ticket_flag(self, ticket_id: int, field: str) -> None:
        assert self.db is not None
        if field not in {"finalize_req_ok", "finalize_seller_ok", "cancel_req_ok", "cancel_seller_ok"}:
            raise ValueError("Invalid ticket flag field")
        await self.db.execute(f"UPDATE tickets SET {field}=1 WHERE id=?;", (int(ticket_id),))
        await self.db.commit()

    async def _get_ticket_flags(self, ticket_id: int) -> Dict[str, int]:
        assert self.db is not None
        async with self.db.execute(
                "SELECT finalize_req_ok, finalize_seller_ok, cancel_req_ok, cancel_seller_ok FROM tickets WHERE id=? LIMIT 1;",
                (int(ticket_id),),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return {"finalize_req_ok": 0, "finalize_seller_ok": 0, "cancel_req_ok": 0, "cancel_seller_ok": 0}
        return {
            "finalize_req_ok": int(row[0] or 0),
            "finalize_seller_ok": int(row[1] or 0),
            "cancel_req_ok": int(row[2] or 0),
            "cancel_seller_ok": int(row[3] or 0),
        }

    # ---------- Discord helpers ----------

    def get_ids(self) -> Tuple[int, int, int]:
        tc = self.cfg.get("tickets_category_id")
        ac = self.cfg.get("archived_category_id")
        sr = self.cfg.get("seller_role_id")
        if not (isinstance(tc, int) and isinstance(ac, int) and isinstance(sr, int)):
            raise RuntimeError("Missing IDs in config.json; run /setup again.")
        return tc, ac, sr

    async def create_ticket_channel(
            self,
            guild: discord.Guild,
            ticket_type: str,
            requester: discord.Member,
            embed: discord.Embed,
            *,
            seller: Optional[discord.Member] = None,
            include_middleman: bool = False,
    ) -> discord.TextChannel:
        tickets_category_id = self.cfg.get("tickets_category_id")
        archived_category_id = self.cfg.get("archived_category_id")
        if not (isinstance(tickets_category_id, int) and isinstance(archived_category_id, int)):
            raise RuntimeError("Missing IDs in config.json; run /setup again.")

        category = guild.get_channel(tickets_category_id)
        if not isinstance(category, discord.CategoryChannel):
            raise RuntimeError("Tickets category not found or is not a category.")

        admin_role = guild.get_role(get_admin_role_id(self.cfg))
        if admin_role is None:
            raise RuntimeError("Admin role not found (ADMIN_ROLE_ID).")

        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            requester: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            admin_role: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            # type: ignore
        }

        if seller is not None:
            overwrites[seller] = discord.PermissionOverwrite(view_channel=True, send_messages=True,
                                                             read_message_history=True)

        if include_middleman:
            mm_role = guild.get_role(MIDDLEMAN_ROLE_ID)
            if mm_role is not None:
                overwrites[mm_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True,
                                                                  read_message_history=True)

        num = await self.next_counter(ticket_type)
        name = f"{ticket_type}-{num:05d}"

        channel = await guild.create_text_channel(
            name=name,
            category=category,
            overwrites=overwrites,
            reason=f"{ticket_type} ticket created by {requester} ({requester.id})",
        )

        view: discord.ui.View
        if ticket_type == "trade":
            view = TradeTicketView(self)
        else:
            view = TicketActionView(self)

        await channel.send(embed=embed, view=view)
        return channel

    async def archive_and_lock(
            self,
            channel: discord.TextChannel,
            requester_id: int,
            seller_id: Optional[int] = None,
    ) -> None:
        """Archive a ticket so ONLY admins (admin role) can see it.

        Root cause of the leak you saw: the channel kept per-member overwrites (e.g. buyer/seller)
        and was NOT synced to the private Archived category. This method hard-resets overwrites.

        Strategy:
          1) Move to archived category
          2) Replace overwrites with a single deny for @everyone
          3) Do NOT include buyer/seller/member overwrites at all
             (Administrators can still see it due to Discord's admin bypass)
        """
        try:
            _, archived_category_id, _ = self.get_ids()
        except Exception as e:
            logging.warning("archive_and_lock: config missing IDs: %s", e)
            return

        guild = channel.guild
        archive_cat = guild.get_channel(int(archived_category_id))
        if not isinstance(archive_cat, discord.CategoryChannel):
            logging.warning("archive_and_lock: archived category not found (%s)", archived_category_id)
            return

        # Hard-lock: ONLY deny @everyone. Do not keep any member/role overwrites.
        overwrites: Dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, send_messages=False),
        }

        # One-shot edit: move + replace overwrites + stop syncing.
        try:
            await channel.edit(
                category=archive_cat,
                overwrites=overwrites,
                sync_permissions=False,
                reason="Ticket archived (hard-lock: admins only)",
            )
        except TypeError:
            # Some discord.py versions don't accept sync_permissions.
            try:
                await channel.edit(
                    category=archive_cat,
                    overwrites=overwrites,
                    reason="Ticket archived (hard-lock: admins only)",
                )
            except Exception as e:
                logging.warning("archive_and_lock: channel.edit failed: %s", e)
                return
        except Exception as e:
            logging.warning("archive_and_lock: channel.edit failed: %s", e)
            return

        # Defensive: if Discord still reports 'not synced', try to sync (inherits private category deny),
        # then re-apply the hard overwrite again.
        try:
            if hasattr(channel, "permissions_synced") and not channel.permissions_synced:
                try:
                    await channel.edit(sync_permissions=True, reason="Archive sync to private category")
                except Exception:
                    pass
                try:
                    await channel.edit(overwrites=overwrites, sync_permissions=False, reason="Re-apply hard-lock")
                except Exception:
                    pass
        except Exception:
            pass

    async def handle_trade_offer_close(self, ticket: Dict[str, Any], outcome: str) -> None:
        """Close linked trade offer: delete trade-board post and write admin summary to trade-history."""
        if self.db is None:
            return
        offer_id = ticket.get("trade_offer_id")
        if not offer_id:
            return

        async with self.db.execute(
                "SELECT creator_id, intent, have_item, have_qty, want_item, want_qty, notes, board_message_id, ticket_channel_id, status FROM trade_offers WHERE id=? LIMIT 1;",
                (int(offer_id),),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return
        creator_id, intent, have_item, have_qty, want_item, want_qty, notes, board_message_id, ticket_channel_id, status = row

        # Mark closed only for finalized/forced-closed outcomes
        if str(status) == "open" and str(outcome) in ("finalized", "closed"):
            await self.db.execute("UPDATE trade_offers SET status='closed' WHERE id=?;", (int(offer_id),))
            await self.db.commit()

        guild = self.get_guild(self.guild_id)
        if not guild:
            return

        # Delete board message (finalized/forced-closed only)
        if board_message_id and str(outcome) in ("finalized", "closed"):
            board = guild.get_channel(TRADE_BOARD_CHANNEL_ID)
            if isinstance(board, discord.TextChannel):
                try:
                    msg = await board.fetch_message(int(board_message_id))
                    await msg.delete()
                except Exception:
                    pass

        # Post summary to trade-history (finalized trades only)
        if str(outcome) != "finalized":
            return

        hist = guild.get_channel(TRADE_HISTORY_CHANNEL_ID)
        if isinstance(hist, discord.TextChannel):
            creator = await safe_fetch_member(guild, int(creator_id))
            creator_txt = user_search_tag(creator) if creator else f"unknown ({creator_id})"
            colors = [0x00FFFF, 0x00FF00, 0xFFFF00, 0xFF0000]
            embed = discord.Embed(title="Trade Closed", color=random.choice(colors), timestamp=now_utc())
            embed.add_field(name="Outcome", value=outcome, inline=True)
            embed.add_field(name="Offer ID", value=str(offer_id), inline=True)
            embed.add_field(name="Intent", value=str(intent or "trade"), inline=True)
            embed.add_field(name="Offer Creator", value=creator_txt, inline=False)

            counterparty_id = int(ticket.get("seller_id") or 0)
            counterparty = await safe_fetch_member(guild, counterparty_id) if counterparty_id else None
            counterparty_txt = user_search_tag(counterparty) if counterparty else (
                f"unknown ({counterparty_id})" if counterparty_id else "unknown")
            embed.add_field(name="Accepted By", value=counterparty_txt, inline=False)

            offer_items = await db_get_trade_offer_items(self, int(offer_id))
            creator_have = offer_items.get("have") or ([(str(have_item), int(have_qty or 1))] if have_item else [])
            creator_want = offer_items.get("want") or ([(str(want_item), int(want_qty or 1))] if want_item else [])
            if str(intent or '').lower() == 'request':
                if len(creator_want) > 1:
                    want_txt, want_rem = format_trade_items_inline(creator_want)
                else:
                    want_txt, want_rem = format_trade_items(creator_want)
                if want_rem:
                    want_txt += f", +{want_rem} more" if len(creator_want) > 1 else f"\n+{want_rem} more"
                embed.add_field(name="Creator wants", value=want_txt[:1024], inline=True)
                embed.add_field(name="Accepted provides", value="(set in ticket)", inline=True)
            else:
                have_txt, have_rem = format_trade_items(creator_have)
                if have_rem:
                    have_txt += f"\n+{have_rem} more"
                embed.add_field(name="Creator gives", value=have_txt[:1024], inline=True)
                if str(intent or '').lower() == 'offers':
                    embed.add_field(name="Accepted gives", value="Offers", inline=True)
                else:
                    want_txt, want_rem = format_trade_items(creator_want)
                    if want_rem:
                        want_txt += f"\n+{want_rem} more"
                    embed.add_field(name="Accepted gives", value=want_txt[:1024], inline=True)
            if notes:
                embed.add_field(name="Notes", value=str(notes)[:1024], inline=False)

            if ticket_channel_id:
                embed.add_field(name="Ticket Channel", value=f"<#{int(ticket_channel_id)}>", inline=False)
            else:
                embed.add_field(name="Ticket Channel", value=str(ticket.get("ticket_number")), inline=False)
            try:
                await hist.send(embed=embed)
            except Exception:
                pass

    async def _build_trade_offer_embed(
            self,
            *,
            offer_id: int,
            creator_id: int,
            intent: str,
            have_item: str,
            have_qty: int,
            want_item: str,
            want_qty: int,
            notes: Optional[str],
            created_ts: Optional[int],
            expires_ts: Optional[int],
    ) -> discord.Embed:
        title = "Trade Offer"
        if intent == "offers":
            title = "Taking Offers"
        elif intent == "request":
            title = "Request"

        embed = discord.Embed(title=title, color=discord.Color.blurple())
        _total_reps, trade_reps = await _count_reps(self, int(creator_id))
        embed.add_field(name="Trader", value=f"<@{int(creator_id)}>\nTrade rep: {trade_reps}", inline=False)

        bundle = await db_get_trade_offer_items(self, int(offer_id))
        have_items = bundle.get("have") or []
        want_items = bundle.get("want") or []

        if have_items or want_items:
            if intent == "request":
                want_src = want_items or ([(str(want_item), int(want_qty or 1))] if want_item else [])
                if len(want_src) > 1:
                    want_txt, want_rem = format_trade_items_inline(want_src)
                else:
                    want_txt, want_rem = format_trade_items(want_src)
                if want_rem:
                    want_txt += f", +{want_rem} more" if len(want_src) > 1 else f"\n+{want_rem} more"
                embed.add_field(name="Wants", value=want_txt[:1024], inline=True)
                if have_items:
                    have_txt, have_rem = format_trade_items(have_items)
                    if have_rem:
                        have_txt += f"\n+{have_rem} more"
                    embed.add_field(name="Offering", value=have_txt[:1024], inline=True)
                else:
                    embed.add_field(name="Offering", value="(set in ticket)", inline=True)
            else:
                have_src = have_items or ([(str(have_item), int(have_qty or 1))] if have_item else [])
                have_txt, have_rem = format_trade_items(have_src)
                if have_rem:
                    have_txt += f"\n+{have_rem} more"
                embed.add_field(name="Has", value=have_txt[:1024], inline=True)
                if intent == "offers":
                    embed.add_field(name="Wants", value="Offers", inline=True)
                else:
                    want_src = want_items or ([(str(want_item), int(want_qty or 1))] if want_item else [])
                    want_txt, want_rem = format_trade_items(want_src)
                    if want_rem:
                        want_txt += f"\n+{want_rem} more"
                    embed.add_field(name="Wants", value=want_txt[:1024], inline=True)
        else:
            if intent == "request":
                embed.add_field(name="Wants", value=f"{want_item} ×{int(want_qty)}", inline=True)
                if have_item:
                    embed.add_field(name="Offering", value=f"{have_item} ×{int(have_qty or 1)}", inline=True)
                else:
                    embed.add_field(name="Offering", value="(set in ticket)", inline=True)
            else:
                embed.add_field(name="Has", value=f"{have_item} ×{int(have_qty)}", inline=True)
                if intent == "offers":
                    embed.add_field(name="Wants", value="Offers", inline=True)
                else:
                    embed.add_field(name="Wants", value=f"{want_item} ×{int(want_qty)}", inline=True)

        if notes:
            embed.add_field(name="Notes", value=str(notes)[:1024], inline=False)

        if expires_ts:
            embed.add_field(
                name="Expires",
                value=discord.utils.format_dt(datetime.fromtimestamp(int(expires_ts), tz=timezone.utc), style='R'),
                inline=False,
            )
        embed.set_footer(text=f"Offer #{offer_id} • {intent}")

        if created_ts:
            embed.timestamp = datetime.fromtimestamp(int(created_ts), tz=timezone.utc)

        return embed

    async def _send_trade_offer_to_board(
            self,
            *,
            guild: discord.Guild,
            offer_id: int,
            intent: str,
            embed: discord.Embed,
    ) -> Optional[int]:
        """Post an offer to #trade-board with a persistent button. Returns message id."""
        try:
            ch = guild.get_channel(int(TRADE_BOARD_CHANNEL_ID))
            if ch is None:
                ch = await guild.fetch_channel(int(TRADE_BOARD_CHANNEL_ID))  # type: ignore
            if not isinstance(ch, discord.TextChannel):
                return None

            view = make_trade_open_view(offer_id=int(offer_id), intent=str(intent or "trade"))
            msg = await ch.send(embed=embed, view=view)
            return int(msg.id)
        except Exception:
            return None

    async def _delete_trade_board_message(self, *, guild: discord.Guild, message_id: Optional[int]) -> None:
        if not message_id:
            return
        try:
            ch = guild.get_channel(int(TRADE_BOARD_CHANNEL_ID))
            if ch is None:
                ch = await guild.fetch_channel(int(TRADE_BOARD_CHANNEL_ID))  # type: ignore
            if not isinstance(ch, discord.TextChannel):
                return
            msg = await ch.fetch_message(int(message_id))
            await msg.delete()
        except Exception:
            return

    async def restore_trade_offer_after_cancel(self, ticket: Dict[str, Any]) -> None:
        """If a trade ticket is canceled, restore the linked offer and extend expiry by 1 day from cancel time."""
        if self.db is None:
            return
        offer_id = ticket.get("trade_offer_id")
        if not offer_id:
            return

        now_ts = int(datetime.now(timezone.utc).timestamp())
        new_expires = now_ts + 86400

        async with self.db.execute(
                "SELECT creator_id, intent, have_item, have_qty, want_item, want_qty, notes, board_message_id, created_ts "
                "FROM trade_offers WHERE id=? LIMIT 1;",
                (int(offer_id),),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return

        creator_id, intent, have_item, have_qty, want_item, want_qty, notes, board_message_id, created_ts = row

        await self.db.execute(
            "UPDATE trade_offers SET ticket_channel_id=NULL, board_message_id=NULL, status='open', expires_ts=? WHERE id=?;",
            (int(new_expires), int(offer_id)),
        )
        await self.db.commit()

        guild = self.get_guild(int(self.guild_id))
        if guild is None:
            return

        embed = await self._build_trade_offer_embed(
            offer_id=int(offer_id),
            creator_id=int(creator_id),
            intent=str(intent),
            have_item=str(have_item),
            have_qty=safe_int(have_qty),
            want_item=str(want_item),
            want_qty=safe_int(want_qty),
            notes=(str(notes) if notes else None),
            created_ts=int(created_ts) if created_ts else None,
            expires_ts=int(new_expires),
        )
        new_msg_id = await self._send_trade_offer_to_board(
            guild=guild,
            offer_id=int(offer_id),
            intent=str(intent),
            embed=embed,
        )
        if new_msg_id:
            await self.db.execute("UPDATE trade_offers SET board_message_id=? WHERE id=?;",
                                  (int(new_msg_id), int(offer_id)))
            await self.db.commit()
        return

    async def open_trade_offer_ticket(self, interaction: discord.Interaction, offer_id: int) -> None:
        """Open a trade ticket from a trade-board offer. Works for persistent buttons across restarts."""
        if self.db is None or not interaction.guild:
            try:
                await interaction.response.send_message("DB not ready.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("DB not ready.", ephemeral=True)
            return

        async with self.db.execute(
                """
                SELECT id, status, creator_id, intent, have_item, have_qty, want_item, want_qty, notes, ticket_channel_id, board_message_id, expires_ts
                FROM trade_offers WHERE id=? LIMIT 1;
                """,
                (int(offer_id),),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            try:
                await interaction.response.send_message("Offer not found.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("Offer not found.", ephemeral=True)
            return

        oid, status, creator_id, intent, have_item, have_qty, want_item, want_qty, notes, ticket_channel_id, board_message_id, expires_ts = row
        if str(status) != "open":
            try:
                await interaction.response.send_message("That offer is not open.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("That offer is not open.", ephemeral=True)
            return

        now_ts = int(datetime.now(timezone.utc).timestamp())
        if expires_ts is not None and int(expires_ts or 0) > 0 and int(expires_ts) <= now_ts:
            try:
                await self.db.execute("UPDATE trade_offers SET status='expired', board_message_id=NULL WHERE id=?;", (int(oid),))
                await self.db.commit()
            except Exception:
                pass
            try:
                await self._delete_trade_board_message(guild=interaction.guild, message_id=int(board_message_id) if board_message_id else None)
            except Exception:
                pass
            try:
                await interaction.response.send_message("That offer has expired.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("That offer has expired.", ephemeral=True)
            return

        if int(creator_id) == int(interaction.user.id):
            logging.info("Blocked self-claim for trade offer %s by user %s", int(oid), int(interaction.user.id))
            try:
                await interaction.response.send_message("Someone else needs to join your offer.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("Someone else needs to join your offer.", ephemeral=True)
            return

        # Defer early so Discord doesn't show "interaction failed" during channel creation / API work.
        # After this point, prefer followups (response may already be used).
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.InteractionResponded:
            pass
        except Exception:
            pass

        # Gate concurrent clicks: atomically claim the offer (first click wins).
        # ticket_channel_id meanings:
        #   NULL  -> unclaimed
        #   -1    -> someone is opening right now
        #   >0    -> ticket channel id
        try:
            # If already marked as "opening", fail fast.
            if ticket_channel_id is not None and int(ticket_channel_id) < 0:
                try:
                    await interaction.response.send_message("Someone is already opening this offer. Try again.",
                                                            ephemeral=True)
                except discord.InteractionResponded:
                    await interaction.followup.send("Someone is already opening this offer. Try again.", ephemeral=True)
                return

            # If a stale channel id is stored but channel doesn't exist, clear it.
            if ticket_channel_id is not None and int(ticket_channel_id) > 0:
                existing = interaction.guild.get_channel(int(ticket_channel_id))
                if existing is None:
                    await self.db.execute("UPDATE trade_offers SET ticket_channel_id=NULL WHERE id=?;", (int(oid),))
                    await self.db.commit()
                    ticket_channel_id = None

            if not ticket_channel_id:
                cur2 = await self.db.execute(
                    "UPDATE trade_offers SET ticket_channel_id=-1 WHERE id=? AND status='open' AND ticket_channel_id IS NULL;",
                    (int(oid),),
                )
                await self.db.commit()
                claimed = int(getattr(cur2, "rowcount", 0) or 0)
                try:
                    await cur2.close()
                except Exception:
                    pass
                if claimed != 1:
                    # Lost the race.
                    try:
                        async with self.db.execute("SELECT ticket_channel_id FROM trade_offers WHERE id=? LIMIT 1;",
                                                   (int(oid),)) as c3:
                            r3 = await c3.fetchone()
                        tcid = int(r3[0]) if r3 and r3[0] is not None else None
                    except Exception:
                        tcid = None
                    if tcid and tcid > 0:
                        chx = interaction.guild.get_channel(int(tcid))
                        if isinstance(chx, discord.TextChannel):
                            msg = f"Ticket already exists: {chx.mention}"
                        else:
                            msg = "Ticket already exists."
                    else:
                        msg = "This offer was just claimed by someone else."
                    try:
                        await interaction.response.send_message(msg, ephemeral=True)
                    except discord.InteractionResponded:
                        await interaction.followup.send(msg, ephemeral=True)
                    return
        except Exception as e:
            logging.warning("offer claim failed: %s", e)

        if ticket_channel_id:
            ch = interaction.guild.get_channel(int(ticket_channel_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await interaction.response.send_message(f"Ticket already exists: {ch.mention}", ephemeral=True)
                except discord.InteractionResponded:
                    await interaction.followup.send(f"Ticket already exists: {ch.mention}", ephemeral=True)
                return


        creator = await safe_fetch_member(interaction.guild, int(creator_id))
        joiner = interaction.user
        if not isinstance(joiner, discord.Member):
            try:
                await interaction.response.send_message("Member only.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("Member only.", ephemeral=True)
            return
        if creator is None:
            try:
                await interaction.response.send_message("Original trader not found.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("Original trader not found.", ephemeral=True)
            return

        intent_s = (str(intent or "trade").strip().lower())
        title = "Trade Ticket"
        if intent_s == "offers":
            title = "Offer Ticket"
        elif intent_s == "request":
            title = "Request Ticket"

        embed = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=now_utc())
        embed.add_field(name="Trader A", value=user_search_tag(creator), inline=False)
        embed.add_field(name="Trader B", value=user_search_tag(joiner), inline=False)

        offer_items = await db_get_trade_offer_items(self, int(oid))
        creator_have_items = offer_items.get("have") or ([(str(have_item), int(have_qty or 1))] if have_item else [])
        creator_want_items = offer_items.get("want") or ([(str(want_item), int(want_qty or 1))] if want_item else [])
        if intent_s == "request":
            want_txt, want_rem = format_trade_items(creator_want_items)
            if want_rem:
                want_txt += f"\n+{want_rem} more"
            embed.add_field(name="A wants", value=want_txt[:1024], inline=True)
            if creator_have_items:
                have_txt, have_rem = format_trade_items(creator_have_items)
                if have_rem:
                    have_txt += f"\n+{have_rem} more"
                embed.add_field(name="A offers", value=have_txt[:1024], inline=True)
            else:
                embed.add_field(name="A offers", value="(set in ticket)", inline=True)
        else:
            have_txt, have_rem = format_trade_items(creator_have_items)
            if have_rem:
                have_txt += f"\n+{have_rem} more"
            embed.add_field(name="A has", value=have_txt[:1024], inline=True)
            if intent_s == "offers":
                embed.add_field(name="A wants", value="Taking offers", inline=True)
            else:
                want_txt, want_rem = format_trade_items(creator_want_items)
                if want_rem:
                    want_txt += f"\n+{want_rem} more"
                embed.add_field(name="A wants", value=want_txt[:1024], inline=True)

        if notes:
            embed.add_field(name="Notes", value=str(notes)[:1024], inline=False)

        embed.set_footer(text=f"Offer #{oid} • {intent_s}")

        try:
            channel = await self.create_ticket_channel(
                interaction.guild,
                "trade",
                creator,
                embed,
                seller=joiner,
                include_middleman=False,
            )

            ticket_number = int(channel.name.split("-")[1])
            await self.db.execute(
                """
                INSERT INTO tickets(
                    ticket_type, ticket_number, channel_id, requester_id, seller_id,
                    status, created_at,
                    trade_offer_id,
                    offer_item, want_item,
                    offer_qty, want_qty
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    "trade",
                    ticket_number,
                    int(channel.id),
                    int(creator.id),
                    int(joiner.id),
                    "open",
                    dt_to_iso(now_utc()),
                    int(oid),
                    (str(have_item)[:200] if have_item is not None else None),
                    str(want_item)[:200],
                    (int(have_qty or 1) if have_item is not None else 0),
                    int(want_qty or 1),
                ),
            )
            await self.db.execute(
                "UPDATE trade_offers SET ticket_channel_id=?, board_message_id=NULL WHERE id=?;",
                (int(channel.id), int(oid)),
            )
            await self.db.commit()
            async with self.db.execute("SELECT id FROM tickets WHERE channel_id=? LIMIT 1;", (int(channel.id),)) as cur_ticket:
                ticket_row = await cur_ticket.fetchone()
            if ticket_row:
                await db_set_ticket_trade_items(self, int(ticket_row[0]), creator_have_items)
        except Exception as e:
            logging.warning("trade offer ticket creation failed for offer %s: %s", int(oid), e, exc_info=True)
            try:
                await self.db.execute(
                    "UPDATE trade_offers SET ticket_channel_id=NULL WHERE id=? AND ticket_channel_id=-1;",
                    (int(oid),),
                )
                await self.db.commit()
            except Exception:
                pass
            try:
                await interaction.followup.send("Failed to create the ticket. Please try again.", ephemeral=True)
            except Exception:
                pass
            return

        # Remove the public trade-board post while the ticket is active.
        try:
            await self._delete_trade_board_message(guild=interaction.guild,
                                                   message_id=int(board_message_id) if board_message_id else None)
        except Exception:
            pass

        try:
            await channel.send(content=f"{creator.mention} {joiner.mention}")
        except Exception:
            pass

        try:
            if intent_s == "offers":
                await channel.send(f"{joiner.mention} — type your offer (items + qty).")
            elif intent_s == "request":
                await channel.send(
                    f"{joiner.mention} — type what you can provide to fulfill the request (items + qty).")
        except Exception:
            pass

        try:
            try:
                await interaction.response.send_message(f"Trade ticket created: {channel.mention}", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send(f"Trade ticket created: {channel.mention}", ephemeral=True)
        except discord.InteractionResponded:
            await interaction.followup.send(f"Trade ticket created: {channel.mention}", ephemeral=True)

    # ---------- Rep role sweep (failsafe) ----------

    @tasks.loop(hours=1)
    async def rep_role_sweep_loop(self) -> None:
        """Hourly sweep to (re)apply trade rep milestone roles to members.

        This is a failsafe if a role assignment failed at rep time due to permissions/hierarchy,
        or if the immediate award call was missed.
        """
        if self.db is None:
            return
        guild = self.get_guild(self.guild_id)
        if guild is None:
            return

        rules = (self.cfg.get("rep_roles") or {}).get("trade") or []
        cleaned: List[Tuple[int, int]] = []
        for r in rules:
            if not isinstance(r, dict):
                continue
            try:
                min_reps = int(r.get("min_reps") or 0)
                role_id = int(r.get("role_id") or 0)
            except Exception:
                continue
            if min_reps > 0 and role_id > 0:
                cleaned.append((min_reps, role_id))
        cleaned.sort(key=lambda x: x[0])
        if not cleaned:
            return

        # Get trade rep counts in one query
        try:
            async with self.db.execute(
                    """
                    SELECT target_id, COUNT(*) AS trade_reps
                      FROM reps
                     WHERE rep_type='trade_partner'
                     GROUP BY target_id;
                    """
            ) as cur:
                rows = await cur.fetchall()
        except Exception as e:
            logging.warning("rep_role_sweep_loop DB query failed: %s", e)
            return

        for target_id, trade_reps in rows:
            try:
                uid = int(target_id)
                reps_count = int(trade_reps or 0)
            except Exception:
                continue
            if reps_count <= 0:
                continue

            member = guild.get_member(uid)
            if member is None or member.bot:
                continue

            have_ids = {r.id for r in member.roles}
            to_add: List[discord.Role] = []
            for min_reps, role_id in cleaned:
                if reps_count < min_reps:
                    continue
                if role_id in have_ids:
                    continue
                role = guild.get_role(int(role_id))
                if role is not None:
                    to_add.append(role)

            if not to_add:
                continue

            try:
                await member.add_roles(*to_add, reason=f"Trade rep milestone sweep: {reps_count} trade reps")
            except Exception as e:
                logging.warning(
                    "rep_role_sweep_loop: failed adding roles to %s (%s): %s",
                    getattr(member, "display_name", "member"),
                    member.id,
                    e,
                )

    @rep_role_sweep_loop.before_loop
    async def _before_rep_role_sweep_loop(self) -> None:
        await self.wait_until_ready()

    # ---------- Views / Modals ----------

    async def cleanup_old_archived_tickets(self) -> None:
        assert self.db is not None
        cutoff = now_utc() - timedelta(days=TICKET_DELETE_AFTER_DAYS)

        async with self.db.execute(
                """
                SELECT channel_id, closed_at
                FROM tickets
                WHERE status='closed' AND closed_at IS NOT NULL;
                """
        ) as cur:
            rows = await cur.fetchall()

        to_delete: List[int] = []
        for ch_id, closed_at in rows:
            try:
                dt = iso_to_dt(closed_at)
            except Exception:
                continue
            if dt <= cutoff:
                to_delete.append(int(ch_id))

        for ch_id in to_delete:
            ch = self.get_channel(ch_id)
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.delete(reason=f"Auto-delete ticket older than {TICKET_DELETE_AFTER_DAYS} days")
                except Exception:
                    continue

        await self.db.execute(
            """
            DELETE FROM tickets
            WHERE status='closed' AND closed_at IS NOT NULL AND closed_at <= ?;
            """,
            (dt_to_iso(cutoff),),
        )
        await self.db.commit()


# ---------- Views / Modals ----------

class BuyModal(discord.ui.Modal, title="Buy request details"):
    qty = discord.ui.TextInput(label="Quantity", placeholder="1", required=True, max_length=5)
    payment = discord.ui.TextInput(label="Payment method", placeholder="paypal / btc / ltc / eth", required=True,
                                   max_length=16)
    notes = discord.ui.TextInput(label="Notes / availability", placeholder="When can you trade? Any details?",
                                 required=False, max_length=400, style=discord.TextStyle.paragraph)

    def __init__(self, bot: ArcTraderBot, listing_id: int):
        super().__init__()
        self.bot = bot
        self.listing_id = listing_id

    async def on_submit(self, interaction: discord.Interaction):
        bot = self.bot
        listing = await bot.get_listing_by_id(self.listing_id)
        if not listing:
            await interaction.response.send_message("That listing no longer exists.", ephemeral=True)
            return

        if listing["trade_only"]:
            await interaction.response.send_message("That listing is trade-only (use /trade).", ephemeral=True)
            return

        if not listing["payment_methods"]:
            await interaction.response.send_message("That listing currently has no payment methods enabled.",
                                                    ephemeral=True)
            return

        qty = safe_int(self.qty.value, 0)
        if qty <= 0:
            await interaction.response.send_message("Quantity must be a positive number.", ephemeral=True)
            return

        available = listing["quantity"] - listing["quantity_reserved"]
        if qty > available:
            await interaction.response.send_message(f"Not enough available. Available: {available}.", ephemeral=True)
            return

        payment = (self.payment.value or "").strip().lower()
        if payment not in listing["payment_methods"]:
            await interaction.response.send_message(
                f"Payment method not allowed. Allowed: {', '.join(listing['payment_methods'])}",
                ephemeral=True
            )
            return

        ok, msg = await bot.reserve_listing(self.listing_id, qty)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True)
            return

        requester = interaction.user
        assert isinstance(requester, discord.Member)

        embed = discord.Embed(
            title="Buy Ticket",
            description="A buy request has been opened.",
            color=discord.Color.green(),
            timestamp=now_utc(),
        )
        embed.add_field(name="Buyer", value=f"{requester.mention} (`{requester.id}`)", inline=False)
        embed.add_field(name="Item", value=listing["item_name"], inline=True)
        embed.add_field(name="Quantity", value=str(qty), inline=True)
        embed.add_field(name="Price (USD)", value=f"${listing['price_usd']:.2f}", inline=False)
        embed.add_field(name="Payment method", value=payment, inline=True)
        embed.add_field(name="Allowed rails", value=", ".join(listing["payment_methods"]), inline=True)
        if listing["trade_allowed"]:
            tn = listing["trade_notes"] or "Trade offers accepted."
            embed.add_field(name="Trade allowed", value=tn[:1024], inline=False)
        if self.notes.value:
            embed.add_field(name="Notes", value=self.notes.value[:1024], inline=False)
        embed.set_footer(text="Use buttons below to Finalize or Release reservation.")

        seller_member = None
        if listing.get("seller_id"):
            seller_member = await safe_fetch_member(interaction.guild, int(listing["seller_id"]))  # type: ignore
        channel = await bot.create_ticket_channel(interaction.guild, "buy", requester, embed,
                                                  seller=seller_member)  # type: ignore
        if seller_member:
            try:
                await channel.send(content=f"{seller_member.mention}")
            except Exception:
                pass

        assert bot.db is not None
        ticket_number = int(channel.name.split("-")[1])

        await bot.db.execute(
            """
            INSERT INTO tickets(
                ticket_type, ticket_number, channel_id, requester_id, seller_id,
                status, created_at,
                listing_id, item_name, reserved_qty,
                price_usd, payment_methods, trade_allowed, trade_only, trade_notes,
                payment_method, buyer_notes
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                "buy", ticket_number, channel.id, requester.id, int(listing.get("seller_id") or 0) or None,
                "open", dt_to_iso(now_utc()),
                listing["id"], listing["item_name"], qty,
                listing["price_usd"], json.dumps(listing["payment_methods"]),
                int(listing["trade_allowed"]), int(listing["trade_only"]), listing["trade_notes"][:400],
                payment, (self.notes.value or "")[:400],
            )
        )
        await bot.db.commit()

        await interaction.response.send_message(
            f"Ticket created: {channel.mention} (reserved {qty}x).",
            ephemeral=True
        )


class TradeModal(discord.ui.Modal, title="Trade request details"):
    qty = discord.ui.TextInput(label="Quantity wanted", placeholder="1", required=True, max_length=5)
    notes = discord.ui.TextInput(label="Notes / availability", placeholder="Details of your offer and timing",
                                 required=False, max_length=400, style=discord.TextStyle.paragraph)

    def __init__(self, bot: ArcTraderBot, listing_id: int, offer_item: str):
        super().__init__()
        self.bot = bot
        self.listing_id = listing_id
        self.offer_item = offer_item

    async def on_submit(self, interaction: discord.Interaction):
        bot = self.bot
        listing = await bot.get_listing_by_id(self.listing_id)
        if not listing:
            await interaction.response.send_message("That wanted listing no longer exists.", ephemeral=True)
            return

        if not listing["trade_allowed"]:
            await interaction.response.send_message("That listing is not accepting trades.", ephemeral=True)
            return

        qty = safe_int(self.qty.value, 0)
        if qty <= 0:
            await interaction.response.send_message("Quantity must be a positive number.", ephemeral=True)
            return

        available = listing["quantity"] - listing["quantity_reserved"]
        if qty > available:
            await interaction.response.send_message(f"Not enough available. Available: {available}.", ephemeral=True)
            return

        ok, msg = await bot.reserve_listing(self.listing_id, qty)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True)
            return

        requester = interaction.user
        assert isinstance(requester, discord.Member)

        embed = discord.Embed(
            title="Trade Ticket",
            description="A trade request has been opened.",
            color=discord.Color.blurple(),
            timestamp=now_utc(),
        )
        embed.add_field(name="Requester", value=f"{requester.mention} (`{requester.id}`)", inline=False)
        embed.add_field(name="They want", value=f"{qty}x {listing['item_name']}", inline=True)
        embed.add_field(name="They offer", value=self.offer_item, inline=True)
        embed.add_field(name="Value (USD baseline)", value=f"${listing['price_usd']:.2f}", inline=True)
        if listing["trade_notes"]:
            embed.add_field(name="Trade notes", value=listing["trade_notes"][:1024], inline=False)
        if self.notes.value:
            embed.add_field(name="Notes", value=self.notes.value[:1024], inline=False)
        embed.set_footer(text="Use buttons below to Finalize or Release reservation.")

        seller_member = None
        if listing.get("seller_id"):
            seller_member = await safe_fetch_member(interaction.guild, int(listing["seller_id"]))  # type: ignore
        channel = await bot.create_ticket_channel(interaction.guild, "trade", requester, embed,
                                                  seller=seller_member)  # type: ignore
        if seller_member:
            try:
                await channel.send(content=f"{seller_member.mention}")
            except Exception:
                pass

        assert bot.db is not None
        ticket_number = int(channel.name.split("-")[1])
        await bot.db.execute(
            """
            INSERT INTO tickets(
                ticket_type, ticket_number, channel_id, requester_id, seller_id,
                status, created_at,
                listing_id, item_name, reserved_qty,
                price_usd, payment_methods, trade_allowed, trade_only, trade_notes,
                offer_item, want_item
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                "trade", ticket_number, channel.id, requester.id, int(listing.get("seller_id") or 0) or None,
                "open", dt_to_iso(now_utc()),
                listing["id"], listing["item_name"], qty,
                listing["price_usd"], json.dumps(listing["payment_methods"]),
                int(listing["trade_allowed"]), int(listing["trade_only"]), listing["trade_notes"][:400],
                self.offer_item[:200], listing["item_name"][:200],
            )
        )
        await bot.db.commit()

        await interaction.response.send_message(
            f"Ticket created: {channel.mention} (reserved {qty}x).",
            ephemeral=True
        )


class SellModal(discord.ui.Modal, title="Sell request details"):
    qty = discord.ui.TextInput(label="Quantity", placeholder="1", required=True, max_length=5)
    ask = discord.ui.TextInput(label="Asking price / expectations", placeholder="e.g. 0.001 BTC or $10", required=False,
                               max_length=64)
    notes = discord.ui.TextInput(label="Notes / availability", placeholder="Any details", required=False,
                                 max_length=400, style=discord.TextStyle.paragraph)

    def __init__(self, bot: ArcTraderBot, sell_item: str, payout_method: str):
        super().__init__()
        self.bot = bot
        self.sell_item = sell_item
        self.payout_method = payout_method

    async def on_submit(self, interaction: discord.Interaction):
        bot = self.bot
        requester = interaction.user
        assert isinstance(requester, discord.Member)

        qty = safe_int(self.qty.value, 0)
        if qty <= 0:
            await interaction.response.send_message("Quantity must be a positive number.", ephemeral=True)
            return

        embed = discord.Embed(
            title="Sell Ticket",
            description="A sell request has been opened.",
            color=discord.Color.orange(),
            timestamp=now_utc(),
        )
        embed.add_field(name="Requester", value=f"{requester.mention} (`{requester.id}`)", inline=False)
        embed.add_field(name="Item", value=self.sell_item, inline=True)
        embed.add_field(name="Quantity", value=str(qty), inline=True)
        embed.add_field(name="Payout method", value=self.payout_method, inline=True)
        if self.ask.value:
            embed.add_field(name="Ask", value=self.ask.value[:64], inline=True)
        if self.notes.value:
            embed.add_field(name="Notes", value=self.notes.value[:1024], inline=False)
        embed.set_footer(text="Seller can close this ticket when finished.")

        channel = await bot.create_ticket_channel(interaction.guild, "sell", requester, embed)  # type: ignore

        assert bot.db is not None
        ticket_number = int(channel.name.split("-")[1])
        await bot.db.execute(
            """
            INSERT INTO tickets(
                ticket_type, ticket_number, channel_id, requester_id, seller_id,
                status, created_at,
                sell_item, payout_method, ask_price, buyer_notes
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                "sell", ticket_number, channel.id, requester.id, None,
                "open", dt_to_iso(now_utc()),
                self.sell_item[:200], self.payout_method[:16], (self.ask.value or "")[:64],
                (self.notes.value or "")[:400],
            )
        )
        await bot.db.commit()

        await interaction.response.send_message(f"Ticket created: {channel.mention}.", ephemeral=True)


# ----- Trade offer board + trade ticket views -----

class TradeOfferView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot, offer_id: int, intent: str = "trade"):
        super().__init__(timeout=None)
        self.bot = bot
        self.offer_id = int(offer_id)
        self.intent = (intent or "trade").strip().lower()

        label = "Open trade ticket"
        if self.intent == "offers":
            label = "Send offer"
        elif self.intent == "request":
            label = "Fulfill request"

        btn = discord.ui.Button(
            label=label,
            style=discord.ButtonStyle.primary,
            custom_id=f"rt:trade_open:{self.offer_id}",
        )
        btn.callback = self._open_ticket  # type: ignore
        self.add_item(btn)

    async def _open_ticket(self, interaction: discord.Interaction):
        await self.bot.open_trade_offer_ticket(interaction, self.offer_id)


class TradeOpenButton(discord.ui.DynamicItem[discord.ui.Button], template=r"rt:trade_open:(\d+)"):
    """Persistent handler for trade-board open buttons (survives restarts)."""

    def __init__(self, bot: ArcTraderBot, offer_id: int):
        super().__init__(
            discord.ui.Button(
                label="Open trade ticket",
                style=discord.ButtonStyle.primary,
                custom_id=f"rt:trade_open:{int(offer_id)}",
            )
        )
        self.bot = bot
        self.offer_id = int(offer_id)

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match: re.Match[str]):
        bot: ArcTraderBot = interaction.client  # type: ignore
        return cls(bot, int(match.group(1)))

    async def callback(self, interaction: discord.Interaction):
        await self.bot.open_trade_offer_ticket(interaction, self.offer_id)


class LegacyTradeOpenButton(discord.ui.DynamicItem[discord.ui.Button], template=r"trade_offer_open"):
    """Back-compat: old board buttons used custom_id='trade_offer_open'. Resolve offer_id via message_id."""

    def __init__(self, bot: ArcTraderBot):
        super().__init__(
            discord.ui.Button(
                label="Open trade ticket",
                style=discord.ButtonStyle.primary,
                custom_id="trade_offer_open",
            )
        )
        self.bot = bot

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Button, match: re.Match[str]):
        bot: ArcTraderBot = interaction.client  # type: ignore
        return cls(bot)

    async def callback(self, interaction: discord.Interaction):
        bot = self.bot
        if bot.db is None or not interaction.message:
            try:
                await interaction.response.send_message("DB not ready.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("DB not ready.", ephemeral=True)
            return

        try:
            async with bot.db.execute(
                    "SELECT id FROM trade_offers WHERE board_message_id=? LIMIT 1;",
                    (int(interaction.message.id),),
            ) as cur:
                row = await cur.fetchone()
        except Exception:
            row = None

        if not row:
            try:
                await interaction.response.send_message("That offer no longer exists.", ephemeral=True)
            except discord.InteractionResponded:
                await interaction.followup.send("That offer no longer exists.", ephemeral=True)
            return

        await bot.open_trade_offer_ticket(interaction, int(row[0]))


class TradeBoardPersistentView(discord.ui.View):
    """Registered once on startup. Provides persistent handlers for trade-board buttons."""

    def __init__(self, bot: ArcTraderBot):
        super().__init__(timeout=None)
        self.add_item(TradeOpenButton(bot, 0))
        self.add_item(LegacyTradeOpenButton(bot))


class TradeTicketView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        if is_admin_member(interaction.user):
            return True

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            return False

        t = await self.bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return False

        if interaction.user.id in (int(t.get("requester_id") or 0), int(t.get("seller_id") or 0)):
            return True

        await interaction.response.send_message("Only the two traders or admins can use these.", ephemeral=True)
        return False

    @discord.ui.button(label="Request Middleman", style=discord.ButtonStyle.secondary, custom_id="trade_middleman")
    async def middleman_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "trade_middleman"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel) or bot.db is None:
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t or t.get("ticket_type") != "trade":
            await interaction.response.send_message("No open trade ticket found.", ephemeral=True)
            return

        if t.get("middleman_requested"):
            await interaction.response.send_message("Middleman already requested for this ticket.", ephemeral=True)
            return

        # Admins can request instantly; normal users require BOTH traders to press.
        is_admin_user = is_admin(interaction)
        ticket_id = int(t["id"])
        requester_id = int(t.get("requester_id") or 0)
        seller_id = int(t.get("seller_id") or 0)

        if is_admin_user:
            await bot.db.execute(
                "UPDATE tickets SET middleman_requested=1, middleman_req_ok=1, middleman_seller_ok=1 WHERE id=?;",
                (ticket_id,),
            )
            await bot.db.commit()
        else:
            uid = int(interaction.user.id)
            if uid == requester_id:
                await bot.db.execute("UPDATE tickets SET middleman_req_ok=1 WHERE id=?;", (ticket_id,))
                await bot.db.commit()
            elif uid == seller_id:
                await bot.db.execute("UPDATE tickets SET middleman_seller_ok=1 WHERE id=?;", (ticket_id,))
                await bot.db.commit()

            async with bot.db.execute(
                    "SELECT middleman_req_ok, middleman_seller_ok FROM tickets WHERE id=? LIMIT 1;",
                    (ticket_id,),
            ) as cur:
                row = await cur.fetchone()

            req_ok = bool(int(row[0] or 0)) if row else False
            sell_ok = bool(int(row[1] or 0)) if row else False

            if not (req_ok and sell_ok):
                who = []
                if req_ok:
                    who.append("requester")
                if sell_ok:
                    who.append("seller")
                status_txt = ", ".join(who) if who else "none"
                await interaction.response.send_message(
                    f"Middleman request recorded. Waiting for the other trader to confirm. (Confirmed: {status_txt})",
                    ephemeral=True,
                )
                return

            await bot.db.execute("UPDATE tickets SET middleman_requested=1 WHERE id=?;", (ticket_id,))
            await bot.db.commit()

        mm_role = ch.guild.get_role(MIDDLEMAN_ROLE_ID)
        if mm_role:
            await ch.send(content=f"{mm_role.mention} (middleman requested)")
        await interaction.response.send_message("Middleman requested.", ephemeral=True)

        button.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="Finalize", style=discord.ButtonStyle.success, custom_id="trade_finalize")
    async def finalize_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "trade_finalize"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        requester_id = int(t.get("requester_id") or 0)
        seller_id = int(t.get("seller_id") or 0) or None
        if not seller_id:
            await interaction.response.send_message("This trade ticket is missing the other trader.", ephemeral=True)
            return

        uid = int(interaction.user.id)
        # Admin override: one-click finalize.
        if interaction.user and isinstance(interaction.user, discord.Member) and is_admin_member(interaction.user):
            await bot._set_ticket_flag(t["id"], "finalize_req_ok")
            await bot._set_ticket_flag(t["id"], "finalize_seller_ok")
        else:
            if uid == requester_id:
                await bot._set_ticket_flag(t["id"], "finalize_req_ok")
            elif uid == int(seller_id):
                await bot._set_ticket_flag(t["id"], "finalize_seller_ok")

        flags = await bot._get_ticket_flags(t["id"])
        if not (flags["finalize_req_ok"] and flags["finalize_seller_ok"]):
            other = "<@%s>" % (seller_id if uid == requester_id else requester_id)
            await interaction.response.send_message(
                f"✅ {interaction.user.mention} confirmed finalize. Waiting on {other}.",
                ephemeral=False,
            )
            return

        # Acknowledge the interaction early to avoid 'Unknown interaction' (3s timeout)
        if not interaction.response.is_done():
            try:
                await interaction.response.defer(ephemeral=True, thinking=True)
            except Exception:
                pass

        await bot.close_ticket_record(t["id"], "finalized")
        await bot.handle_trade_offer_close(t, outcome="finalized")
        # DM both parties a summary + rep reminder (best-effort; DMs may be closed).
        try:
            if bot.db is not None:
                async with bot.db.execute(
                        "SELECT trade_offer_id, offer_item, offer_qty, want_item, want_qty FROM tickets WHERE id=? LIMIT 1;",
                        (int(t["id"]),),
                ) as cur:
                    tr = await cur.fetchone()
            else:
                tr = None
        except Exception:
            tr = None

        offer_id = int(tr[0]) if tr and tr[0] is not None else 0
        offer_item = tr[1] if tr else None
        offer_qty = int(tr[2] or 0) if tr else 0
        want_item = tr[3] if tr else None
        want_qty = int(tr[4] or 0) if tr else 0

        # If this ticket came from a board offer, prefer the canonical trade_offers row for intent + wording.
        offer_intent = None
        if offer_id and bot.db is not None:
            try:
                async with bot.db.execute(
                        "SELECT intent, have_item, have_qty, want_item, want_qty FROM trade_offers WHERE id=? LIMIT 1;",
                        (int(offer_id),),
                ) as cur:
                    orow = await cur.fetchone()
                if orow:
                    offer_intent = str(orow[0] or "").lower()
                    offer_item = orow[1]
                    offer_qty = int(orow[2] or 0) if orow[2] is not None else 0
                    want_item = orow[3]
                    want_qty = int(orow[4] or 0)
            except Exception:
                pass

        # Build a readable summary depending on intent.
        offer_have_items: List[Tuple[str, int]] = []
        offer_want_items: List[Tuple[str, int]] = []
        if offer_id and bot.db is not None:
            try:
                offer_bundle = await db_get_trade_offer_items(bot, int(offer_id))
                offer_have_items = offer_bundle.get("have") or []
                offer_want_items = offer_bundle.get("want") or []
            except Exception:
                offer_have_items = []
                offer_want_items = []

        if not offer_have_items and offer_item:
            offer_have_items = [(str(offer_item), max(1, int(offer_qty or 1)))]
        if not offer_want_items and want_item:
            offer_want_items = [(str(want_item), max(1, int(want_qty or 1)))]

        if (offer_intent or "") == "request":
            if len(offer_want_items) > 1:
                want_txt, want_rem = format_trade_items_inline(offer_want_items)
            else:
                want_txt, want_rem = format_trade_items(offer_want_items)
            if want_rem:
                want_txt += f", +{want_rem} more" if len(offer_want_items) > 1 else f"\n+{want_rem} more"
            trade_summary = f"**Request finalized**\nWanted:\n{want_txt}\nOffering: (set in ticket)"
        elif (offer_intent or "") == "offers":
            have_txt, have_rem = format_trade_items(offer_have_items)
            if have_rem:
                have_txt += f"\n+{have_rem} more"
            trade_summary = f"**Offer finalized**\nHas:\n{have_txt}\nWanted: Offers"
        else:
            if offer_have_items or offer_want_items:
                have_txt, have_rem = format_trade_items(offer_have_items)
                want_txt, want_rem = format_trade_items(offer_want_items)
                if have_rem:
                    have_txt += f"\n+{have_rem} more"
                if want_rem:
                    want_txt += f"\n+{want_rem} more"
                trade_summary = f"**Trade finalized**\nGives:\n{have_txt}\n\nGets:\n{want_txt}"
            else:
                trade_summary = "**Trade finalized**"

        for pid in (int(requester_id), int(seller_id)):
            try:
                m = await safe_fetch_member(interaction.guild, int(pid))
                if m is None:
                    continue

                other_id = int(seller_id if int(pid) == int(requester_id) else requester_id)
                rep_hint = (
                    f"Go to <#{REP_REMINDER_CHANNEL_ID}> and run:\n`/rep <@{other_id}>`\n"
                    "You can then fill out the review parameter"
                )

                dm_text = (
                    f"{trade_summary}\n\n"
                    f"Rep reminder:\n{rep_hint}\n\n"
                    "Command format:\n"
                    "`/rep target review`\n"
                )

                await m.send(dm_text)
            except Exception:
                pass

        try:
            if interaction.response.is_done():
                await interaction.followup.send("Finalized. Archiving in 10 seconds.", ephemeral=True)
            else:
                await interaction.response.send_message("Finalized. Archiving in 10 seconds.", ephemeral=True)
        except Exception:
            pass
        await ch.send(
            f"✅ Trade finalized. (Both confirmed) Make sure to rep by using /rep in <#{REP_REMINDER_CHANNEL_ID}>\n"
            "This ticket will be archived in 10 seconds."
        )
        await asyncio.sleep(10)
        await bot.archive_and_lock(ch, requester_id, seller_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="trade_cancel")
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "trade_cancel"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        requester_id = int(t.get("requester_id") or 0)
        seller_id = int(t.get("seller_id") or 0) or None
        if not seller_id:
            await interaction.response.send_message("This trade ticket is missing the other trader.", ephemeral=True)
            return

        uid = int(interaction.user.id)
        # Admin override: one-click cancel.
        if interaction.user and isinstance(interaction.user, discord.Member) and is_admin_member(interaction.user):
            await bot._set_ticket_flag(t["id"], "cancel_req_ok")
            await bot._set_ticket_flag(t["id"], "cancel_seller_ok")
        else:
            if uid == requester_id:
                await bot._set_ticket_flag(t["id"], "cancel_req_ok")
            elif uid == int(seller_id):
                await bot._set_ticket_flag(t["id"], "cancel_seller_ok")

        flags = await bot._get_ticket_flags(t["id"])
        if not (flags["cancel_req_ok"] and flags["cancel_seller_ok"]):
            other = "<@%s>" % (seller_id if uid == requester_id else requester_id)
            await interaction.response.send_message(
                f"✅ {interaction.user.mention} confirmed cancel. Waiting on {other}.",
                ephemeral=False,
            )
            return

        listing_id = t.get("listing_id")
        reserved_qty = int(t.get("reserved_qty") or 0)
        if listing_id and reserved_qty > 0:
            await bot.release_reservation(listing_id, reserved_qty)

        await bot.close_ticket_record(t["id"], "canceled")
        await bot.archive_and_lock(ch, requester_id, seller_id)
        await bot.restore_trade_offer_after_cancel(t)

        await interaction.response.send_message("Canceled and archived.", ephemeral=True)
        await ch.send("🧊 Trade canceled. (Both confirmed)")

    @discord.ui.button(label="Close", style=discord.ButtonStyle.secondary, custom_id="trade_close")
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "trade_close"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        if not (interaction.user and isinstance(interaction.user, discord.Member) and is_admin_member(
                interaction.user)):
            await interaction.response.send_message(
                "Admins only. Use Cancel/Finalize to close by agreement.",
                ephemeral=True,
            )
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        listing_id = t.get("listing_id")
        reserved_qty = int(t.get("reserved_qty") or 0)
        if listing_id and reserved_qty > 0:
            await bot.release_reservation(listing_id, reserved_qty)

        await bot.close_ticket_record(t["id"], "closed")
        await bot.archive_and_lock(ch, int(t["requester_id"]), int(t.get("seller_id") or 0) or None)
        await bot.handle_trade_offer_close(t, outcome="closed")

        await interaction.response.send_message("Closed and archived.", ephemeral=True)
        await ch.send("🚪 Trade closed.")


class TicketActionView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Admins can always interact
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        if is_admin_member(interaction.user):
            return True

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            return False

        t = await self.bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return False

        uid = int(interaction.user.id)
        ttype = str(t.get("ticket_type") or "")

        # BUY tickets: buyer/requester cannot use buttons. Only seller (and admins) can.
        if ttype == "buy":
            seller_id = int(t.get("seller_id") or 0)
            if seller_id and uid == seller_id:
                return True
            await interaction.response.send_message("Buy tickets: only the seller (or an admin) can use these buttons.",
                                                    ephemeral=True)
            return False

        # SELL tickets: requester is the seller.
        if ttype == "sell":
            requester_id = int(t.get("requester_id") or 0)
            if uid == requester_id:
                return True
            await interaction.response.send_message(
                "Sell tickets: only the seller (or an admin) can use these buttons.", ephemeral=True)
            return False

        # Fallback (shouldn't be used for trade; trade has its own view)
        if uid in (int(t.get("requester_id") or 0), int(t.get("seller_id") or 0)):
            return True

        await interaction.response.send_message("Only the ticket participants or admins can use these.", ephemeral=True)
        return False

    @discord.ui.button(label="Finalize", style=discord.ButtonStyle.success, custom_id="ticket_finalize")
    async def finalize_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "ticket_finalize"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        ttype = str(t.get("ticket_type") or "")
        requester_id = int(t.get("requester_id") or 0)
        seller_id = int(t.get("seller_id") or 0) or None

        # Trades use TradeTicketView (two-party confirmation). Do not allow finalize here.
        if ttype == "trade":
            await interaction.response.send_message("Use the trade buttons in this ticket.", ephemeral=True)
            return

        # BUY: seller/admin finalizes, buyer cannot.
        if ttype == "buy":
            if not (interaction.user and isinstance(interaction.user, discord.Member) and (
                    is_admin_member(interaction.user) or (seller_id and interaction.user.id == int(seller_id)))):
                await interaction.response.send_message("Buy tickets: only the seller (or an admin) can finalize.",
                                                        ephemeral=True)
                return

            listing_id = t.get("listing_id")
            reserved_qty = int(t.get("reserved_qty") or 0)
            if listing_id and reserved_qty > 0:
                ok, msg = await bot.finalize_sale(listing_id, reserved_qty)
                if not ok:
                    await interaction.response.send_message(f"Finalize failed: {msg}", ephemeral=True)
                    return

            await bot.close_ticket_record(t["id"], "finalized")
            await bot.archive_and_lock(ch, requester_id, seller_id)
            await interaction.response.send_message("Finalized and archived.", ephemeral=True)
            await ch.send("✅ Sale finalized.")
            return

        # SELL: just close (no inventory mutation).
        if ttype == "sell":
            await bot.close_ticket_record(t["id"], "closed")
            await bot.archive_and_lock(ch, requester_id, seller_id)
            await interaction.response.send_message("Closed and archived.", ephemeral=True)
            await ch.send("✅ Ticket closed.")
            return

        # Any other non-trade ticket type: finalize like a sale if listing-backed.
        listing_id = t.get("listing_id")
        reserved_qty = int(t.get("reserved_qty") or 0)
        if listing_id and reserved_qty > 0:
            ok, msg = await bot.finalize_sale(listing_id, reserved_qty)
            if not ok:
                await interaction.response.send_message(f"Finalize failed: {msg}", ephemeral=True)
                return

        await bot.close_ticket_record(t["id"], "finalized")
        await bot.archive_and_lock(ch, requester_id, seller_id)
        await interaction.response.send_message("Finalized and archived.", ephemeral=True)
        await ch.send("✅ Ticket finalized.")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, custom_id="ticket_release")
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "ticket_release"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        ttype = str(t.get("ticket_type") or "")
        requester_id = int(t.get("requester_id") or 0)
        seller_id = int(t.get("seller_id") or 0) or None

        # Trades use TradeTicketView (two-party confirmation). Do not allow cancel here.
        if ttype == "trade":
            await interaction.response.send_message("Use the trade buttons in this ticket.", ephemeral=True)
            return

        # BUY: seller/admin cancels, restores reservation.
        if ttype == "buy":
            if not (interaction.user and isinstance(interaction.user, discord.Member) and (
                    is_admin_member(interaction.user) or (seller_id and interaction.user.id == int(seller_id)))):
                await interaction.response.send_message("Buy tickets: only the seller (or an admin) can cancel.",
                                                        ephemeral=True)
                return

            listing_id = t.get("listing_id")
            reserved_qty = int(t.get("reserved_qty") or 0)
            if listing_id and reserved_qty > 0:
                await bot.release_reservation(listing_id, reserved_qty)

            await bot.close_ticket_record(t["id"], "released")
            await bot.archive_and_lock(ch, requester_id, seller_id)
            await interaction.response.send_message("Canceled and archived.", ephemeral=True)
            await ch.send("🧊 Sale canceled. (Reservation restored)")
            return

        # SELL: simple close
        if ttype == "sell":
            await bot.close_ticket_record(t["id"], "closed")
            await bot.archive_and_lock(ch, requester_id, seller_id)
            await interaction.response.send_message("Closed and archived.", ephemeral=True)
            await ch.send("🧊 Ticket closed.")
            return

        # Other: if listing-backed, restore reservation.
        listing_id = t.get("listing_id")
        reserved_qty = int(t.get("reserved_qty") or 0)
        if listing_id and reserved_qty > 0:
            await bot.release_reservation(listing_id, reserved_qty)

        await bot.close_ticket_record(t["id"], "released")
        await bot.archive_and_lock(ch, requester_id, seller_id)
        await interaction.response.send_message("Canceled and archived.", ephemeral=True)
        await ch.send("🧊 Ticket canceled.")

    @discord.ui.button(label="Close", style=discord.ButtonStyle.secondary, custom_id="ticket_close")
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if await bot.guard_ticket_button_once(interaction, "ticket_close"):
            return

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Not a ticket channel.", ephemeral=True)
            return

        if not (interaction.user and isinstance(interaction.user, discord.Member) and is_admin_member(
                interaction.user)):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        t = await bot.find_open_ticket_by_channel(ch.id)
        if not t:
            await interaction.response.send_message("No open ticket record found.", ephemeral=True)
            return

        # Admin force-close: restore reservation if listing-backed
        listing_id = t.get("listing_id")
        reserved_qty = int(t.get("reserved_qty") or 0)
        if listing_id and reserved_qty > 0:
            await bot.release_reservation(listing_id, reserved_qty)

        await bot.close_ticket_record(t["id"], "closed")
        await bot.archive_and_lock(ch, int(t["requester_id"]), int(t.get("seller_id") or 0) or None)
        await interaction.response.send_message("Closed and archived.", ephemeral=True)
        await ch.send("🚪 Ticket closed by admin.")


class BuyOfferView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot, item_name: str, listing_ids: List[int], total_avail: int):
        super().__init__(timeout=300)
        self.bot = bot
        self.item_name = item_name
        self.listing_ids = listing_ids
        self.total_avail = total_avail
        self.selected_listing_id = listing_ids[0]

        options = []
        for lid in listing_ids[:25]:
            options.append(discord.SelectOption(label=f"Offer #{lid}", value=str(lid)))

        if len(options) > 1:
            self.offer_select.options = options  # type: ignore

    def _build_embed(self, listing: Dict[str, Any], seller: Optional[discord.Member]) -> discord.Embed:
        embed = discord.Embed(title="Buy Listing", color=discord.Color.green(), timestamp=now_utc())
        embed.add_field(name="Item", value=self.item_name, inline=True)
        embed.add_field(name="Available (total)", value=str(self.total_avail), inline=True)
        if seller:
            embed.add_field(name="Seller", value=user_search_tag(seller), inline=True)
        avail = int(listing["quantity"]) - int(listing["quantity_reserved"])
        embed.add_field(name="Available (this offer)", value=str(avail), inline=True)
        embed.add_field(name="Price (USD)", value=f"${listing['price_usd']:.2f}", inline=True)
        rails = ", ".join(listing["payment_methods"]) if listing["payment_methods"] else "none"
        embed.add_field(name="Payment rails", value=rails, inline=False)
        if listing["trade_allowed"]:
            embed.add_field(name="Trade allowed", value=(listing["trade_notes"] or "Trade offers accepted.")[:1024],
                            inline=False)
        embed.set_footer(text="Select an offer, then click Create Buy Ticket (reserves inventory).")
        return embed

    async def _refresh(self, interaction: discord.Interaction):
        listing = await self.bot.get_listing_by_id(self.selected_listing_id)
        if not listing:
            await interaction.response.send_message("That offer no longer exists.", ephemeral=True)
            return
        seller_member = None
        if listing.get("seller_id"):
            seller_member = await safe_fetch_member(interaction.guild, int(listing["seller_id"]))  # type: ignore
        embed = self._build_embed(listing, seller_member)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.select(placeholder="Select another offer (optional)", min_values=1, max_values=1,
                       options=[discord.SelectOption(label="(loading...)", value="0")])
    async def offer_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        self.selected_listing_id = int(select.values[0])
        await self._refresh(interaction)

    @discord.ui.button(label="Create Buy Ticket", style=discord.ButtonStyle.success)
    async def create_buy(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(BuyModal(self.bot, self.selected_listing_id))


class TradeRequestView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot, listing_id: int, offer_item: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.listing_id = listing_id
        self.offer_item = offer_item

    @discord.ui.button(label="Create Trade Ticket", style=discord.ButtonStyle.primary)
    async def create_trade(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(TradeModal(self.bot, self.listing_id, self.offer_item))


class SellRequestView(discord.ui.View):
    def __init__(self, bot: ArcTraderBot, sell_item: str, payout_method: str):
        super().__init__(timeout=300)
        self.bot = bot
        self.sell_item = sell_item
        self.payout_method = payout_method

    @discord.ui.button(label="Create Sell Ticket", style=discord.ButtonStyle.secondary)
    async def create_sell(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(SellModal(self.bot, self.sell_item, self.payout_method))


async def autocomplete_my_listings(interaction: discord.Interaction, current: str):
    """Autocomplete listing IDs owned by the invoking user."""
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None or not isinstance(interaction.user, discord.Member):
        return []
    cur_txt = (current or "").strip().lower()
    out: List[app_commands.Choice[int]] = []
    async with bot.db.execute(
            """
            SELECT id, item_name, quantity, quantity_reserved, price_usd
              FROM listings
             WHERE seller_id = ?
               AND (COALESCE(quantity,0) > 0 OR COALESCE(quantity_reserved,0) > 0)
             ORDER BY item_name COLLATE NOCASE, id ASC
             LIMIT 50;
            """,
            (int(interaction.user.id),),
    ) as c:
        async for lid, name, qty, res, price in c:
            label = f"{name} • ${float(price or 0.0):.2f} • avail {int(qty) - int(res or 0)}/{int(qty)}"
            if not cur_txt or cur_txt in str(name).lower() or cur_txt in str(lid):
                out.append(app_commands.Choice(name=label[:100], value=int(lid)))
            if len(out) >= AC_MAX:
                break
    return out


# ---------- Autocomplete helpers ----------
async def autocomplete_buy_listings(interaction: discord.Interaction, current: str):
    """Autocomplete item names that currently have at least 1 unit available across all listings."""
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        return []

    cur = (current or "").strip()
    like = f"%{cur}%"

    # Aggregate by item name so the user sees one entry per item (not per listing row).
    q = """
        SELECT item_name,
               SUM(quantity - COALESCE(quantity_reserved, 0)) AS avail,
               MIN(COALESCE(price_usd, 1e18)) AS min_price
          FROM listings
         WHERE (quantity - COALESCE(quantity_reserved, 0)) > 0
           AND item_name LIKE ?
         GROUP BY item_name
         ORDER BY item_name COLLATE NOCASE
         LIMIT 25
    """

    out = []
    async with bot.db.execute(q, (like,)) as c:
        async for item_name, avail, min_price in c:
            # Keep the Choice value as the raw item name so the command receives exactly what it expects.
            # Put useful info in the display name.
            if min_price is None or float(min_price) >= 1e17:
                name = f"{item_name}"
            else:
                name = f"{item_name} (from ${float(min_price):.2f})"

            # Discord limits Choice.name to 100 chars.
            out.append(app_commands.Choice(name=name[:100], value=str(item_name)))
    return out


async def autocomplete_all_items(interaction: discord.Interaction, current: str):
    """Autocomplete from the optional items.json catalog (fast) with DB fallback."""
    bot: ArcTraderBot = interaction.client  # type: ignore

    cur = (current or "").strip().lower()
    out: List[app_commands.Choice[str]] = []

    # Prefer items.json cache if present
    if getattr(bot, "items_cache", None):
        for name in bot.items_cache:
            if not cur or cur in name.lower():
                out.append(app_commands.Choice(name=name[:100], value=name))
            if len(out) >= AC_MAX:
                return out

    # Fallback: distinct names from listings table
    if bot.db is None:
        return out

    like = f"%{current or ''}%"
    q = """
        SELECT DISTINCT item_name
          FROM listings
         WHERE item_name LIKE ?
         ORDER BY item_name COLLATE NOCASE
         LIMIT 25
    """
    async with bot.db.execute(q, (like,)) as c:
        async for (item_name,) in c:
            name = str(item_name)
            # Avoid duplicates if items_cache already added some
            if any(ch.value.lower() == name.lower() for ch in out):
                continue
            out.append(app_commands.Choice(name=name[:100], value=name))
            if len(out) >= AC_MAX:
                break
    return out



def _bp_search_name(name: str) -> str:
    base = str(name or "").strip()
    if base.lower().endswith(" blueprint"):
        base = base[:-10]
    return " ".join(base.lower().replace("-", " ").replace("_", " ").split())


def _bp_search_score(query: str, name: str) -> Optional[Tuple[int, int, str]]:
    clean = _bp_search_name(name)
    if not clean:
        return None
    if not query:
        return (99, len(clean), str(name))

    words = clean.split()
    if clean == query:
        score = 0
    elif clean.startswith(query):
        score = 1
    elif any(word.startswith(query) for word in words):
        score = 2
    elif query in clean:
        score = 3
    else:
        return None

    return (score, len(clean), str(name))


async def autocomplete_blueprints(interaction: discord.Interaction, current: str):
    """Autocomplete only blueprint items (from items.json), ignoring the trailing 'Blueprint' suffix for search."""
    bot: ArcTraderBot = interaction.client  # type: ignore
    query = _bp_search_name(current or "")

    if not getattr(bot, "bp_cache", None):
        bot.bp_cache = get_all_blueprints_from_items_cache()

    scored: List[Tuple[int, int, str]] = []
    for name in bot.bp_cache:
        scored_row = _bp_search_score(query, name)
        if scored_row is not None:
            scored.append(scored_row)

    scored.sort(key=lambda x: (x[0], x[1], x[2].lower()))
    best = [name for _, _, name in scored[:AC_MAX]]
    return [app_commands.Choice(name=name[:100], value=name) for name in best]


@app_commands.command(name="buy",
                      description="Buy an item that is currently listed (USD baseline; pay in allowed rails).")
@app_commands.describe(listing="Select an item (autocomplete)")
@app_commands.autocomplete(listing=autocomplete_buy_listings)
async def buy_cmd(interaction: discord.Interaction, listing: str):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return

    item_name = (listing or "").strip()
    if not item_name:
        await interaction.response.send_message("Invalid selection.", ephemeral=True)
        return

    # Gather offers for this item, sorted by cheapest first.
    async with bot.db.execute(
            """
            SELECT id
            FROM listings
            WHERE item_name = ?
              AND COALESCE(trade_only, 0) = 0
              AND COALESCE(payment_methods, '[]') <> '[]'
              AND (quantity - quantity_reserved) > 0
            ORDER BY COALESCE(price_usd, 0) ASC, (quantity - quantity_reserved) DESC, id ASC
            LIMIT 25
            """,
            (item_name,),
    ) as cur:
        id_rows = await cur.fetchall()

    listing_ids = [int(r[0]) for r in id_rows]
    if not listing_ids:
        await interaction.response.send_message("That item is currently reserved/out of stock.", ephemeral=True)
        return

    async with bot.db.execute(
            """
            SELECT COALESCE(SUM(quantity - quantity_reserved), 0)
            FROM listings
            WHERE item_name = ?
              AND COALESCE(trade_only, 0) = 0
              AND COALESCE(payment_methods, '[]') <> '[]'
              AND (quantity - quantity_reserved) > 0
            """,
            (item_name,),
    ) as cur:
        row = await cur.fetchone()
    total_avail = int(row[0] or 0) if row else 0

    first = await bot.get_listing_by_id(listing_ids[0])
    if not first:
        await interaction.response.send_message("That offer no longer exists.", ephemeral=True)
        return

    seller_member = None
    if first.get("seller_id"):
        seller_member = await safe_fetch_member(interaction.guild, int(first["seller_id"]))  # type: ignore

    view = BuyOfferView(bot, item_name=item_name, listing_ids=listing_ids, total_avail=total_avail)
    embed = view._build_embed(first, seller_member)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


trade_group = app_commands.Group(name="trade", description="Post and manage trade offers.")

items_group = app_commands.Group(name="items", description="Admin: manage the item catalog (autocomplete list).")


@items_group.command(name="remove", description="Remove an item from the catalog (admins only).")
@app_commands.describe(item="Item name (must exist in the catalog)")
@app_commands.autocomplete(item=autocomplete_all_items)
async def items_remove_cmd(interaction: discord.Interaction, item: str):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return
    if not is_admin(interaction):
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    if not getattr(bot, "items_cache", None):
        await interaction.response.send_message("Item catalog is empty or not configured.", ephemeral=True)
        return

    target = resolve_item_from_cache(bot.items_cache, item)
    if not target:
        await interaction.response.send_message(f"Item not found in catalog: `{item}`", ephemeral=True)
        return

    new_items = [x for x in bot.items_cache if normalize_item_name(x) != normalize_item_name(target)]
    bot.items_cache = new_items
    save_items_cache(new_items)
    await interaction.response.send_message(f"Removed `{target}` from the item catalog.", ephemeral=True)


@items_group.command(name="list", description="Show how many items are in the catalog (admins only).")
async def items_list_cmd(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return
    if not is_admin(interaction):
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return
    count = len(getattr(bot, "items_cache", []) or [])
    await interaction.response.send_message(f"Item catalog contains {count} items.", ephemeral=True)


async def autocomplete_trade_offer_cancel(interaction: discord.Interaction, current: str) -> List[
    app_commands.Choice[int]]:
    """Autocomplete open trade offers for /trade cancel."""
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        return []
    is_admin_user = is_admin(interaction)
    q = (current or "").strip().lower()

    # Pull a small set of open offers (user's own, or all if admin).
    if is_admin_user:
        sql = (
            "SELECT id, creator_id, intent, have_item, have_qty, want_item, want_qty "
            "FROM trade_offers WHERE status='open' ORDER BY id DESC LIMIT 25;"
        )
        params = ()
    else:
        sql = (
            "SELECT id, creator_id, intent, have_item, have_qty, want_item, want_qty "
            "FROM trade_offers WHERE status='open' AND creator_id=? ORDER BY id DESC LIMIT 25;"
        )
        params = (int(interaction.user.id),)

    choices: List[app_commands.Choice[int]] = []
    async with bot.db.execute(sql, params) as cur:
        rows = await cur.fetchall()

    for offer_id, creator_id, intent, have_item, have_qty, want_item, want_qty in rows:
        intent_s = str(intent)
        if intent_s == "request":
            have_s = f"{want_item} ×{int(want_qty)}"
        else:
            have_s = f"{have_item} ×{int(have_qty or 1)}"
        if intent_s == "offers":
            want_s = "Offers"
        else:
            want_s = f"{want_item} ×{int(want_qty)}"

        label = f"#{int(offer_id)} • {intent_s} • {have_s} → {want_s}"
        # Optional filtering: if user typed something, match on id or item text.
        if q:
            if q not in str(offer_id) and q not in str(have_item).lower() and q not in str(want_item).lower():
                continue
        if len(label) > 100:
            label = label[:97] + "..."
        choices.append(app_commands.Choice(name=label, value=int(offer_id)))

    return choices[:25]



class TradeMultiConfirmView(discord.ui.View):
    def __init__(
        self,
        bot: ArcTraderBot,
        *,
        author_id: int,
        intent: str,
        have_items: List[Tuple[str, int]],
        want_items: List[Tuple[str, int]],
        notes: Optional[str],
    ):
        super().__init__(timeout=180)
        self.bot = bot
        self.author_id = int(author_id)
        self.intent = str(intent or "trade").strip().lower()
        self.have_items = list(have_items or [])
        self.want_items = list(want_items or [])
        self.notes = (notes or "").strip() or None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.author_id:
            await interaction.response.send_message("This preview isn't for you.", ephemeral=True)
            return False
        return True

    def build_embed(self, user_id: int) -> discord.Embed:
        title = "Trade Offer Preview"
        if self.intent == "offers":
            title = "Taking Offers Preview"
        elif self.intent == "request":
            title = "Request Preview"

        embed = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=now_utc())
        embed.add_field(name="Trader", value=f"<@{int(user_id)}>", inline=False)

        if self.intent == "request":
            want_txt, want_rem = format_trade_items(self.want_items)
            if want_rem:
                want_txt += f"\n+{want_rem} more"
            embed.add_field(name="Wants", value=want_txt[:1024], inline=True)
            if self.have_items:
                have_txt, have_rem = format_trade_items(self.have_items)
                if have_rem:
                    have_txt += f"\n+{have_rem} more"
                embed.add_field(name="Offering", value=have_txt[:1024], inline=True)
            else:
                embed.add_field(name="Offering", value="(set in ticket)", inline=True)
        else:
            have_txt, have_rem = format_trade_items(self.have_items)
            if have_rem:
                have_txt += f"\n+{have_rem} more"
            embed.add_field(name="Has", value=have_txt[:1024], inline=True)
            if self.intent == "offers":
                embed.add_field(name="Wants", value="Offers", inline=True)
            else:
                want_txt, want_rem = format_trade_items(self.want_items)
                if want_rem:
                    want_txt += f"\n+{want_rem} more"
                embed.add_field(name="Wants", value=want_txt[:1024], inline=True)

        if self.notes:
            embed.add_field(name="Notes", value=str(self.notes)[:1024], inline=False)
        embed.set_footer(text=f"Intent: {self.intent}")
        return embed

    @discord.ui.button(label="Confirm & Post", style=discord.ButtonStyle.success)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bot = self.bot
        if bot.db is None or not interaction.guild:
            await interaction.response.send_message("DB not ready.", ephemeral=True)
            return

        now_dt = now_utc()
        created = dt_to_iso(now_dt)
        created_ts = int(now_dt.timestamp())
        expires_ts = created_ts + (TRADE_OFFER_EXPIRE_DAYS * 86400)

        first_have = self.have_items[0] if self.have_items else (None, None)
        first_want = self.want_items[0] if self.want_items else (None, None)

        if self.intent == "request":
            have_item = first_have[0] if self.have_items else None
            have_qty = int(first_have[1]) if self.have_items else None
            want_item = first_want[0] if self.want_items else "(missing)"
            want_qty = int(first_want[1]) if self.want_items else 1
        else:
            have_item = first_have[0] if self.have_items else "(missing)"
            have_qty = int(first_have[1]) if self.have_items else 1
            want_item = "Offers" if self.intent == "offers" else (first_want[0] if self.want_items else "(missing)")
            want_qty = 1 if self.intent == "offers" else (int(first_want[1]) if self.want_items else 1)

        await bot.db.execute(
            """
            INSERT INTO trade_offers(created_at, created_ts, expires_ts, status, creator_id, intent, have_item, have_qty, want_item, want_qty, notes)
            VALUES(?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                created,
                created_ts,
                expires_ts,
                int(interaction.user.id),
                self.intent,
                (str(have_item)[:200] if have_item else None),
                (int(have_qty) if have_qty is not None else None),
                str(want_item)[:200],
                int(want_qty),
                (self.notes or None),
            ),
        )
        await bot.db.commit()

        async with bot.db.execute("SELECT last_insert_rowid();") as cur:
            row = await cur.fetchone()
        offer_id = int(row[0]) if row else 0

        await db_set_trade_offer_items(bot, offer_id, self.have_items, self.want_items)

        embed = await bot._build_trade_offer_embed(
            offer_id=int(offer_id),
            creator_id=int(interaction.user.id),
            intent=str(self.intent),
            have_item=str(have_item) if have_item else "",
            have_qty=int(have_qty or 1) if have_qty is not None else 1,
            want_item=str(want_item),
            want_qty=int(want_qty or 1),
            notes=self.notes,
            created_ts=int(created_ts),
            expires_ts=int(expires_ts),
        )

        board_msg_id: Optional[int] = None
        try:
            ch = interaction.guild.get_channel(int(TRADE_BOARD_CHANNEL_ID))
            if ch is None:
                ch = await interaction.guild.fetch_channel(int(TRADE_BOARD_CHANNEL_ID))  # type: ignore
            if isinstance(ch, discord.TextChannel):
                view = make_trade_open_view(offer_id=int(offer_id), intent=str(self.intent))
                msg = await ch.send(embed=embed, view=view)
                board_msg_id = int(msg.id)
                await bot.db.execute("UPDATE trade_offers SET board_message_id=? WHERE id=?;", (int(board_msg_id), int(offer_id)))
                await bot.db.commit()
        except Exception:
            board_msg_id = None

        try:
            if self.have_items:
                for bp_name, _qty in self.have_items:
                    if is_blueprint_item(str(bp_name)):
                        asyncio.create_task(
                            notify_blueprint_needed_from_trade_post(
                                bot,
                                interaction.guild,
                                blueprint_name=str(bp_name),
                                offer_id=int(offer_id),
                                board_channel_id=int(TRADE_BOARD_CHANNEL_ID),
                                board_message_id=(int(board_msg_id) if board_msg_id else None),
                                creator_id=int(interaction.user.id),
                            )
                        )
                        break
        except Exception:
            pass

        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True  # type: ignore

        msg = f"Saved offer #{offer_id} and posted it in <#{TRADE_BOARD_CHANNEL_ID}>." if board_msg_id else f"Saved offer #{offer_id} (could not post to <#{TRADE_BOARD_CHANNEL_ID}>)."
        await interaction.response.edit_message(content=msg, embed=embed, view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True  # type: ignore
        await interaction.response.edit_message(content="Canceled multi-item trade post.", embed=None, view=self)


class TradeMultiModal(discord.ui.Modal):
    def __init__(
        self,
        bot: ArcTraderBot,
        *,
        intent: str,
        initial_offering: Optional[str] = None,
        initial_wanting: Optional[str] = None,
        initial_notes: Optional[str] = None,
    ):
        super().__init__(title="Post multi-item trade")
        self.bot = bot
        self.intent = str(intent or "trade").strip().lower()

        self.offering = discord.ui.TextInput(
            label="Offering list",
            style=discord.TextStyle.paragraph,
            placeholder="Railgun Blueprint x1\nShield Core x2",
            required=(self.intent != "request"),
            max_length=4000,
            default=(str(initial_offering or "")[:4000] or None),
        )
        self.wanting = discord.ui.TextInput(
            label="Want list",
            style=discord.TextStyle.paragraph,
            placeholder=("Offers" if self.intent == "offers" else "Deadline Blueprint x1\nAmmo Pack x5"),
            required=(self.intent != "offers"),
            max_length=4000,
            default=(str(initial_wanting or "")[:4000] or None),
        )
        self.notes = discord.ui.TextInput(
            label="Notes",
            style=discord.TextStyle.paragraph,
            placeholder="Optional notes",
            required=False,
            max_length=400,
            default=(str(initial_notes or "")[:400] or None),
        )

        self.add_item(self.offering)
        self.add_item(self.wanting)
        self.add_item(self.notes)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        if bot.db is None or not interaction.guild:
            await interaction.response.send_message("DB not ready.", ephemeral=True)
            return

        raw_have = str(self.offering.value or "")
        raw_want = str(self.wanting.value or "")
        parsed_have = parse_trade_item_list(raw_have)
        parsed_want = parse_trade_item_list(raw_want)

        if self.intent != "request" and not parsed_have:
            await interaction.response.send_message("You need to list at least one offered item.", ephemeral=True)
            return
        if self.intent not in ("offers",) and not parsed_want:
            await interaction.response.send_message("You need to list at least one wanted item.", ephemeral=True)
            return

        all_items = load_items_cache()
        have_items, have_unknown = resolve_trade_items(parsed_have, all_items)
        want_items, want_unknown = resolve_trade_items(parsed_want, all_items)

        if self.intent != "request" and not have_items:
            await interaction.response.send_message("None of the offered items matched your item catalog.", ephemeral=True)
            return
        if self.intent != "offers" and not want_items:
            await interaction.response.send_message("None of the wanted items matched your item catalog.", ephemeral=True)
            return

        unknown = have_unknown + want_unknown
        if unknown:
            await interaction.response.send_message(
                "These items were not recognized:\n- " + "\n- ".join(unknown[:15]),
                ephemeral=True,
            )
            return

        view = TradeMultiConfirmView(
            bot,
            author_id=int(interaction.user.id),
            intent=self.intent,
            have_items=have_items,
            want_items=want_items,
            notes=(str(self.notes.value or "").strip() or None),
        )
        await interaction.response.send_message(
            "Review your multi-item trade post below, then confirm.",
            embed=view.build_embed(int(interaction.user.id)),
            view=view,
            ephemeral=True,
        )


@trade_group.command(name="post", description="Post a trade offer (stored in the DB).")
@app_commands.describe(
    intent="What kind of post this is (trade/offers/request)",
    offering="Item you have (required for trade/offers; optional for request)",
    offering_qty="How many (default 1)",
    want="What you want (not required for 'offers')",
    want_qty="How many (default 1; ignored for 'offers')",
    notes="Optional notes (e.g. 'mods preferred', 'no lowballs')",
    use_bp_need="For request posts, use your saved missing blueprints and ignore the item fields",
)
@app_commands.choices(intent=[
    app_commands.Choice(name="trade (have -> want)", value="trade"),
    app_commands.Choice(name="offers (have -> offers)", value="offers"),
    app_commands.Choice(name="request (want -> offering)", value="request"),
])
@app_commands.autocomplete(offering=autocomplete_all_items, want=autocomplete_all_items)
async def trade_post_cmd(
        interaction: discord.Interaction,
        intent: app_commands.Choice[str],
        offering: Optional[str] = None,
        want: Optional[str] = None,
        offering_qty: int = 1,
        want_qty: int = 1,
        notes: Optional[str] = None,
        use_bp_need: bool = False,
):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    intent_s = (getattr(intent, "value", None) or "trade").strip().lower()
    if intent_s not in {"trade", "offers", "request"}:
        intent_s = "trade"

    have_item: Optional[str] = (offering or "").strip() if offering is not None else ""
    have_item = have_item.strip() if isinstance(have_item, str) else ""

    note_txt = (notes or "").strip()
    if note_txt and len(note_txt) > 400:
        note_txt = note_txt[:400]

    if use_bp_need:
        if intent_s != "request":
            await interaction.response.send_message(
                "`use_bp_need` only works with `intent=request`.",
                ephemeral=True,
            )
            return

        all_bps = get_all_blueprints_from_items_cache()
        owned_keys = await db_bp_owned_keys(bot, int(interaction.user.id))
        missing = [bp for bp in all_bps if blueprint_key(bp) not in owned_keys]
        if not missing:
            await interaction.response.send_message(
                "You don't have any missing blueprints saved right now.",
                ephemeral=True,
            )
            return

        want_items = [(bp, 1) for bp in missing]
        now_dt = now_utc()
        created = dt_to_iso(now_dt)
        created_ts = int(now_dt.timestamp())
        expires_ts = created_ts + (TRADE_OFFER_EXPIRE_DAYS * 86400)

        first_want = want_items[0]
        want_item = str(first_want[0])
        want_qty = int(first_want[1])

        await bot.db.execute(
            """
            INSERT INTO trade_offers(created_at, created_ts, expires_ts, status, creator_id, intent, have_item, have_qty, want_item, want_qty, notes, bp_need_auto)
            VALUES(?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, 1);
            """,
            (
                created,
                created_ts,
                expires_ts,
                int(interaction.user.id),
                "request",
                None,
                None,
                want_item[:200],
                want_qty,
                (note_txt or None),
            ),
        )
        await bot.db.commit()

        async with bot.db.execute("SELECT last_insert_rowid();") as cur:
            row = await cur.fetchone()
        offer_id = int(row[0]) if row else 0

        await db_set_trade_offer_items(bot, offer_id, [], want_items)

        embed = await bot._build_trade_offer_embed(
            offer_id=int(offer_id),
            creator_id=int(interaction.user.id),
            intent="request",
            have_item="",
            have_qty=1,
            want_item=want_item,
            want_qty=want_qty,
            notes=(note_txt or None),
            created_ts=int(created_ts),
            expires_ts=int(expires_ts),
        )

        board_msg_id: Optional[int] = None
        try:
            ch = interaction.guild.get_channel(int(TRADE_BOARD_CHANNEL_ID))
            if ch is None:
                ch = await interaction.guild.fetch_channel(int(TRADE_BOARD_CHANNEL_ID))  # type: ignore
            if isinstance(ch, discord.TextChannel):
                view = make_trade_open_view(offer_id=int(offer_id), intent="request")
                msg = await ch.send(embed=embed, view=view)
                board_msg_id = int(msg.id)
                await bot.db.execute(
                    "UPDATE trade_offers SET board_message_id=? WHERE id=?;",
                    (int(board_msg_id), int(offer_id)),
                )
                await bot.db.commit()
        except Exception:
            board_msg_id = None

        if board_msg_id:
            await interaction.response.send_message(
                f"Saved offer #{offer_id} and posted it in <#{TRADE_BOARD_CHANNEL_ID}>.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"Saved offer #{offer_id} (could not post to <#{TRADE_BOARD_CHANNEL_ID}>).",
                ephemeral=True,
            )
        return

    # Option B: request posts only require WANT. Offering is optional/omitted.
    if intent_s == "request":
        want_item = (want or "").strip()
        if not want_item:
            await interaction.response.send_message("Want must be set for request.", ephemeral=True)
            return
        if want_qty <= 0:
            await interaction.response.send_message("want_qty must be >= 1.", ephemeral=True)
            return
        # Store no offering up-front.
        have_item = None
        have_qty = None
    else:
        # trade/offers require an offering (have_item)
        if not have_item:
            await interaction.response.send_message("Offering/Has must be set for trade/offers.", ephemeral=True)
            return
        have_qty = int(offering_qty)
        if have_qty <= 0:
            await interaction.response.send_message("offering_qty must be >= 1.", ephemeral=True)
            return

        # 'offers' means you are taking offers; 'want' is not required and is forced to a sentinel value.
        if intent_s == "offers":
            want_item = "Offers"
            want_qty = 1
        else:
            want_item = (want or "").strip()
            if not want_item:
                await interaction.response.send_message("Want must be set for trade.", ephemeral=True)
                return
            if want_qty <= 0:
                await interaction.response.send_message("want_qty must be >= 1.", ephemeral=True)
                return

    now_dt = now_utc()
    created = dt_to_iso(now_dt)
    created_ts = int(now_dt.timestamp())
    expires_ts = created_ts + (TRADE_OFFER_EXPIRE_DAYS * 86400)

    await bot.db.execute(
        """
        INSERT INTO trade_offers(created_at, created_ts, expires_ts, status, creator_id, intent, have_item, have_qty, want_item, want_qty, notes)
        VALUES(?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?);
        """,
        (created, created_ts, expires_ts, interaction.user.id, intent_s, (have_item[:200] if have_item else None),
         (int(have_qty) if have_qty is not None else None),
         want_item[:200], int(want_qty), (note_txt or None)),
    )
    await bot.db.commit()

    async with bot.db.execute("SELECT last_insert_rowid();") as cur:
        row = await cur.fetchone()
    offer_id = int(row[0]) if row else 0

    title = "Trade Offer"
    if intent_s == "offers":
        title = "Taking Offers"
    elif intent_s == "request":
        title = "Request"

    embed = discord.Embed(title=title, color=discord.Color.blurple(), timestamp=now_utc())
    _total_reps, trade_reps = await _count_reps(bot, interaction.user.id)
    embed.add_field(name="Trader", value=f"<@{interaction.user.id}>\nTrade rep: {trade_reps}", inline=False)

    if intent_s == "request":
        embed.add_field(name="Wants", value=f"{want_item} ×{int(want_qty)}", inline=True)
        embed.add_field(name="Offering", value="(set in ticket)", inline=True)
    else:
        embed.add_field(name="Has", value=f"{have_item} ×{int(offering_qty)}", inline=True)
        embed.add_field(name="Wants", value=("Offers" if intent_s == "offers" else f"{want_item} ×{int(want_qty)}"),
                        inline=True)

    if note_txt:
        embed.add_field(name="Notes", value=note_txt[:1024], inline=False)

    # Put expiry in a field (footer won't render <t:...>).
    embed.add_field(
        name="Expires",
        value=discord.utils.format_dt(datetime.fromtimestamp(expires_ts, tz=timezone.utc), style="R"),
        inline=False,
    )
    embed.set_footer(text=f"Offer #{offer_id} • {intent_s}")

    # Post to trade-board with a persistent button
    board_msg_id: Optional[int] = None
    try:
        ch = interaction.guild.get_channel(int(TRADE_BOARD_CHANNEL_ID))
        if ch is None:
            ch = await interaction.guild.fetch_channel(int(TRADE_BOARD_CHANNEL_ID))  # type: ignore
        if isinstance(ch, discord.TextChannel):
            view = make_trade_open_view(offer_id=int(offer_id), intent=str(intent_s))
            msg = await ch.send(embed=embed, view=view)
            board_msg_id = int(msg.id)
            await bot.db.execute("UPDATE trade_offers SET board_message_id=? WHERE id=?;",
                                 (int(board_msg_id), int(offer_id)))
            await bot.db.commit()
    except Exception:
        board_msg_id = None

    # Blueprint DM notifications (if someone is offering a blueprint)
    try:
        if have_item and is_blueprint_item(str(have_item)):
            asyncio.create_task(
                notify_blueprint_needed_from_trade_post(
                    bot,
                    interaction.guild,
                    blueprint_name=str(have_item),
                    offer_id=int(offer_id),
                    board_channel_id=int(TRADE_BOARD_CHANNEL_ID),
                    board_message_id=(int(board_msg_id) if board_msg_id else None),
                    creator_id=int(interaction.user.id),
                )
            )
    except Exception:
        pass

    await interaction.response.send_message(
        content=(
            f"Saved offer #{offer_id} and posted it in <#{TRADE_BOARD_CHANNEL_ID}>." if board_msg_id else f"Saved offer #{offer_id} (could not post to <#{TRADE_BOARD_CHANNEL_ID}>)."),
        embed=embed,
        ephemeral=True,
    )


@trade_group.command(name="cancel", description="Remove a trade offer (your own, or any offer if you are an admin).")
@app_commands.describe(offer="Select an open offer to remove")
@app_commands.autocomplete(offer=autocomplete_trade_offer_cancel)
async def trade_cancel_cmd(interaction: discord.Interaction, offer: int):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    is_admin_user = is_admin(interaction)

    # Cooldown (non-admins only)
    if not is_admin_user:
        now_m = asyncio.get_running_loop().time()
        last = _TRADE_OFFER_REMOVE_LAST.get(int(interaction.user.id), 0.0)
        remaining = TRADE_OFFER_REMOVE_COOLDOWN_SECONDS - (now_m - last)
        if remaining > 0:
            await interaction.response.send_message(
                f"You're removing offers too fast. Try again in {int(remaining)}s.",
                ephemeral=True,
            )
            return

    async with bot.db.execute(
            "SELECT creator_id, status, ticket_channel_id, board_message_id FROM trade_offers WHERE id=? LIMIT 1;",
            (int(offer),),
    ) as cur:
        row = await cur.fetchone()

    if not row:
        await interaction.response.send_message("Offer not found.", ephemeral=True)
        return

    creator_id, status, ticket_channel_id, board_message_id = row

    if str(status) != "open":
        await interaction.response.send_message("That offer is not open anymore.", ephemeral=True)
        return

    if not is_admin_user and int(creator_id) != int(interaction.user.id):
        await interaction.response.send_message("You can only cancel your own offers.", ephemeral=True)
        return

    # Only a positive channel ID counts as a real active ticket.
    # -1 is just the temporary "opening in progress" lock and should not brick cancel forever.
    if ticket_channel_id is not None and int(ticket_channel_id) > 0:
        await interaction.response.send_message(
            "That offer has an active ticket linked to it. Close the ticket first, then cancel the offer.",
            ephemeral=True,
        )
        return

    # If the offer is stuck in the temporary opening state, clear it so the post can be canceled cleanly.
    if ticket_channel_id is not None and int(ticket_channel_id) < 0:
        try:
            await bot.db.execute(
                "UPDATE trade_offers SET ticket_channel_id=NULL WHERE id=? AND ticket_channel_id < 0;",
                (int(offer),),
            )
            await bot.db.commit()
        except Exception:
            pass

    # Delete any legacy board post if it exists (older offers).
    if board_message_id:
        try:
            board = interaction.guild.get_channel(TRADE_BOARD_CHANNEL_ID)
            if isinstance(board, discord.TextChannel):
                msg = await board.fetch_message(int(board_message_id))
                await msg.delete()
        except Exception:
            pass

    await bot.db.execute(
        "UPDATE trade_offers SET status='closed', board_message_id=NULL WHERE id=?;",
        (int(offer),),
    )
    await bot.db.commit()

    if not is_admin_user:
        _TRADE_OFFER_REMOVE_LAST[int(interaction.user.id)] = asyncio.get_running_loop().time()

    await interaction.response.send_message(f"Offer #{int(offer)} removed.", ephemeral=True)


@app_commands.command(name="sell", description="Offer to sell an item (opens a ticket to negotiate).")
@app_commands.describe(item="Item you want to sell", payout_method="How you want to be paid (paypal/btc/ltc/eth)")
@app_commands.autocomplete(item=autocomplete_all_items)
async def sell_cmd(interaction: discord.Interaction, item: str, payout_method: str):
    bot: ArcTraderBot = interaction.client  # type: ignore

    sell_item = (item or "").strip()
    if not sell_item:
        await interaction.response.send_message("Item can't be empty.", ephemeral=True)
        return

    pm = (payout_method or "").strip().lower()
    if pm not in ALLOWED_SELL_PAYOUTS:
        await interaction.response.send_message(
            f"Invalid payout_method. Allowed: {', '.join(sorted(ALLOWED_SELL_PAYOUTS))}",
            ephemeral=True
        )
        return

    embed = discord.Embed(
        title="Sell Request",
        color=discord.Color.orange(),
        timestamp=now_utc(),
    )
    embed.add_field(name="Item", value=sell_item, inline=True)
    embed.add_field(name="Payout method", value=pm, inline=True)
    embed.set_footer(text="Click below to open a sell ticket.")

    await interaction.response.send_message(
        embed=embed,
        view=SellRequestView(bot, sell_item, pm),
        ephemeral=True
    )


# ---------- Core slash commands (admin + listings) ----------

@app_commands.command(name="ping", description="Check bot latency.")
async def ping(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    ms = int(getattr(bot, "latency", 0.0) * 1000)
    await interaction.response.send_message(f"Pong: {ms}ms", ephemeral=True)


@app_commands.command(name="sync", description="(Admin) Sync slash commands to this guild.")
async def sync_cmd(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if not is_admin(interaction):
        await interaction.response.send_message("Admin only.", ephemeral=True)
        return
    guild_obj = discord.Object(id=bot.guild_id)
    bot.tree.clear_commands(guild=guild_obj)
    bot.tree.copy_global_to(guild=guild_obj)
    synced = await bot.tree.sync(guild=guild_obj)
    await interaction.response.send_message(f"Synced {len(synced)} commands.", ephemeral=True)


SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class UpdateModal(discord.ui.Modal):
    def __init__(self, bot: ArcTraderBot):
        self.bot = bot

        # Robustly read current version from config (handles dict or JSON string)
        cfg = getattr(bot, "cfg", {}) or {}
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:
                cfg = {}
        if not isinstance(cfg, dict):
            cfg = {}

        current_version = str(cfg.get("bot_version", "Not set"))

        super().__init__(title="Update RaiderTrader")

        self.version = discord.ui.TextInput(
            label="Version (MAJOR.MINOR.PATCH)",
            placeholder=f"Current: {current_version}",
            required=True,
            max_length=32,
        )
        self.notes = discord.ui.TextInput(
            label="Changelog (supports - bullets)",
            style=discord.TextStyle.paragraph,
            placeholder="- change 1\n- change 2",
            required=True,
            max_length=4000,
        )

        self.add_item(self.version)
        self.add_item(self.notes)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        if not interaction.guild:
            await interaction.response.send_message("Guild only.", ephemeral=True)
            return

        v = str(self.version.value or "").strip()
        if not SEMVER_RE.match(v):
            await interaction.response.send_message(
                "Invalid version. Use `MAJOR.MINOR.PATCH` like `0.3.1`.",
                ephemeral=True,
            )
            return

        # Persist version in config
        try:
            bot.cfg["bot_version"] = v
            # Ensure changelog ids are also persisted in case config was empty
            bot.cfg.setdefault("changelog_channel_id", CHANGELOG_CHANNEL_ID)
            bot.cfg.setdefault("changelog_role_id", CHANGELOG_ROLE_ID)
            save_config(bot.cfg)
        except Exception:
            pass

        # Resolve changelog destination
        ch_id = int(bot.cfg.get("changelog_channel_id") or CHANGELOG_CHANNEL_ID)
        role_id = int(bot.cfg.get("changelog_role_id") or CHANGELOG_ROLE_ID)

        ch = interaction.guild.get_channel(ch_id)
        if ch is None:
            try:
                ch = await bot.fetch_channel(ch_id)
            except Exception:
                ch = None

        if not isinstance(ch, discord.TextChannel):
            await interaction.response.send_message("Changelog channel not found.", ephemeral=True)
            return

        notes = str(self.notes.value or "").strip()
        content = f"<@&{role_id}>\n# Version {v}\n{notes}"

        try:
            await ch.send(
                content,
                allowed_mentions=discord.AllowedMentions(roles=True, users=False, everyone=False, replied_user=False),
            )
        except Exception:
            await interaction.response.send_message("Failed to post changelog.", ephemeral=True)
            return

        # Confirm + restart (systemd will restart the service)
        await interaction.response.send_message("Posted changelog. Restarting…", ephemeral=True)
        asyncio.get_running_loop().call_later(0.8, lambda: os._exit(0))


@app_commands.command(name="update", description="(Admin) Guided update: post changelog + restart.")
async def update_cmd(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if not is_admin(interaction):
        await interaction.response.send_message("Admin only.", ephemeral=True)
        return

    # Use config defaults if not set
    try:
        bot.cfg.setdefault("changelog_channel_id", CHANGELOG_CHANNEL_ID)
        bot.cfg.setdefault("changelog_role_id", CHANGELOG_ROLE_ID)
        save_config(bot.cfg)
    except Exception:
        pass

    await interaction.response.send_modal(UpdateModal(bot))


@app_commands.command(name="restart", description="(Admin) Restart the bot service.")
async def restart_cmd(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("Admin only.", ephemeral=True)
        return
    await interaction.response.send_message("Restarting...", ephemeral=True)
    # Let systemd restart the service. Avoid raising SystemExit inside the interaction task.
    loop = asyncio.get_running_loop()
    loop.call_later(0.5, lambda: os._exit(0))


@app_commands.command(name="setup", description="(Admin) Configure channel/category/role IDs.")
@app_commands.describe(
    listings_channel="Channel where listings are posted (optional).",
    tickets_category="Category to create tickets in.",
    archived_category="Category to move closed tickets to.",
    seller_role="Role allowed to create listings (optional)."
)
async def setup_cmd(
        interaction: discord.Interaction,
        tickets_category: discord.CategoryChannel,
        archived_category: discord.CategoryChannel,
        listings_channel: Optional[discord.TextChannel] = None,
        seller_role: Optional[discord.Role] = None,
):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if not is_admin(interaction):
        await interaction.response.send_message("Admin only.", ephemeral=True)
        return

    bot.cfg["tickets_category_id"] = tickets_category.id
    bot.cfg["archived_category_id"] = archived_category.id
    if listings_channel is not None:
        bot.cfg["listings_channel_id"] = listings_channel.id
    if seller_role is not None:
        bot.cfg["seller_role_id"] = seller_role.id

    save_config(bot.cfg)
    await interaction.response.send_message("Saved config.json IDs.", ephemeral=True)


listing_group = app_commands.Group(name="listing", description="Manage sale listings.")


# ---------- Blueprint commands ----------

bp_group = app_commands.Group(name="bp", description="Blueprint collection tracking + notifications.")

def _bp_visible_list(items: List[str], *, limit: int = 40) -> Tuple[str, int]:
    """Return (formatted_text, remaining_count)."""
    items = items or []
    if len(items) <= limit:
        return "\n".join(f"• {x}" for x in items) or "(none)", 0
    head = items[:limit]
    return "\n".join(f"• {x}" for x in head), len(items) - limit

class BpBulkModal(discord.ui.Modal):
    def __init__(self, bot: ArcTraderBot, *, action: Literal["add", "remove"], target_id: int):
        super().__init__(title=f"Blueprint {action}")
        self.bot = bot
        self.action = action
        self.target_id = int(target_id)

        self.blueprints = discord.ui.TextInput(
            label="Blueprints (comma or newline separated)",
            style=discord.TextStyle.paragraph,
            placeholder="Deadline Blueprint\nAnvil Blueprint\n...",
            required=True,
            max_length=4000,
        )
        self.add_item(self.blueprints)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        if bot.db is None:
            await interaction.response.send_message("DB not ready.", ephemeral=True)
            return

        raw = str(self.blueprints.value or "")
        inputs = parse_bp_list(raw)
        all_bps = get_all_blueprints_from_items_cache()
        resolved, unknown = resolve_blueprints(inputs, all_bps)
        if not resolved:
            await interaction.response.send_message("No valid blueprints found.", ephemeral=True)
            return

        if self.action == "add":
            await db_bp_add_many(bot, self.target_id, resolved)
            msg = f"Added {len(resolved)} blueprint(s)."
        else:
            removed = await db_bp_remove_many(bot, self.target_id, resolved)
            msg = f"Removed {removed} blueprint(s)."

        if unknown:
            msg += f"\nUnknown/ignored: {', '.join(unknown[:10])}" + ("..." if len(unknown) > 10 else "")

        await interaction.response.send_message(msg, ephemeral=True)

        # Congrats check (only for self-adds and only in guild)
        if interaction.guild and self.action == "add":
            try:
                member = interaction.guild.get_member(self.target_id) or await interaction.guild.fetch_member(self.target_id)  # type: ignore
                if isinstance(member, discord.Member):
                    await maybe_congrats_all_blueprints(bot, interaction.guild, member)
            except Exception:
                pass




class BpSearchModal(discord.ui.Modal):
    def __init__(self, parent_view: "BpSelectView"):
        super().__init__(title="Search blueprints")
        self.parent_view = parent_view
        self.query = discord.ui.TextInput(
            label="Search",
            placeholder="Type part of a blueprint name (e.g., Anvil)",
            required=True,
            max_length=80,
        )
        self.add_item(self.query)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        q = str(self.query.value or "").strip()
        self.parent_view.set_filter(q)
        await interaction.response.edit_message(content=self.parent_view.render_header(), view=self.parent_view)


class BpSelect(discord.ui.Select):
    def __init__(self, parent_view: "BpSelectView", options: List[discord.SelectOption]):
        max_vals = max(1, min(len(options), 25))
        super().__init__(
            placeholder="Select blueprint(s)… (type to filter)",
            min_values=1,
            max_values=max_vals,
            options=options,
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction) -> None:
        self.parent_view.store_selection(list(self.values))
        await interaction.response.edit_message(content=self.parent_view.render_header(), view=self.parent_view)


class BpSelectView(discord.ui.View):
    PAGE_SIZE = 25

    def __init__(
        self,
        bot: "ArcTraderBot",
        *,
        action: Literal["add", "remove", "pick"],
        target_id: int,
        author_id: int,
        base_candidates: List[str],
        owned_candidates: Optional[List[str]] = None,
        confirm_callback: Optional[Callable[[discord.Interaction, List[str]], Awaitable[None]]] = None,
    ):
        super().__init__(timeout=180)
        self.bot = bot
        self.action = action
        self.target_id = int(target_id)
        self.author_id = int(author_id)
        self.confirm_callback = confirm_callback

        self._base = sorted(list(base_candidates or []), key=lambda x: x.lower())
        self._owned = sorted(list(owned_candidates or []), key=lambda x: x.lower())
        self._filter: str = ""
        self._page: int = 0
        self._pending: Set[str] = set()

        self._rebuild()

    def set_filter(self, q: str) -> None:
        self._filter = (q or "").strip()
        self._page = 0
        self._rebuild()

    def render_header(self) -> str:
        who = f"<@{self.target_id}>"
        action_word = "add" if self.action == "add" else ("remove" if self.action == "remove" else "use")
        cand = self._candidates()
        total = len(cand)
        pages = self._page_count(cand)
        current_page = min(self._page + 1, pages)
        start = (self._page * self.PAGE_SIZE) + 1 if total else 0
        end = min(total, (self._page + 1) * self.PAGE_SIZE)
        if self.action == "pick":
            lines = [f"Select blueprint(s) to **{action_word}** in a request post for {who}."]
        else:
            lines = [f"Select blueprint(s) to **{action_word}** for {who}."]
        lines.append(f"Page {current_page}/{pages} | Showing {start}-{end} of {total}")
        if self._filter:
            lines.append(f"Filter: `{self._filter}`")
        lines.append(f"Selected: {len(self._pending)}")
        if self._pending:
            preview = sorted(self._pending, key=lambda x: x.lower())[:8]
            extra = len(self._pending) - len(preview)
            preview_txt = ", ".join(preview)
            if extra > 0:
                preview_txt += f", +{extra} more"
            lines.append(f"Pending: {preview_txt}")
            lines.append("Selections stay saved while you move between pages.")
        return "\n".join(lines)

    def _candidates(self) -> List[str]:
        base = self._owned if self.action == "remove" else self._base
        if not self._filter:
            return list(base)

        query = _bp_search_name(self._filter)
        scored: List[Tuple[int, int, str]] = []
        for name in base:
            row = _bp_search_score(query, name)
            if row is not None:
                scored.append(row)
        scored.sort(key=lambda x: (x[0], x[1], x[2].lower()))
        return [name for _, _, name in scored]

    def _page_count(self, cand: List[str]) -> int:
        if not cand:
            return 1
        return max(1, (len(cand) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)

    def _slice(self, cand: List[str]) -> List[str]:
        start = self._page * self.PAGE_SIZE
        return cand[start:start + self.PAGE_SIZE]

    def _rebuild(self) -> None:
        self.clear_items()

        cand = self._candidates()
        pages = self._page_count(cand)
        self._page = max(0, min(self._page, pages - 1))

        page_items = self._slice(cand)
        opts: List[discord.SelectOption] = []
        for x in page_items:
            selected = x in self._pending
            label = x[:100]
            if selected and len(label) <= 98:
                label = f"✓ {label}"
            opts.append(
                discord.SelectOption(
                    label=label[:100],
                    value=x[:100],
                    default=selected,
                )
            )

        if opts:
            self.add_item(BpSelect(self, opts))

        self.add_item(self.BpPrevButton(disabled=self._page <= 0))
        self.add_item(self.BpNextButton(disabled=self._page >= pages - 1 or not page_items))
        self.add_item(self.BpSearchButton())
        self.add_item(self.BpClearButton(disabled=not self._pending))
        if self.action in ("add", "remove"):
            self.add_item(self.BpPasteButton())
        if self.action == "pick":
            self.add_item(self.BpSelectAllButton(disabled=not cand))
        self.add_item(self.BpConfirmButton(disabled=not self._pending))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) != self.author_id:
            await interaction.response.send_message("This menu isn't for you.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True  # type: ignore

    def store_selection(self, selected: List[str]) -> None:
        valid = {x for x in selected if x}
        self._pending.update(valid)
        if self.action == "remove":
            owned_keys = {blueprint_key(x) for x in self._owned}
            self._pending = {x for x in self._pending if blueprint_key(x) in owned_keys}
        else:
            base_keys = {blueprint_key(x) for x in self._base}
            self._pending = {x for x in self._pending if blueprint_key(x) in base_keys}
        self._rebuild()

    async def apply_selection(self, interaction: discord.Interaction) -> None:
        bot = self.bot
        if self.action in ("add", "remove") and bot.db is None:
            await interaction.response.send_message("DB not ready.", ephemeral=True)
            return

        if not self._pending:
            await interaction.response.send_message("Nothing selected yet.", ephemeral=True)
            return

        selected = sorted(self._pending, key=lambda x: x.lower())
        all_bps = get_all_blueprints_from_items_cache()
        resolved, unknown = resolve_blueprints(selected, all_bps)
        if not resolved:
            await interaction.response.send_message("No valid blueprints selected.", ephemeral=True)
            return

        if self.action == "pick":
            if self.confirm_callback is None:
                await interaction.response.send_message("No follow-up action is configured for this picker.", ephemeral=True)
                return
            await self.confirm_callback(interaction, resolved)
            return

        if self.action == "add":
            await db_bp_add_many(bot, self.target_id, resolved)
            msg = f"Added {len(resolved)} blueprint(s)."
        else:
            removed = await db_bp_remove_many(bot, self.target_id, resolved)
            msg = f"Removed {removed} blueprint(s)." if removed else "You didn't have any of those saved."

        if unknown:
            msg += f"\nIgnored: {', '.join(unknown[:10])}" + ("..." if len(unknown) > 10 else "")

        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True  # type: ignore

        await interaction.response.edit_message(content=msg, view=self)

        if interaction.guild and self.action == "add" and int(self.target_id) == int(interaction.user.id):
            try:
                member = interaction.guild.get_member(self.target_id) or await interaction.guild.fetch_member(self.target_id)  # type: ignore
                if isinstance(member, discord.Member):
                    await maybe_congrats_all_blueprints(bot, interaction.guild, member)
            except Exception:
                pass

    class BpPrevButton(discord.ui.Button):
        def __init__(self, *, disabled: bool = False):
            super().__init__(style=discord.ButtonStyle.secondary, label="Prev", row=1, disabled=disabled)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            view._page = max(0, view._page - 1)
            view._rebuild()
            await interaction.response.edit_message(content=view.render_header(), view=view)

    class BpNextButton(discord.ui.Button):
        def __init__(self, *, disabled: bool = False):
            super().__init__(style=discord.ButtonStyle.secondary, label="Next", row=1, disabled=disabled)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            cand = view._candidates()
            pages = view._page_count(cand)
            view._page = min(pages - 1, view._page + 1)
            view._rebuild()
            await interaction.response.edit_message(content=view.render_header(), view=view)

    class BpSearchButton(discord.ui.Button):
        def __init__(self):
            super().__init__(style=discord.ButtonStyle.primary, label="Search", row=1)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            await interaction.response.send_modal(BpSearchModal(view))

    class BpClearButton(discord.ui.Button):
        def __init__(self, *, disabled: bool = False):
            super().__init__(style=discord.ButtonStyle.secondary, label="Clear Selected", row=1, disabled=disabled)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            view._pending.clear()
            view._rebuild()
            await interaction.response.edit_message(content=view.render_header(), view=view)

    class BpSelectAllButton(discord.ui.Button):
        def __init__(self, *, disabled: bool = False):
            super().__init__(style=discord.ButtonStyle.secondary, label="Select All", row=2, disabled=disabled)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            candidates = view._candidates()
            view._pending = {x for x in candidates if x}
            view._rebuild()
            await interaction.response.edit_message(content=view.render_header(), view=view)

    class BpPasteButton(discord.ui.Button):
        def __init__(self):
            super().__init__(style=discord.ButtonStyle.secondary, label="Paste list", row=2)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            await interaction.response.send_modal(BpBulkModal(view.bot, action=view.action, target_id=view.target_id))

    class BpConfirmButton(discord.ui.Button):
        def __init__(self, *, disabled: bool = False):
            super().__init__(style=discord.ButtonStyle.success, label="Apply Selected", row=2, disabled=disabled)

        async def callback(self, interaction: discord.Interaction) -> None:
            view: BpSelectView = self.view  # type: ignore
            if view.action == "add":
                self.label = "Add Selected"
            elif view.action == "remove":
                self.label = "Remove Selected"
            else:
                self.label = "Use Selected"
            await view.apply_selection(interaction)

def _bp_target_or_self(interaction: discord.Interaction, user: Optional[discord.Member]) -> discord.Member:
    if user is None:
        assert isinstance(interaction.user, discord.Member)
        return interaction.user
    return user

def _bp_build_owned_embed(target: discord.Member, owned: List[str], all_bps: List[str]) -> discord.Embed:
    embed = discord.Embed(title="Blueprints owned", color=discord.Color.blurple())
    embed.add_field(name="User", value=f"<@{target.id}>", inline=False)
    embed.add_field(name="Progress", value=f"{len(owned)}/{len(all_bps)}", inline=False)
    txt, rem = _bp_visible_list(owned, limit=40)
    embed.add_field(name="Owned", value=(txt[:1024] if txt else "(none)"), inline=False)
    if rem:
        embed.set_footer(text=f"+{rem} more not shown")
    return embed


def _bp_build_missing_embed(target: discord.Member, missing: List[str], all_bps: List[str]) -> discord.Embed:
    embed = discord.Embed(title="Blueprints missing", color=discord.Color.orange())
    embed.add_field(name="User", value=f"<@{target.id}>", inline=False)
    embed.add_field(name="Progress", value=f"{len(all_bps)-len(missing)}/{len(all_bps)}", inline=False)
    txt, rem = _bp_visible_list(missing, limit=40)
    embed.add_field(name="Missing", value=(txt[:1024] if txt else "(none)"), inline=False)
    if rem:
        embed.set_footer(text=f"+{rem} more not shown")
    return embed


async def _bp_ticket_targets(
    interaction: discord.Interaction,
    *,
    user: Optional[discord.Member],
) -> Tuple[Optional[List[discord.Member]], Optional[str]]:
    bot: ArcTraderBot = interaction.client  # type: ignore
    ch = interaction.channel
    if bot.db is None or not interaction.guild or not isinstance(ch, discord.TextChannel):
        return None, None

    ticket = await bot.find_open_ticket_by_channel(ch.id)
    if not ticket:
        return None, None

    requester_id = int(ticket.get("requester_id") or 0)
    seller_id = int(ticket.get("seller_id") or 0)
    allowed_ids = {uid for uid in (requester_id, seller_id) if uid > 0}
    if int(interaction.user.id) not in allowed_ids and not is_admin(interaction):
        return None, "Only the ticket participants or admins can do that in this ticket."

    members: List[discord.Member] = []
    for uid in (requester_id, seller_id):
        if uid <= 0:
            continue
        m = interaction.guild.get_member(uid)
        if m is None:
            try:
                m = await interaction.guild.fetch_member(uid)
            except Exception:
                m = None
        if isinstance(m, discord.Member):
            members.append(m)

    if user is not None:
        if int(user.id) not in allowed_ids and not is_admin(interaction):
            return None, "In tickets, you can only check the two people in that ticket."
        return [user], None

    if members:
        return members, None
    return None, "Couldn't resolve the ticket participants."


@bp_group.command(name="have", description="Show which blueprints a user has.")
@app_commands.describe(user="User to check (defaults to you)")
async def bp_have_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    ticket_targets, ticket_err = await _bp_ticket_targets(interaction, user=user)
    if ticket_err:
        await interaction.response.send_message(ticket_err, ephemeral=True)
        return
    if ticket_targets is not None:
        all_bps = get_all_blueprints_from_items_cache()
        embeds: List[discord.Embed] = []
        for target in ticket_targets:
            owned_rows = await bot.db.execute_fetchall(
                "SELECT blueprint_name FROM bp_owned WHERE user_id=? ORDER BY blueprint_name COLLATE NOCASE;",
                (int(target.id),),
            )
            owned = [str(r[0]) for r in owned_rows if r and r[0]]
            embeds.append(_bp_build_owned_embed(target, owned, all_bps))
        await interaction.response.send_message(embeds=embeds[:10], ephemeral=False)
        return

    target = _bp_target_or_self(interaction, user)
    owned_rows = await bot.db.execute_fetchall(
        "SELECT blueprint_name FROM bp_owned WHERE user_id=? ORDER BY blueprint_name COLLATE NOCASE;",
        (int(target.id),),
    )
    owned = [str(r[0]) for r in owned_rows if r and r[0]]
    all_bps = get_all_blueprints_from_items_cache()
    embed = _bp_build_owned_embed(target, owned, all_bps)
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bp_group.command(name="need", description="Show which blueprints a user is missing.")
@app_commands.describe(user="User to check (defaults to you)")
async def bp_need_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    ticket_targets, ticket_err = await _bp_ticket_targets(interaction, user=user)
    if ticket_err:
        await interaction.response.send_message(ticket_err, ephemeral=True)
        return
    if ticket_targets is not None:
        if user is None:
            target = _bp_target_or_self(interaction, user)
            all_bps = get_all_blueprints_from_items_cache()
            owned_keys = await db_bp_owned_keys(bot, target.id)
            missing = [bp for bp in all_bps if blueprint_key(bp) not in owned_keys]
            embed = _bp_build_missing_embed(target, missing, all_bps)
            await interaction.response.send_message(embed=embed, ephemeral=False)
            return

        all_bps = get_all_blueprints_from_items_cache()
        embeds: List[discord.Embed] = []
        for target in ticket_targets:
            owned_keys = await db_bp_owned_keys(bot, target.id)
            missing = [bp for bp in all_bps if blueprint_key(bp) not in owned_keys]
            embeds.append(_bp_build_missing_embed(target, missing, all_bps))
        await interaction.response.send_message(embeds=embeds[:10], ephemeral=False)
        return

    target = _bp_target_or_self(interaction, user)
    all_bps = get_all_blueprints_from_items_cache()
    owned_keys = await db_bp_owned_keys(bot, target.id)
    missing = [bp for bp in all_bps if blueprint_key(bp) not in owned_keys]
    embed = _bp_build_missing_embed(target, missing, all_bps)
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bp_group.command(name="add", description="Add blueprint(s) to your collection.")
@app_commands.describe(blueprint="Blueprint to add (optional; leave empty to paste a list)", user="(Admin) User to edit")
@app_commands.autocomplete(blueprint=autocomplete_blueprints)
async def bp_add_cmd(interaction: discord.Interaction, blueprint: Optional[str] = None, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    target = _bp_target_or_self(interaction, user)
    if int(target.id) != int(interaction.user.id) and not is_admin(interaction):
        await interaction.response.send_message("You can only edit yourself.", ephemeral=True)
        return

    if not blueprint:
        # Interactive multi-select menu (with paging + search); also offers "Paste list" fallback.
        view = BpSelectView(
            bot,
            action="add",
            target_id=int(target.id),
            author_id=int(interaction.user.id),
            base_candidates=get_all_blueprints_from_items_cache(),
        )
        await interaction.response.send_message(view.render_header(), view=view, ephemeral=True)
        return

    all_bps = get_all_blueprints_from_items_cache()
    resolved, unknown = resolve_blueprints([blueprint], all_bps)
    if not resolved:
        await interaction.response.send_message("Not a recognized blueprint.", ephemeral=True)
        return

    await db_bp_add_many(bot, int(target.id), resolved)
    await interaction.response.send_message(f"Added: **{resolved[0]}**", ephemeral=False)

    if interaction.guild and int(target.id) == int(interaction.user.id):
        await maybe_congrats_all_blueprints(bot, interaction.guild, target)

@bp_group.command(name="remove", description="Remove blueprint(s) from your collection.")
@app_commands.describe(blueprint="Blueprint to remove (optional; leave empty to paste a list)", user="(Admin) User to edit")
@app_commands.autocomplete(blueprint=autocomplete_blueprints)
async def bp_remove_cmd(interaction: discord.Interaction, blueprint: Optional[str] = None, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    target = _bp_target_or_self(interaction, user)
    if int(target.id) != int(interaction.user.id) and not is_admin(interaction):
        await interaction.response.send_message("You can only edit yourself.", ephemeral=True)
        return

    if not blueprint:
        # Interactive multi-select menu (with paging + search); also offers "Paste list" fallback.
        owned_rows = await bot.db.execute_fetchall(
            "SELECT blueprint_name FROM bp_owned WHERE user_id=? ORDER BY blueprint_name COLLATE NOCASE;",
            (int(target.id),),
        )
        owned = [str(r[0]) for r in owned_rows if r and r[0]]
        view = BpSelectView(
            bot,
            action="remove",
            target_id=int(target.id),
            author_id=int(interaction.user.id),
            base_candidates=get_all_blueprints_from_items_cache(),
            owned_candidates=owned,
        )
        await interaction.response.send_message(view.render_header(), view=view, ephemeral=True)
        return

    all_bps = get_all_blueprints_from_items_cache()
    resolved, _unknown = resolve_blueprints([blueprint], all_bps)
    if not resolved:
        await interaction.response.send_message("Not a recognized blueprint.", ephemeral=True)
        return

    removed = await db_bp_remove_many(bot, int(target.id), resolved)
    await interaction.response.send_message(
        f"Removed: **{resolved[0]}**" if removed else "You didn't have that blueprint saved.",
        ephemeral=False,
    )

@bp_group.command(name="wipe", description="Wipe all saved blueprints (run twice within 10s to confirm).")
@app_commands.describe(user="(Admin) User to wipe")
async def bp_wipe_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    target = _bp_target_or_self(interaction, user)
    if int(target.id) != int(interaction.user.id) and not is_admin(interaction):
        await interaction.response.send_message("You can only wipe yourself.", ephemeral=True)
        return

    now_ts = int(now_utc().timestamp())
    row = await bot.db.execute_fetchall(
        "SELECT expires_ts FROM bp_wipe_confirms WHERE user_id=? LIMIT 1;",
        (int(target.id),),
    )
    expires = int(row[0][0]) if row else 0
    if expires and expires >= now_ts:
        # confirmed
        await bot.db.execute("DELETE FROM bp_wipe_confirms WHERE user_id=?;", (int(target.id),))
        await bot.db.commit()
        removed = await db_bp_wipe(bot, int(target.id))
        await interaction.response.send_message(f"Wiped {removed} blueprint(s).", ephemeral=True)
        return

    # set/refresh confirm
    await bot.db.execute(
        "INSERT OR REPLACE INTO bp_wipe_confirms(user_id, expires_ts) VALUES(?, ?);",
        (int(target.id), now_ts + BP_WIPE_CONFIRM_SECONDS),
    )
    await bot.db.commit()
    await interaction.response.send_message(
        f"Run `/bp wipe` again within {BP_WIPE_CONFIRM_SECONDS}s to confirm.",
        ephemeral=True,
    )

@bp_group.command(name="notify", description="Enable/disable DM notifications when a blueprint you need is posted.")
@app_commands.describe(state="on/off")
@app_commands.choices(state=[
    app_commands.Choice(name="on", value="on"),
    app_commands.Choice(name="off", value="off"),
])
async def bp_notify_cmd(interaction: discord.Interaction, state: app_commands.Choice[str]):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    val = (getattr(state, "value", "on") or "on").lower()
    enabled = 1 if val == "on" else 0
    await db_ensure_bp_user(bot, int(interaction.user.id))
    await bot.db.execute("UPDATE bp_users SET dms_enabled=? WHERE user_id=?;", (enabled, int(interaction.user.id)))
    await bot.db.commit()
    await interaction.response.send_message(
        "DM notifications enabled." if enabled else "DM notifications disabled.",
        ephemeral=True,
    )



def _can_list(member: discord.Member) -> bool:
    # If config seller_role_id exists, require it (or admin); else fall back to SELLER_ROLE_ID constant.
    role_id = 0
    try:
        role_id = int(load_config().get("seller_role_id") or 0)
    except Exception:
        role_id = 0
    role_id = role_id or SELLER_ROLE_ID
    return is_admin_member(member) or has_role(member, role_id)


@listing_group.command(name="add", description="Create a sale listing (USD baseline).")
@app_commands.describe(
    item="Item name",
    quantity="Total quantity",
    price_usd="Unit price in USD (baseline)",
    payment_methods="Comma-separated rails: paypal, btc, ltc, eth",
    trade_allowed="Accept trade offers too",
    trade_only="Trade-only (no payment rails)",
    trade_notes="Notes about what trades you accept"
)
async def listing_add(
        interaction: discord.Interaction,
        item: str,
        quantity: int,
        price_usd: float,
        payment_methods: str = "paypal",
        trade_allowed: bool = False,
        trade_only: bool = False,
        trade_notes: str = "",
):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member) or not _can_list(interaction.user):
        await interaction.response.send_message("Seller role or admin only.", ephemeral=True)
        return

    item_name = (item or "").strip()
    if not item_name:
        await interaction.response.send_message("Item name can't be empty.", ephemeral=True)
        return
    if quantity <= 0:
        await interaction.response.send_message("Quantity must be > 0.", ephemeral=True)
        return
    if price_usd < 0:
        await interaction.response.send_message("price_usd must be >= 0.", ephemeral=True)
        return

    pms = parse_payment_methods(payment_methods, ALLOWED_PAYMENT_METHODS)
    if trade_only:
        pms = []
    elif pms is None:
        await interaction.response.send_message(
            f"Invalid payment_methods. Allowed: {', '.join(sorted(ALLOWED_PAYMENT_METHODS))}",
            ephemeral=True,
        )
        return

    # price_type/price_value are legacy columns that may be NOT NULL in older DBs.
    # Keep them populated so inserts work across all migrated schemas.
    await bot.db.execute(
        "INSERT INTO listings(item_name, quantity, seller_id, payment_methods, price_usd, trade_allowed, trade_only, trade_notes, price_type, price_value) "
        "VALUES(?,?,?,?,?,?,?,?,?,?);",
        (
            item_name,
            int(quantity),
            int(interaction.user.id),
            json.dumps(pms),
            float(price_usd),
            1 if trade_allowed else 0,
            1 if trade_only else 0,
            (trade_notes or "")[:400],
            "usd",
            f"{float(price_usd):.2f}",
        ),
    )
    await bot.db.commit()

    await interaction.response.send_message(
        f"Listing added: **{item_name}** x{quantity} @ ${price_usd:.2f}",
        ephemeral=True,
    )


@listing_group.command(name="remove", description="Remove some/all of one of your listings.")
@app_commands.describe(listing="Pick one of your listings", amount="How many to remove (defaults to all available)")
@app_commands.autocomplete(listing=autocomplete_my_listings)
async def listing_remove(interaction: discord.Interaction, listing: int, amount: Optional[int] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Member only.", ephemeral=True)
        return

    listing_id = int(listing)
    row = await bot.get_listing_by_id(listing_id)
    if not row:
        await interaction.response.send_message("Listing not found.", ephemeral=True)
        return

    owner_id = int(row.get("seller_id") or 0)
    if not (is_admin_member(interaction.user) or owner_id == interaction.user.id):
        await interaction.response.send_message("Not allowed (not your listing).", ephemeral=True)
        return

    item_name = str(row.get("item_name") or f"listing {listing_id}")
    total_qty = int(row["quantity"])
    reserved = int(row["quantity_reserved"])
    available = max(0, total_qty - reserved)

    if available <= 0:
        await interaction.response.send_message("Nothing available to remove (all reserved).", ephemeral=True)
        return

    # If no amount is supplied, treat it as "remove all available".
    # If anything is reserved, we keep the listing at the reserved quantity.
    if amount is None:
        if reserved == 0:
            await bot.db.execute("DELETE FROM listings WHERE id=?;", (listing_id,))
            await bot.db.commit()
            await bot.cleanup_dead_listings()
            await interaction.response.send_message(f"Removed listing: **{item_name}** (all {total_qty}).",
                                                    ephemeral=True)
            return

        # Reduce to reserved only (i.e., remove all available units).
        await bot.db.execute("UPDATE listings SET quantity = ? WHERE id=?;", (reserved, listing_id))
        await bot.db.commit()
        await bot.cleanup_dead_listings()
        await interaction.response.send_message(
            f"Removed all available units of **{item_name}**: removed {available}, kept {reserved} reserved.",
            ephemeral=True,
        )
        return

    amt = int(amount)
    if amt <= 0:
        await interaction.response.send_message("amount must be >= 1.", ephemeral=True)
        return
    if amt > available:
        await interaction.response.send_message(f"Can't remove {amt}. Available to remove: {available}.",
                                                ephemeral=True)
        return

    # Delete only if we're removing everything and nothing is reserved.
    if amt == total_qty and reserved == 0:
        await bot.db.execute("DELETE FROM listings WHERE id=?;", (listing_id,))
        await bot.db.commit()
        await bot.cleanup_dead_listings()
        await interaction.response.send_message(f"Removed listing: **{item_name}** (all {amt}).", ephemeral=True)
        return

    await bot.db.execute("UPDATE listings SET quantity = quantity - ? WHERE id=?;", (amt, listing_id))
    await bot.db.commit()
    await bot.cleanup_dead_listings()
    new_total = total_qty - amt
    await interaction.response.send_message(
        f"Removed {amt} from **{item_name}**. Now {max(0, new_total - reserved)}/{new_total} available.",
        ephemeral=True,
    )


@listing_group.command(name="mine", description="Show your current listings.")
async def listing_mine(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Member only.", ephemeral=True)
        return

    await bot.cleanup_dead_listings()

    async with bot.db.execute(
            "SELECT id, item_name, quantity, quantity_reserved, price_usd, trade_allowed, trade_only "
            "FROM listings WHERE seller_id=? "
            "ORDER BY item_name COLLATE NOCASE, id ASC LIMIT 25;",
            (interaction.user.id,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await interaction.response.send_message("You have no listings.", ephemeral=True)
        return

    lines = []
    for lid, name, qty, res, price, ta, to in rows:
        avail = int(qty) - int(res or 0)
        flags = []
        if int(to or 0):
            flags.append("trade-only")
        elif int(ta or 0):
            flags.append("trade-ok")
        ftxt = f" ({', '.join(flags)})" if flags else ""
        lines.append(f"`{lid}` • {name} • avail {avail} • ${float(price or 0.0):.2f}{ftxt}")

    await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=True)


async def _count_reps(bot: ArcTraderBot, target_id: int) -> Tuple[int, int]:
    """Return (total_reps, trade_reps). trade_reps counts rep_type='trade_partner'.
    Admin overrides (rep_overrides) take precedence when set."""
    assert bot.db is not None

    # Default computed counts
    async with bot.db.execute(
            "SELECT COUNT(*) FROM reps WHERE target_id=?;",
            (int(target_id),),
    ) as cur:
        row = await cur.fetchone()
    total = int(row[0] or 0) if row else 0

    async with bot.db.execute(
            "SELECT COUNT(*) FROM reps WHERE target_id=? AND rep_type='trade_partner';",
            (int(target_id),),
    ) as cur:
        row2 = await cur.fetchone()
    trade = int(row2[0] or 0) if row2 else 0

    # Overrides
    async with bot.db.execute(
            "SELECT total_override, trade_override FROM rep_overrides WHERE user_id=? LIMIT 1;",
            (int(target_id),),
    ) as cur:
        ovr = await cur.fetchone()
    if ovr:
        tot_ovr, tr_ovr = ovr
        if tr_ovr is not None:
            trade = int(tr_ovr)
        if tot_ovr is not None:
            total = int(tot_ovr)
        else:
            # If only trade is overridden, keep total at least trade
            total = max(total, trade)

    return total, trade


async def _award_trade_rep_roles(bot: ArcTraderBot, member: discord.Member) -> None:
    """Stacking milestone roles based on trade reps only."""
    if not bot.cfg:
        return
    rep_roles = bot.cfg.get("rep_roles") or {}
    trade_rules = []
    try:
        trade_rules = list(rep_roles.get("trade") or [])
    except Exception:
        trade_rules = []

    # Filter + sort by min_reps
    cleaned = []
    for r in trade_rules:
        if not isinstance(r, dict):
            continue
        try:
            min_reps = int(r.get("min_reps") or 0)
            role_id = int(r.get("role_id") or 0)
        except Exception:
            continue
        if min_reps <= 0 or role_id <= 0:
            continue
        cleaned.append((min_reps, role_id))
    cleaned.sort(key=lambda x: x[0])

    if not cleaned:
        return

    if bot.db is None:
        return

    _, trade_reps = await _count_reps(bot, member.id)

    # Add all roles whose threshold is reached (stacking).
    for min_reps, role_id in cleaned:
        if trade_reps < min_reps:
            continue
        role = member.guild.get_role(int(role_id))
        if role is None:
            continue
        if role in member.roles:
            continue
        try:
            await member.add_roles(role, reason=f"Trade rep milestone reached: {trade_reps} (>= {min_reps})")
        except Exception:
            # Missing perms / role hierarchy, ignore.
            continue


@app_commands.command(name="repcheck", description="Check a user's rep totals.")
@app_commands.describe(user="User to check")
async def repcheck_cmd(interaction: discord.Interaction, user: discord.Member):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return

    total, trade = await _count_reps(bot, user.id)

    embed = discord.Embed(title="Rep Check", color=discord.Color.gold(), timestamp=now_utc())
    embed.add_field(name="User", value=user_search_tag(user), inline=False)
    embed.add_field(name="Total reps", value=str(total), inline=True)
    embed.add_field(name="Trade reps (role milestones)", value=str(trade), inline=True)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@app_commands.command(name="checkrep", description="Check a user's rep totals (ephemeral).")
@app_commands.describe(user="User to check (defaults to you)")
async def checkrep_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    target = user or interaction.user
    if not isinstance(target, discord.Member):
        await interaction.response.send_message("User not found.", ephemeral=True)
        return
    total, trade = await _count_reps(bot, target.id)
    embed = discord.Embed(title="Reputation", color=discord.Color.blurple())
    embed.add_field(name="User", value=f"<@{target.id}>", inline=False)
    embed.add_field(name="Trade rep", value=str(trade), inline=True)
    embed.add_field(name="Total rep", value=str(total), inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@app_commands.command(name="setrep", description="(Admin) Override a user's rep totals.")
@app_commands.describe(user="User to set", trade_rep="New trade rep value",
                       total_rep="Optional total rep override (defaults to max(current,total,trade))")
async def setrep_cmd(interaction: discord.Interaction, user: discord.Member, trade_rep: int,
                     total_rep: Optional[int] = None):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.user or not isinstance(interaction.user, discord.Member) or not is_admin_member(
            interaction.user):
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    trade_rep = max(0, int(trade_rep))
    if total_rep is not None:
        total_rep = max(0, int(total_rep))

    # Compute a safe default total override if not provided
    cur_total, _ = await _count_reps(bot, user.id)
    if total_rep is None:
        total_rep = max(cur_total, trade_rep)

    await bot.db.execute(
        "INSERT INTO rep_overrides(user_id, total_override, trade_override, updated_at, updated_by) "
        "VALUES(?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET total_override=excluded.total_override, trade_override=excluded.trade_override, updated_at=excluded.updated_at, updated_by=excluded.updated_by;",
        (int(user.id), int(total_rep), int(trade_rep), dt_to_iso(now_utc()), int(interaction.user.id)),
    )
    await bot.db.commit()

    # Recompute milestone roles
    try:
        rep_roles_cfg = bot.cfg.get("rep_roles", {}).get("trade", [])
        role_ids = {int(x["role_id"]) for x in rep_roles_cfg if "role_id" in x}
        to_remove = [r for r in user.roles if int(r.id) in role_ids]
        if to_remove:
            await user.remove_roles(*to_remove, reason="Rep override set")
        await _award_trade_rep_roles(bot, user)
    except Exception:
        pass

    await interaction.response.send_message(f"Set <@{user.id}> rep overrides: trade={trade_rep}, total={total_rep}.",
                                            ephemeral=True)


@app_commands.command(name="rep", description="Leave a rep note for someone you completed a ticket with.")
@app_commands.describe(target="User you are repping", review="Short review")
async def rep_cmd(interaction: discord.Interaction, target: discord.Member, review: str):
    if not interaction.guild:
        await interaction.response.send_message("Guild only.", ephemeral=True)
        return

    if target.bot:
        await interaction.response.send_message("You can’t rep bot accounts.", ephemeral=True)
        return

    if interaction.user.id == target.id:
        await interaction.response.send_message("You can’t rep yourself.", ephemeral=True)
        return

    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return

    repper_id = int(interaction.user.id)
    target_id = int(target.id)

    # Find the most recent finalized ticket between these two users.
    async with bot.db.execute(
            """
            SELECT id, ticket_type, requester_id, seller_id, item_name, offer_item, offer_qty, want_item, want_qty, closed_at
            FROM tickets
            WHERE status='closed' AND outcome='finalized'
              AND ((requester_id=? AND seller_id=?) OR (requester_id=? AND seller_id=?))
            ORDER BY COALESCE(closed_at, created_at) DESC
            LIMIT 1;
            """,
            (repper_id, target_id, target_id, repper_id),
    ) as cur:
        row = await cur.fetchone()

    if not row:
        await interaction.response.send_message(
            "No finalized ticket found between you two. Finish a trade/sale first.",
            ephemeral=True,
        )
        return

    ticket_id, ticket_type, requester_id, seller_id, item_name, offer_item, offer_qty, want_item, want_qty, closed_at = row

    # Determine rep type + item context
    rep_type = "trade_partner" if ticket_type == "trade" else "seller"

    # For non-trade tickets, only allow repping the seller side.
    if ticket_type != "trade":
        if int(seller_id or 0) != target_id:
            await interaction.response.send_message("For sales, you can only rep the seller.", ephemeral=True)
            return

    context_item = None
    if ticket_type == "trade":
        context_item = f"{offer_item} ×{int(offer_qty or 1)} ↔ {want_item} ×{int(want_qty or 1)}"
    else:
        context_item = item_name

    try:
        await bot.db.execute(
            """
            INSERT INTO reps(created_at, ticket_id, repper_id, target_id, rep_type, item_name, review, is_anonymous)
            VALUES(?,?,?,?,?,?,?,0);
            """,
            (dt_to_iso(now_utc()), int(ticket_id), repper_id, target_id, rep_type, (context_item or "")[:200],
             (review or "")[:800]),
        )
        await bot.db.commit()
        # Award cosmetic milestone roles (trade reps only)
        if rep_type == "trade_partner":
            await _award_trade_rep_roles(bot, target)
            # Announce 100+ trade rep milestone once
            try:
                _, trade_reps = await _count_reps(bot, target.id)
                await bot.maybe_announce_trade_rep_milestone(interaction.guild, target, trade_reps)
            except Exception:
                pass
    except sqlite3.IntegrityError:
        await interaction.response.send_message("You already repped this ticket.", ephemeral=True)
        return

    embed = discord.Embed(title="Rep", color=bot.next_rep_embed_color(), timestamp=now_utc())
    embed.add_field(name="From", value=user_search_tag(interaction.user), inline=False)
    embed.add_field(name="To", value=user_search_tag(target), inline=False)
    embed.add_field(name="Ticket", value=f"#{int(ticket_id)} ({ticket_type})", inline=False)
    if context_item:
        embed.add_field(name="Context", value=str(context_item)[:1024], inline=False)
    embed.add_field(name="Review", value=(review or "")[:1024] or "(none)", inline=False)

    rep_ch = interaction.guild.get_channel(REP_CHANNEL_ID)
    if isinstance(rep_ch, discord.TextChannel):
        await rep_ch.send(embed=embed)

    await interaction.response.send_message("Rep submitted.", ephemeral=False)


@app_commands.command(name="drop", description="In a trade ticket, decide who should drop first based on trade rep.")
async def drop_cmd(interaction: discord.Interaction):
    bot: ArcTraderBot = interaction.client  # type: ignore
    if bot.db is None:
        await interaction.response.send_message("DB not ready.", ephemeral=True)
        return
    if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message("Guild ticket channels only.", ephemeral=True)
        return

    ticket = await bot.find_open_ticket_by_channel(interaction.channel.id)
    if not ticket or str(ticket.get("ticket_type") or "") != "trade":
        await interaction.response.send_message("/drop only works in open trade tickets.", ephemeral=True)
        return

    requester_id = int(ticket.get("requester_id") or 0)
    seller_id = int(ticket.get("seller_id") or 0)
    allowed_ids = {uid for uid in (requester_id, seller_id) if uid > 0}
    if int(interaction.user.id) not in allowed_ids and not is_admin(interaction):
        await interaction.response.send_message("Only the two traders or admins can use /drop here.", ephemeral=True)
        return

    requester = await safe_fetch_member(interaction.guild, requester_id)
    seller = await safe_fetch_member(interaction.guild, seller_id)
    if requester is None or seller is None:
        await interaction.response.send_message("Couldn't resolve both traders for this ticket.", ephemeral=True)
        return

    _req_total, req_trade = await _count_reps(bot, requester.id)
    _sell_total, sell_trade = await _count_reps(bot, seller.id)

    tie = req_trade == sell_trade
    if req_trade < sell_trade:
        first = requester
        second = seller
        reason = f"Lower trade rep goes first: **{req_trade} < {sell_trade}**"
        footer = None
    elif sell_trade < req_trade:
        first = seller
        second = requester
        reason = f"Lower trade rep goes first: **{sell_trade} < {req_trade}**"
        footer = None
    else:
        first, second = random.sample([requester, seller], 2)
        reason = f"Both traders have the same trade rep: **{req_trade}**"
        footer = "Tie detected — drop order was randomized. Either trader can request a middleman for extra safety."

    embed = discord.Embed(title="Drop Order", color=discord.Color.blurple(), timestamp=now_utc())
    embed.description = (
        "```\n"
        f"{first.display_name} should drop first.\n"
        f"{second.display_name} should wait.\n"
        "```"
    )
    embed.add_field(name="Trader A", value=f"<@{requester.id}>\nTrade rep: **{req_trade}**", inline=True)
    embed.add_field(name="Trader B", value=f"<@{seller.id}>\nTrade rep: **{sell_trade}**", inline=True)
    embed.add_field(name="Decision", value=f"<@{first.id}> drops first.", inline=False)
    embed.add_field(name="Why", value=reason, inline=False)
    if footer:
        embed.set_footer(text=footer)

    await interaction.response.send_message(embed=embed, ephemeral=False)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    load_dotenv(BASE_DIR / ".env")
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN in .env")

    guild_id = require_env_int("GUILD_ID")

    bot = ArcTraderBot(guild_id=guild_id)

    bot.tree.add_command(ping)
    bot.tree.add_command(setup_cmd)
    bot.tree.add_command(restart_cmd)
    bot.tree.add_command(setrep_cmd)
    bot.tree.add_command(checkrep_cmd)
    bot.tree.add_command(update_cmd)
    bot.tree.add_command(sync_cmd)
    bot.tree.add_command(rep_cmd)
    bot.tree.add_command(repcheck_cmd)
    bot.tree.add_command(drop_cmd)

    bot.tree.add_command(trade_group)
    bot.tree.add_command(bp_group)
    bot.tree.add_command(items_group)
    # RMT-related commands (/buy, /sell, /listing ...) intentionally not registered.

    bot.run(token)


if __name__ == "__main__":
    main()

