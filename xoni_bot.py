#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import io
import time
import json
import uuid
import zlib
import socket
import struct
import random
import sqlite3
import secrets
import hashlib
import asyncio
import logging
import threading
import concurrent.futures
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone, timedelta

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand
)
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    import zstandard as zstd
except ImportError:
    zstd = None

try:
    from Crypto.Cipher import AES
except ImportError:
    try:
        from Cryptodome.Cipher import AES
    except ImportError:
        AES = None

# ─── CONFIG (ပြင်ဆင်ပြီး) ─────────────────────────────────────────
OWNER_NAME = "Xoni"
BRAND_NAME = "Xoni"
VERSION = "v3.0-VIP-ALL-IN-ONE"

# ✅ Token ကို တန်းအလုပ်လုပ်အောင် ပြင်ပေးထားပါသည်
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8626624044:AAGFNruvRzTxhw31uONhXmbnvychBfr-VPU")

_ADMIN_ENV = os.environ.get("ADMIN_IDS", "")
if _ADMIN_ENV.strip():
    ADMIN_IDS = [int(x) for x in _ADMIN_ENV.split(",") if x.strip()]
else:
    ADMIN_IDS = [
        1896158899,
        7567720140,
        8735530895,
        8696507663,
        6912974998,
    ]

MAX_WORKERS_PER_BOOST = 500
MAX_CONCURRENT_LOGIN = 100
_LOGIN_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT_LOGIN)

PLAN_LIMITS = {
    "free":    50,
    "vip":     1000,
    "premium": 999999,
}
PLAN_WORKERS = {
    "free":    100,
    "vip":     300,
    "premium": 500,
}

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "XoniData"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = str(DATA_DIR / "xoni.db")
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("xoni")

# ─── DATABASE LAYER ──────────────────────────────────────────────
_db_lock = threading.Lock()

def db_conn():
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con

def init_db():
    with _db_lock, db_conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT UNIQUE,
            source TEXT,
            added_by INTEGER,
            added_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS boost_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER DEFAULT 0,
            target_id INTEGER,
            target_zone INTEGER,
            target_nick TEXT,
            success INTEGER,
            fail INTEGER,
            banned INTEGER,
            duration INTEGER,
            created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS vip_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            plan TEXT DEFAULT 'free',
            expires_at INTEGER DEFAULT 0,
            daily_limit INTEGER DEFAULT 50,
            total_boosts INTEGER DEFAULT 0,
            added_by INTEGER,
            created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS user_usage (
            user_id INTEGER,
            date TEXT,
            boost_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        );
        """)

def count_devices():
    with _db_lock, db_conn() as c:
        return c.execute("SELECT COUNT(*) FROM devices").fetchone()[0]

def load_device_ids(limit=10000):
    with _db_lock, db_conn() as c:
        rows = c.execute(
            "SELECT device_id FROM devices ORDER BY id LIMIT ?",
            (limit,)
        ).fetchall()
    return [r[0] for r in rows]

def remove_device(device_id):
    with _db_lock, db_conn() as c:
        cur = c.execute("DELETE FROM devices WHERE device_id=?", (device_id,))
    return cur.rowcount > 0

def bulk_add_devices(device_list, source="upload"):
    now = int(time.time())
    added = skipped = invalid = 0
    clean_list = []
    seen = set()
    for raw in device_list:
        cd = clean_device_id(raw)
        if not cd or len(cd) < 15:
            invalid += 1
            continue
        if cd in seen:
            skipped += 1
            continue
        seen.add(cd)
        clean_list.append(cd)
    if not clean_list:
        return added, skipped, invalid
    with _db_lock, db_conn() as c:
        for cd in clean_list:
            try:
                cur = c.execute(
                    "INSERT OR IGNORE INTO devices(device_id, source, added_by, added_at) "
                    "VALUES (?, ?, 0, ?)",
                    (cd, source, now),
                )
                if cur.rowcount > 0:
                    added += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
    return added, skipped, invalid

def log_boost(target_id, target_zone, target_nick,
              success, fail, banned, duration, user_id=0):
    now = int(time.time())
    with _db_lock, db_conn() as c:
        c.execute("""
            INSERT INTO boost_history(user_id, target_id, target_zone,
                target_nick, success, fail, banned, duration, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (int(user_id), int(target_id), int(target_zone), target_nick or "",
              int(success), int(fail), int(banned), int(duration), now))

def get_boost_history(limit=10, user_id=None):
    with _db_lock, db_conn() as c:
        if user_id:
            return c.execute("""
                SELECT target_id, target_nick, success, fail, banned,
                       duration, created_at
                FROM boost_history WHERE user_id=?
                ORDER BY created_at DESC LIMIT ?
            """, (user_id, limit)).fetchall()
        return c.execute("""
            SELECT target_id, target_nick, success, fail, banned,
                   duration, created_at
            FROM boost_history
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()

# ─── VIP SYSTEM ──────────────────────────────────────────────────
def get_user_plan(user_id):
    with _db_lock, db_conn() as c:
        row = c.execute(
            "SELECT plan, expires_at, daily_limit FROM vip_users WHERE user_id=?",
            (user_id,)
        ).fetchone()
    if not row:
        return {"plan": "free", "expires_at": 0, "limit": PLAN_LIMITS["free"]}
    plan, expires, limit = row
    if expires and expires > 0 and time.time() > expires:
        with _db_lock, db_conn() as c:
            c.execute(
                "UPDATE vip_users SET plan='free', daily_limit=? WHERE user_id=?",
                (PLAN_LIMITS["free"], user_id)
            )
        return {"plan": "free", "expires_at": 0, "limit": PLAN_LIMITS["free"]}
    return {
        "plan": plan,
        "expires_at": expires,
        "limit": limit or PLAN_LIMITS.get(plan, 50),
    }

def set_user_vip(user_id, username="", plan="vip", days=30, added_by=0):
    now = int(time.time())
    if plan == "free":
        expires = 0
        limit = PLAN_LIMITS["free"]
    else:
        expires = now + days * 86400 if days > 0 else 0
        limit = PLAN_LIMITS.get(plan, 1000)
    with _db_lock, db_conn() as c:
        c.execute("""
            INSERT INTO vip_users(user_id, username, plan, expires_at,
                                  daily_limit, added_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                plan=excluded.plan,
                expires_at=excluded.expires_at,
                daily_limit=excluded.daily_limit,
                added_by=excluded.added_by
        """, (user_id, username or "", plan, expires, limit, added_by, now))
    return True

def get_today_usage(user_id):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _db_lock, db_conn() as c:
        row = c.execute(
            "SELECT boost_count, success_count FROM user_usage "
            "WHERE user_id=? AND date=?",
            (user_id, today)
        ).fetchone()
    return {"boost": row[0] if row else 0, "success": row[1] if row else 0}

def can_user_boost(user_id):
    plan = get_user_plan(user_id)
    usage = get_today_usage(user_id)
    remaining = plan["limit"] - usage["boost"]
    return {
        "allowed": remaining > 0,
        "plan": plan["plan"],
        "used": usage["boost"],
        "success_today": usage["success"],
        "limit": plan["limit"],
        "remaining": max(0, remaining),
        "expires_at": plan["expires_at"],
        "max_workers": PLAN_WORKERS.get(plan["plan"], 100),
    }

def increment_usage(user_id, success=0):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _db_lock, db_conn() as c:
        c.execute("""
            INSERT INTO user_usage(user_id, date, boost_count, success_count)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(user_id, date) DO UPDATE SET
                boost_count = boost_count + 1,
                success_count = success_count + excluded.success_count
        """, (user_id, today, int(success)))
        c.execute(
            "UPDATE vip_users SET total_boosts = total_boosts + 1 WHERE user_id=?",
            (user_id,)
        )

def list_vip_users(limit=30):
    now = int(time.time())
    with _db_lock, db_conn() as c:
        return c.execute("""
            SELECT user_id, username, plan, expires_at, daily_limit, total_boosts
            FROM vip_users
            WHERE plan != 'free' AND (expires_at = 0 OR expires_at > ?)
            ORDER BY created_at DESC LIMIT ?
        """, (now, limit)).fetchall()

def get_all_users_stats():
    with _db_lock, db_conn() as c:
        total = c.execute("SELECT COUNT(*) FROM vip_users").fetchone()[0]
        vip = c.execute(
            "SELECT COUNT(*) FROM vip_users WHERE plan='vip' AND (expires_at=0 OR expires_at>?)",
            (int(time.time()),)
        ).fetchone()[0]
        premium = c.execute(
            "SELECT COUNT(*) FROM vip_users WHERE plan='premium' AND (expires_at=0 OR expires_at>?)",
            (int(time.time()),)
        ).fetchone()[0]
    return {"total": total, "vip": vip, "premium": premium}

def register_user(user_id, username=""):
    now = int(time.time())
    with _db_lock, db_conn() as c:
        c.execute("""
            INSERT OR IGNORE INTO vip_users
            (user_id, username, plan, expires_at, daily_limit, created_at)
            VALUES (?, ?, 'free', 0, ?, ?)
        """, (user_id, username or "", PLAN_LIMITS["free"], now))

# ─── SDP + CONNECTION ────────────────────────────────────────────
AES_KEY = bytes.fromhex("f5a193d50ade553e9835595f5cd75ddd")
AES_IV = b"\x00" * 16
SERVER_HOST = "login.ml.youngjoygame.com"
SERVER_PORT = 30021
CLIENT_VERSION = "2.1.97.1232.1"
CHANNEL = "and_usa"
LANGUAGE = "en"
CONNECT_TIMEOUT = 8.0
READ_TIMEOUT = 12.0

class SdpDataType(Enum):
    INTEGER_POSITIVE = 0
    INTEGER_NEGATIVE = 1
    FLOAT = 2
    DOUBLE = 3
    STRING = 4
    LIST = 5
    DICT = 6
    STRUCT_BEGIN = 7
    STRUCT_END = 8

class SdpException(Exception):
    pass

class SdpStruct(dict):
    def __init__(self, data=None):
        super().__init__()
        self.data = b""
        self.offset = 0
        if isinstance(data, bytes):
            self.data = data
            self.offset = 0
            self._unpack_from_binary()
        elif data is not None:
            super().update(data)
            self._pack_to_binary()

    def _pack_to_binary(self):
        self.data = bytes([SdpDataType.STRUCT_BEGIN.value << 4])
        for tag, value in sorted(self.items()):
            self._pack(tag, value)
        self.data += bytes([SdpDataType.STRUCT_END.value << 4])

    def _write_number(self, value):
        result = bytearray()
        while value >= 0x80:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value & 0x7F)
        return bytes(result)

    def _pack_header(self, tag, data_type):
        if tag < 15:
            self.data += bytes([(data_type.value << 4) | tag])
        else:
            self.data += bytes([(data_type.value << 4) | 15])
            self.data += self._write_number(tag)

    def _pack(self, tag, value):
        if isinstance(value, bool):
            self._pack_header(tag, SdpDataType.INTEGER_POSITIVE)
            self.data += self._write_number(1 if value else 0)
        elif isinstance(value, int):
            if value < 0:
                self._pack_header(tag, SdpDataType.INTEGER_NEGATIVE)
                self.data += self._write_number(-value)
            else:
                self._pack_header(tag, SdpDataType.INTEGER_POSITIVE)
                self.data += self._write_number(value)
        elif isinstance(value, float):
            self._pack_header(tag, SdpDataType.DOUBLE)
            packed = struct.pack("<d", value)
            self.data += self._write_number(len(packed))
            self.data += packed
        elif isinstance(value, (str, bytes)):
            self._pack_header(tag, SdpDataType.STRING)
            encoded = value.encode("utf-8") if isinstance(value, str) else value
            self.data += self._write_number(len(encoded))
            self.data += encoded
        elif isinstance(value, list):
            self._pack_header(tag, SdpDataType.LIST)
            self.data += self._write_number(len(value))
            for item in value:
                self._pack(0, item)
        elif isinstance(value, dict):
            if isinstance(value, SdpStruct):
                self._pack_header(tag, SdpDataType.STRUCT_BEGIN)
                for k, v in sorted(value.items()):
                    self._pack(k, v)
                self.data += bytes([SdpDataType.STRUCT_END.value << 4])
            else:
                self._pack_header(tag, SdpDataType.DICT)
                self.data += self._write_number(len(value))
                for k, v in sorted(value.items()):
                    self._pack(0, k)
                    self._pack(0, v)
        else:
            raise SdpException(f"Unsupported type: {type(value)}")

    def _read_number(self):
        n = 1
        val = self.data[self.offset] & 0x7F
        while (self.offset + n - 1 < len(self.data)
               and self.data[self.offset + n - 1] >= 0x80):
            if self.offset + n >= len(self.data):
                break
            val |= (self.data[self.offset + n] & 0x7F) << (7 * n)
            n += 1
        self.offset += n
        return val

    def _unpack_from_binary(self):
        if not self.data:
            return
        if self.data[0] >> 4 == SdpDataType.STRUCT_BEGIN.value:
            self.offset = 1
        while self.offset < len(self.data):
            tag, value = self._unpack()
            if isinstance(value, SdpDataType) and value == SdpDataType.STRUCT_END:
                break
            self[tag] = value

    def _unpack(self):
        try:
            if self.offset >= len(self.data):
                return 0, None
            header = self.data[self.offset]
            tag = header & 0xF
            data_type = SdpDataType(header >> 4)
            self.offset += 1
            if tag == 15:
                tag = self._read_number()
            if data_type == SdpDataType.INTEGER_POSITIVE:
                return tag, self._read_number()
            elif data_type == SdpDataType.INTEGER_NEGATIVE:
                return tag, -self._read_number()
            elif data_type == SdpDataType.FLOAT:
                n = self._read_number()
                raw = self.data[self.offset:self.offset + n].ljust(4, b"\x00")
                self.offset += n
                return tag, struct.unpack("<f", raw)[0]
            elif data_type == SdpDataType.DOUBLE:
                n = self._read_number()
                raw = self.data[self.offset:self.offset + n].ljust(8, b"\x00")
                self.offset += n
                return tag, struct.unpack("<d", raw)[0]
            elif data_type == SdpDataType.STRING:
                length = self._read_number()
                try:
                    value = self.data[self.offset:self.offset + length].decode("utf-8")
                except UnicodeDecodeError:
                    value = self.data[self.offset:self.offset + length]
                self.offset += length
                return tag, value
            elif data_type == SdpDataType.LIST:
                length = self._read_number()
                value = []
                for _ in range(length):
                    _, item = self._unpack()
                    value.append(item)
                return tag, value
            elif data_type == SdpDataType.DICT:
                length = self._read_number()
                value = {}
                for _ in range(length):
                    _, k = self._unpack()
                    _, v = self._unpack()
                    value[k] = v
                return tag, value
            elif data_type == SdpDataType.STRUCT_BEGIN:
                struct_data = {}
                while True:
                    sub_tag, sub_value = self._unpack()
                    if (isinstance(sub_value, SdpDataType)
                            and sub_value == SdpDataType.STRUCT_END):
                        break
                    struct_data[sub_tag] = sub_value
                return tag, SdpStruct(struct_data)
            elif data_type == SdpDataType.STRUCT_END:
                return tag, SdpDataType.STRUCT_END
            else:
                raise SdpException("Unknown data type")
        except Exception:
            raise SdpException("Error unpacking SDP")

def clean_device_id(raw):
    if not raw:
        return ""
    s = str(raw).strip().strip("\"' ,;\r\n\t")
    if not s:
        return ""
    if s.startswith(("and_", "ios_")) and len(s) >= 15:
        for d in (" ", ",", ";", "\t", "|", "\"", "'"):
            if d in s:
                s = s.split(d)[0]
        return s
    i = s.find("and_")
    if i == -1:
        i = s.find("ios_")
    if i != -1:
        e = i + 4
        while e < len(s) and (s[e].isalnum() or s[e] in "-_"):
            e += 1
        return s[i:e]
    return ""

def extract_device_components(device_id):
    raw = str(device_id).strip()
    if raw.startswith(("and_", "ios_")):
        raw = raw[4:]
    if len(raw) == 84 and raw[32:48].isalnum() and "-" in raw[48:]:
        return raw[:32], raw[32:48], raw[48:]
    m = re.search(
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", raw,
    )
    if m:
        adid = m.group(1)
        before = raw[:m.start()].rstrip("-_")
        if len(before) >= 48:
            return before[:32], before[32:48], adid
        if len(before) >= 32:
            return before[:32], before[32:], adid
        if len(before) > 0:
            return before, "", adid
        clean = raw.replace("-", "")
        return (clean[:32] if len(clean) >= 32 else raw), "", adid
    if raw.endswith("none"):
        b = raw[:-4].rstrip("-_")
        return (b[:32] if len(b) >= 32 else b), \
               (b[32:48] if len(b) >= 48 else ""), "none"
    if raw.endswith("00000000-0000-0000-0000-000000000000"):
        b = raw[:-36].rstrip("-_")
        return (b[:32] if len(b) >= 32 else b), \
               (b[32:48] if len(b) >= 48 else ""), \
               "00000000-0000-0000-0000-000000000000"
    if len(raw) >= 48:
        return raw[:32], raw[32:48], raw[48:]
    if len(raw) >= 32:
        return raw[:32], raw[32:], ""
    return raw, "", ""

class BaseConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sequence = 1
        self.socket = None
        self.queue_data = b""
        self.last_header_size = 0

    def connect(self):
        if not self.host or not isinstance(self.host, str):
            raise OSError("Invalid host")
        if not self.port or not (1 <= self.port <= 65535):
            raise OSError("Invalid port")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(CONNECT_TIMEOUT)
        self.socket.connect((self.host, self.port))
        self.socket.settimeout(READ_TIMEOUT)

    def cleanup(self):
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.sequence = 1
            self.socket = None
        self.queue_data = b""

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *a):
        self.cleanup()

    def send_data(self, pkt_id, sdp):
        if not self.socket:
            raise ConnectionError("Socket not connected")
        packet = SdpStruct({0: pkt_id, 1: self.sequence, 5: sdp.data}).data
        if zstd is not None and pkt_id < 10000:
            buf = zstd.compress(packet)
            flags = (len(buf) + 4) | (16 << 24)
        else:
            buf = zlib.compress(packet)
            flags = (len(buf) + 4) | (1 << 24)
        self.socket.sendall(flags.to_bytes(4, "big") + buf)
        self.sequence += 1

    def recv_data(self):
        try:
            if not self.socket:
                return None, None
            while len(self.queue_data) < 4:
                data = self.socket.recv(4096)
                if not data:
                    return None, None
                self.queue_data += data
            flags = int.from_bytes(self.queue_data[:4], "big")
            size = flags & 0xFFFFFF
            ctype = flags >> 24
            self.last_header_size = size
            if size < 4 or size > 10_000_000:
                return None, None
            while len(self.queue_data) < size:
                data = self.socket.recv(4096)
                if not data:
                    return None, None
                self.queue_data += data
            data = self.queue_data[4:size]
            self.queue_data = self.queue_data[size:]
            if ctype == 1:
                data = zlib.decompress(data)
            elif ctype == 16:
                if zstd is None:
                    return None, None
                data = zstd.decompress(data)
            elif ctype in (2, 3, 18):
                if AES is None:
                    return None, None
                cipher = AES.new(AES_KEY, AES.MODE_CBC, iv=AES_IV)
                data = cipher.decrypt(
                    data[:-1] if len(data) % 16 != 0 else data
                ).rstrip(b"\x00")
                if ctype == 3:
                    data = zlib.decompress(data)
                elif ctype == 18 and zstd:
                    data = zstd.decompress(data)
            result = SdpStruct(data)
            pkt_id = result.get(0)
            if pkt_id is None:
                return None, None
            body = result.get(6)
            if not body or not isinstance(body, bytes):
                body = result.get(5)
                if not body or not isinstance(body, bytes):
                    return pkt_id, None
            return pkt_id, SdpStruct(body)
        except socket.timeout:
            return -1, None
        except Exception:
            return None, None

def _extract_zone(field):
    if field is None:
        return 0
    if isinstance(field, list) and field:
        first = field[0]
        if isinstance(first, dict):
            return int(first.get(0, 0) or 0)
        try:
            return int(first)
        except (TypeError, ValueError):
            return 0
    if isinstance(field, dict):
        return int(field.get(0, 0) or 0)
    try:
        return int(field)
    except (TypeError, ValueError):
        return 0

class GameConnection(BaseConnection):
    def __init__(self, device_id):
        super().__init__(SERVER_HOST, SERVER_PORT)
        self.device_id = device_id
        self.imei, self.android, self.adid = extract_device_components(device_id)
        self.account_id = 0
        self.session_key = ""
        self.zone_id = 0
        self.game_host = ""
        self.game_port = 0
        self.creation_ts = 0
        self.ban_status = "NORMAL"
        self.packet_cache = {}
        self.login_response = {}

    def login_to_login_server(self):
        if self.host != SERVER_HOST or self.port != SERVER_PORT:
            self.cleanup()
            self.host = SERVER_HOST
            self.port = SERVER_PORT
            self.connect()
        self.send_data(1, SdpStruct({
            0: self.device_id,
            1: (f"gps_adid={self.adid}&android_id={self.android}"
                f"&device_unique_id={self.imei}"),
            2: CLIENT_VERSION, 3: CHANNEL, 4: LANGUAGE,
        }))
        pid, res = self.recv_data()
        if pid == 2 and res:
            self.account_id = res.get(0)
            self.session_key = res.get(1, "")
            self.zone_id = _extract_zone(res.get(2))
            self.creation_ts = res.get(19, 0)
            if not self.zone_id:
                for tag in (3, 5):
                    z = _extract_zone(res.get(tag))
                    if z:
                        self.zone_id = z
                        break
            try:
                self.login_response = dict(res)
            except Exception:
                self.login_response = {}
            if self.account_id and self.zone_id:
                self.ban_status = "NORMAL"
                return True
        if res and isinstance(res, dict):
            for v in res.values():
                if isinstance(v, str) and any(
                    b in v.lower()
                    for b in ("ban", "suspend", "freeze", "limit")
                ):
                    self.ban_status = f"BANNED: {v}"
                    return False
        self.ban_status = f"LOGIN_FAILED(PID={pid})"
        return False

    def get_game_server(self):
        self.send_data(5, SdpStruct({
            0: self.account_id, 1: self.session_key,
            2: CLIENT_VERSION, 5: self.zone_id, 6: CHANNEL,
        }))
        pid, res = self.recv_data()
        if pid == 6 and res:
            gs = res.get(1, "")
            if isinstance(gs, str) and ":" in gs:
                host, port_s = gs.split(":", 1)
                self.game_host = host.strip()
                try:
                    self.game_port = int(port_s.strip())
                except ValueError:
                    return False
                if not (1 <= self.game_port <= 65535):
                    return False
                return True
        return False

    def connect_to_game_server(self):
        if not self.game_host or not self.game_port:
            return False
        self.cleanup()
        self.host = self.game_host
        self.port = self.game_port
        self.connect()
        try:
            self.send_data(10001, SdpStruct({
                0: self.account_id, 1: self.session_key,
                2: self.zone_id, 4: CLIENT_VERSION,
                13: CHANNEL, 15: self.device_id,
            }))
            self.send_data(10101, SdpStruct({0: 0, 2: 2}))
        except Exception:
            return False
        deadline = time.time() + 15.0
        while time.time() < deadline:
            pid, res = self.recv_data()
            if pid is None:
                return False
            if pid == 10002:
                return True
            if pid == -1:
                continue
        return False

    def __enter__(self):
        super().__enter__()
        if not self.login_to_login_server():
            raise ConnectionError(f"LOGIN_FAILED: {self.ban_status}")
        if not self.get_game_server():
            raise ConnectionError("SERVER_SELECTION_FAILED")
        return self

FALLBACK_PROBE = (
    "and_a82ae0f8332b8cac0a04f709175d1311"
    "bl45ikx872hey44ke9d88f34-c2b7-4af3-ac92-ca3e1c4ac902"
)

def validate_session_account(conn):
    if not conn.login_to_login_server():
        return False, f"Login failed: {conn.ban_status}", 0, ""
    if any(b in conn.ban_status.lower() for b in ("ban", "suspend", "freeze")):
        return False, "Account BANNED", 0, ""
    if not conn.get_game_server():
        return False, "Gateway server failed", 0, ""
    if not conn.connect_to_game_server():
        return False, "Game server connection failed", 0, ""
    conn.send_data(10143, SdpStruct({0: conn.account_id, 1: conn.zone_id}))
    lvl = 30
    nk = f"Player_{conn.account_id}"
    for _ in range(6):
        pid, res = conn.recv_data()
        if pid == 10144 and isinstance(res, (dict, SdpStruct)):
            lvl = int(res.get(3, 30) or 30)
            nk = str(res.get(2, nk) or nk)
            if lvl < 3:
                return False, f"Level {lvl} too low", lvl, nk
            break
        if pid in (-1, None):
            break
    return True, "OK", lvl, nk

def check_target_standalone(target_id, probe_device, target_zone=None):
    cd = clean_device_id(probe_device)
    if not cd:
        return {"success": False, "error": "Empty device ID"}
    try:
        with GameConnection(cd) as conn:
            ok, err, lv, nk = validate_session_account(conn)
            if not ok:
                return {"success": False, "error": err}
            if not target_zone:
                conn.socket.settimeout(3.0)
                conn.send_data(11153, SdpStruct({1: int(target_id)}))
                for _ in range(8):
                    try:
                        p, r = conn.recv_data()
                        if p == 11154 and isinstance(r, (dict, SdpStruct)):
                            conn.packet_cache[11154] = r
                            bi = r.get(0, [{}])[0] if (isinstance(r.get(0), list) and r.get(0)) else {}
                            z = bi.get(1)
                            if z:
                                target_zone = int(z)
                                break
                    except Exception:
                        break
            if not target_zone:
                return {"success": False, "error": f"Zone ID not found for {target_id}"}
            try:
                conn.send_data(10143, SdpStruct({0: int(target_id), 1: int(target_zone)}))
                conn.send_data(11153, SdpStruct({1: int(target_id)}))
                for _ in range(12):
                    p, r = conn.recv_data()
                    if p in (-1, None):
                        break
                    conn.packet_cache[p] = r
                    if (11154 in conn.packet_cache and 10144 in conn.packet_cache):
                        break
            except Exception:
                pass
            pi = conn.packet_cache.get(11154, {})
            ph = conn.packet_cache.get(10144, {})
            bi = pi.get(0, [{}])[0] if (isinstance(pi.get(0), list) and pi.get(0)) else {}
            nk = ph.get(2) or bi.get(2, f"Player_{target_id}")
            lv = ph.get(3) or bi.get(3, 1)
            return {
                "success": True, "target_id": target_id,
                "target_zone": target_zone, "nickname": nk, "level": lv,
            }
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_album_photos(target_id, probe_device="", target_zone=None):
    candidates = []
    cd = clean_device_id(probe_device) if probe_device else ""
    if cd:
        candidates.append(cd)
    candidates.append(FALLBACK_PROBE)
    for d in load_device_ids(limit=10):
        if d not in candidates:
            candidates.append(d)

    if not target_zone:
        for pd in candidates[:3]:
            try:
                with GameConnection(pd) as conn:
                    conn.socket.settimeout(2.5)
                    validate_session_account(conn)
                    conn.send_data(11153, SdpStruct({1: int(target_id)}))
                    for _ in range(8):
                        p, r = conn.recv_data()
                        if p == 11154 and isinstance(r, (dict, SdpStruct)):
                            bi = r.get(0, [{}])[0] if (isinstance(r.get(0), list) and r.get(0)) else {}
                            target_zone = int(bi.get(1, 0))
                            if target_zone:
                                break
                        if p in (-1, None):
                            break
                    if target_zone:
                        break
            except Exception:
                pass

    if not target_zone:
        return {"success": False, "error": "Zone ID not found", "album_slots": {}}

    def fetch_slot(item):
        slot, device = item
        for attempt in range(2):
            try:
                with GameConnection(device) as conn:
                    conn.socket.settimeout(3.0)
                    validate_session_account(conn)
                    conn.send_data(42011, SdpStruct({
                        0: SdpStruct({0: int(target_zone), 1: int(target_id)}),
                        1: int(slot),
                    }))
                    for _ in range(6):
                        p, r = conn.recv_data()
                        if p == 42012 and isinstance(r, (dict, SdpStruct)):
                            photo_id = _extract_photo_id(r)
                            if photo_id:
                                return slot, {"slot": slot, "photo_id": int(photo_id)}
                        if p in (-1, None):
                            break
            except Exception:
                pass
        return slot, None

    items = [(s, candidates[(s - 1) % len(candidates)]) for s in [1, 2, 3, 4]]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        results = list(ex.map(fetch_slot, items))

    slots = {s: info for s, info in results if info}
    return {"success": True, "target_id": target_id, "target_zone": target_zone, "album_slots": slots}

def _extract_photo_id(res):
    pid = None
    stack = [res]
    seen = set()
    while stack:
        obj = stack.pop()
        oid = id(obj)
        if oid in seen:
            continue
        seen.add(oid)
        if isinstance(obj, (dict, SdpStruct)):
            for k, v in obj.items():
                if isinstance(v, int) and v > 100000000 and pid is None:
                    pid = v
                elif isinstance(v, (dict, list, SdpStruct)):
                    stack.append(v)
        elif isinstance(obj, list):
            for it in obj:
                if isinstance(it, (dict, list, SdpStruct)):
                    stack.append(it)
    return pid

def get_valid_session_device(min_level=6):
    devices = [FALLBACK_PROBE] + load_device_ids(limit=10)
    for dev in devices:
        try:
            with GameConnection(dev) as conn:
                conn.socket.settimeout(2.0)
                ok, err, lvl, nk = validate_session_account(conn)
                if ok and lvl >= min_level:
                    return dev, nk, lvl
        except Exception:
            continue
    return FALLBACK_PROBE, "VerifiedProbe", 30

# ─── BOOST ENGINE ────────────────────────────────────────────────
def run_boost(device_pool, target_id, target_zone, target_nick,
              features, comments, workers, target_success,
              loop_mode, auto_clean, album_slots,
              progress_cb=None, stop_event=None, live_stats=None):
    if stop_event is None:
        stop_event = threading.Event()

    stats = {
        "total_acc": 0, "success": 0, "fail": 0,
        "banned_removed": 0, "low_removed": 0, "db_removed": 0,
        "like_prof": 0, "like_alb": 0, "view_alb": 0,
        "comment": 0, "follow": 0, "like_coll": 0,
    }
    if live_stats is not None:
        live_stats.clear()
        live_stats.update(stats)
        stats = live_stats

    lock = threading.Lock()
    t0 = time.time()

    def process_one(item):
        _LOGIN_SEMAPHORE.acquire()
        try:
            if stop_event.is_set():
                return
            idx, device = item
            with lock:
                if stop_event.is_set() or stats["success"] >= target_success:
                    return
                attempt = stats["total_acc"] + 1
                stats["total_acc"] += 1

            cd = clean_device_id(device)
            if not cd:
                with lock:
                    stats["fail"] += 1
                return

            comment = comments[(attempt - 1) % len(comments)]
            cnt = {"lp": 0, "la": 0, "va": 0, "ck": 0, "fl": 0, "kc": 0}
            any_ok = False
            banned = False
            low_lv = False
            lvl = 0

            for retry in range(50):
                if stop_event.is_set() or stats["success"] >= target_success:
                    return
                try:
                    with GameConnection(cd) as conn:
                        conn.socket.settimeout(3.5)
                        ok, err, lvl, nick = validate_session_account(conn)
                        if not ok:
                            el = err.lower()
                            if any(b in el for b in ("ban", "block", "suspend")):
                                banned = True
                                break
                            if retry == 0 and not stop_event.is_set():
                                time.sleep(0.2)
                                continue
                            break
                        if lvl < 3:
                            low_lv = True
                            break

                        if features.get("like_prof") and _like_profile(conn, target_id, target_zone):
                            cnt["lp"] += 1; any_ok = True
                        if stop_event.is_set() or stats["success"] >= target_success: return
                        if features.get("view_alb"):
                            if _view_album(conn, target_id, target_zone):
                                cnt["va"] += 1; any_ok = True
                        if stop_event.is_set() or stats["success"] >= target_success: return
                        if features.get("like_alb") and album_slots:
                            for slot, photo_id in album_slots:
                                if _like_album(conn, target_id, target_zone, photo_id):
                                    cnt["la"] += 1; any_ok = True
                        if stop_event.is_set() or stats["success"] >= target_success: return
                        if features.get("comment") and album_slots:
                            for slot, photo_id in album_slots:
                                if _comment_album(conn, target_id, target_zone, photo_id, comment):
                                    cnt["ck"] += 1; any_ok = True
                        if stop_event.is_set() or stats["success"] >= target_success: return
                        if features.get("follow") and _follow(conn, target_id, target_zone):
                            cnt["fl"] += 1; any_ok = True
                        if stop_event.is_set() or stats["success"] >= target_success: return
                        if features.get("like_coll") and _like_collection(conn, target_id, target_zone):
                            cnt["kc"] += 1; any_ok = True
                        break
                except Exception:
                    if retry < 49 and not stop_event.is_set():
                        time.sleep(min(4.0, 0.1 * (1.5 ** (retry % 8))))
                        continue

            if stop_event.is_set():
                return

            short_dev = device[:20] + "…" if len(device) > 22 else device
            event = None

            if banned:
                if auto_clean:
                    removed = remove_device(cd)
                    with lock:
                        stats["banned_removed"] += 1
                        stats["db_removed"] += removed
                with lock:
                    stats["fail"] += 1
                event = {"level": "error", "line": f"[BANNED] #{attempt:04d} | {short_dev}"}
            elif low_lv:
                if auto_clean:
                    removed = remove_device(cd)
                    with lock:
                        stats["low_removed"] += 1
                        stats["db_removed"] += removed
                with lock:
                    stats["fail"] += 1
                event = {"level": "warning", "line": f"[LOW LV.{lvl}] #{attempt:04d} | {short_dev}"}
            else:
                should_clean = (auto_clean and not any_ok)
                if should_clean:
                    removed = remove_device(cd)
                    with lock:
                        stats["db_removed"] += removed
                with lock:
                    if any_ok:
                        if stats["success"] < target_success:
                            stats["success"] += 1
                            stats["like_prof"] += cnt["lp"]
                            stats["like_alb"] += cnt["la"]
                            stats["view_alb"] += cnt["va"]
                            stats["comment"] += cnt["ck"]
                            stats["follow"] += cnt["fl"]
                            stats["like_coll"] += cnt["kc"]
                            current_succ = stats["success"]
                            if current_succ >= target_success:
                                stop_event.set()
                        else:
                            current_succ = stats["success"]
                            stop_event.set()

                        tag = f"SUCCESS ({current_succ}/{target_success})"
                        event = {"level": "success", "line": f"[OK] #{attempt:04d} | {short_dev} | {tag}"}
                    else:
                        stats["fail"] += 1
                        event = {"level": "warning", "line": f"[FAIL] #{attempt:04d} | {short_dev}"}

            if progress_cb and event:
                try:
                    progress_cb(event)
                except Exception:
                    pass
        finally:
            _LOGIN_SEMAPHORE.release()

    task_list = [(i, device_pool[(i - 1) % len(device_pool)])
                 for i in range(1, target_success + 1)]

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    futures = [ex.submit(process_one, item) for item in task_list]
    try:
        for f in concurrent.futures.as_completed(futures):
            if stop_event.is_set() or stats["success"] >= target_success:
                for fut in futures:
                    fut.cancel()
                break
    finally:
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    duration = time.time() - t0
    return {
        **stats, "duration": duration,
        "speed": (stats["total_acc"] / duration) if duration > 0 else 0,
        "was_stopped": stop_event.is_set(),
        "target_id": target_id, "target_zone": target_zone, "target_nick": target_nick,
    }

def _like_profile(conn, target_id, target_zone):
    try:
        conn.send_data(10173, SdpStruct({0: int(target_zone), 1: int(target_id)}))
        for _ in range(4):
            pid, res = conn.recv_data()
            if pid == 10174: return True
            if pid == 20001:
                return (res.get(5) if isinstance(res, dict) else None) == 5355
            if pid in (-1, None): break
    except Exception: pass
    return False

def _view_album(conn, target_id, target_zone):
    try:
        conn.send_data(42011, SdpStruct({0: SdpStruct({0: int(target_zone), 1: int(target_id)}), 1: 1}))
        for _ in range(3):
            pid, res = conn.recv_data()
            if pid == 42012: return True
            if pid in (-1, None, 20001): break
    except Exception: pass
    return False

def _like_album(conn, target_id, target_zone, photo_id):
    try:
        conn.send_data(42015, SdpStruct({0: SdpStruct({0: int(target_zone), 1: int(target_id)}), 1: 1, 2: int(photo_id)}))
        for _ in range(4):
            pid, res = conn.recv_data()
            if pid == 42016: return True
            if pid == 20001:
                return (res.get(5) if isinstance(res, dict) else None) == 5355
            if pid in (-1, None): break
    except Exception: pass
    return False

def _comment_album(conn, target_id, target_zone, photo_id, text):
    try:
        conn.send_data(42019, SdpStruct({0: SdpStruct({0: int(target_zone), 1: int(target_id)}), 1: 1, 2: int(photo_id), 3: str(text)[:100]}))
        for _ in range(4):
            pid, res = conn.recv_data()
            if pid == 42020: return True
            if pid in (-1, None, 20001): break
    except Exception: pass
    return False

def _follow(conn, target_id, target_zone):
    try:
        conn.send_data(12107, SdpStruct({0: int(target_id), 1: int(target_zone), 5: 3, 6: "", 7: 5, 10: "", 12: "", 14: ""}))
        for _ in range(8):
            pid, res = conn.recv_data()
            if pid == 12108: return True
            if pid == 20001:
                return (res.get(5) if isinstance(res, dict) else None) in (5355, 4101)
            if pid in (-1, None): break
    except Exception: pass
    return False

def _like_collection(conn, target_id, target_zone):
    try:
        conn.send_data(19721, SdpStruct({0: int(target_zone), 1: int(target_id)}))
        for _ in range(8):
            pid, res = conn.recv_data()
            if pid == 19722: return True
            if pid == 20001:
                return (res.get(5) if isinstance(res, dict) else None) in (5355, 4101)
            if pid in (-1, None): break
    except Exception: pass
    return False

# ─── TELEGRAM BOT — UI ───────────────────────────────────────────
(ASK_ROLE, ASK_ZONE, ASK_FEATURES, ASK_COMMENTS,
 ASK_WORKERS, ASK_TARGET) = range(6)

ACTIVE_BOOSTS = {}
BOOST_LOCK = threading.Lock()

FEATURE_NAMES = {
    1: "like_prof", 2: "like_alb", 3: "view_alb",
    4: "comment", 5: "follow", 6: "like_coll",
}
FEATURE_LABELS = {
    "like_prof": "Like Profile", "like_alb": "Like Album",
    "view_alb": "View Album", "comment": "Comment",
    "follow": "Follow", "like_coll": "Like Collection",
}

def admin_only(func):
    async def wrapper(update, ctx):
        uid = update.effective_user.id
        if uid not in ADMIN_IDS:
            if update.callback_query:
                await update.callback_query.answer("⛔ Admin only!", show_alert=True)
            else:
                await update.message.reply_text("⛔ Admin only!")
            return
        return await func(update, ctx)
    return wrapper

def track_user(func):
    async def wrapper(update, ctx):
        u = update.effective_user
        if u:
            register_user(u.id, u.username or "")
        return await func(update, ctx)
    return wrapper

def plan_emoji(plan):
    return {"free": "🆓", "vip": "💎", "premium": "👑"}.get(plan, "🆓")

def fmt_expiry(ts):
    if not ts or ts == 0:
        return "∞"
    dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=7)
    if ts < time.time():
        return f"❌ {dt.strftime('%d/%m/%Y')}"
    return dt.strftime("%d/%m/%Y %H:%M")

def back_kb(target="main_menu"):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data=target)]
    ])

def main_menu_kb(user_id):
    rows = [
        [InlineKeyboardButton("🚀 Start Boost", callback_data="menu_boost")],
        [
            InlineKeyboardButton("📱 Device Pool", callback_data="menu_devices"),
            InlineKeyboardButton("📊 Stats", callback_data="menu_stats"),
        ],
        [
            InlineKeyboardButton("💎 My VIP", callback_data="myvip"),
            InlineKeyboardButton("📜 History", callback_data="menu_history"),
        ],
    ]
    if user_id in ADMIN_IDS:
        rows.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
    rows.append([InlineKeyboardButton("❌ Close", callback_data="menu_close")])
    return InlineKeyboardMarkup(rows)

def main_menu_text(user_id):
    total = count_devices()
    status = can_user_boost(user_id)
    emoji = plan_emoji(status["plan"])
    limit_text = status["limit"] if status["limit"] < 999999 else "∞"
    rem_text = status["remaining"] if status["limit"] < 999999 else "∞"
    return (
        f"╔══════════════════════════════╗\n"
        f"║   🎯 <b>XONI BOOST BOT</b>   ║\n"
        f"╚══════════════════════════════╝\n\n"
        f"👤 Plan: <b>{emoji} {status['plan'].upper()}</b>\n"
        f"📊 Today: <b>{status['used']}</b> / <b>{limit_text}</b>\n"
        f"✅ Remaining: <b>{rem_text}</b>\n"
        f"📱 Devices: <b>{total:,}</b>\n\n"
        f"<i>Select an option below 👇</i>"
    )

@track_user
async def cmd_start(update, ctx):
    uid = update.effective_user.id
    await update.message.reply_text(
        main_menu_text(uid),
        reply_markup=main_menu_kb(uid),
        parse_mode=ParseMode.HTML,
    )

@track_user
async def cb_main_menu(update, ctx):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    await q.edit_message_text(
        main_menu_text(uid),
        reply_markup=main_menu_kb(uid),
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cb_close(update, ctx):
    await update.callback_query.answer()
    await update.callback_query.message.delete()

@track_user
async def cmd_myvip(update, ctx):
    await send_vip_status(update.message, update.effective_user.id)

async def send_vip_status(target, uid, edit=False):
    status = can_user_boost(uid)
    emoji = plan_emoji(status["plan"])
    limit_text = status["limit"] if status["limit"] < 999999 else "∞"
    rem_text = status["remaining"] if status["limit"] < 999999 else "∞"
    text = (
        f"{emoji} <b>YOUR VIP STATUS</b>\n\n"
        f"👤 ID: <code>{uid}</code>\n"
        f"📋 Plan: <b>{status['plan'].upper()}</b>\n"
        f"📊 Today: <b>{status['used']}</b> / <b>{limit_text}</b>\n"
        f"✅ Remaining: <b>{rem_text}</b>\n"
        f"⏰ Expires: <b>{fmt_expiry(status['expires_at'])}</b>\n"
        f"🔧 Max Workers: <b>{status['max_workers']}</b>\n"
    )
    kb = back_kb()
    if edit:
        await target.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await target.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

@track_user
async def cb_myvip(update, ctx):
    q = update.callback_query
    await q.answer()
    await send_vip_status(q, update.effective_user.id, edit=True)

@admin_only
async def cb_admin_panel(update, ctx):
    q = update.callback_query
    await q.answer()
    stats = get_all_users_stats()
    text = (
        f"👑 <b>ADMIN PANEL</b>\n\n"
        f"👥 Total Users: <b>{stats['total']}</b>\n"
        f"💎 VIP: <b>{stats['vip']}</b>\n"
        f"👑 Premium: <b>{stats['premium']}</b>\n\n"
        f"<b>Commands:</b>\n"
        f"<code>/givevip &lt;id&gt; &lt;plan&gt; &lt;days&gt;</code>\n"
        f"<code>/revokevip &lt;id&gt;</code>\n"
        f"<code>/listvip</code>\n"
        f"<code>/userinfo &lt;id&gt;</code>\n\n"
        f"<i>Plan: vip | premium | free</i>"
    )
    await q.edit_message_text(text, reply_markup=back_kb(), parse_mode=ParseMode.HTML)

@admin_only
async def cmd_givevip(update, ctx):
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text(
            "📖 <b>Usage:</b>\n"
            "<code>/givevip &lt;user_id&gt; &lt;plan&gt; &lt;days&gt;</code>\n\n"
            "Plan: <code>vip</code> | <code>premium</code> | <code>free</code>\n"
            "Days: <code>0</code> = unlimited\n\n"
            "Example: <code>/givevip 123456789 vip 30</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        target_uid = int(args[0])
        plan = args[1].lower()
        days = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return
    if plan not in ("vip", "premium", "free"):
        await update.message.reply_text("❌ Plan must be vip / premium / free")
        return
    set_user_vip(target_uid, "", plan=plan, days=days, added_by=update.effective_user.id)
    emoji = plan_emoji(plan)
    await update.message.reply_text(
        f"✅ <b>VIP Updated</b>\n\n"
        f"👤 User: <code>{target_uid}</code>\n"
        f"📋 Plan: <b>{emoji} {plan.upper()}</b>\n"
        f"⏰ Days: <b>{days if days > 0 else '∞'}</b>\n"
        f"📊 Daily limit: <b>{PLAN_LIMITS.get(plan, 50)}</b>",
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cmd_revokevip(update, ctx):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: <code>/revokevip &lt;user_id&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        uid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    set_user_vip(uid, "", plan="free", days=0, added_by=update.effective_user.id)
    await update.message.reply_text(
        f"✅ Revoked VIP for <code>{uid}</code>",
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cmd_listvip(update, ctx):
    rows = list_vip_users(limit=30)
    if not rows:
        await update.message.reply_text("📋 No active VIP users.")
        return
    lines = ["💎 <b>VIP USERS</b>\n"]
    for uid, uname, plan, exp, limit, total in rows:
        emoji = plan_emoji(plan)
        lines.append(
            f"{emoji} <code>{uid}</code> — <b>{plan}</b>\n"
            f"   ⏰ {fmt_expiry(exp)} | 📊 {limit}/day | 🚀 {total} boosts\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

@admin_only
async def cmd_userinfo(update, ctx):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: <code>/userinfo &lt;id&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        uid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return
    status = can_user_boost(uid)
    emoji = plan_emoji(status["plan"])
    limit_text = status["limit"] if status["limit"] < 999999 else "∞"
    await update.message.reply_text(
        f"👤 <b>USER INFO</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📋 Plan: <b>{emoji} {status['plan'].upper()}</b>\n"
        f"📊 Today: <b>{status['used']}</b> / <b>{limit_text}</b>\n"
        f"⏰ Expires: <b>{fmt_expiry(status['expires_at'])}</b>",
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cb_menu_devices(update, ctx):
    q = update.callback_query
    await q.answer()
    total = count_devices()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Upload .txt File", callback_data="dev_upload")],
        [InlineKeyboardButton("📋 View Sample IDs", callback_data="dev_sample")],
        [InlineKeyboardButton("🗑 Clear All Devices", callback_data="dev_clear")],
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
    ])
    await q.edit_message_text(
        f"📱 <b>DEVICE POOL</b>\n\n"
        f"📊 Total Devices: <b>{total:,}</b>",
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cb_dev_upload(update, ctx):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "📤 <b>Upload Device File</b>\n\n"
        "Send me a <b>.txt</b> file with device IDs\n"
        "(one per line or comma-separated).",
        reply_markup=back_kb("menu_devices"),
        parse_mode=ParseMode.HTML,
    )
    ctx.user_data["awaiting_file"] = True

@admin_only
async def cb_dev_sample(update, ctx):
    q = update.callback_query
    await q.answer()
    rows = load_device_ids(limit=10)
    if not rows:
        text = "📋 <b>No devices in pool.</b>"
    else:
        sample = "\n".join(f"<code>{d[:60]}{'…' if len(d) > 60 else ''}</code>" for d in rows)
        text = f"📋 <b>Sample Device IDs (first 10):</b>\n\n{sample}"
    await q.edit_message_text(text, reply_markup=back_kb("menu_devices"), parse_mode=ParseMode.HTML)

@admin_only
async def cb_dev_clear(update, ctx):
    q = update.callback_query
    await q.answer()
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Delete All", callback_data="dev_clear_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="menu_devices"),
        ]
    ])
    await q.edit_message_text(
        "⚠️ <b>Are you sure?</b>\n\nThis will delete <b>ALL</b> devices!",
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def cb_dev_clear_confirm(update, ctx):
    q = update.callback_query
    await q.answer()
    with _db_lock, db_conn() as c:
        cur = c.execute("DELETE FROM devices")
        count = cur.rowcount
    await q.edit_message_text(
        f"✅ Deleted <b>{count:,}</b> devices.",
        reply_markup=back_kb("menu_devices"),
        parse_mode=ParseMode.HTML,
    )

@admin_only
async def handle_document(update, ctx):
    if not ctx.user_data.get("awaiting_file"):
        return
    doc = update.message.document
    if not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("❌ Please send a .txt file only.")
        return
    await update.message.chat.send_action(ChatAction.TYPING)
    file = await doc.get_file()
    raw = await file.download_as_bytearray()
    content = bytes(raw).decode("utf-8", errors="ignore")
    items = re.split(r"[\r\n,;|\s]+", content)
    added, skipped, invalid = bulk_add_devices(items, "telegram_upload")
    ctx.user_data["awaiting_file"] = False
    await update.message.reply_text(
        f"✅ <b>Import Complete!</b>\n\n"
        f"➕ Added: <b>{added:,}</b>\n"
        f"⏭ Skipped: <b>{skipped:,}</b>\n"
        f"❌ Invalid: <b>{invalid:,}</b>\n\n"
        f"📊 Total Pool: <b>{count_devices():,}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=back_kb("menu_devices"),
    )

@admin_only
async def cb_menu_stats(update, ctx):
    q = update.callback_query
    await q.answer()
    with _db_lock, db_conn() as c:
        total = c.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
        bh = c.execute(
            "SELECT COUNT(*), SUM(success), SUM(fail), SUM(banned) FROM boost_history"
        ).fetchone()
        boosts, succ, fail, banned = bh if bh else (0, 0, 0, 0)
    us = get_all_users_stats()
    text = (
        f"📊 <b>STATISTICS</b>\n\n"
        f"📱 Devices: <b>{total:,}</b>\n"
        f"👥 Users: <b>{us['total']:,}</b>\n"
        f"💎 VIP: <b>{us['vip']:,}</b> | 👑 Premium: <b>{us['premium']:,}</b>\n\n"
        f"🚀 Total Boosts: <b>{boosts or 0:,}</b>\n"
        f"✅ Success: <b>{(succ or 0):,}</b>\n"
        f"❌ Fail: <b>{(fail or 0):,}</b>\n"
        f"🚫 Banned: <b>{(banned or 0):,}</b>"
    )
    await q.edit_message_text(text, reply_markup=back_kb(), parse_mode=ParseMode.HTML)

@track_user
async def cb_menu_history(update, ctx):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    rows = get_boost_history(limit=10, user_id=uid)
    if not rows:
        text = "📜 <b>No boost history yet.</b>"
    else:
        lines = ["📜 <b>YOUR LAST 10 BOOSTS</b>\n"]
        for r in rows:
            tid, nick, s, f, b, dur, ts = r
            dt = datetime.fromtimestamp(ts, timezone.utc) + timedelta(hours=7)
            lines.append(
                f"🎯 <b>{nick or tid}</b> (<code>{tid}</code>)\n"
                f"   ✅ {s} | ❌ {f} | 🚫 {b} | ⏱ {dur}s\n"
                f"   🕐 {dt.strftime('%d/%m %H:%M')}\n"
            )
        text = "\n".join(lines)
    await q.edit_message_text(text, reply_markup=back_kb(), parse_mode=ParseMode.HTML)

@track_user
async def cb_menu_boost(update, ctx):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id

    status = can_user_boost(uid)
    if not status["allowed"]:
        emoji = plan_emoji(status["plan"])
        await q.edit_message_text(
            f"⛔ <b>Daily limit reached!</b>\n\n"
            f"{emoji} Plan: <b>{status['plan'].upper()}</b>\n"
            f"📊 Used: <b>{status['used']}</b> / <b>{status['limit']}</b>\n\n"
            f"<i>Resets at midnight UTC.</i>",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    if ACTIVE_BOOSTS:
        running = "\n".join(
            f"• <code>{v['target']}</code> — {v['stats'].get('success', 0)} ok"
            for v in ACTIVE_BOOSTS.values()
        )
        await q.edit_message_text(
            f"⚠️ <b>Boost already running!</b>\n\n{running}\n\n"
            f"Stop it first with /stop",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    total = count_devices()
    if total == 0:
        await q.edit_message_text(
            "❌ <b>Device pool is empty!</b>",
            reply_markup=back_kb("menu_devices"),
            parse_mode=ParseMode.HTML,
        )
        return ConversationHandler.END

    ctx.user_data["boost"] = {}
    await q.edit_message_text(
        "🚀 <b>START BOOST</b>\n\n"
        "Step 1/5 — Send the <b>Account ID (Role ID)</b>:",
        reply_markup=back_kb(),
        parse_mode=ParseMode.HTML,
    )
    return ASK_ROLE

@track_user
async def boost_got_role(update, ctx):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ Send a numeric Account ID:")
        return ASK_ROLE
    ctx.user_data["boost"]["role_id"] = int(text)
    await update.message.reply_text(
        "Step 2/5 — Send the <b>Zone ID</b>:",
        parse_mode=ParseMode.HTML,
    )
    return ASK_ZONE

@track_user
async def boost_got_zone(update, ctx):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ Send a numeric Zone ID:")
        return ASK_ZONE
    ctx.user_data["boost"]["zone_id"] = int(text)
    ctx.user_data["boost"]["features"] = set()

    await update.message.reply_text(
        "Step 3/5 — <b>Select Features</b>\n\n"
        "Tap to select, then press ✅ Done.",
        reply_markup=build_feature_kb(set()),
        parse_mode=ParseMode.HTML,
    )
    return ASK_FEATURES

def build_feature_kb(selected):
    def label(n):
        key = FEATURE_NAMES[n]
        mark = "✅" if key in selected else "⬜"
        return f"{mark} {n}️⃣ {FEATURE_LABELS[key]}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label(1), callback_data="feat_1"),
         InlineKeyboardButton(label(2), callback_data="feat_2")],
        [InlineKeyboardButton(label(3), callback_data="feat_3"),
         InlineKeyboardButton(label(4), callback_data="feat_4")],
        [InlineKeyboardButton(label(5), callback_data="feat_5"),
         InlineKeyboardButton(label(6), callback_data="feat_6")],
        [InlineKeyboardButton("🔥 Select ALL", callback_data="feat_all"),
         InlineKeyboardButton("✅ Done", callback_data="feat_done")],
        [InlineKeyboardButton("🔙 Cancel", callback_data="main_menu")],
    ])

@track_user
async def cb_feature_select(update, ctx):
    q = update.callback_query
    await q.answer()
    if "boost" not in ctx.user_data:
        await q.edit_message_text("⚠️ Session expired.", reply_markup=back_kb())
        return ConversationHandler.END
    feats = ctx.user_data["boost"].setdefault("features", set())

    if q.data == "feat_all":
        feats.clear()
        feats.update(FEATURE_NAMES.values())
    else:
        num = int(q.data.split("_")[1])
        key = FEATURE_NAMES[num]
        if key in feats:
            feats.discard(key)
        else:
            feats.add(key)

    chosen = ", ".join(FEATURE_LABELS[f] for f in feats) or "none"
    await q.edit_message_text(
        f"Step 3/5 — <b>Select Features</b>\n\n"
        f"Selected: <b>{chosen}</b>",
        reply_markup=build_feature_kb(feats),
        parse_mode=ParseMode.HTML,
    )
    return ASK_FEATURES

@track_user
async def cb_feat_done(update, ctx):
    q = update.callback_query
    await q.answer()
    feats = ctx.user_data["boost"].get("features", set())
    if not feats:
        await q.answer("⚠️ Select at least one feature!", show_alert=True)
        return ASK_FEATURES

    if "comment" in feats:
        await q.edit_message_text(
            "Step 4/5 — <b>Comments</b>\n\n"
            "Send comments separated by <code>|</code>\n"
            "Example: <code>Nice!|GG|Awesome</code>\n\n"
            "Or send <code>skip</code> for default.",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return ASK_COMMENTS
    ctx.user_data["boost"]["comments"] = ["GG WP! Awesome!"]
    await q.edit_message_text(
        "Step 5/5 — <b>Workers Count</b>\n\n"
        "Send a number (1-500).\n"
        "Recommended: <b>100</b>",
        reply_markup=back_kb(),
        parse_mode=ParseMode.HTML,
    )
    return ASK_WORKERS

@track_user
async def boost_got_comments(update, ctx):
    text = update.message.text.strip()
    if text.lower() == "skip":
        comments = ["GG WP! Awesome!"]
    else:
        comments = [c.strip() for c in text.split("|") if c.strip()] or ["GG WP! Awesome!"]
    ctx.user_data["boost"]["comments"] = comments
    await update.message.reply_text(
        "Step 5/5 — <b>Workers Count</b>\n\n"
        "Send a number (1-500).",
        reply_markup=back_kb(),
        parse_mode=ParseMode.HTML,
    )
    return ASK_WORKERS

@track_user
async def boost_got_workers(update, ctx):
    uid = update.effective_user.id
    max_w = PLAN_WORKERS.get(get_user_plan(uid)["plan"], 100)
    text = update.message.text.strip()
    if not text.isdigit() or not (1 <= int(text) <= max_w):
        await update.message.reply_text(
            f"❌ Send a number between 1 and {max_w}:",
            parse_mode=ParseMode.HTML,
        )
        return ASK_WORKERS
    ctx.user_data["boost"]["workers"] = int(text)

    total = count_devices()
    status = can_user_boost(uid)
    await update.message.reply_text(
        f"🎯 <b>Final Step</b>\n\n"
        f"How many successful boosts?\n"
        f"Max available: <b>{total:,}</b>\n"
        f"Your remaining today: <b>{status['remaining']}</b>\n\n"
        f"Send a number or <code>max</code>.",
        parse_mode=ParseMode.HTML,
    )
    return ASK_TARGET

@track_user
async def boost_got_target(update, ctx):
    uid = update.effective_user.id
    text = update.message.text.strip().lower()
    total = count_devices()
    status = can_user_boost(uid)
    max_allowed = min(total, status["remaining"]) if status["limit"] < 999999 else total

    if text == "max":
        target_success = max_allowed
    elif text.isdigit():
        target_success = min(int(text), max_allowed)
    else:
        await update.message.reply_text("❌ Send a number or 'max':")
        return ASK_TARGET

    if target_success < 1:
        await update.message.reply_text("❌ Target must be at least 1.")
        return ASK_TARGET

    b = ctx.user_data["boost"]
    b["target_success"] = target_success
    feats = ", ".join(FEATURE_LABELS[f] for f in b["features"])

    await update.message.reply_text(
        f"📋 <b>CONFIRM BOOST</b>\n\n"
        f"🎯 Target ID: <code>{b['role_id']}</code>\n"
        f"🌍 Zone: <code>{b['zone_id']}</code>\n"
        f"⚡ Features: <b>{feats}</b>\n"
        f"💬 Comments: <b>{len(b['comments'])}</b>\n"
        f"🔧 Workers: <b>{b['workers']}</b>\n"
        f"🎯 Target Success: <b>{target_success:,}</b>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 LAUNCH BOOST", callback_data="boost_launch")],
            [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")],
        ]),
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END

@track_user
async def cb_boost_launch(update, ctx):
    q = update.callback_query
    await q.answer("🚀 Launching...")

    b = ctx.user_data.get("boost")
    if not b:
        await q.edit_message_text("⚠️ No boost config.", reply_markup=back_kb())
        return

    uid = update.effective_user.id
    await q.edit_message_text("🔍 Fetching target info...", parse_mode=ParseMode.HTML)

    probe_dev, _, _ = await asyncio.to_thread(get_valid_session_device, 6)
    info = await asyncio.to_thread(check_target_standalone, b["role_id"], probe_dev, b["zone_id"])
    if info.get("success"):
        zone = info.get("target_zone", b["zone_id"])
        nick = info.get("nickname", f"Player_{b['role_id']}")
    else:
        zone = b["zone_id"]
        nick = f"Player_{b['role_id']}"

    album_res = await asyncio.to_thread(get_album_photos, b["role_id"], probe_dev, zone)
    album_slots = album_res.get("album_slots", {})
    active_photos = [(s, album_slots[s]["photo_id"]) for s in album_slots]

    features = {k: (k in b["features"]) for k in
                ["like_prof", "like_alb", "view_alb", "comment", "follow", "like_coll"]}
    pool = load_device_ids(limit=10000)

    progress_msg = await q.edit_message_text(
        f"🚀 <b>BOOST RUNNING</b>\n\n"
        f"🎯 {nick} (<code>{b['role_id']}</code>)\n"
        f"🌍 Zone: {zone} | 🔧 Workers: {b['workers']}\n"
        f"📸 Album photos: {len(active_photos)}\n\n"
        f"<i>Starting...</i>",
        parse_mode=ParseMode.HTML,
    )

    stop_event = threading.Event()
    stats_holder = {}
    last_update = {"t": 0.0}
    recent_lines = []
    loop = asyncio.get_running_loop()

    def progress_cb(ev):
        with BOOST_LOCK:
            line = ev.get("line", "")
            line = re.sub(r"\x1b\[[0-9;]*m", "", line)
            recent_lines.append(line)
            if len(recent_lines) > 5:
                recent_lines.pop(0)

            now = time.time()
            if now - last_update["t"] < 1.5:
                return
            last_update["t"] = now

            text = (
                f"🚀 <b>BOOST RUNNING</b>\n\n"
                f"🎯 {nick} (<code>{b['role_id']}</code>)\n"
                f"🌍 Zone: {zone} | 🔧 Workers: {b['workers']}\n\n"
                f"✅ Success: <b>{stats_holder.get('success', 0)}</b> / {b['target_success']}\n"
                f"❌ Fail: <b>{stats_holder.get('fail', 0)}</b>\n"
                f"🚫 Banned: <b>{stats_holder.get('banned_removed', 0)}</b>\n"
                f"📊 Total: <b>{stats_holder.get('total_acc', 0)}</b>\n\n"
                f"<b>Recent:</b>\n<code>" +
                "\n".join(recent_lines[-4:]) +
                "</code>"
            )
            asyncio.run_coroutine_threadsafe(safe_edit(progress_msg, text), loop)

    def run_thread():
        try:
            res = run_boost(
                device_pool=pool,
                target_id=b["role_id"],
                target_zone=zone,
                target_nick=nick,
                features=features,
                comments=b["comments"],
                workers=b["workers"],
                target_success=b["target_success"],
                loop_mode=False,
                auto_clean=True,
                album_slots=active_photos,
                progress_cb=progress_cb,
                stop_event=stop_event,
                live_stats=stats_holder,
            )
            log_boost(b["role_id"], zone, nick,
                      res["success"], res["fail"],
                      res["banned_removed"], int(res["duration"]),
                      user_id=uid)
            increment_usage(uid, success=res["success"])

            final = (
                f"✅ <b>BOOST FINISHED</b>\n\n"
                f"🎯 {nick} (<code>{b['role_id']}</code>)\n\n"
                f"✅ Success: <b>{res['success']:,}</b>\n"
                f"❌ Fail: <b>{res['fail']:,}</b>\n"
                f"🚫 Banned: <b>{res['banned_removed']:,}</b>\n"
                f"⏱ Duration: <b>{res['duration']:.1f}s</b>\n"
                f"⚡ Speed: <b>{res['speed']:.1f}</b> acc/s"
            )
            asyncio.run_coroutine_threadsafe(safe_edit(progress_msg, final), loop)
        except Exception as e:
            log.exception("Boost error")
            asyncio.run_coroutine_threadsafe(
                safe_edit(progress_msg, f"❌ <b>Error:</b>\n<code>{e}</code>"), loop
            )
        finally:
            with BOOST_LOCK:
                ACTIVE_BOOSTS.pop(uid, None)

    with BOOST_LOCK:
        ACTIVE_BOOSTS[uid] = {
            "target": b["role_id"],
            "stats": stats_holder,
            "stop": stop_event,
        }

    th = threading.Thread(target=run_thread, daemon=True, name=f"boost-{uid}")
    th.start()

async def safe_edit(msg, text):
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        pass

@track_user
async def cmd_stop(update, ctx):
    uid = update.effective_user.id
    with BOOST_LOCK:
        info = ACTIVE_BOOSTS.get(uid)
    if info:
        info["stop"].set()
        await update.message.reply_text("🛑 Stopping boost...")
    else:
        await update.message.reply_text("ℹ️ No active boost.")

@track_user
async def cmd_cancel(update, ctx):
    ctx.user_data.pop("boost", None)
    await update.message.reply_text(
        "❌ Cancelled.",
        reply_markup=back_kb(),
    )
    return ConversationHandler.END

async def post_init(app):
    await app.bot.set_my_commands([
        BotCommand("start", "🏠 Main menu"),
        BotCommand("myvip", "💎 My VIP status"),
        BotCommand("givevip", "👑 [Admin] Give VIP"),
        BotCommand("revokevip", "🚫 [Admin] Revoke VIP"),
        BotCommand("listvip", "📋 [Admin] List VIPs"),
        BotCommand("userinfo", "ℹ️ [Admin] User info"),
        BotCommand("stop", "🛑 Stop boost"),
        BotCommand("cancel", "❌ Cancel"),
    ])

def main():
    init_db()
    log.info(f"Starting {BRAND_NAME} Bot — Owner: {OWNER_NAME}")
    log.info(f"Admins: {ADMIN_IDS}")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_menu_boost, pattern="^menu_boost$")],
        states={
            ASK_ROLE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, boost_got_role)],
            ASK_ZONE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, boost_got_zone)],
            ASK_FEATURES: [
                CallbackQueryHandler(cb_feature_select, pattern="^feat_[1-6]$|^feat_all$"),
                CallbackQueryHandler(cb_feat_done, pattern="^feat_done$"),
            ],
            ASK_COMMENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, boost_got_comments)],
            ASK_WORKERS:  [MessageHandler(filters.TEXT & ~filters.COMMAND, boost_got_workers)],
            ASK_TARGET:   [MessageHandler(filters.TEXT & ~filters.COMMAND, boost_got_target)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("myvip", cmd_myvip))
    app.add_handler(CommandHandler("givevip", cmd_givevip))
    app.add_handler(CommandHandler("revokevip", cmd_revokevip))
    app.add_handler(CommandHandler("listvip", cmd_listvip))
    app.add_handler(CommandHandler("userinfo", cmd_userinfo))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    app.add_handler(conv)

    app.add_handler(CallbackQueryHandler(cb_main_menu, pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(cb_close, pattern="^menu_close$"))
    app.add_handler(CallbackQueryHandler(cb_myvip, pattern="^myvip$"))
    app.add_handler(CallbackQueryHandler(cb_admin_panel, pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(cb_menu_devices, pattern="^menu_devices$"))
    app.add_handler(CallbackQueryHandler(cb_dev_upload, pattern="^dev_upload$"))
    app.add_handler(CallbackQueryHandler(cb_dev_sample, pattern="^dev_sample$"))
    app.add_handler(CallbackQueryHandler(cb_dev_clear, pattern="^dev_clear$"))
    app.add_handler(CallbackQueryHandler(cb_dev_clear_confirm, pattern="^dev_clear_confirm$"))
    app.add_handler(CallbackQueryHandler(cb_menu_stats, pattern="^menu_stats$"))
    app.add_handler(CallbackQueryHandler(cb_menu_history, pattern="^menu_history$"))
    app.add_handler(CallbackQueryHandler(cb_boost_launch, pattern="^boost_launch$"))

    app.add_handler(MessageHandler(filters.Document.TXT, handle_document))

    log.info("🤖 Bot polling started...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bot stopped.")
