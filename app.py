# backend/app.py
from __future__ import annotations
from flask import Flask, jsonify, request
from flask_cors import CORS

import os
import time
import threading
import json
import sqlite3
import secrets
import requests
import random
import math
from typing import Optional, Dict, Any

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from market_data import get_pair_data
from watchlist import get_watchlist
from safety import evaluate_safety
import grid_sim

# --- Grid simulator adapter (supports grid_sim exposing functions OR class methods) ---
import inspect as _inspect

def _grid_build(cfg):
    # Try function-style first
    if hasattr(grid_sim, "build_grid") and callable(getattr(grid_sim, "build_grid")):
        try:
            return grid_sim.build_grid(cfg)
        except TypeError as e:
            # If build_grid is an unbound method (expects self,cfg), fall through to class scan
            msg = str(e)
            if "positional argument" not in msg or "cfg" not in msg:
                raise
    # Class-scan fallback
    for _name, _cls in _inspect.getmembers(grid_sim, _inspect.isclass):
        if hasattr(_cls, "build_grid") and callable(getattr(_cls, "build_grid")):
            try:
                _inst = _cls()
                return _inst._grid_build(cfg)
            except TypeError:
                continue
    raise RuntimeError("grid_sim: could not find callable _grid_build(cfg)")

def _grid_step(state, cfg, snapshot):
    if hasattr(grid_sim, "step_sim") and callable(getattr(grid_sim, "step_sim")):
        try:
            return grid_sim._grid_step(state, cfg, snapshot)
        except TypeError as e:
            msg = str(e)
            if "positional argument" not in msg:
                raise
    for _name, _cls in _inspect.getmembers(grid_sim, _inspect.isclass):
        if hasattr(_cls, "step_sim") and callable(getattr(_cls, "step_sim")):
            try:
                _inst = _cls()
                return _inst._grid_step(state, cfg, snapshot)
            except TypeError:
                continue
    raise RuntimeError("grid_sim: could not find callable _grid_step(state,cfg,snapshot)")

# Expose GridConfig regardless of how grid_sim defines it
GridConfig = getattr(grid_sim, "GridConfig", None)
if GridConfig is None:
    raise ImportError("grid_sim does not export GridConfig")


# -------------------------
# App init
# -------------------------
app = Flask(__name__)

"""CORS

Frontend (Vite) calls the backend from http://localhost:5173 and uses
fetch(..., { credentials: 'include' }).

That requires:
  - Access-Control-Allow-Origin must NOT be '*'
  - Access-Control-Allow-Credentials must be 'true'
  - Preflight (OPTIONS) must include the same headers
"""

# IMPORTANT: when supports_credentials=True, origins cannot be '*'
FRONTEND_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://nexus-analyt-ui.onrender.com",
]

CORS(
    app,
    resources={r"/api/*": {"origins": FRONTEND_ORIGINS}},
    supports_credentials=True,
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type"],
    max_age=86400,
)

from flask import make_response

@app.before_request
def _handle_options_preflight():
    """Ensure preflight requests always get the CORS + credentials headers."""
    if request.method != "OPTIONS":
        return None

    origin = request.headers.get("Origin", "")
    resp = make_response("", 204)
    if origin in FRONTEND_ORIGINS:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Vary"] = "Origin"
    return resp

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "service": "Nexus-Analyt backend",
        "hint": "Try /api/health or /api/watchlist"
    })

@app.route("/api/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok"})


# -------------------------
# CoinGecko proxy (avoid browser CORS + basic throttling)
# -------------------------
_CG_CACHE: dict[str, tuple[float, dict]] = {}
_CG_TTL_SEC = int(os.getenv("COINGECKO_CACHE_TTL_SEC", "20"))

def _cg_get(url: str) -> dict:
    now = time.time()
    hit = _CG_CACHE.get(url)
    if hit and (now - hit[0]) < _CG_TTL_SEC:
        return hit[1]
    headers = {"User-Agent": "NexusAnalyt/1.0 (+Render/Flask)"}
    r = requests.get(url, headers=headers, timeout=12)
    r.raise_for_status()
    data = r.json()
    _CG_CACHE[url] = (now, data)
    return data

@app.route("/api/coingecko/simple_price", methods=["GET"])
def coingecko_simple_price():
    # Pass-through query params (ids, vs_currencies, include_* etc.)
    qs = request.query_string.decode("utf-8", errors="ignore")
    url = "https://api.coingecko.com/api/v3/simple/price"
    if qs:
        url = f"{url}?{qs}"
    try:
        data = _cg_get(url)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "coingecko_proxy_failed", "detail": str(e)}), 502

@app.route("/api/coingecko/token_price/<platform>", methods=["GET"])
def coingecko_token_price(platform: str):
    qs = request.query_string.decode("utf-8", errors="ignore")
    url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
    if qs:
        url = f"{url}?{qs}"
    try:
        data = _cg_get(url)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "coingecko_proxy_failed", "detail": str(e)}), 502

# Flask secret key for signing tokens (set FLASK_SECRET_KEY in env for production)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)
_serializer = URLSafeTimedSerializer(app.secret_key)

# Demo/simulation starting capital per asset (USD)
INITIAL_CAPITAL_USD = float(os.getenv("NEXUS_INITIAL_CAPITAL_USD", "5000"))

# -------------------------
# Helpers
# -------------------------
def now_ts() -> int:
    return int(time.time())

def _sim_seed(item_id: str) -> int:
    """Deterministic seed per item_id so simulations are stable across restarts."""
    s = (str(item_id) if item_id is not None else "").strip()
    # simple stable hash (FNV-1a like) to avoid Python's randomized hash()
    h = 2166136261
    for ch in s:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return int(h or 1)

def ok(payload=None):
    base = {"status": "ok", "ts": now_ts()}
    if payload:
        base.update(payload)
    return jsonify(base)

def err(msg, code=400):
    return jsonify({"status": "error", "error": str(msg), "ts": now_ts()}), code


# -------------------------
# Grid persistence (JSON) + limits
# -------------------------
GRID_STATE_PATH = os.getenv('NEXUS_GRID_STATE_PATH', os.path.join(os.path.dirname(__file__), 'grid_state.json'))
GRID_MAX_HISTORY = int(os.getenv('NEXUS_GRID_MAX_HISTORY', '500'))
_GRID_PERSIST_LOCK = threading.Lock()

def _grid_state_load() -> dict:
    if not os.path.exists(GRID_STATE_PATH):
        return {}
    try:
        with open(GRID_STATE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _grid_state_save(state: dict) -> None:
    try:
        tmp = GRID_STATE_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, GRID_STATE_PATH)
    except Exception as e:
        # best-effort persistence; never crash the API because of IO
        print('[WARN] grid_state_save failed:', e)

def _trim_grid_session(sess: dict) -> dict:
    """Keep OPEN orders always; cap non-OPEN order history + fills to GRID_MAX_HISTORY."""
    if not isinstance(sess, dict):
        return sess
    orders = sess.get('orders') if isinstance(sess.get('orders'), list) else []
    open_orders = [o for o in orders if isinstance(o, dict) and o.get('status') == 'OPEN']
    closed_orders = [o for o in orders if isinstance(o, dict) and o.get('status') != 'OPEN']
    # keep newest closed orders
    if len(closed_orders) > GRID_MAX_HISTORY:
        closed_orders = closed_orders[-GRID_MAX_HISTORY:]
    sess['orders'] = open_orders + closed_orders
    fills = sess.get('fills') if isinstance(sess.get('fills'), list) else []
    if len(fills) > GRID_MAX_HISTORY:
        sess['fills'] = fills[-GRID_MAX_HISTORY:]
    return sess


def _persist_grid_state() -> None:
    with _GRID_PERSIST_LOCK:
        _grid_state_save({'GRID_SESSIONS': GRID_SESSIONS, 'GRID_CONFIGS': GRID_CONFIGS})
# -------------------------
# Persistence (SQLite) + Token utilities
# -------------------------
DB_PATH = os.getenv("NEXUS_DB_PATH", os.path.join(os.path.dirname(__file__), "nexus.db"))
TOKEN_TTL_SEC = int(os.getenv("NEXUS_TOKEN_TTL_SEC", "604800"))  # 7 days

def _db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT UNIQUE,
            created_ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS auth_nonces (
            wallet_address TEXT PRIMARY KEY,
            nonce TEXT,
            expires_ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS policies (
            wallet_address TEXT PRIMARY KEY,
            policy_json TEXT,
            updated_ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS intents (
            id TEXT PRIMARY KEY,
            wallet_address TEXT,
            chain_id INTEGER,
            pair TEXT,
            side TEXT,
            amount TEXT,
            max_slippage_bps INTEGER,
            deadline_ts INTEGER,
            allowed_contracts_json TEXT,
            status TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_payments (
            tx_hash TEXT PRIMARY KEY,
            wallet_address TEXT,
            chain_id INTEGER,
            token TEXT,
            amount_units INTEGER,
            plan TEXT,
            created_ts INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    # AI memory schema migration + index (avoid 'ts' mismatch)
    try:
        cur.execute("PRAGMA table_info(ai_memory)")
        cols = {row[1] for row in cur.fetchall()}
        if "updated_ts" not in cols:
            cur.execute("ALTER TABLE ai_memory ADD COLUMN updated_ts INTEGER")
            cur.execute("UPDATE ai_memory SET updated_ts = COALESCE(updated_ts, strftime('%s','now'))")
        # Index for fast per-wallet history lookup
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_memory_wallet_updated ON ai_memory(wallet_address, updated_ts)")
    except Exception:
        # If migration fails (very old sqlite / locked db), keep running without the index.
        pass

    # Access (plan/status) + Unlimited codes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_state (
            wallet_address TEXT PRIMARY KEY,
            plan TEXT,
            source TEXT,
            expires_ts INTEGER,
            chains_allowed_json TEXT,
            ai_limit INTEGER,
            can_open_new_trades INTEGER,
            updated_ts INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS access_codes (
            code TEXT PRIMARY KEY,
            redeemed_by TEXT,
            redeemed_ts INTEGER
        )
    """)


    # NFT activations (non-burn, 2-month lock enforced by backend)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nft_activations (
            wallet_address TEXT,
            tier TEXT,
            contract_address TEXT,
            chain_id INTEGER,
            activated_ts INTEGER,
            expires_ts INTEGER,
            PRIMARY KEY(wallet_address, tier)
        )
    """)


    conn.commit()
    conn.close()



def _ai_mem_get(wallet_address: str):
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return []
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT memory_json FROM ai_memory WHERE wallet_address = ?", (wa,))
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return []
    try:
        mem = json.loads(row[0])
        if isinstance(mem, list):
            out = []
            for m in mem:
                if isinstance(m, dict) and isinstance(m.get("role"), str) and isinstance(m.get("content"), str):
                    out.append({"role": m["role"], "content": m["content"]})
            return out
    except Exception:
        return []
    return []


def _ai_mem_put(wallet_address: str, mem_list):
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return
    try:
        s = json.dumps(mem_list, ensure_ascii=False)
    except Exception:
        s = "[]"
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ai_memory(wallet_address, memory_json, updated_ts) VALUES (?, ?, ?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET memory_json=excluded.memory_json, updated_ts=excluded.updated_ts",
        (wa, s, now_ts()),
    )
    conn.commit()
    conn.close()


def _ai_mem_append(wallet_address: str, user_text: str, assistant_text: str, max_msgs: int = 10):
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return
    mem = _ai_mem_get(wa)
    if user_text:
        mem.append({"role": "user", "content": str(user_text)})
    if assistant_text:
        mem.append({"role": "assistant", "content": str(assistant_text)})
    mem = mem[-max_msgs:]
    _ai_mem_put(wa, mem)

def _norm_addr(addr: str) -> str:
    return (addr or "").strip().lower()

def _require_auth() -> Optional[str]:
    auth = request.headers.get("Authorization", "").strip()
    if not auth.lower().startswith("bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    try:
        data = _serializer.loads(token, max_age=TOKEN_TTL_SEC)
        return _norm_addr(data.get("wallet_address", ""))
    except (BadSignature, SignatureExpired):
        return None

def issue_token(wallet_address: str) -> str:
    return _serializer.dumps({"wallet_address": _norm_addr(wallet_address)})

def upsert_user(wallet_address: str):
    wa = _norm_addr(wallet_address)
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO users(wallet_address, created_ts) VALUES (?, ?)",
        (wa, now_ts()),
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    conn.commit()
    conn.close()

def get_policy(wallet_address: str) -> dict:
    wa = _norm_addr(wallet_address)
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT policy_json FROM policies WHERE wallet_address=?", (wa,))
    row = cur.fetchone()
    conn.close()

    if row and row["policy_json"]:
        try:
            return json.loads(row["policy_json"])
        except Exception:
            pass

    # defaults
    return {
        "max_exposure_usd": 250,
        "max_order_usd": 50,
        "max_slippage_bps": 75,
        "daily_loss_limit_usd": 50,
        "allowed_pairs": [],
        "allowed_contracts": [],
        "kill_switch": False,
        # Manual trading permission gate (must be enabled before grid/actions are allowed)
        "trading_enabled": False,
        # Preference used by /api/trading/suitability (informational only)
        "trading_profile": "conservative",
    }

def set_policy(wallet_address: str, policy: dict):
    wa = _norm_addr(wallet_address)
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO policies(wallet_address, policy_json, updated_ts) VALUES (?, ?, ?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET policy_json=excluded.policy_json, updated_ts=excluded.updated_ts",
        (wa, json.dumps(policy, ensure_ascii=False), now_ts()),
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    conn.commit()
    conn.close()


# -------------------------
# -------------------------
# Access (central gate)
# -------------------------
# Minimal implementation per "Nexus Analyt – Gesamt-Konzept & Umsetzungsreihenfolge".
# Source of truth:
#   GET  /api/access/status
# Codes:
#   POST /api/access/redeem   { code }
#
# NOTE: NFT / subscription integrations are intentionally stubbed; they can be added later
# without changing the response schema.

# Chains (features) unlocked by access
_CHAINS_SILVER = ["ETH", "BNB", "POL"]
_CHAINS_GOLD = [
    "ETH", "BNB", "POL",
    "BASE", "ARBITRUM", "OPTIMISM", "AVALANCHE",
]

# For now we model AI limit as an integer per day (free=1). Unlimited = -1.
_AI_LIMIT_FREE = 1
_AI_LIMIT_UNLIMITED = -1

# Pre-generated 50 one-time unlimited access codes (redeemable once each)
REDEEM_CODES = [
    "NEXUS-8RDA-HSJT",
    "NEXUS-PHV9-IOXF",
    "NEXUS-LY8S-OZA5",
    "NEXUS-02TA-DMNN",
    "NEXUS-UJKT-JFPR",
    "NEXUS-YK5N-CIS1",
    "NEXUS-W57X-FUWZ",
    "NEXUS-ERC9-FPVW",
    "NEXUS-2IGX-7Z7O",
    "NEXUS-FU0W-82Y5",
    "NEXUS-IL6T-F53Y",
    "NEXUS-6K8S-15WP",
    "NEXUS-6UR4-OJK2",
    "NEXUS-IJLG-O6OI",
    "NEXUS-8DWI-5F89",
    "NEXUS-40H9-NJKO",
    "NEXUS-83S3-J7T6",
    "NEXUS-M8UU-VI0Y",
    "NEXUS-HQ7S-3VN6",
    "NEXUS-M799-XM8I",
    "NEXUS-MGFP-YQD8",
    "NEXUS-PF3W-PUXE",
    "NEXUS-4SVG-OFZP",
    "NEXUS-JMSC-4UC4",
    "NEXUS-HBID-B5AA",
    "NEXUS-7RVE-JKBW",
    "NEXUS-J7OV-A5QC",
    "NEXUS-9A1J-3TVY",
    "NEXUS-UCP7-RWQG",
    "NEXUS-8IXK-H0O7",
    "NEXUS-I8SX-306T",
    "NEXUS-GKD6-LFGX",
    "NEXUS-6UDM-O1S4",
    "NEXUS-7YOV-KGOI",
    "NEXUS-D9EB-EQ8X",
    "NEXUS-4DRW-KDOT",
    "NEXUS-34PE-BIVP",
    "NEXUS-4KEI-CTU8",
    "NEXUS-VNGV-6L78",
    "NEXUS-L0XS-QOIG",
    "NEXUS-7Y7L-LRPX",
    "NEXUS-NZZL-JTCZ",
    "NEXUS-KZY9-J66P",
    "NEXUS-KCMC-GMHH",
    "NEXUS-DS5F-P60V",
    "NEXUS-LTJB-4WYD",
    "NEXUS-S24Y-VF4V",
    "NEXUS-ISGU-RJP9",
    "NEXUS-QL6S-D1J7",
    "NEXUS-NTE9-8KN0"
]

# -------------------------
# Onchain subscription payments (USDC/USDT -> Treasury)
# -------------------------
TREASURY_ADDRESS = (os.getenv("NEXUS_TREASURY_ADDRESS") or "").strip().lower()

# Supported chains for onchain verify (extend as needed)
_RPC_URL_BY_CHAIN = {
    1: os.getenv("RPC_URL_ETH") or os.getenv("RPC_URL_1"),
    56: os.getenv("RPC_URL_BNB") or os.getenv("RPC_URL_56"),
    137: os.getenv("RPC_URL_POL") or os.getenv("RPC_URL_POLYGON") or os.getenv("RPC_URL_137"),
}

_USDC_BY_CHAIN = {
    1: os.getenv("USDC_ADDRESS_ETH") or os.getenv("USDC_ADDRESS_1"),
    56: os.getenv("USDC_ADDRESS_BNB") or os.getenv("USDC_ADDRESS_56"),
    137: os.getenv("USDC_ADDRESS_POL") or os.getenv("USDC_ADDRESS_POLYGON") or os.getenv("USDC_ADDRESS_137"),
}

_USDT_BY_CHAIN = {
    1: os.getenv("USDT_ADDRESS_ETH") or os.getenv("USDT_ADDRESS_1"),
    56: os.getenv("USDT_ADDRESS_BNB") or os.getenv("USDT_ADDRESS_56"),
    137: os.getenv("USDT_ADDRESS_POL") or os.getenv("USDT_ADDRESS_POLYGON") or os.getenv("USDT_ADDRESS_137"),
}

_USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))
_USDT_DECIMALS = int(os.getenv("USDT_DECIMALS", "6"))

PRICE_SILVER_USD = float(os.getenv("PRICE_SILVER_USD", "10"))
PRICE_GOLD_USD = float(os.getenv("PRICE_GOLD_USD", "25"))

# keccak256("Transfer(address,address,uint256)")
ERC20_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


# NFT contracts (provided by owner)
NFT_SILVER_CONTRACT = (_norm_addr(os.getenv("NEXUS_NFT_SILVER_CONTRACT", "0x4BD84783E7427Db4E4b10107073DF7C50e14dF9F")) or "").lower()
NFT_GOLD_CONTRACT   = (_norm_addr(os.getenv("NEXUS_NFT_GOLD_CONTRACT",   "0xEd31fF81056fB5B1195F5D965Eb5561A66f2C699")) or "").lower()

# Chain IDs where the NFT contracts live (default: Polygon 137). Override via ENV if needed.
NFT_SILVER_CHAIN_ID = int(os.getenv("NEXUS_NFT_SILVER_CHAIN_ID", "137"))
NFT_GOLD_CHAIN_ID   = int(os.getenv("NEXUS_NFT_GOLD_CHAIN_ID",   "137"))

# Optional ERC1155 token ids (if your contracts are ERC1155). If empty, we use ERC721 balanceOf(address).
NFT_SILVER_TOKEN_ID = os.getenv("NEXUS_NFT_SILVER_TOKEN_ID", "").strip()
NFT_GOLD_TOKEN_ID   = os.getenv("NEXUS_NFT_GOLD_TOKEN_ID", "").strip()

# Activation lock duration (~2 months). Keep it simple: 60 days.
NFT_LOCK_SECONDS = int(os.getenv("NEXUS_NFT_LOCK_SECONDS", str(60*24*3600)))

# Function selectors
# balanceOf(address) for ERC721/ERC20: 0x70a08231
ERC721_BALANCEOF_SELECTOR = "0x70a08231"
# balanceOf(address,uint256) for ERC1155: 0x00fdd58e
ERC1155_BALANCEOF_SELECTOR = "0x00fdd58e"


def _pad32(hex_no0x: str) -> str:
    return hex_no0x.rjust(64, "0")


def _addr_to_32(addr: str) -> str:
    a = (addr or "").lower()
    if a.startswith("0x"):
        a = a[2:]
    return _pad32(a)


def _int_to_32(i: int) -> str:
    return _pad32(hex(int(i))[2:])


def _eth_call(chain_id: int, to_addr: str, data: str) -> str:
    return _rpc_call(int(chain_id), "eth_call", [{"to": to_addr, "data": data}, "latest"])


def _try_parse_int(v) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    s = str(v)
    if s.startswith("0x"):
        return _hex_to_int(s)
    try:
        return int(s)
    except Exception:
        return 0


def _nft_balance_erc721(chain_id: int, contract: str, owner: str) -> int:
    data = ERC721_BALANCEOF_SELECTOR + _addr_to_32(owner)
    res = _eth_call(chain_id, contract, data)
    return _hex_to_int(res or "0x0")


def _nft_balance_erc1155(chain_id: int, contract: str, owner: str, token_id: int) -> int:
    data = ERC1155_BALANCEOF_SELECTOR + _addr_to_32(owner) + _int_to_32(token_id)
    res = _eth_call(chain_id, contract, data)
    return _hex_to_int(res or "0x0")


def _owns_nft(wallet_address: str, tier: str) -> bool:
    wa = _norm_addr(wallet_address)
    if not wa:
        return False

    tier_l = (tier or "").strip().lower()
    if tier_l == "silver":
        contract = NFT_SILVER_CONTRACT
        chain_id = NFT_SILVER_CHAIN_ID
        token_id_raw = NFT_SILVER_TOKEN_ID
    elif tier_l == "gold":
        contract = NFT_GOLD_CONTRACT
        chain_id = NFT_GOLD_CHAIN_ID
        token_id_raw = NFT_GOLD_TOKEN_ID
    else:
        return False

    if not contract:
        return False

    # Try ERC1155 if token id is provided, else ERC721.
    try:
        if token_id_raw:
            bal = _nft_balance_erc1155(chain_id, contract, wa, int(token_id_raw))
        else:
            bal = _nft_balance_erc721(chain_id, contract, wa)
        return int(bal) > 0
    except Exception:
        return False


def _nft_activation_get(wallet_address: str, tier: str):
    wa = _norm_addr(wallet_address)
    if not wa:
        return None
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT wallet_address, tier, contract_address, chain_id, activated_ts, expires_ts FROM nft_activations WHERE wallet_address=? AND tier=?", (wa, str(tier).lower()))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "wallet_address": row[0],
        "tier": row[1],
        "contract_address": row[2],
        "chain_id": row[3],
        "activated_ts": row[4],
        "expires_ts": row[5],
    }


def _nft_activation_put(wallet_address: str, tier: str, contract: str, chain_id: int, activated_ts: int, expires_ts: int):
    wa = _norm_addr(wallet_address)
    if not wa:
        return
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO nft_activations(wallet_address, tier, contract_address, chain_id, activated_ts, expires_ts) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(wallet_address, tier) DO UPDATE SET contract_address=excluded.contract_address, chain_id=excluded.chain_id, activated_ts=excluded.activated_ts, expires_ts=excluded.expires_ts",
        (wa, str(tier).lower(), str(contract).lower(), int(chain_id), int(activated_ts), int(expires_ts)),
    )
    conn.commit()
    conn.close()

def _rpc_call(chain_id: int, method: str, params: list):
    url = _RPC_URL_BY_CHAIN.get(int(chain_id) or 0)
    if not url:
        raise RuntimeError(f"rpc url not configured for chain_id={chain_id}")
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()
    j = r.json() or {}
    if j.get("error"):
        raise RuntimeError(str(j.get("error")))
    return j.get("result")

def _topic_to_addr(topic_hex: str) -> str:
    # topic is 32-byte hex: 0x000.. + 20-byte address
    t = (topic_hex or "").lower()
    if t.startswith("0x"):
        t = t[2:]
    if len(t) != 64:
        return ""
    return ("0x" + t[-40:]).lower()

def _hex_to_int(h: str) -> int:
    try:
        return int(h, 16)
    except Exception:
        return 0

def _verify_erc20_payment(chain_id: int, tx_hash: str, payer: str, plan: str):
    if not TREASURY_ADDRESS:
        raise RuntimeError("missing NEXUS_TREASURY_ADDRESS")

    plan_l = (plan or "").strip().lower()
    if plan_l not in ("silver", "gold"):
        raise RuntimeError("plan must be 'silver' or 'gold'")

    price = PRICE_SILVER_USD if plan_l == "silver" else PRICE_GOLD_USD

    txh = (tx_hash or "").strip().lower()
    if not txh.startswith("0x") or len(txh) < 20:
        raise RuntimeError("invalid tx_hash")

    payer = _norm_addr(payer)

    rcpt = _rpc_call(int(chain_id), "eth_getTransactionReceipt", [txh])
    if not rcpt:
        raise RuntimeError("tx not found")

    status_hex = rcpt.get("status")
    if status_hex is not None and _hex_to_int(status_hex) != 1:
        raise RuntimeError("tx failed")

    logs = rcpt.get("logs") or []

    # accept either USDC or USDT on that chain
    usdc = (_USDC_BY_CHAIN.get(int(chain_id)) or "").strip().lower()
    usdt = (_USDT_BY_CHAIN.get(int(chain_id)) or "").strip().lower()

    candidates = []
    if usdc:
        candidates.append((usdc, _USDC_DECIMALS, "USDC"))
    if usdt:
        candidates.append((usdt, _USDT_DECIMALS, "USDT"))
    if not candidates:
        raise RuntimeError("token addresses not configured for this chain")

    min_units_by_token = {}
    for _addr, _dec, _sym in candidates:
        units = int(round(price * (10 ** int(_dec))))
        min_units_by_token[_addr] = units

    # scan logs for Transfer(from=payer, to=treasury) on accepted token
    for lg in logs:
        try:
            addr = (lg.get("address") or "").strip().lower()
            topics = lg.get("topics") or []
            if not addr or not isinstance(topics, list) or len(topics) < 3:
                continue
            if str(topics[0]).lower() != ERC20_TRANSFER_TOPIC0:
                continue
            if addr not in min_units_by_token:
                continue

            frm = _topic_to_addr(str(topics[1]))
            to = _topic_to_addr(str(topics[2]))
            if frm != payer:
                continue
            if to != TREASURY_ADDRESS:
                continue

            value = _hex_to_int(lg.get("data") or "0x0")
            if value >= int(min_units_by_token[addr]):
                # ok
                sym = "USDC" if addr == usdc else "USDT"
                return {
                    "token": sym,
                    "token_address": addr,
                    "amount_units": int(value),
                    "required_units": int(min_units_by_token[addr]),
                }
        except Exception:
            continue

    raise RuntimeError("no matching USDC/USDT transfer found")

def _access_state_get(wallet_address: str) -> dict | None:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return None
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "SELECT wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades FROM access_state WHERE wallet_address=?",
        (wa,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["chains_allowed"] = json.loads(d.get("chains_allowed_json") or "[]")
    except Exception:
        d["chains_allowed"] = []
    return d


def _access_state_put(wallet_address: str, plan: str, source: str, expires_ts: int | None, chains_allowed: list, ai_limit: int, can_open_new_trades: bool):
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO access_state(wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades, updated_ts) "
        "VALUES (?,?,?,?,?,?,?,?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET plan=excluded.plan, source=excluded.source, expires_ts=excluded.expires_ts, chains_allowed_json=excluded.chains_allowed_json, ai_limit=excluded.ai_limit, can_open_new_trades=excluded.can_open_new_trades, updated_ts=excluded.updated_ts",
        (
            wa,
            str(plan or "free"),
            str(source or "default"),
            int(expires_ts) if expires_ts is not None else None,
            json.dumps(chains_allowed or [], ensure_ascii=False),
            int(ai_limit),
            1 if bool(can_open_new_trades) else 0,
            now_ts(),
        ),
    )
    conn.commit()
    conn.close()


def _access_defaults() -> dict:
    return {
        "plan": "free",
        "source": "default",
        "expires_at": None,
        "chains_allowed": [],
        "ai_limit": _AI_LIMIT_FREE,
        "can_open_new_trades": False,
        "can_close_trades": True,
    }


def _is_expired(expires_ts: int | None) -> bool:
    if expires_ts is None:
        return False
    try:
        return int(expires_ts) <= now_ts()
    except Exception:
        return True


def _compute_access_status(wallet_address: str | None) -> dict:
    # unauthenticated -> free
    if not wallet_address:
        return _access_defaults()

    wa = _norm_addr(wallet_address)
    st = _access_state_get(wa)
    if not st:
        # authenticated but no plan assigned yet -> free
        base = _access_defaults()
        base["source"] = "auth"
        return base

    # expired -> free
    exp = st.get("expires_ts")
    if _is_expired(exp):
        base = _access_defaults()
        base["source"] = st.get("source") or "expired"
        base["expires_at"] = int(exp) if exp is not None else None
        return base

    # stored plan
    plan = str(st.get("plan") or "free").lower()
    source = str(st.get("source") or "db")
    chains = st.get("chains_allowed") if isinstance(st.get("chains_allowed"), list) else []
    ai_limit = st.get("ai_limit")
    try:
        ai_limit = int(ai_limit) if ai_limit is not None else _AI_LIMIT_FREE
    except Exception:
        ai_limit = _AI_LIMIT_FREE

    # If access comes from NFT activation, ensure NFT is still owned (no onchain lock possible).
    try:
        if str(st.get("source") or "").lower().startswith("nft_"):
            tier = "gold" if str(st.get("plan") or "").lower() == "gold" else "silver"
            if not _owns_nft(wa, tier):
                base = _access_defaults()
                base["source"] = "nft_not_owned"
                base["expires_at"] = int(exp) if exp is not None else None
                return base
    except Exception:
        pass

    can_open = bool(st.get("can_open_new_trades"))

    return {
        "plan": plan,
        "source": source,
        "expires_at": int(exp) if exp is not None else None,
        "chains_allowed": chains,
        "ai_limit": ai_limit,
        "can_open_new_trades": can_open,
        "can_close_trades": True,
    }


def _require_access_open() -> tuple[str | None, dict | None, tuple | None]:
    """Enforce access for endpoints that OPEN new trades."""
    wa = _require_auth()
    if not wa:
        return None, None, err("unauthorized", 401)

    st = _compute_access_status(wa)
    if not bool(st.get("can_open_new_trades")):
        return wa, st, err("access required (no new trades allowed)", 403)
    return wa, st, None


@app.route("/api/access/status", methods=["GET"])
def api_access_status():
    wa = _require_auth()
    st = _compute_access_status(wa)
    return jsonify({"status": "ok", **st})


def _seed_unlimited_codes_if_needed(cur):
    """Seed access_codes table if empty.

    Priority:
      1) hardcoded REDEEM_CODES (50 one-time codes)
      2) env NEXUS_UNLIMITED_CODES (CSV) as optional override/extension
    """
    try:
        cur.execute("SELECT COUNT(1) as n FROM access_codes")
        row = cur.fetchone()
        n = int(row[0] if row else 0)
    except Exception:
        n = 0

    if n > 0:
        return

    codes = list(REDEEM_CODES or [])

    raw = str(os.getenv("NEXUS_UNLIMITED_CODES", "")).strip()
    if raw:
        for c in raw.split(","):
            c = (c or "").strip()
            if c and c not in codes:
                codes.append(c)

    if not codes:
        return

    for c in codes[:500]:
        try:
            cur.execute("INSERT OR IGNORE INTO access_codes(code, redeemed_by, redeemed_ts) VALUES (?,?,?)", (c, None, None))
        except Exception:
            pass



@app.route("/api/access/redeem", methods=["POST"])
def api_access_redeem():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    code = str(body.get("code") or "").strip()
    if not code:
        return err("missing code", 400)

    conn = _db()
    cur = conn.cursor()

    # best-effort seed (if env provides codes)
    try:
        _seed_unlimited_codes_if_needed(cur)
    except Exception:
        pass

    # validate code exists and not redeemed
    cur.execute("SELECT code, redeemed_by, redeemed_ts FROM access_codes WHERE code=?", (code,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return err("invalid code", 404)

    redeemed_by = (row[1] or "")
    if redeemed_by:
        conn.close()
        return err("code already redeemed", 409)

    # redeem
    cur.execute(
        "UPDATE access_codes SET redeemed_by=?, redeemed_ts=? WHERE code=?",
        (wa, now_ts(), code),
    )

    # set plan = unlimited (lifetime)
    _access_state_put(
        wallet_address=wa,
        plan="unlimited",
        source="code",
        expires_ts=None,
        chains_allowed=_CHAINS_GOLD,  # all currently offered chains
        ai_limit=_AI_LIMIT_UNLIMITED,
        can_open_new_trades=True,
    )

    conn.commit()
    conn.close()

    return jsonify({"status": "ok", "plan": "unlimited", "source": "code", "expires_at": None, "chains_allowed": _CHAINS_GOLD, "ai_limit": _AI_LIMIT_UNLIMITED, "can_open_new_trades": True, "can_close_trades": True})


# Trading permission helpers
# -------------------------
def _require_trading_enabled() -> tuple[Optional[str], Optional[dict], Optional[tuple]]:
    """
    Returns (wallet_address, policy, error_response_tuple_or_None).
    Enforces:
      - valid Bearer token
      - kill_switch == False
      - trading_enabled == True
    """
    wa = _require_auth()
    if not wa:
        return None, None, err("unauthorized", 401)
    policy = get_policy(wa)
    if policy.get("kill_switch"):
        return wa, policy, err("trading disabled (kill_switch)", 403)
    if not bool(policy.get("trading_enabled", False)):
        return wa, policy, err("trading not enabled; set policy.trading_enabled=true", 403)
    return wa, policy, None

def _get_owned_session(item_id: str, wa: str) -> Optional[dict]:
    """Return the grid session if it belongs to wallet `wa`. Legacy sessions without owner are treated as owned."""
    sess = GRID_SESSIONS.get(item_id)
    if not isinstance(sess, dict):
        return None
    owner = _norm_addr(sess.get("wallet_address") or "")
    if not owner or owner == _norm_addr(wa):
        return sess
    return None

def create_intent(
    wallet_address: str,
    chain_id: int,
    pair: str,
    side: str,
    amount: str,
    max_slippage_bps: int,
    deadline_ts: int,
    allowed_contracts: list,
) -> str:
    intent_id = secrets.token_hex(16)
    wa = _norm_addr(wallet_address)

    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO intents(id, wallet_address, chain_id, pair, side, amount, max_slippage_bps, deadline_ts, allowed_contracts_json, status, created_ts, updated_ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            intent_id,
            wa,
            int(chain_id or 0),
            pair,
            side,
            str(amount),
            int(max_slippage_bps or 0),
            int(deadline_ts or 0),
            json.dumps(allowed_contracts or [], ensure_ascii=False),
            "created",
            now_ts(),
            now_ts(),
        ),
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    conn.commit()
    conn.close()
    return intent_id

try:
    init_db()
except Exception as _e:
    print("[WARN] init_db failed:", _e)


# -------------------------
# In-memory state
# -------------------------
SNAPSHOTS: Dict[str, Dict[str, Any]] = {}   # key: item_id -> {"ts":..., "data": {...}}
GRID_SESSIONS: Dict[str, Dict[str, Any]] = {}    # key: item_id -> GridState
GRID_CONFIGS: Dict[str, GridConfig] = {}    # key: item_id -> GridConfig

# Load persisted grid sessions/configs (best-effort)
try:
    _persisted = _grid_state_load()
    if isinstance(_persisted.get('GRID_SESSIONS'), dict):
        GRID_SESSIONS = _persisted.get('GRID_SESSIONS') or {}
    if isinstance(_persisted.get('GRID_CONFIGS'), dict):
        GRID_CONFIGS = _persisted.get('GRID_CONFIGS') or {}
except Exception as _e:
    print('[WARN] grid_state_load failed:', _e)

# -------------------------
# Grid PnL helpers (simulation)
# -------------------------

def _ensure_pnl(sess: dict) -> dict:
    # Position-based PnL simulation (qty units, average cost) + demo equity/ROI
    if not isinstance(sess, dict):
        return {}
    # demo capital basis
    sess.setdefault("initial_capital_usd", INITIAL_CAPITAL_USD)
    # derived fields (kept updated by _pnl_mark)
    sess.setdefault("equity_usd", float(sess.get("initial_capital_usd") or INITIAL_CAPITAL_USD))
    sess.setdefault("pnl_pct", 0.0)

    # position-based pnl
    sess.setdefault("position_qty", 0.0)
    sess.setdefault("avg_cost", 0.0)
    sess.setdefault("realized_pnl", 0.0)
    sess.setdefault("unrealized_pnl", 0.0)
    sess.setdefault("total_pnl", 0.0)
    sess.setdefault("last_price", None)
    return sess

def _pnl_apply_fill(sess: dict, fill: dict, qty: float = 1.0) -> float:
    # Returns pnl_delta for this fill (realized only)
    _ensure_pnl(sess)
    side = (fill.get("side") or "").upper()
    try:
        px = float(fill.get("fill_price") or fill.get("price") or 0.0)
    except Exception:
        px = 0.0
    if px <= 0:
        return 0.0

    pos = float(sess.get("position_qty") or 0.0)
    avg = float(sess.get("avg_cost") or 0.0)
    realized = float(sess.get("realized_pnl") or 0.0)

    pnl_delta = 0.0
    if side == "BUY":
        new_pos = pos + qty
        new_avg = ((pos * avg) + (qty * px)) / new_pos if new_pos > 0 else 0.0
        sess["position_qty"] = new_pos
        sess["avg_cost"] = new_avg
    elif side == "SELL":
        sell_qty = min(qty, pos) if pos > 0 else qty
        pnl_delta = (px - avg) * sell_qty if sell_qty > 0 else 0.0
        sess["realized_pnl"] = realized + pnl_delta
        sess["position_qty"] = max(0.0, pos - sell_qty)
        if sess["position_qty"] <= 0:
            sess["avg_cost"] = 0.0
    return pnl_delta

def _pnl_mark(sess: dict, last_price):
    _ensure_pnl(sess)
    sess["last_price"] = last_price
    pos = float(sess.get("position_qty") or 0.0)
    avg = float(sess.get("avg_cost") or 0.0)
    realized = float(sess.get("realized_pnl") or 0.0)
    try:
        px = float(last_price) if last_price is not None else None
    except Exception:
        px = None
    if px is None or px <= 0:
        sess["unrealized_pnl"] = 0.0
        sess["total_pnl"] = realized
        return
    sess["unrealized_pnl"] = (px - avg) * pos if pos > 0 else 0.0
    sess["total_pnl"] = float(sess.get("realized_pnl") or 0.0) + float(sess.get("unrealized_pnl") or 0.0)

# normalize persisted sessions
try:
    for _it, _sess in (GRID_SESSIONS or {}).items():
        if isinstance(_sess, dict):
            _ensure_pnl(_sess)
except Exception:
    pass


# -------------------------
# Auth (Wallet Sign-In) - Nonce + Verify Signature
# -------------------------
@app.route("/api/auth/nonce", methods=["POST"])
def api_auth_nonce():
    body = request.get_json(silent=True) or {}
    address = _norm_addr(body.get("address") or "")
    if not address:
        return err("missing address", 400)

    nonce = secrets.token_hex(16)
    expires = now_ts() + 10 * 60  # 10 minutes

    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO auth_nonces(wallet_address, nonce, expires_ts) VALUES (?, ?, ?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET nonce=excluded.nonce, expires_ts=excluded.expires_ts",
        (address, nonce, expires),
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    conn.commit()
    conn.close()

    # IMPORTANT: client should sign EXACTLY this message
    message = f"Nexus Analyt login\nAddress: {address}\nNonce: {nonce}"
    return ok({"nonce": nonce, "message": message, "expires_ts": expires})

@app.route("/api/nft/activate", methods=["POST"])
def api_nft_activate():
    """Activate Silver/Gold NFT access for ~2 months (backend lock).

    Body: { tier: "silver"|"gold" }

    Rules:
      - NFT is NOT burned.
      - Activation locks re-activation for the tier until expires_ts.
      - Access gating is still handled via /api/access/status.
    """
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    tier = str(body.get("tier") or "").strip().lower()
    if tier not in ("silver", "gold"):
        return err("tier must be 'silver' or 'gold'", 400)

    # Unlimited has highest priority; do not downgrade.
    st = _compute_access_status(wa)
    if str(st.get("plan") or "").lower() == "unlimited":
        return jsonify({"status": "ok", "note": "already unlimited", "access": st})

    # ownership check
    if not _owns_nft(wa, tier):
        return err("nft not owned", 403)

    now = now_ts()
    act = _nft_activation_get(wa, tier)
    if act and not _is_expired(act.get("expires_ts")):
        # already active and still locked
        st2 = _compute_access_status(wa)
        return jsonify({"status": "ok", "already_active": True, "access": st2, "expires_at": int(act.get("expires_ts") or 0)})

    expires = now + int(NFT_LOCK_SECONDS)

    if tier == "silver":
        contract = NFT_SILVER_CONTRACT
        chain_id = NFT_SILVER_CHAIN_ID
        plan = "silver"
        chains_allowed = _CHAINS_SILVER
        source = "nft_silver"
    else:
        contract = NFT_GOLD_CONTRACT
        chain_id = NFT_GOLD_CHAIN_ID
        plan = "gold"
        chains_allowed = _CHAINS_GOLD
        source = "nft_gold"

    _nft_activation_put(wa, tier, contract, chain_id, now, expires)

    # write to access_state (so /api/access/status stays stable)
    _access_state_put(
        wa,
        plan=plan,
        source=source,
        expires_ts=int(expires),
        chains_allowed=chains_allowed,
        ai_limit=_AI_LIMIT_UNLIMITED,
        can_open_new_trades=True,
    )

    st3 = _compute_access_status(wa)
    return jsonify({"status": "ok", "access": st3})


@app.route("/api/access/subscribe/verify", methods=["POST"])
def api_access_subscribe_verify():
    """Verify an onchain USDC/USDT payment to Treasury and activate Silver/Gold access.

    Body: { chain_id, tx_hash, plan: "silver"|"gold" }

    Notes:
      - expects an ERC20 Transfer from the caller wallet -> TREASURY_ADDRESS
      - idempotent per tx_hash (stored in access_payments)
    """
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    chain_id = body.get("chain_id")
    tx_hash = str(body.get("tx_hash") or "").strip()
    plan = str(body.get("plan") or "").strip().lower()

    try:
        chain_id = int(chain_id)
    except Exception:
        return err("invalid chain_id", 400)

    if plan not in ("silver", "gold"):
        return err("plan must be 'silver' or 'gold'", 400)

    if not tx_hash:
        return err("missing tx_hash", 400)

    conn = _db()
    cur = conn.cursor()

    # prevent double-use of the same tx
    cur.execute("SELECT tx_hash FROM access_payments WHERE tx_hash=?", (tx_hash.lower(),))
    row = cur.fetchone()
    if row:
        conn.close()
        # already verified earlier -> return current status
        st = _compute_access_status(wa)
        return jsonify({"status": "ok", "already_verified": True, "access": st})

    try:
        proof = _verify_erc20_payment(chain_id=chain_id, tx_hash=tx_hash, payer=wa, plan=plan)
    except Exception as e:
        conn.close()
        return err(str(e), 400)

    # record payment
    try:
        cur.execute(
            "INSERT INTO access_payments(tx_hash, wallet_address, chain_id, token, amount_units, plan, created_ts) VALUES (?,?,?,?,?,?,?)",
            (tx_hash.lower(), wa, int(chain_id), str(proof.get("token") or ""), int(proof.get("amount_units") or 0), plan, now_ts()),
        )
    except Exception as e:
        conn.close()
        return err(str(e), 500)

    # activate access (2 months)
    expires_ts = now_ts() + int(60 * 60 * 24 * 60)  # ~60 days
    if plan == "silver":
        chains_allowed = ["ETH", "BNB", "POL"]
        ai_limit = _AI_LIMIT_UNLIMITED
    else:
        # gold: all chains (backend doesn't enforce list beyond UI, but we provide it)
        chains_allowed = ["ALL"]
        ai_limit = _AI_LIMIT_UNLIMITED

    _access_state_put(
        wallet_address=wa,
        plan=plan,
        source=str(proof.get("token") or "payment").lower(),
        expires_ts=expires_ts,
        chains_allowed=chains_allowed,
        ai_limit=ai_limit,
        can_open_new_trades=True,
    )

    conn.commit()
    conn.close()

    st = _compute_access_status(wa)
    return jsonify({"status": "ok", "verified": True, "payment": proof, "access": st})


@app.route("/api/auth/verify", methods=["POST"])
def api_auth_verify():
    body = request.get_json(silent=True) or {}
    address = _norm_addr(body.get("address") or "")
    signature = (body.get("signature") or "").strip()
    message = (body.get("message") or "").strip()
    nonce = (body.get("nonce") or "").strip()

    if not address or not signature or not message:
        return err("missing address, signature, or message", 400)

    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT nonce, expires_ts FROM auth_nonces WHERE wallet_address=?", (address,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return err("nonce not found; request /api/auth/nonce first", 400)

    db_nonce, expires_ts = row["nonce"], row["expires_ts"]
    if now_ts() > int(expires_ts or 0):
        return err("nonce expired; request a new nonce", 400)

    if nonce and nonce != db_nonce:
        return err("nonce mismatch", 400)

    # Ensure message contains the expected nonce (basic safety)
    if db_nonce not in message:
        return err("message does not contain expected nonce", 400)

    try:
        from eth_account.messages import encode_defunct
        from eth_account import Account
    except Exception:
        return err("eth-account not installed. Run: pip install eth-account", 500)

    try:
        recovered = Account.recover_message(encode_defunct(text=message), signature=signature)
        if _norm_addr(recovered) != address:
            return err("signature verification failed", 401)
    except Exception as e:
        return err(f"signature verification error: {e}", 400)

    upsert_user(address)
    token = issue_token(address)
    return ok({"token": token, "wallet_address": address})


# -------------------------
# Policy (Risk limits)
# -------------------------
@app.route("/api/policy", methods=["GET"])
def api_policy_get():
    wa = _require_auth()
    # NOTE: During early UX phases we allow reading a default policy without auth,
    # to avoid the UI spamming 401s before a full Privy<->backend auth bridge exists.
    if not wa:
        return ok({"policy": get_policy(""), "unauthenticated": True})
    return ok({"policy": get_policy(wa), "unauthenticated": False})

@app.route("/api/policy", methods=["POST"])
def api_policy_set():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    policy = body.get("policy") or {}
    if not isinstance(policy, dict):
        return err("policy must be an object", 400)

    cur = get_policy(wa)
    cur.update(policy)
    cur["kill_switch"] = bool(cur.get("kill_switch", False))

    # Normalize extra fields
    if "trading_enabled" in cur:
        cur["trading_enabled"] = bool(cur.get("trading_enabled", False))
    prof = str(cur.get("trading_profile") or "conservative").strip().lower()
    if prof not in ("conservative", "balanced", "volatility"):
        prof = "conservative"
    cur["trading_profile"] = prof

    set_policy(wa, cur)
    return ok({"policy": cur})


# -------------------------
# Trade Intents (Strategy -> Execution)
# -------------------------
@app.route("/api/intents/create", methods=["POST"])
def api_intent_create():
    wa, access, e_access = _require_access_open()
    if e_access:
        return e_access

    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    chain_id = body.get("chain_id") or 1
    pair = (body.get("pair") or "").strip()
    side = (body.get("side") or "").strip().lower()
    amount = body.get("amount")  # keep as string for precision
    max_slippage_bps = body.get("max_slippage_bps")
    deadline_ts = body.get("deadline_ts") or (now_ts() + 10 * 60)
    allowed_contracts = body.get("allowed_contracts") or []

    if not pair or side not in ("buy", "sell"):
        return err("missing pair or invalid side", 400)

    policy = get_policy(wa)
    if policy.get("kill_switch"):
        return err("trading disabled (kill_switch)", 403)

    if max_slippage_bps is None:
        max_slippage_bps = policy.get("max_slippage_bps", 75)

    intent_id = create_intent(
        wallet_address=wa,
        chain_id=int(chain_id),
        pair=pair,
        side=side,
        amount=str(amount),
        max_slippage_bps=int(max_slippage_bps),
        deadline_ts=int(deadline_ts),
        allowed_contracts=list(allowed_contracts),
    )
    return ok({"intent_id": intent_id})

@app.route("/api/intents/<intent_id>", methods=["GET"])
def api_intent_get(intent_id):
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM intents WHERE id=? AND wallet_address=?", (intent_id, wa))
    row = cur.fetchone()
    conn.close()
    if not row:
        return err("not found", 404)

    data = dict(row)
    try:
        data["allowed_contracts"] = json.loads(data.get("allowed_contracts_json") or "[]")
    except Exception:
        data["allowed_contracts"] = []
    data.pop("allowed_contracts_json", None)
    return ok({"intent": data})

@app.route("/api/intents/<intent_id>/submit", methods=["POST"])
def api_intent_submit(intent_id):
    # Stub for later: AA / smart-contract / keeper submission.
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT status FROM intents WHERE id=? AND wallet_address=?", (intent_id, wa))
    row = cur.fetchone()
    if not row:
        conn.close()
        return err("not found", 404)

    cur.execute(
        "UPDATE intents SET status=?, updated_ts=? WHERE id=? AND wallet_address=?",
        ("submitted", now_ts(), intent_id, wa),
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_memory (
            wallet_address TEXT PRIMARY KEY,
            memory_json TEXT,
            updated_ts INTEGER
        )
    """)

    conn.commit()
    conn.close()
    return ok({"intent_id": intent_id, "status": "submitted"})


# -------------------------
# Health
# -------------------------
@app.route("/api/health", methods=["GET"])
def api_health():
    item = request.args.get("item")
    out = {
        "service": "nexus-grid-backend",
        "status": "ok",
        "mode": "SAFE",
    }
    if item:
        out["item"] = item
        out["has_grid_session"] = (item in GRID_SESSIONS)
    return jsonify(out)


# -------------------------
# Market Health (CoinGecko) — server-side + cache
# -------------------------
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_KNOWN = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "SOL": "solana",
    "MATIC": "polygon-ecosystem-token",
    "POL": "polygon-ecosystem-token",
}

# Cache TTL in seconds (default 180s). You can set CG_TTL_SEC in env.
_CG_CACHE = {"by_key": {}, "ts": {}}
_CG_TTL_SEC = int(os.getenv("CG_TTL_SEC", "180"))

def _cg_cache_get(key: str):
    now = time.time()
    ts = _CG_CACHE["ts"].get(key, 0)
    if key in _CG_CACHE["by_key"] and (now - ts) < _CG_TTL_SEC:
        return _CG_CACHE["by_key"][key]
    return None

def _cg_cache_set(key: str, value):
    _CG_CACHE["by_key"][key] = value
    _CG_CACHE["ts"][key] = time.time()

def _cg_cache_get_any(key: str):
    # Return cached value even if TTL expired (fallback on 429/outage)
    return _CG_CACHE["by_key"].get(key)

# -------------------------
# Generic stale cache (non-health endpoints)
# -------------------------
# NOTE: CoinGecko health cache keys are local to /api/health/market.
# Other endpoints must NOT reference `health_cache_key` (it is not in scope and can crash).
# We keep a tiny "last known good" cache per endpoint/params to avoid UI blanks on transient errors.
_GEN_CACHE = {"by_key": {}, "ts": {}}
_GEN_TTL_SEC = int(os.getenv("GEN_TTL_SEC", "300"))

def _gen_cache_set(key: str, value):
    _GEN_CACHE["by_key"][key] = value
    _GEN_CACHE["ts"][key] = time.time()

def _gen_cache_get_any(key: str):
    # Return cached value even if TTL expired (best-effort fallback)
    return _GEN_CACHE["by_key"].get(key)

def _gen_cache_get_fresh(key: str):
    now = time.time()
    ts = _GEN_CACHE["ts"].get(key, 0)
    if key in _GEN_CACHE["by_key"] and (now - ts) < _GEN_TTL_SEC:
        return _GEN_CACHE["by_key"][key]
    return None


# -------------------------
# Watchlist + Compare caches (frontend stability)
# -------------------------
_WATCH_SNAP_CACHE = {"by_key": {}, "ts": {}}
_WATCH_SNAP_TTL_SEC = int(os.getenv("WATCH_SNAP_TTL_SEC", "60"))  # 1 min default

_COMPARE_CACHE = {"by_key": {}, "ts": {}}
_COMPARE_TTL_SEC = int(os.getenv("COMPARE_TTL_SEC", "900"))  # 15 min default
_COMPARE_LOCKS: Dict[str, threading.Lock] = {}

def _cache_get_fresh(store: dict, key: str, ttl: int):
    now = time.time()
    ts = store.get("ts", {}).get(key, 0)
    if key in store.get("by_key", {}) and (now - ts) < ttl:
        return store["by_key"][key]
    return None

def _cache_get_any(store: dict, key: str):
    return store.get("by_key", {}).get(key)

def _cache_set(store: dict, key: str, value):
    store.setdefault("by_key", {})[key] = value
    store.setdefault("ts", {})[key] = time.time()

def _key_from_items(items: list) -> str:
    # Stable key for watchlist snapshot POST body
    parts = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").strip().upper()
        mode = str(it.get("mode") or "market").strip().lower()
        ident = ""
        if mode == "dex":
            ident = str(it.get("contract") or it.get("id") or "").strip().lower()
        else:
            ident = str(it.get("id") or "").strip().lower()
        parts.append(f"{sym}|{mode}|{ident}")
    return "wl|" + ",".join(parts)

def _lock_for(key: str) -> threading.Lock:
    # single-flight lock per cache key
    if key not in _COMPARE_LOCKS:
        _COMPARE_LOCKS[key] = threading.Lock()
    return _COMPARE_LOCKS[key]




# -------------------------
# Resolver history cache (for multi-coin charts)
# -------------------------
# Longer TTL than generic endpoints because historical series doesn't need frequent refresh.
_RES_HIST_CACHE = {"by_key": {}, "ts": {}}
_RES_HIST_TTL_SEC = int(os.getenv("RES_HIST_TTL_SEC", "900"))  # 15 min default

def _res_hist_cache_get_fresh(key: str):
    now = time.time()
    ts = _RES_HIST_CACHE["ts"].get(key, 0)
    if key in _RES_HIST_CACHE["by_key"] and (now - ts) < _RES_HIST_TTL_SEC:
        return _RES_HIST_CACHE["by_key"][key]
    return None

def _res_hist_cache_get_any(key: str):
    return _RES_HIST_CACHE["by_key"].get(key)

def _res_hist_cache_set(key: str, value):
    _RES_HIST_CACHE["by_key"][key] = value
    _RES_HIST_CACHE["ts"][key] = time.time()

def _cg_request_json(url: str, params: dict, timeout: int = 20):
    # CoinGecko GET with small retry/backoff on 429.
    last_exc = None
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        wait = min(30.0, float(ra))
                    except Exception:
                        wait = 2.0
                else:
                    wait = min(30.0, 1.5 ** attempt)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(min(10.0, 0.5 * (attempt + 1)))
    raise last_exc or RuntimeError("CoinGecko request failed")

def _resolve_cg_id(symbol: str) -> Optional[str]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if sym in COINGECKO_KNOWN:
        return COINGECKO_KNOWN[sym]

    cache_key = f"resolve|{sym}"
    cached = _cg_cache_get(cache_key)
    if isinstance(cached, str) and cached:
        return cached

    r = requests.get(f"{COINGECKO_BASE}/search", params={"query": sym}, timeout=15)
    r.raise_for_status()
    j = r.json() or {}
    coins = j.get("coins") or []
    pick = None

    for c in coins:
        if str(c.get("symbol") or "").upper() == sym:
            pick = c
            break
    if not pick and coins:
        pick = coins[0]

    cid = pick.get("id") if isinstance(pick, dict) else None
    if cid:
        _cg_cache_set(cache_key, cid)
    return cid

def _cg_market_snapshot(coin_id: str):
    key = f"snap|{coin_id}"
    cached = _cg_cache_get(key)
    if cached is not None:
        return cached

    url = f"{COINGECKO_BASE}/coins/markets"
    try:
        arr = _cg_request_json(
            url,
            params={
                "vs_currency": "usd",
                "ids": coin_id,
                "price_change_percentage": "24h",
                "per_page": 1,
                "page": 1,
            },
            timeout=20,
        ) or []
        if not arr:
            raise RuntimeError(f"CoinGecko id not found: {coin_id}")
        c = arr[0]
        out = {
            "price": c.get("current_price"),
            "change24h": (c.get("price_change_percentage_24h")
                        if c.get("price_change_percentage_24h") is not None
                        else (c.get("price_change_percentage_24h_in_currency")
                              if c.get("price_change_percentage_24h_in_currency") is not None
                              else _cg_change24h_from_chart(coin_id))),
            "volume24h": c.get("total_volume"),
            "liquidity": None,
            "source": "coingecko",
        }
        _cg_cache_set(key, out)
        return out
    except Exception as e:
        stale = _cg_cache_get_any(key)
        if stale is not None:
            return stale
        raise e



def _cg_market_snapshots_batch(coin_ids):
    """Batch /coins/markets for many ids. Returns dict id -> snapshot (same shape as _cg_market_snapshot)."""
    if not coin_ids:
        return {}
    ids_unique=[]
    seen=set()
    for cid in coin_ids:
        if cid and cid not in seen:
            ids_unique.append(cid); seen.add(cid)

    out={}
    missing=[]
    for cid in ids_unique:
        key=f"snap|{cid}"
        cached=_cg_cache_get(key)
        if cached is not None:
            out[cid]=cached
        else:
            missing.append(cid)

    if missing:
        url=f"{COINGECKO_BASE}/coins/markets"
        try:
            arr=_cg_request_json(
                url,
                params={
                    "vs_currency":"usd",
                    "ids":",".join(missing),
                    "price_change_percentage":"24h",
                    "per_page":250,
                    "page":1,
                    "sparkline":"false",
                },
                timeout=15,
            ) or []
            if not isinstance(arr, list):
                arr=[]
            for row in arr:
                cid=row.get("id")
                if not cid:
                    continue
                snap={
                    "id": cid,
                    "symbol": (row.get("symbol") or "").upper(),
                    "name": row.get("name"),
                    "price": row.get("current_price"),
                    "change24": row.get("price_change_percentage_24h"),
                    "volume24": row.get("total_volume"),
                    "liquidity": None,
                    "source": "coingecko",
                }
                out[cid]=snap
                _cg_cache_set(f"snap|{cid}", snap)
        except Exception:
            # best-effort: fallback to any stale cache
            for cid in missing:
                stale=_cg_cache_get_any(f"snap|{cid}")
                if stale is not None:
                    out[cid]=stale
    return out
def _cg_market_chart_usd(coin_id: str, days: int):
    key = f"chart|{coin_id}|{days}"
    cached = _cg_cache_get(key)
    if cached is not None:
        return cached

    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    try:
        j = _cg_request_json(url, params={"vs_currency": "usd", "days": days}, timeout=25) or {}
        _cg_cache_set(key, j)
        return j
    except Exception as e:
        stale = _cg_cache_get_any(key)
        if stale is not None:
            return stale
        raise e

def _compute_history_metrics(points):
    if not isinstance(points, list) or len(points) < 10:
        return None

    vals = []
    for p in points:
        try:
            v = float(p[1])
            if v > 0:
                vals.append(v)
        except Exception:
            pass
    if len(vals) < 10:
        return None

    first, last = vals[0], vals[-1]
    ret_pct = ((last - first) / first) * 100.0 if first else None

    rets = []
    for i in range(1, len(vals)):
        a, b = vals[i - 1], vals[i]
        if a > 0 and b > 0:
            import math
            rets.append(math.log(b / a))
    if rets:
        mean = sum(rets) / len(rets)
        varr = sum((x - mean) ** 2 for x in rets) / len(rets)
        import math
        vol = (math.sqrt(varr) * 100.0)
    else:
        vol = None

    peak = vals[0]
    max_dd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        dd = (v - peak) / peak
        if dd < max_dd:
            max_dd = dd
    max_drawdown_pct = max_dd * 100.0

    return {"retPct": ret_pct, "vol": vol, "maxDrawdownPct": max_drawdown_pct}



def _cg_change24h_from_chart(coin_id: str):
    """Fallback: derive 24h % change from market_chart (oldest->newest). Cached via _cg_cache.*"""
    key = f"ch24|{coin_id}"
    cached = _cg_cache_get(key)
    if cached is not None:
        return cached
    try:
        d1 = _cg_market_chart_usd(coin_id, 1)
        prices = (d1 or {}).get("prices") or []
        if isinstance(prices, list) and len(prices) >= 2:
            p0 = float(prices[0][1])
            p1 = float(prices[-1][1])
            if p0 > 0:
                ch = (p1 - p0) / p0 * 100.0
                _cg_cache_set(key, ch)
                return ch
    except Exception:
        pass
    return None

def _cg_search(query: str, limit: int = 25):
    """Search CoinGecko coins by query. Returns list of {id,name,symbol,market_cap_rank}."""
    q = (query or "").strip()
    if not q:
        return []
    url = f"{COINGECKO_BASE}/search"
    r = requests.get(url, params={"query": q}, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    coins = data.get("coins") or []
    out = []
    for c in coins[: max(1, min(int(limit), 50))]:
        out.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "symbol": (c.get("symbol") or "").upper(),
            "market_cap_rank": c.get("market_cap_rank"),
        })
    return out


# --- CoinGecko symbol->id fast path / cache (prevents slow /search on every refresh) ---
_CG_SYMBOL_ID_CACHE = {}  # SYM -> {"ts": int, "id": str}

# very common tickers (CoinGecko ids)
_CG_COMMON_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "SOL": "solana",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "MATIC": "polygon",
    "POL": "polygon-ecosystem-token",
    "USDT": "tether",
    "USDC": "usd-coin",
}
_CG_SYMBOL_CACHE_TTL_SEC = 24 * 3600

def _cg_resolve_symbol(symbol: str):
    """Resolve ticker symbol to CoinGecko coin id.

    Important: CoinGecko /search is relatively slow and rate-limited, so we:
      1) use a small hardcoded map for very common symbols
      2) use an in-process cache (24h TTL)
      3) only then call /search
    """
    s = (symbol or "").strip().upper()
    if not s:
        return None

    # Fast path for common tickers
    if s in _CG_COMMON_IDS:
        return _CG_COMMON_IDS[s]

    # Cache
    try:
        hit = _CG_SYMBOL_ID_CACHE.get(s)
        if hit and isinstance(hit, dict):
            ts = int(hit.get("ts") or 0)
            if (int(time.time()) - ts) < _CG_SYMBOL_CACHE_TTL_SEC and hit.get("id"):
                return str(hit["id"])
    except Exception:
        pass

    # Network search (slow)
    results = _cg_search(s, limit=50)
    exact = [c for c in results if (c.get("symbol") or "").upper() == s and c.get("id")]
    picked = None
    if exact:
        exact.sort(key=lambda x: (x.get("market_cap_rank") is None, x.get("market_cap_rank") or 10**9))
        picked = exact[0].get("id")
    else:
        for c in results:
            if c.get("id"):
                picked = c.get("id")
                break

    if picked:
        _CG_SYMBOL_ID_CACHE[s] = {"ts": int(time.time()), "id": str(picked)}
        return str(picked)
    return None


    # Search by symbol; pick exact symbol matches and best (lowest) market_cap_rank
    results = _cg_search(s, limit=50)
    exact = [c for c in results if (c.get("symbol") or "").upper() == s and c.get("id")]
    if exact:
        exact.sort(key=lambda x: (x.get("market_cap_rank") is None, x.get("market_cap_rank") or 10**9))
        return exact[0]
    # fallback: first result with id
    for c in results:
        if c.get("id"):
            return c
    return None


def _cg_price_series(cg_id: str, days: int = 14):
    """
    Real historical price series from CoinGecko.
    Returns list of floats (prices) ordered oldest->newest.
    Uses /coins/{id}/market_chart.
    """
    url = f"{COINGECKO_BASE}/coins/{cg_id}/market_chart"
    r = requests.get(url, params={"vs_currency": "usd", "days": str(days)}, timeout=15)
    r.raise_for_status()
    data = r.json() or {}
    prices = data.get("prices") or []
    series = []
    for pt in prices:
        try:
            series.append(float(pt[1]))
        except Exception:
            pass
    # de-dup consecutive equals to make ticks meaningful
    compact = []
    last = None
    for p in series:
        if last is None or p != last:
            compact.append(p)
        last = p
    return compact

def _clamp(n: float, a: float, b: float) -> float:
    return max(a, min(b, n))

def _fmt_pct(x):
    try:
        n = float(x)
        sign = "+" if n > 0 else ""
        return f"{sign}{n:.2f}%"
    except Exception:
        return str(x)

def _fmt_usd(x):
    try:
        n = float(x)
        return f"{n:,.2f}"
    except Exception:
        return str(x)

def compute_market_health(row: Dict[str, Any], label: str, hist: Optional[Dict[str, Any]]):
    ch = row.get("change24h")
    vol24 = row.get("volume24h")
    price = row.get("price")

    has_price = isinstance(price, (int, float)) and price > 0
    has_vol = isinstance(vol24, (int, float)) and vol24 >= 0
    has_ch = isinstance(ch, (int, float))

    score = 65.0

    if has_ch:
        score -= _clamp(abs(ch) * 1.8, 0, 35)

    if has_vol:
        if vol24 >= 5_000_000:
            score += 10
        elif vol24 >= 1_000_000:
            score += 7
        elif vol24 >= 250_000:
            score += 4
        elif vol24 >= 50_000:
            score += 1
        else:
            score -= 6

    if not has_price:
        score -= 20

    if hist:
        t30 = hist.get("trend30d")
        t180 = hist.get("trend180d")
        v30 = hist.get("vol30d")
        dd180 = hist.get("dd180d")

        if isinstance(t30, (int, float)):
            score += _clamp(t30 * 0.25, -12, 12)
        if isinstance(t180, (int, float)):
            score += _clamp(t180 * 0.12, -12, 12)

        if isinstance(v30, (int, float)):
            score -= _clamp(v30 * 0.35, 0, 15)

        if isinstance(dd180, (int, float)):
            score -= _clamp(abs(dd180) * 0.12, 0, 18)

    score = round(_clamp(score, 0, 100))

    reasons = []
    if has_ch:
        abs_ch = abs(ch)
        if abs_ch >= 20:
            reasons.append(f"{label}: very high 24h volatility ({_fmt_pct(ch)})")
        elif abs_ch >= 10:
            reasons.append(f"{label}: elevated 24h volatility ({_fmt_pct(ch)})")
        else:
            reasons.append(f"{label}: 24h volatility is moderate ({_fmt_pct(ch)})")
    else:
        reasons.append(f"{label}: 24h change not available")

    if has_vol:
        if vol24 >= 1_000_000:
            reasons.append(f"{label}: strong 24h volume (${_fmt_usd(vol24)})")
        elif vol24 >= 250_000:
            reasons.append(f"{label}: decent 24h volume (${_fmt_usd(vol24)})")
        elif vol24 >= 50_000:
            reasons.append(f"{label}: low 24h volume (${_fmt_usd(vol24)})")
        else:
            reasons.append(f"{label}: very low 24h volume (${_fmt_usd(vol24)})")

    if hist:
        if hist.get("trend30d") is not None:
            reasons.append(f"{label}: 30d trend {_fmt_pct(hist.get('trend30d'))}")
        if hist.get("trend180d") is not None:
            reasons.append(f"{label}: 180d trend {_fmt_pct(hist.get('trend180d'))}")
        if hist.get("dd180d") is not None:
            reasons.append(f"{label}: 180d max drawdown {_fmt_pct(hist.get('dd180d'))}")
    else:
        reasons.append(f"{label}: multi-day history not loaded yet (score uses mostly 24h data)")

    if score >= 80:
        status = "Strong"
    elif score >= 65:
        status = "Healthy"
    elif score >= 50:
        status = "Caution"
    else:
        status = "High Risk"

    confidence = 0.55
    if hist:
        confidence = 0.8
    if not has_price:
        confidence = min(confidence, 0.35)

    return {
        "score": score,
        "status": status,
        "reasons": reasons,
        "confidence": confidence,
        "metrics": {"row": row, "hist": hist},
    }

@app.route("/api/health/market", methods=["GET"])
def api_health_market():
    symbol = (request.args.get("symbol") or "").strip().upper()
    coin_id = (request.args.get("id") or "").strip()

    if not coin_id:
        if not symbol:
            return err("missing symbol", 400)
        coin_id = _resolve_cg_id(symbol)
        if not coin_id:
            return err("could not resolve CoinGecko id", 404)

    label = symbol or coin_id

    fast = str(request.args.get("fast") or "").strip() in ("1", "true", "yes")

    health_cache_key = f"health|{coin_id}|{'fast' if fast else 'full'}"

    try:
        snap = _cg_market_snapshot(coin_id)
        row = {
            "price": snap.get("price"),
            "change24h": snap.get("change24h"),
            "volume24h": snap.get("volume24h"),
        }

        hist = None
        if not fast:
            try:
                d30 = _cg_market_chart_usd(coin_id, 30)
                d180 = _cg_market_chart_usd(coin_id, 180)
                m30 = _compute_history_metrics((d30 or {}).get("prices"))
                m180 = _compute_history_metrics((d180 or {}).get("prices"))
                hist = {
                    "trend30d": (m30 or {}).get("retPct"),
                    "vol30d": (m30 or {}).get("vol"),
                    "trend180d": (m180 or {}).get("retPct"),
                    "dd180d": (m180 or {}).get("maxDrawdownPct"),
                }
            except Exception:
                hist = None

        out = compute_market_health(row, label, hist)
        out["symbol"] = symbol or None
        out["id"] = coin_id
        out["source"] = "coingecko"
        out["fast"] = fast
        _cg_cache_set(health_cache_key, {"status": "ok", "data": out})
        return jsonify({"status": "ok", "data": out})

    except Exception as e:
        stale_health = _cg_cache_get_any(health_cache_key)
        if stale_health is not None:
            # Return last known health to reduce UI "dropouts" during transient API issues.
            return ok(stale_health)
        return err(str(e), 500)


# -------------------------
# Market test / Market data
# -------------------------
@app.route("/api/market-test", methods=["GET"])
def api_market_test():
    item = request.args.get("item", "polygon_weth_usdc_quickswap")
    addr = _norm_addr(request.args.get("addr") or request.args.get("address") or "")
    cache_key = f"market-test|{(item or '').strip()}|{(addr or '').strip()}"
    try:
        data = get_pair_data(item, addr=addr) if addr else get_pair_data(item)
        resp = {"status": "ok", "item": item, "addr": addr, "data": data}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)

@app.route("/api/watchlist", methods=["GET"])
def api_watchlist():
    cache_key = "watchlist"
    try:
        data = get_watchlist()
        resp = {"status": "ok", "items": data}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)

@app.route("/api/watchlist/live", methods=["GET"])
def api_watchlist_live():
    item = request.args.get("item")
    addr = request.args.get("addr")
    if not item:
        return err("missing 'item' query param", 400)

    cache_key = f"watchlist-live|{(item or '').strip()}|{(addr or '').strip()}"
    try:
        data = get_pair_data(item, addr=addr) if addr else get_pair_data(item)
        if isinstance(data, dict) and "items" in data:
            resp = data
        else:
            resp = {"items": [data]}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)

@app.route("/api/watchlist/safety", methods=["GET"])
def api_watchlist_safety():
    item = request.args.get("item")
    addr = request.args.get("addr")
    if not item:
        return err("missing 'item' query param", 400)

    cache_key = f"watchlist-safety|{(item or '').strip()}|{(addr or '').strip()}"
    try:
        out = evaluate_safety(item, addr=addr) if addr else evaluate_safety(item)
        resp = {"status": "ok", "item": item, "addr": addr, "data": out}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)

@app.route("/api/market/search", methods=["GET"])
def api_market_search():
    q = request.args.get("query") or ""
    cache_key = f"market-search|{q.strip().lower()}"
    try:
        results = _cg_search(q, limit=25)
        resp = {"query": q, "results": results}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)

@app.route("/api/market/resolve", methods=["GET"])
def api_market_resolve():
    symbol = request.args.get("symbol") or ""
    cache_key = f"market-resolve|{symbol.strip().upper()}"
    try:
        resolved = _cg_resolve_symbol(symbol)
        if not resolved:
            return err("not found", 404)
        resp = {"symbol": symbol.upper(), "resolved": resolved}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return err(str(e), 500)
@app.route("/api/watchlist/snapshot", methods=["GET", "POST"])
def api_watchlist_snapshot():
    """
    Watchlist snapshot.

    - GET: Uses server-side configured watchlist (get_watchlist()) for backwards compatibility.
    - POST: Expects JSON: { "items": [{symbol, mode: "market"|"dex", id?, chain?, contract?}, ...] }
            Returns normalized rows for the frontend:
            { symbol, mode, id, price, change24h, volume24h, liquidity, source }
    """
    try:
        items = None

        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            items = body.get("items")

        if not isinstance(items, list) or not items:
            # Fallback to server-side watchlist for GET (or empty POST)
            wl = get_watchlist()
            items = wl.get("items", []) if isinstance(wl, dict) else []

        # ---- Normalize input items ----
        norm_items = []
        for it in items:
            if isinstance(it, str):
                sym = it.strip().upper()
                if sym:
                    norm_items.append({"symbol": sym, "mode": "market"})
                continue

            if not isinstance(it, dict):
                continue

            sym = (it.get("symbol") or it.get("sym") or "").strip().upper()
            mode = (it.get("mode") or "market").strip().lower()
            if not sym:
                continue

            row = {"symbol": sym, "mode": ("dex" if mode == "dex" else "market")}

            # market extras
            if row["mode"] == "market":
                if it.get("id"):
                    row["id"] = str(it.get("id")).strip()

            # dex extras
            if row["mode"] == "dex":
                if it.get("chain"):
                    row["chain"] = str(it.get("chain")).strip()
                if it.get("contract"):
                    row["contract"] = str(it.get("contract")).strip()

            norm_items.append(row)

        # de-dupe keep order
        seen = set()
        ordered = []
        for it in norm_items:
            key = (it.get("symbol"), it.get("mode"), it.get("contract") or it.get("id") or "")
            if key in seen:
                continue
            seen.add(key)
            ordered.append(it)

        if not ordered:
            return jsonify({"status": "ok", "results": [], "ts": int(time.time())})


        # Fast-path cache (avoid repeated upstream calls while user clicks quickly)
        wl_cache_key = _key_from_items(ordered)
        fresh_cached = _cache_get_fresh(_WATCH_SNAP_CACHE, wl_cache_key, _WATCH_SNAP_TTL_SEC)
        if fresh_cached is not None:
            return jsonify(fresh_cached)

        # ---- Market batch (CoinGecko) ----
        market_items = [it for it in ordered if it.get("mode") == "market"]
        ids_by_symbol = {}
        coin_ids = []

        for it in market_items:
            sym = it["symbol"]
            cid = it.get("id")
            if not cid:
                cid = _cg_resolve_symbol(sym)
            if cid:
                ids_by_symbol[sym] = cid
                coin_ids.append(cid)

        snaps_by_id = _cg_market_snapshots_batch(coin_ids) if coin_ids else {}

        # ---- Build results (normalized keys expected by frontend) ----
        results = []

        for it in ordered:
            sym = it.get("symbol")
            mode = it.get("mode") or "market"

            if mode == "dex":
                contract = (it.get("contract") or "").strip()
                if not contract:
                    results.append({
                        "symbol": sym,
                        "mode": "dex",
                        "id": None,
                        "price": None,
                        "change24h": None,
                        "volume24h": None,
                        "liquidity": None,
                        "source": "error",
                        "error": "missing_contract",
                    })
                    continue
                try:
                    snap = _dexscreener_snapshot(contract)
                    results.append({
                        "symbol": sym,
                        "mode": "dex",
                        "id": contract,
                        "price": snap.get("price"),
                        "change24h": snap.get("change24h"),
                        "volume24h": snap.get("volume24h"),
                        "liquidity": snap.get("liquidity"),
                        "source": snap.get("source") or "dexscreener",
                    })
                except Exception as e:
                    results.append({
                        "symbol": sym,
                        "mode": "dex",
                        "id": contract,
                        "price": None,
                        "change24h": None,
                        "volume24h": None,
                        "liquidity": None,
                        "source": "error",
                        "error": str(e),
                    })
                continue

            # market
            cid = ids_by_symbol.get(sym) or it.get("id")
            snap = snaps_by_id.get(cid) if cid else None
            if snap:
                results.append({
                    "symbol": sym,
                    "mode": "market",
                    "id": cid,
                    "price": snap.get("price"),
                    "change24h": snap.get("change24") if "change24" in snap else snap.get("change24h"),
                    "volume24h": snap.get("volume24") if "volume24" in snap else snap.get("volume24h"),
                    "liquidity": None,
                    "source": snap.get("source") or "coingecko",
                })
            else:
                results.append({
                    "symbol": sym,
                    "mode": "market",
                    "id": cid,
                    "price": None,
                    "change24h": None,
                    "volume24h": None,
                    "liquidity": None,
                    "source": "error",
                })

        _cache_set(_WATCH_SNAP_CACHE, wl_cache_key, {"status": "ok", "results": results, "ts": int(time.time())})
        return jsonify({"status": "ok", "results": results, "ts": int(time.time())})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e), "results": [], "ts": int(time.time())}), 500



# -------------------------
# Compare (normalized series) + Health (aggregate)
# -------------------------
_RANGE_TO_DAYS = {
    "15M": 1,
    "1H": 1,
    "1D": 1,
    "7D": 7,
    "30D": 30,
    "90D": 90,
    "1Y": 365,
    "3Y": 1095,
}

def _downsample_points(prices, max_points: int = 240):
    """Downsample CoinGecko [ms, price] points to keep payload small."""
    if not isinstance(prices, list):
        return []
    n = len(prices)
    if n <= max_points:
        return prices
    import math
    step = int(math.ceil(n / max_points))
    out = prices[::step]
    # ensure last point is included
    if out and prices and out[-1] != prices[-1]:
        out.append(prices[-1])
    return out

@app.route("/api/compare", methods=["GET"])
def api_compare():
    """
    GET /api/compare?symbols=BTC,ETH&range=30D

    Returns:
    {
      "status":"ok",
      "range":"30D",
      "days":30,
      "symbols":["BTC","ETH"],
      "series": {"BTC":[{"t":..., "v":...}, ...], ...},
      "health": {"BTC":{"score":78,"label":"Strong","status":"healthy"}, ...},
      "updated_at": 1700000000
    }
    """
    try:
        symbols_raw = (request.args.get("symbols") or "").strip()
        if not symbols_raw:
            return err("missing symbols", 400)

        range_key = (request.args.get("range") or "30D").strip().upper()
        days = _RANGE_TO_DAYS.get(range_key, 30)

        include_health = str(request.args.get("include_health", "1")).strip().lower() not in ("0","false","no","off")

        symbols = []
        for s in symbols_raw.split(","):
            s = (s or "").strip().upper()
            if s and s not in symbols:
                symbols.append(s)
        if not symbols:
            return err("missing symbols", 400)

        symbols = symbols[:8]  # hard limit to protect API + payload

        cache_key = "compare|" + range_key + "|" + ",".join(symbols)
        cached = _cache_get_fresh(_COMPARE_CACHE, cache_key, _COMPARE_TTL_SEC)
        if cached is not None:
            return jsonify(cached)

        # single-flight: if multiple requests for same key arrive, compute once
        lock = _lock_for(cache_key)
        with lock:
            cached2 = _cache_get_fresh(_COMPARE_CACHE, cache_key, _COMPARE_TTL_SEC)
            if cached2 is not None:
                return jsonify(cached2)


        series_out = {}
        health_out = {} if include_health else None

        for sym in symbols:
            coin_id = _resolve_cg_id(sym)
            if not coin_id:
                # Skip unknown symbols but keep response stable
                continue

            # Series for selected range
            chart = _cg_market_chart_usd(coin_id, days) or {}
            prices = _downsample_points(chart.get("prices") or [], max_points=240)

            # Return RAW prices (indexing is done in the frontend).
            series = []
            now_ms = int(time.time() * 1000)

            # Intraday ranges: use last 15 minutes / 1 hour from the 1D market chart.
            min_ms = None
            if range_key == "15M":
                min_ms = now_ms - 15 * 60 * 1000
            elif range_key == "1H":
                min_ms = now_ms - 60 * 60 * 1000

            for p in prices:
                try:
                    t_ms = int(p[0])
                    px = float(p[1])
                    if px <= 0:
                        continue
                    if min_ms is not None and t_ms < min_ms:
                        continue
                    series.append({"t": t_ms, "v": px})
                except Exception:
                    continue
            if series:
                series_out[sym] = series

            if include_health:
                # Health (reuse existing market health logic; cached)
                snap = _cg_market_snapshot(coin_id) or {}
                row = {
                    "price": snap.get("price"),
                    "change24h": snap.get("change24h"),
                    "volume24h": snap.get("volume24h"),
                }

                # Use the same hist inputs as /api/health/market (cached)
                hist = None
                try:
                    d30 = _cg_market_chart_usd(coin_id, 30)
                    d180 = _cg_market_chart_usd(coin_id, 180)
                    m30 = _compute_history_metrics((d30 or {}).get("prices"))
                    m180 = _compute_history_metrics((d180 or {}).get("prices"))
                    hist = {
                        "trend30d": (m30 or {}).get("retPct"),
                        "vol30d": (m30 or {}).get("vol"),
                        "trend180d": (m180 or {}).get("retPct"),
                        "dd180d": (m180 or {}).get("maxDrawdownPct"),
                    }
                except Exception:
                    hist = None

                h = compute_market_health(row, sym, hist) or {}
                score = h.get("score")
                # Map to frontend labels
                if isinstance(score, (int, float)):
                    if score >= 71:
                        label = "Strong"
                    elif score >= 51:
                        label = "Medium"
                    else:
                        label = "Weak"
                else:
                    label = None

                health_out[sym] = {
                    "score": score,
                    "label": label,
                    "status": h.get("status"),
                }



        out = {
            "status": "ok",
            "range": range_key,
            "days": days,
            "symbols": symbols,
            "series": series_out,
            "updated_at": now_ts(),
        }
        if include_health:
            out["health"] = health_out

        _cache_set(_COMPARE_CACHE, cache_key, out)
        return jsonify(out)

    except Exception as e:
        # Best-effort: return last cached compare result to avoid UI blanks
        try:
            stale = _cache_get_any(_COMPARE_CACHE, cache_key) if 'cache_key' in locals() else None
            if stale is not None:
                return jsonify(stale)
        except Exception:
            pass
        return err(str(e), 500)


# -------------------------
# Trading Suitability (informational)
# -------------------------
def _suitability_for_snapshot(sym: str, snap: dict, profile: str) -> dict:
    """
    Convert market snapshot -> suitability score (0..100) and label.
    Profiles:
      - conservative: prioritize liquidity/volume, penalize volatility
      - volatility: prioritize movement (volatility) but still require some volume
      - balanced: middle ground
    """
    sym = (sym or "").strip().upper()
    profile = (profile or "conservative").strip().lower()
    if profile not in ("conservative", "balanced", "volatility"):
        profile = "conservative"

    price = snap.get("price")
    ch = snap.get("change24h")
    vol = snap.get("volume24h")

    # Volume bucket score (0..60)
    vscore = 0
    try:
        v = float(vol or 0)
        if v >= 50_000_000: vscore = 60
        elif v >= 10_000_000: vscore = 52
        elif v >= 1_000_000: vscore = 44
        elif v >= 250_000: vscore = 34
        elif v >= 50_000: vscore = 22
        elif v > 0: vscore = 12
        else: vscore = 0
    except Exception:
        vscore = 0

    # Volatility proxy from abs(24h change)
    try:
        ach = abs(float(ch)) if ch is not None else None
    except Exception:
        ach = None

    # Movement score (0..40) depending on profile
    mscore = 0
    reasons = []

    if ach is None:
        reasons.append("24h change not available")
    else:
        if profile == "conservative":
            # Lower volatility preferred
            if ach <= 2: mscore = 40
            elif ach <= 5: mscore = 30
            elif ach <= 10: mscore = 18
            elif ach <= 20: mscore = 8
            else: mscore = 0
            reasons.append(f"volatility: {ach:.2f}% (lower is better)")
        elif profile == "volatility":
            # Moderate-high movement preferred, extreme is risky
            if 5 <= ach <= 20: mscore = 40
            elif 2 <= ach < 5: mscore = 26
            elif 20 < ach <= 35: mscore = 22
            elif ach < 2: mscore = 10
            else: mscore = 12
            reasons.append(f"volatility: {ach:.2f}% (movement preferred)")
        else:  # balanced
            if ach <= 2: mscore = 28
            elif ach <= 5: mscore = 34
            elif ach <= 10: mscore = 30
            elif ach <= 20: mscore = 20
            else: mscore = 10
            reasons.append(f"volatility: {ach:.2f}%")

    # Basic sanity checks
    try:
        if float(price or 0) <= 0:
            reasons.append("price missing")
    except Exception:
        reasons.append("price missing")

    score = int(_clamp(vscore + mscore, 0, 100))

    # Labels
    if score >= 75:
        label = "Good for Grid"
        band = "good"
    elif score >= 55:
        label = "OK"
        band = "ok"
    else:
        label = "Not suitable"
        band = "bad"

    # Add volume reason
    if vscore >= 52:
        reasons.append("high 24h volume")
    elif vscore >= 34:
        reasons.append("decent 24h volume")
    elif vscore >= 12:
        reasons.append("low 24h volume")
    else:
        reasons.append("very low/no volume data")

    return {"symbol": sym, "score": score, "label": label, "band": band, "profile": profile, "metrics": {"price": price, "change24h": ch, "volume24h": vol}, "reasons": reasons}

@app.route("/api/trading/suitability", methods=["GET"])
def api_trading_suitability():
    """
    GET /api/trading/suitability?symbols=BTC,ETH&profile=conservative|balanced|volatility
    Returns suitability for the requested symbols, sorted by score desc.
    """
    symbols_raw = (request.args.get("symbols") or request.args.get("symbol") or "").strip()
    profile = (request.args.get("profile") or "").strip().lower()

    # profile default can be stored in policy, but query param overrides
    if not profile:
        # if authenticated, use stored preference
        wa = _require_auth()
        if wa:
            try:
                profile = str(get_policy(wa).get("trading_profile") or "conservative").strip().lower()
            except Exception:
                profile = "conservative"
        else:
            profile = "conservative"

    if not symbols_raw:
        return err("missing symbols", 400)

    symbols = []
    for s in symbols_raw.split(","):
        s = (s or "").strip().upper()
        if s and s not in symbols:
            symbols.append(s)
    symbols = symbols[:12]

    out = []
    for sym in symbols:
        cid = _resolve_cg_id(sym)
        if not cid:
            out.append({"symbol": sym, "score": 0, "label": "Not suitable", "band": "bad", "profile": profile, "reasons": ["unknown symbol"]})
            continue
        snap = _cg_market_snapshot(cid) or {}
        out.append(_suitability_for_snapshot(sym, snap, profile))

    out.sort(key=lambda x: int(x.get("score") or 0), reverse=True)
    return jsonify({"status": "ok", "profile": profile, "results": out, "ts": now_ts()})

@app.route("/api/resolver/history", methods=["POST"])
def api_resolver_history():
    """
    Multi-coin historical price series for Resolver compare charts.

    Request JSON:
      { "ids": ["bitcoin","ethereum", ...], "days": 7|30|90 }

    Notes:
    - max 20 ids
    - cached with longer TTL (default 15 min via RES_HIST_TTL_SEC)
    - best-effort fallback to last cached value if upstream is down/rate-limited
    """
    try:
        payload = request.get_json(silent=True) or {}
        ids = payload.get("ids") or []
        days = int(payload.get("days") or 30)

        if not isinstance(ids, list) or not ids:
            return err("Missing ids (list).", 400)
        if len(ids) > 20:
            return err("Max 20 ids.", 400)
        if days not in (7, 30, 90):
            return err("days must be 7, 30, or 90.", 400)

        # normalize ids
        norm_ids = []
        for cid in ids:
            if not isinstance(cid, str):
                continue
            cid = cid.strip().lower()
            if cid:
                norm_ids.append(cid)
        if not norm_ids:
            return err("No valid ids.", 400)

        cache_key = "resolver_hist|" + str(days) + "|" + ",".join(sorted(set(norm_ids)))

        fresh = _res_hist_cache_get_fresh(cache_key)
        if fresh is not None:
            return jsonify(fresh)

        series = {}
        errors = {}

        for cid in norm_ids:
            try:
                j = _cg_market_chart_usd(cid, days) or {}
                prices = j.get("prices") or []
                # prices is [[ts_ms, price], ...]
                if isinstance(prices, list) and prices:
                    series[cid] = prices
                else:
                    errors[cid] = "no_prices"
            except Exception as e:
                errors[cid] = str(e)

        out = {"days": days, "series": series}
        if errors:
            out["errors"] = errors

        # If we got at least one series, cache it as last known good
        if series:
            _res_hist_cache_set(cache_key, out)
            return jsonify(out)

        # If nothing succeeded, return last cached value if any
        stale = _res_hist_cache_get_any(cache_key)
        if stale is not None:
            return jsonify(stale)

        return err("No data available.", 502)

    except Exception as e:
        # Best-effort fallback
        try:
            payload = request.get_json(silent=True) or {}
            ids = payload.get("ids") or []
            days = int(payload.get("days") or 30)
            norm_ids = [str(x).strip().lower() for x in ids if isinstance(x, str) and str(x).strip()]
            cache_key = "resolver_hist|" + str(days) + "|" + ",".join(sorted(set(norm_ids)))
            stale = _res_hist_cache_get_any(cache_key)
            if stale is not None:
                return jsonify(stale)
        except Exception:
            pass
        return err(str(e), 500)


@app.route("/api/grid/start", methods=["POST"])
def api_grid_start():
    body = request.get_json(silent=True) or {}
    wa, access, e_access = _require_access_open()
    if e_access:
        return e_access
    item_id = body.get("item") or body.get("item_id") or body.get("id")
    addr = body.get("addr") or body.get("wallet_address")
    mode = (body.get("mode") or "SAFE").upper()
    order_mode = str(body.get("order_mode") or body.get("orders_mode") or body.get("grid_order_mode") or "MANUAL").upper().strip()

    # Manual-only: automatic grid order generation is disabled
    if order_mode != "MANUAL":
        return err("AUTO grid order mode is disabled; use MANUAL orders only", 400)

    if not item_id:
        return err("missing 'item' in body", 400)

    item_id = str(item_id).strip()

    # ✅ Use real price if provided or from cached watchlist snapshot
    start_price = body.get("price") or body.get("start_price")
    if start_price is None:
        snap = SNAPSHOTS.get(item_id)
        if snap and isinstance(snap.get("data"), dict):
            start_price = snap["data"].get("price")

    try:
        start_price = float(start_price) if start_price is not None else 1.0
        if not math.isfinite(start_price) or start_price <= 0:
            start_price = 1.0
    except Exception:
        start_price = 1.0

    try:
        cfg = {
            "item_id": item_id,
            "mode": mode,
            "order_mode": order_mode,
            "addr": addr,
            "price": start_price,
            "grid_step_pct": body.get("grid_step_pct"),
            "grid_levels_each_side":  (body.get("grid_levels_each_side") if body.get("grid_levels_each_side") is not None else 5),
            "take_profit_pct": body.get("take_profit_pct"),
            "stop_loss_pct": body.get("stop_loss_pct"),
            "levels": body.get("levels"),
            "initial_capital_usd": (body.get("invest_usd") or body.get("initial_capital_usd") or body.get("capital_usd") or body.get("budget_usd")),
        }

        session = _sim_build(cfg)
        session["wallet_address"] = _norm_addr(addr) if addr else ""
        # If MANUAL, do not auto-create initial grid orders
        if order_mode == 'MANUAL':
            session.setdefault('orders', [])
            session['orders'] = [o for o in session.get('orders', []) if isinstance(o, dict) and o.get('level') == 'MANUAL']
        session['order_mode'] = order_mode
        session["last_price"] = start_price

        # ✅ Attach real historical series (for "Tick" backtest stepping)
        if str(os.getenv("NEXUS_ATTACH_HISTORY_ON_START", "1")).lower() in ("1", "true", "yes"):
            try:
                # If watchlist snapshot tells us the CoinGecko id, use it
                cg_id = None
                snap = SNAPSHOTS.get(item_id)
                if snap and isinstance(snap.get("data"), dict):
                    cg_id = snap["data"].get("id") if snap["data"].get("mode") == "market" else None

                if cg_id:
                    if cg_id in PRICE_SERIES_CACHE and PRICE_SERIES_CACHE[cg_id].get("series"):
                        series = PRICE_SERIES_CACHE[cg_id]["series"]
                    else:
                        series = _cg_price_series(cg_id, days=14)
                        PRICE_SERIES_CACHE[cg_id] = {"ts": now_ts(), "series": series}

                    if series:
                        session["price_series"] = series
                        # Start near the last ~60 points so user can tick through recent history
                        session["series_idx"] = max(0, len(series) - 60)
                        session["series_cg_id"] = cg_id
            except Exception:
                pass


        GRID_CONFIGS[item_id] = cfg
        GRID_SESSIONS[item_id] = _trim_grid_session(session)
        _persist_grid_state()

        # --- PnL + demo equity init ---
        try:
            _ensure_pnl(session)
            _pnl_mark(session, session.get("price"))
        except Exception:
            pass

        # --- Wallet demo budget init (Available/Locked) ---
        try:
            if session.get('wallet_available_usd') is None:
                session['wallet_available_usd'] = float(session.get('initial_capital_usd') or INITIAL_CAPITAL_USD)
            if session.get('wallet_locked_usd') is None:
                session['wallet_locked_usd'] = 0.0
        except Exception:
            pass

        return jsonify({
            "status": "ok",
            "item": item_id,
            "mode": mode,
            "config": cfg,
            "price": session.get("price"),
            "tick": int(session.get("ticks") or 0),
            "price_source": ("frontend" if (body.get("price") or body.get("start_price")) is not None else "snapshot"),
            "sim": {
                "simulation": True,
                "uses_real_market_data": True,
                "initial_capital_usd": float(session.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
                "equity_usd": float(session.get("equity_usd") or 0.0),
                "pnl_pct": float(session.get("pnl_pct") or 0.0),
            },
            "pnl": {
                "pos": float(session.get("position_qty") or 0.0),
                "avg_cost": float(session.get("avg_cost") or 0.0),
                "realized": float(session.get("realized_pnl") or 0.0),
                "unrealized": float(session.get("unrealized_pnl") or 0.0),
                "total": float(session.get("total_pnl") or 0.0),
            },
            "orders": session.get("orders", []),
            "filled_now": int(session.get("filled_now") or 0),
            "fills": session.get("fills", []),
        })
    except Exception as e:
        return err(str(e), 500)

@app.route("/api/grid/tick", methods=["GET", "POST"])
def api_grid_tick():
    if request.method == "GET":
        item_id = request.args.get("item") or request.args.get("item_id")
        price = request.args.get("price")
    else:
        body = request.get_json(silent=True) or {}
        item_id = body.get("item") or body.get("item_id")
        price = body.get("price")

    if not item_id:
        return err("missing 'item' in body", 400)

    item_id = str(item_id).strip()
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    # Only owner can tick a session manually
    session = _get_owned_session(item_id, wa)
    if not session:
        return err("forbidden", 403)
    if not session:
        return err("grid not started (press Start first)", 404)

    # ✅ Prefer explicit live price from frontend; otherwise use cached snapshot price.
    new_price = None
    if price is not None and price != "":
        try:
            new_price = float(price)
        except Exception:
            new_price = None

    if new_price is None:
        snap = SNAPSHOTS.get(item_id)
        if snap and isinstance(snap.get("data"), dict):
            try:
                new_price = float(snap["data"].get("price"))
            except Exception:
                new_price = None

    # ✅ If we have a real historical series attached, Tick advances through it (real backtest),
    # otherwise we use live price (frontend or snapshot).
    series = None
    try:
        series = session.get("price_series")
    except Exception:
        series = None

    if isinstance(series, list) and len(series) >= 2:
        idx = int(session.get("series_idx") or 0)
        # advance one step
        idx = min(idx + 1, len(series) - 1)
        session["series_idx"] = idx
        new_price = float(series[idx])
        updated = _sim_tick(session, new_price=new_price)
        price_source_label = "history"
    else:
        updated = _sim_tick(session, new_price=new_price)
        price_source_label = ("frontend" if price is not None else ("snapshot" if new_price is not None else "none"))

    GRID_SESSIONS[item_id] = _trim_grid_session(updated)
    _persist_grid_state()

    fills = updated.get("fills") if isinstance(updated, dict) else []
    # --- PnL update (simulation) + wallet budget (simple) ---
    try:
        _ensure_pnl(session)
        if isinstance(fills, list):
            for _f in fills:
                if isinstance(_f, dict) and _f.get("filled_ts"):
                    # apply only newly filled if not yet tagged
                    if not _f.get("_pnl_applied"):
                        _delta = _pnl_apply_fill(
                            session,
                            _f,
                            qty=float(_f.get("amount") or 1.0)
                            if str(_f.get("amount") or "").replace(".", "", 1).isdigit()
                            else 1.0,
                        )
                        _f["pnl_delta"] = _delta
                        _f["_pnl_applied"] = True

                        # release locked wallet budget on BUY fills (simple model)
                        try:
                            if str(_f.get("side") or "").upper() == "BUY":
                                oid = _f.get("id")
                                if oid:
                                    for oo in (session.get("orders") or []):
                                        if isinstance(oo, dict) and str(oo.get("id")) == str(oid):
                                            locked = oo.get("usd_locked") or oo.get("usd")
                                            if locked is not None:
                                                locked = float(locked)
                                                session["wallet_locked_usd"] = max(
                                                    0.0, float(session.get("wallet_locked_usd") or 0.0) - locked
                                                )
                                            break
                        except Exception:
                            pass
        _pnl_mark(session, updated.get("price") if isinstance(updated, dict) else None)
        _persist_grid_state()
    except Exception:
        pass

    return jsonify({
        "status": "ok",
        "item": item_id,
        "tick": int(updated.get("ticks") or 0) if isinstance(updated, dict) else 0,
        "price": float(updated.get("price") or 0) if isinstance(updated, dict) else 0,
        "price_source": price_source_label,

        "sim": {
            "simulation": True,
            "uses_real_market_data": True,
            "initial_capital_usd": float(session.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
            "equity_usd": float(session.get("equity_usd") or 0.0),
            "pnl_pct": float(session.get("pnl_pct") or 0.0),
        },

        "pnl": {
            "pos": float(session.get("position_qty") or 0),
            "avg_cost": float(session.get("avg_cost") or 0),
            "realized": float(session.get("realized_pnl") or 0),
            "unrealized": float(session.get("unrealized_pnl") or 0),
            "total": float(session.get("total_pnl") or 0),
        },
        "orders": (updated.get("orders") if isinstance(updated, dict) else []),
        "filled_now": int(updated.get("filled_now") or 0) if isinstance(updated, dict) else 0,
        "fills": (fills if isinstance(fills, list) else []),
        "note": ("No live price available; tick did not move price." if new_price is None else None),
    })


@app.route("/api/grid/summary", methods=["GET"])
def api_grid_summary():
    """Return per-item grid summary incl. PnL and running status."""
    out = []
    for item_id, sess in (GRID_SESSIONS or {}).items():
        if not isinstance(sess, dict):
            continue
        _ensure_pnl(sess)
        out.append({
            "item": item_id,
            "running": bool(sess.get("running")) and not bool(sess.get("stopped")),
            "tick": int(sess.get("ticks") or 0),
            "last_price": sess.get("price"),
            "pnl": {
                "pos": float(sess.get("position_qty") or 0),
                "avg_cost": float(sess.get("avg_cost") or 0),
                "realized": float(sess.get("realized_pnl") or 0),
                "unrealized": float(sess.get("unrealized_pnl") or 0),
                "total": float(sess.get("total_pnl") or 0),
            },
        })
    out.sort(key=lambda x: x.get("item") or "")
    return jsonify({"items": out})
@app.route("/api/grid/stop", methods=["POST"])
def api_grid_stop():
    """Stop a single grid session without deleting its history."""
    body = request.get_json(silent=True) or {}
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    item_id = body.get("item") or body.get("item_id")
    if not item_id:
        return err("missing 'item' in body", 400)

    item_id = str(item_id).strip()
    session = GRID_SESSIONS.get(item_id)
    if not isinstance(session, dict):
        return err("grid not started (press Start first)", 404)

    now = now_ts()
    # cancel open orders
    for o in session.get("orders") or []:
        if isinstance(o, dict) and o.get("status") == "OPEN":
            o["status"] = "CANCELLED"
            # Release locked budget for BUY orders (simple)
            try:
                if str(o.get('side') or '').upper() == 'BUY':
                    locked = o.get('usd_locked') or o.get('usd')
                    if locked is not None:
                        locked = float(locked)
                        session['wallet_locked_usd'] = max(0.0, float(session.get('wallet_locked_usd') or 0.0) - locked)
                        session['wallet_available_usd'] = float(session.get('wallet_available_usd') or 0.0) + locked
            except Exception:
                pass
            o["cancelled_ts"] = now
    session["stopped"] = True
    GRID_SESSIONS[item_id] = _trim_grid_session(session)
    _persist_grid_state()
    return jsonify({"status": "ok", "item": item_id, "stopped": True, "orders": session.get("orders", []), "ts": now})



@app.route("/api/grid/stop_all", methods=["POST"])
def api_grid_stop_all():
    """Stop ALL grid sessions: cancel OPEN orders, keep history."""
    now = now_ts()
    for item_id, session in (GRID_SESSIONS or {}).items():
        if not isinstance(session, dict):
            continue
        for o in session.get("orders") or []:
            if isinstance(o, dict) and o.get("status") == "OPEN":
                o["status"] = "CANCELLED"
                # Release locked budget for BUY orders (simple)
                try:
                    if str(o.get('side') or '').upper() == 'BUY':
                        locked = o.get('usd_locked') or o.get('usd')
                        if locked is not None:
                            locked = float(locked)
                            session['wallet_locked_usd'] = max(0.0, float(session.get('wallet_locked_usd') or 0.0) - locked)
                            session['wallet_available_usd'] = float(session.get('wallet_available_usd') or 0.0) + locked
                except Exception:
                    pass
                o["cancelled_ts"] = now
        session["stopped"] = True
        GRID_SESSIONS[item_id] = _trim_grid_session(session)
    _persist_grid_state()
    return jsonify({"status":"ok","stopped_all": True, "ts": now})

@app.route("/api/grid/history/clear", methods=["POST"])
def api_grid_history_clear():
    """Clear history to free space.

    Body:
      { "all": true } -> clears FILLED/EXPIRED/CANCELLED + fills for all items (keeps OPEN)
      { "item": "BTC" } -> same but only one item
    """
    body = request.get_json(silent=True) or {}
    item = body.get("item")
    clear_all = bool(body.get("all", False)) or (item is None and bool(body.get("clear_all", False)))

    def _clear_one(item_id: str):
        sess = GRID_SESSIONS.get(item_id)
        if not isinstance(sess, dict):
            return
        orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
        open_orders = [o for o in orders if isinstance(o, dict) and o.get("status") == "OPEN"]
        sess["orders"] = open_orders
        sess["fills"] = []
        sess.pop("filled_now", None)
        GRID_SESSIONS[item_id] = sess

    if clear_all:
        for item_id in list((GRID_SESSIONS or {}).keys()):
            _clear_one(item_id)
    else:
        if not item:
            return err("provide {all:true} or {item:...}", 400)
        _clear_one(str(item).strip())

    _persist_grid_state()
    return jsonify({"status":"ok","cleared": True, "all": clear_all, "item": item, "ts": now_ts()})

@app.route("/api/grid/reset_all", methods=["POST"])
def api_grid_reset_all():
    """Hard reset: delete ALL sessions and configs."""
    GRID_SESSIONS.clear()
    GRID_CONFIGS.clear()
    _persist_grid_state()
    return jsonify({"status":"ok","reset_all": True, "ts": now_ts()})

@app.route("/api/grid/orders", methods=["GET"])
def api_grid_orders():
    """Return grid orders.

    - If ?item=... is provided: return orders for that item.
    - If no item is provided: return ALL orders across items (for multi-coin table).
    """
    wa = _require_auth()

    # Early UX: if the user isn't authenticated yet, return an empty list instead
    # of spamming 401s / triggering CORS errors in the UI.
    if not wa:
        item_id = request.args.get("item") or request.args.get("item_id")
        if item_id:
            item_id = str(item_id).strip()
            return jsonify({"status": "ok", "item": item_id, "orders": [], "unauthenticated": True, "ts": now_ts()})
        return jsonify({"status": "ok", "orders": [], "unauthenticated": True, "ts": now_ts()})

    item_id = request.args.get("item") or request.args.get("item_id")

    if item_id:
        item_id = str(item_id).strip()
        session = _get_owned_session(item_id, wa)
        if not session:
            return jsonify({"status": "ok", "item": item_id, "orders": [], "ts": now_ts()})
        orders = session.get("orders") if isinstance(session, dict) else []
        # ensure item field
        out=[]
        for o in (orders or []):
            if isinstance(o, dict):
                oo=dict(o)
                oo["item"]=oo.get("item") or item_id
                out.append(oo)
        return jsonify({"status":"ok","item": item_id, "orders": out, "ts": now_ts()})

    # all items
    all_orders=[]
    for it, sess in (GRID_SESSIONS or {}).items():
        owner = _norm_addr(sess.get("wallet_address") or "") if isinstance(sess, dict) else ""
        if owner and owner != _norm_addr(wa):
            continue
        if not isinstance(sess, dict):
            continue
        for o in (sess.get("orders") or []):
            if isinstance(o, dict):
                oo=dict(o)
                oo["item"]=oo.get("item") or it
                all_orders.append(oo)
    return jsonify({"status":"ok","orders": all_orders, "ts": now_ts()})


@app.route("/api/grid/order/stop", methods=["POST"])
def api_grid_order_stop():
    """Cancel/stop a single grid order for an item.
    Accepts: {item, id} or {item, side, price, level}
    """
    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400

    sess = GRID_SESSIONS.get(item_id)
    if not isinstance(sess, dict):
        return jsonify({"error": f"no grid session for item '{item_id}'"}), 404

    oid = payload.get("id")
    side = payload.get("side")
    price = payload.get("price")
    level = payload.get("level")

    orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
    updated = False
    for o in orders:
        if not isinstance(o, dict):
            continue
        if o.get("status") != "OPEN":
            continue

        # Prefer id match if present
        if oid and str(o.get("id")) == str(oid):
            o["status"] = "CANCELLED"
            # Release locked budget (simple)
            try:
                if str(o.get("side") or "").upper() == "BUY":
                    locked = o.get("usd_locked") or o.get("usd")
                    if locked is not None:
                        locked = float(locked)
                        sess_locked = float(sess.get("wallet_locked_usd") or 0.0)
                        sess_avail = float(sess.get("wallet_available_usd") or 0.0)
                        sess["wallet_locked_usd"] = max(0.0, sess_locked - locked)
                        sess["wallet_available_usd"] = sess_avail + locked
            except Exception:
                pass
            o["cancelled_ts"] = int(time.time())
            updated = True
            break

        # Fallback match (for older orders without id)
        if (oid is None or oid == "") and side and o.get("side") == side and level is not None and o.get("level") == level:
            if price is None or o.get("price") == price:
                o["status"] = "CANCELLED"
                # Release locked budget (simple)
                try:
                    if str(o.get("side") or "").upper() == "BUY":
                        locked = o.get("usd_locked") or o.get("usd")
                        if locked is not None:
                            locked = float(locked)
                            sess_locked = float(sess.get("wallet_locked_usd") or 0.0)
                            sess_avail = float(sess.get("wallet_available_usd") or 0.0)
                            sess["wallet_locked_usd"] = max(0.0, sess_locked - locked)
                            sess["wallet_available_usd"] = sess_avail + locked
                except Exception:
                    pass
                o["cancelled_ts"] = int(time.time())
                updated = True
                break

    # Keep history tidy
    _grid_prune_history(sess)

    if not updated:
        return jsonify({"error": "order not found or not open"}), 404

    return jsonify({"status": "ok", "orders": sess.get("orders", []), "fills": sess.get("fills", []), "tick": sess.get("ticks", 0), "price": sess.get("price")})


@app.route("/api/grid/config", methods=["GET"])
def api_grid_config():
    item_id = request.args.get("item") or request.args.get("item_id")
    if not item_id:
        return err("missing 'item' query param", 400)

    item_id = str(item_id)
    cfg = GRID_CONFIGS.get(item_id)
    if not cfg:
        return err("no config for item (start grid first)", 404)

    return jsonify({"item": item_id, "config": cfg})



@app.route("/api/grid/autorun", methods=["POST"])
def api_grid_autorun():
    """
    Enable/disable automatic ticking with real live prices.
    Body: { item, enable: true/false, interval: seconds }
    """
    body = request.get_json(silent=True) or {}
    wa, policy, e = _require_trading_enabled()
    if e:
        return e
    access = _compute_access_status(wa)
    if not bool(access.get("can_open_new_trades")):
        return err("access required (no new trades allowed)", 403)
    item_id = str(body.get("item") or "").strip()
    session = _get_owned_session(item_id, wa)
    if not session:
        return err("forbidden", 403)
    # Only the owner wallet may control autorun for this grid session
    sess = _get_owned_session(item_id, wa)
    if sess is None:
        return err("forbidden", 403)
    if not item_id:
        return err("missing 'item' in body", 400)

    enable = bool(body.get("enable", True))
    interval = body.get("interval", 10)
    try:
        interval = float(interval)
        if interval < 2:
            interval = 2.0
    except Exception:
        interval = 10.0

    # stop existing if any
    cur = GRID_AUTORUN.pop(item_id, None)
    if cur and cur.get("stop"):
        try:
            cur["stop"].set()
        except Exception:
            pass

    if not enable:
        return jsonify({"status": "ok", "item": item_id, "autorun": False})

    stop_evt = threading.Event()
    th = threading.Thread(target=_autorun_loop, args=(item_id, stop_evt, interval), daemon=True)
    GRID_AUTORUN[item_id] = {"stop": stop_evt, "thread": th, "interval": interval}
    th.start()

    return jsonify({"status": "ok", "item": item_id, "autorun": True, "interval": interval})



@app.route("/api/grid/manual/add", methods=["POST"])
def api_grid_manual_add():
    """
    Add a manual simulated order to the current grid session.
    Body: { item, side: BUY/SELL, price, qty(optional) }
    """
    body = request.get_json(silent=True) or {}
    wa, policy, e = _require_trading_enabled()
    if e:
        return e
    # Access gate: manual add opens a new trade/order
    st = _compute_access_status(wa)
    if not bool(st.get('can_open_new_trades')):
        return err('access required (no new trades allowed)', 403)
    item_id = str(body.get("item") or "").strip()
    side = str(body.get("side") or "").upper().strip()
    price = body.get("price")
    qty = body.get("qty", body.get("amount", None))
    usd = body.get("usd")
    ttl_s = body.get("ttl_s") or body.get("ttl")
    # ttl_s is currently informational (frontend may send it); simulation keeps manual orders until filled/cancelled.

    if not item_id:
        return err("missing 'item' in body", 400)
    if side not in ("BUY", "SELL"):
        return err("side must be BUY or SELL", 400)
    try:
        price = float(price)
        if not (price > 0):
            raise ValueError()
    except Exception:
        return err("invalid 'price'", 400)

    session = GRID_SESSIONS.get(item_id)
    session = _get_owned_session(item_id, wa)
    if not session:
        return err("forbidden", 403)
    if not session:
        return err("grid not started (press Start first)", 404)

    try:
        # Allow BUY orders specified by USD budget (compute qty from the trigger price).
        # SELL orders remain qty-based.
        if qty is None and usd is not None and side == "BUY":
            usd = float(usd)
            if usd <= 0:
                return err("invalid 'usd' amount", 400)
            qty = usd / float(price)

        if qty is not None:
            qty = float(qty)

        # Wallet budget gate (simple): prevent BUY orders from exceeding available USD
        try:
            if side == 'BUY' and usd is not None:
                usd_f = float(usd)
                avail = float(session.get('wallet_available_usd') or 0.0)
                if usd_f > avail + 1e-9:
                    return err('insufficient available wallet budget', 403)
                session['wallet_locked_usd'] = float(session.get('wallet_locked_usd') or 0.0) + usd_f
                session['wallet_available_usd'] = max(0.0, avail - usd_f)
        except Exception:
            pass
        oid = f"m{int(time.time()*1000)}"
        order = {
            "id": oid,
            "item": item_id,
            "side": side,
            "price": round(price, 8),
            "status": "OPEN",
            "level": "MANUAL",
            "manual": True,
            "created_ts": int(time.time()),
        }
        if qty is not None:
            order["qty"] = round(float(qty), 8)
        if usd is not None:
            # informational (front-end can show "buy $X at price Y")
            order["usd"] = round(float(usd), 2)
            if side == "BUY":
                order["usd_locked"] = round(float(usd), 2)

        session.setdefault("orders", []).append(order)
        # Keep stable ordering: BUY desc, SELL asc, manual mixed in appropriately
        try:
            buys = sorted([o for o in session["orders"] if o.get("side") == "BUY"], key=lambda x: float(x.get("price") or 0), reverse=True)
            sells = sorted([o for o in session["orders"] if o.get("side") == "SELL"], key=lambda x: float(x.get("price") or 0))
            session["orders"] = buys + sells
        except Exception:
            pass

        GRID_SESSIONS[item_id] = _trim_grid_session(session)
        _persist_grid_state()
        return jsonify({"status": "ok", "item": item_id, "order": order, "orders": session.get("orders", []), "fills": session.get("fills", []), "tick": int(session.get("ticks") or 0), "price": session.get("price")})
    except Exception as e:
        return err(str(e), 500)


# --- Manual order aliases (compat for older frontends) ---
@app.route("/api/grid/add", methods=["POST"])
def api_grid_add_alias():
    return api_grid_manual_add()

@app.route("/api/grid/order/add", methods=["POST"])
def api_grid_order_add_alias():
    return api_grid_manual_add()

@app.route("/api/add", methods=["POST"])
def api_add_alias():
    return api_grid_manual_add()

@app.route("/api/grid/manual", methods=["POST"])
def api_grid_manual_alias():
    return api_grid_manual_add()


# -------------------------
# AI Run (backend-native context builder)
# -------------------------

def _ai_call_openai(sys_prompt: str, user_payload: dict, wallet_address: str | None = None, mem_msgs: list | None = None):
    """Shared OpenAI call helper used by /api/ai and /api/ai/run."""
    openai_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_KEY") or ""
    if not openai_key:
        return None, ("missing OPENAI_API_KEY", 500)

    model = os.getenv("OPENAI_MODEL") or "gpt-4.1-mini"

    headers = {
        "Authorization": f"Bearer {openai_key}",
        "Content-Type": "application/json",
    }

    user = json.dumps(user_payload, ensure_ascii=False)

    payload = {
        "model": model,
        "input": ([{"role": "system", "content": sys_prompt}] + (mem_msgs if isinstance(mem_msgs, list) else []) + [{"role": "user", "content": user}]),
        "temperature": 0.3,
        "max_output_tokens": 900,
    }

    try:
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json=payload,
            timeout=45,
        )
        r.raise_for_status()
        data = r.json() or {}

        # Extract output text from Responses API
        ans = ""
        try:
            out = data.get("output") or []
            for item in out:
                cont = item.get("content") if isinstance(item, dict) else None
                if not isinstance(cont, list):
                    continue
                for c in cont:
                    if isinstance(c, dict) and c.get("type") == "output_text":
                        ans += str(c.get("text") or "")
        except Exception:
            ans = ""

        if not ans:
            # Fallback: try legacy shape
            ans = (data.get("output_text") or "").strip()

        if wallet_address:
            try:
                _ai_mem_append(wallet_address, str(user_payload.get("question") or ""), ans, max_msgs=10)
            except Exception:
                pass

        return {"status": "ok", "answer": ans, "model": model}, None

    except requests.exceptions.HTTPError as e:
        try:
            err_body = r.text  # type: ignore
        except Exception:
            err_body = str(e)
        return None, (f"OpenAI HTTP error: {err_body}", 502)
    except Exception as e:
        return None, (f"OpenAI request failed: {e}", 502)


def _resolve_ids_from_symbols(symbols: list[str]) -> dict:
    """Return dict symbol->coingecko_id for symbols we can resolve."""
    out = {}
    for s in symbols or []:
        sym = (s or "").strip().upper()
        if not sym:
            continue
        cid = _cg_resolve_symbol(sym) or _resolve_cg_id(sym)
        if cid:
            out[sym] = cid
    return out


def _normalize_snapshot(snap: dict) -> dict:
    if not isinstance(snap, dict):
        return {}
    # Support both single and batch shapes
    if "change24h" in snap or "volume24h" in snap:
        return {
            "price": snap.get("price"),
            "change24h": snap.get("change24h"),
            "volume24h": snap.get("volume24h"),
            "source": snap.get("source") or "coingecko",
        }
    return {
        "price": snap.get("price"),
        "change24h": snap.get("change24") if snap.get("change24") is not None else snap.get("change24h"),
        "volume24h": snap.get("volume24") if snap.get("volume24") is not None else snap.get("volume24h"),
        "source": snap.get("source") or "coingecko",
    }


def _build_ai_market_context(symbols: list[str], profile: str = "conservative", include_health: bool = True) -> dict:
    """Build compact numeric-only context for AI from market snapshots."""
    symbols = [(s or "").strip().upper() for s in (symbols or []) if (s or "").strip()]
    symbols = list(dict.fromkeys(symbols))  # de-dupe preserve order
    if len(symbols) > 6:
        symbols = symbols[:6]

    id_map = _resolve_ids_from_symbols(symbols)
    ids = [id_map.get(sym) for sym in symbols if id_map.get(sym)]
    snaps_by_id = _cg_market_snapshots_batch(ids)

    coins = []
    for sym in symbols:
        cid = id_map.get(sym)
        snap_raw = snaps_by_id.get(cid) if cid else None
        snap = _normalize_snapshot(snap_raw or {})
        item = {
            "symbol": sym,
            "id": cid,
            "price": snap.get("price"),
            "change24h": snap.get("change24h"),
            "volume24h": snap.get("volume24h"),
            "source": snap.get("source"),
        }

        # Trading suitability (always safe/informational)
        try:
            if cid and snap:
                item["suitability"] = _suitability_for_snapshot(sym, snap, profile)
        except Exception:
            pass

        # Health (optional)
        if include_health and cid and snap:
            try:
                row = {"price": snap.get("price"), "change24h": snap.get("change24h"), "volume24h": snap.get("volume24h")}
                item["health"] = compute_market_health(row, sym, None)
            except Exception:
                pass

        coins.append(item)

    return {
        "ts": now_ts(),
        "profile": (profile or "conservative").strip().lower(),
        "include_health": bool(include_health),
        "coins": coins,
        "note": "Numbers are snapshots; suitability is informational only.",
    }


def _ai_kind_instructions(kind: str) -> str:
    k = (kind or "").strip().lower()
    if k in ("quick_overview", "overview"):
        return "Give a concise market overview for the selected coins."
    if k in ("risk_check", "risk"):
        return "Focus on risks, liquidity/volume, volatility, and what could invalidate a grid setup."
    if k in ("compare", "comparison"):
        return "Compare the selected coins and rank which are most suitable for grid trading under the chosen profile."
    if k in ("grid_plan", "grid", "plan"):
        return "Provide an educational manual grid plan template (range, spacing, number of orders, risk notes). Do NOT output specific buy/sell price levels."
    return "Answer the user's question based on the provided context."


@app.route("/api/ai/run", methods=["POST"])
def api_ai_run():
    """Backend-native AI endpoint. Builds context from symbols and profile/health toggle.

    Expects JSON:
      {
        "kind": "quick_overview"|"risk_check"|"compare"|"grid_plan"|"ask",
        "symbols": ["BTC","ETH", ...]  (max 6),
        "profile": "conservative"|"balanced"|"volatility",
        "include_health": true|false,
        "question": "..." (optional; required for kind=ask)
      }

    Returns: {status, answer, model, context_used}
    """
    body = request.get_json(silent=True) or {}
    kind = str(body.get("kind") or "ask")
    symbols = body.get("symbols") or []
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
    if not isinstance(symbols, list):
        return err("symbols must be a list or comma-separated string", 400)

    profile = str(body.get("profile") or "conservative").strip().lower()
    if profile not in ("conservative", "balanced", "volatility"):
        profile = "conservative"

    include_health = bool(body.get("include_health", True))
    question = str(body.get("question") or "").strip()

    # Enforce max 6 coins server-side
    sym_norm = [(s or "").strip().upper() for s in symbols if (s or "").strip()]
    sym_norm = list(dict.fromkeys(sym_norm))
    if len(sym_norm) > 6:
        return err("max 6 symbols allowed", 400)
    if not sym_norm:
        return err("no symbols provided", 400)

    # Auth is optional for AI, but if present we use it for memory scoping
    wa = _require_auth()

    context = _build_ai_market_context(sym_norm, profile=profile, include_health=include_health)

    sys = f"""You are Nexus Analyt AI, a crypto market analyst.

Rules:
0) Always respond in the same language as the user's question. If the user mixes languages, use the dominant one.
1) Use ONLY the symbols present in the provided JSON context.
2) Use ONLY the numbers provided in the JSON (do not invent prices, volumes, metrics, scores, or levels).
3) Provide informational analysis only. No financial advice. No buy/sell instructions.
4) Do NOT output exact trade entries/exits or prescriptive price levels. If asked, provide an educational template instead.
5) The app is MANUAL-only: never suggest automatic order placement; focus on manual decision support.

Task:
{_ai_kind_instructions(kind)}
"""

    user_payload = {
        "kind": kind,
        "question": question,
        "profile": profile,
        "include_health": include_health,
        "context": context,
    }

    resp, err_pair = _ai_call_openai(sys, user_payload, wallet_address=wa, mem_msgs=_ai_mem_get(wa) if wa else None)
    if err_pair:
        msg, code = err_pair
        return err(msg, code)

    # Return a small echo of which symbols were used for transparency
    resp["context_used"] = {"symbols": sym_norm, "profile": profile, "include_health": include_health}
    return jsonify(resp)

# -------------------------
# AI proxy (Frontend -> Backend -> TBP-Advisor -> OpenAI)
# -------------------------


@app.route("/api/ai/memory", methods=["GET"])
def api_ai_memory_get():
    wa = str(request.args.get("wallet_address") or "").strip()
    mem = _ai_mem_get(wa) if wa else []
    return jsonify({"status": "ok", "wallet_address": _norm_addr(wa), "memory": mem})


@app.route("/api/ai/memory/clear", methods=["POST"])
def api_ai_memory_clear():
    body = request.get_json(silent=True) or {}
    wa = str(body.get("wallet_address") or "").strip()
    wa = _norm_addr(wa)
    if not wa:
        return jsonify({"status": "error", "error": "missing_wallet_address"}), 400
    _ai_mem_put(wa, [])
    return jsonify({"status": "ok", "wallet_address": wa})


@app.route("/api/ai", methods=["POST"])
def api_ai():
    """
    Direct OpenAI-backed AI endpoint (NO TBP fallback).
    Expects JSON:
      { "mode": "...", "question": "...", "context": {...} }

    The model is instructed to ONLY use provided context JSON and to NEVER mention unrelated tokens.
    """
    body = request.get_json(silent=True) or {}

    mode = str(body.get("mode") or "analysis").strip().lower()
    question = str(body.get("question") or "").strip()
    context = body.get("context") or {}
    wallet_address = str(body.get("wallet_address") or "").strip()
    mem_msgs = _ai_mem_get(wallet_address) if wallet_address else []

    if not question:
        return err("missing question", 400)

    openai_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not openai_key:
        return err("missing_openai_key (set OPENAI_API_KEY in backend env)", 400)

    # You can override via env; keep a safe default.
    model = str(os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

    # System prompt differs by mode:
    # - analysis: used by Quick Buttons (structured, concise)
    # - chat: used by Ask AI (answers the user's question directly)
    if mode == "chat":
        sys = """You are Nexus Analyt AI, a crypto market analyst.

Rules:
0) Always respond in the same language as the user's question. If the user mixes languages, use the dominant one.
1) Use ONLY the symbols present in the provided JSON context.
2) Use ONLY the numbers provided in the JSON (do not invent prices, volumes, metrics, scores, or levels).
3) NEVER mention TurboPepe/TBP or any unrelated token unless it appears in context.
4) Answer the user's question FIRST and directly. Do NOT force a fixed report format.
   - If the user asks for a "grid plan", provide an educational, step-by-step grid plan template (range, spacing, number of orders, risk notes).
   - Do NOT output specific buy/sell price levels or prescriptive trading signals.
   - If a required value is missing from context, say "data not available".
5) No financial advice. No buy/sell instructions.
"""
    else:
        sys = """You are Nexus Analyt AI, a crypto market analyst.

Rules:
0) Always respond in the same language as the user's question. If the user mixes languages, use the dominant one.
1) Analyze ONLY the symbols present in the provided JSON context.
2) Use ONLY the numbers provided in the JSON (do not invent prices, volumes, or metrics).
3) NEVER mention TurboPepe/TBP or any unrelated token unless it appears in context.
4) Provide a structured answer: Summary, Why, What to watch, Risks, and (if compare present) Comparison.
5) No financial advice. No buy/sell instructions.
"""


    # Keep user content compact but complete.
    user = json.dumps(
        {
            "mode": mode,
            "question": question,
            "context": context,
        },
        ensure_ascii=False,
    )

    headers = {
        "Authorization": f"Bearer {openai_key}",
        "Content-Type": "application/json",
    }

    # Prefer Responses API; it works well for both simple and complex prompts.
    payload = {
        "model": model,
        "input": ([{"role": "system", "content": sys}] + (mem_msgs[-6:] if isinstance(mem_msgs, list) else []) + [{"role": "user", "content": user}]),
        "temperature": 0.3,
        "max_output_tokens": 900,
    }

    try:
        r = requests.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json=payload,
            timeout=45,
        )
        r.raise_for_status()
        data = r.json()

        # Extract output text from Responses API
        ans = ""
        out = data.get("output")
        if isinstance(out, list):
            parts = []
            for item in out:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict) and c.get("type") in ("output_text", "text") and isinstance(c.get("text"), str):
                            parts.append(c["text"])
            if parts:
                ans = "\n".join(parts).strip()

        # Fallback: some SDK/proxies may return "output_text" at top-level
        if not ans and isinstance(data.get("output_text"), str):
            ans = data["output_text"].strip()

        # Fallback: Chat Completions-style response (if gateway returns that)
        if not ans:
            choices = data.get("choices")
            if isinstance(choices, list) and choices:
                msg = choices[0].get("message") if isinstance(choices[0], dict) else None
                if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                    ans = msg["content"].strip()

        if not ans:
            # As a last resort, return the raw JSON (trimmed)
            return jsonify({"status": "ok", "answer": json.dumps(data, ensure_ascii=False, indent=2), "model": model})

        # Guardrail: if model still mentions TBP, replace with a safe message.
        if "tbp" in ans.lower() or "turbopepe" in ans.lower():
            return jsonify(
                {
                    "status": "ok",
                    "answer": "⚠️ AI output contained an unrelated token reference (TBP). Please retry; the request context will be re-sent and restricted.",
                    "model": model,
                }
            )

        if wallet_address:
            try:
                _ai_mem_append(wallet_address, question, ans, max_msgs=10)
            except Exception:
                pass

        return jsonify({"status": "ok", "answer": ans, "model": model})

    except requests.exceptions.HTTPError as e:
        # Return OpenAI HTTP error body for debugging
        try:
            err_body = r.text
        except Exception:
            err_body = str(e)
        return err(f"openai_http_error: {err_body}", 502)
    except Exception as e:
        return err(f"openai_error: {str(e)}", 502)


def _cg_set_symbol_id_cache(symbol: str, coin_id: str):
    if not symbol or not coin_id:
        return
    symbol = symbol.strip().upper()
    _cg_cache_set(f"id|{symbol}", coin_id)

def _cg_search_best_id_for_symbol(symbol: str):
    symbol = (symbol or "").strip()
    if not symbol:
        return None
    url = f"{COINGECKO_BASE}/search"
    j = _cg_request_json(url, params={"query": symbol}, timeout=12) or {}
    coins = j.get("coins") if isinstance(j, dict) else None
    if not isinstance(coins, list) or not coins:
        return None
    sym_u = symbol.upper()
    for c in coins:
        if isinstance(c, dict) and (c.get("symbol") or "").upper() == sym_u:
            return c.get("id")
    c0 = coins[0]
    return c0.get("id") if isinstance(c0, dict) else None

# backend/app.py


# --- (dedup) removed embedded duplicate Flask app block ---

def _sim_build(cfg: dict) -> dict:
    item = str(cfg.get("item_id") or cfg.get("item") or "").strip()
    mode = str(cfg.get("mode") or "SAFE").upper()

    # Optional: allow frontend to pass current price; otherwise start at 1.0
    try:
        base_price = float(cfg.get("price") or cfg.get("start_price") or 1.0)
        if not math.isfinite(base_price) or base_price <= 0:
            base_price = 1.0
    except Exception:
        base_price = 1.0

    # Defaults tuned for demo
    step_pct = float(cfg.get("grid_step_pct") or (0.25 if mode == "AGGRESSIVE" else 0.5))
    levels_each_side = int(cfg.get("levels") or cfg.get("grid_levels_each_side") or (12 if mode == "AGGRESSIVE" else 10))
    tp_pct = float(cfg.get("take_profit_pct") or (30.0 if mode == "AGGRESSIVE" else 50.0))
    sl_pct = float(cfg.get("stop_loss_pct") or (15.0 if mode == "AGGRESSIVE" else 20.0))

    # --- AUTO: invest_usd -> qty planning for BUY orders ---
    # Frontend can send invest_usd (e.g. 1000). We split it evenly across BUY legs.
    invest_usd = cfg.get("invest_usd") if cfg.get("invest_usd") is not None else cfg.get("initial_capital_usd")
    try:
        invest_usd = float(invest_usd) if invest_usd is not None else None
        if invest_usd is not None and invest_usd <= 0:
            invest_usd = None
    except Exception:
        invest_usd = None

    buy_orders_count = int(levels_each_side)  # 1 BUY per level
    budget_per_buy = (invest_usd / buy_orders_count) if (invest_usd is not None and buy_orders_count > 0) else None

    # Build initial grid levels (as "planned" orders)
    orders = []
    for i in range(1, levels_each_side + 1):
        buy_p = base_price * (1.0 - (step_pct/100.0) * i)
        sell_p = base_price * (1.0 + (step_pct/100.0) * i)
        buy_order = {
            "id": f"a{item}_B{-i}",
            "item": item,
            "side": "BUY",
            "price": round(buy_p, 8),
            "status": "OPEN",
            "level": -i,
        }

        if budget_per_buy is not None:
            try:
                if buy_p > 0:
                    buy_order["qty"] = round(budget_per_buy / buy_p, 8)
                    buy_order["usd"] = round(budget_per_buy, 2)  # optional (nice for UI)
            except Exception:
                pass

        orders.append(buy_order)
        orders.append({
            "id": f"a{item}_S{i}",
            "item": item,
            "side": "SELL",
            "price": round(sell_p, 8),
            "status": "OPEN",
            "level": i,
        })

    session = {
        "item": item,
        "mode": mode,
        "ticks": 0,
        "price": base_price,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "orders": orders,
        "fills": [],
        "created_ts": int(time.time()),
        "rng": random.Random(_sim_seed(item)),
        "initial_capital_usd": float(cfg.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
        # Wallet budget (simple model): used to prevent overtrading
        "wallet_total_usd": float(cfg.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
        "wallet_locked_usd": 0.0,
        "wallet_available_usd": float(cfg.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
    }
    _ensure_pnl(session)
    _pnl_mark(session, base_price)
    return session

def _sim_tick(session: dict, new_price: Optional[float] = None) -> dict:
    """
    One simulation step using REAL price (frontend/snapshot/history).

    - Uses prev_price = session["price"] as the previous tick reference.
    - If new_price is None, falls back to SNAPSHOTS[item]["data"]["price"] (if available).
    - Fills OPEN orders when price crosses (or jumps beyond) order levels.
    - Keeps OPEN orders, caps history via _trim_grid_session() upstream.
    """
    # previous price (truth source)
    try:
        prev_price = float(session.get("price") or 0.0)
    except Exception:
        prev_price = 0.0

    # pick current price
    price = None
    if new_price is not None and new_price != "":
        try:
            price = float(new_price)
        except Exception:
            price = None

    if price is None:
        item_key = str(session.get("item") or session.get("item_id") or "").strip()
        snap = SNAPSHOTS.get(item_key)
        if snap and isinstance(snap.get("data"), dict):
            try:
                price = float(snap["data"].get("price"))
            except Exception:
                price = None

    # No reliable new price -> only tick counter
    if price is None or not (price > 0):
        session["ticks"] = int(session.get("ticks") or 0) + 1
        session["last_price"] = prev_price
        session["filled_now"] = 0
        return session

    # advance tick
    session["ticks"] = int(session.get("ticks") or 0) + 1
    session["last_price"] = prev_price
    session["price"] = float(price)

    fills = session.get("fills") if isinstance(session.get("fills"), list) else []
    filled_now = 0

    for o in session.get("orders") or []:
        if not isinstance(o, dict):
            continue
        if o.get("status") != "OPEN":
            continue
        try:
            op = float(o.get("price") or 0.0)
        except Exception:
            continue
        side = str(o.get("side") or "").upper()

        if side == "BUY":
            crossed = (prev_price > op and price <= op) or (prev_price == 0 and price <= op)
            if crossed or price <= op:  # jump-through safety
                o["status"] = "FILLED"
                o["filled_ts"] = int(time.time())
                o["fill_price"] = round(float(price), 8)
                fills.append({k: o.get(k) for k in ("id", "side", "level", "price", "fill_price", "filled_ts", "qty", "usd")})
                filled_now += 1

        elif side == "SELL":
            crossed = (prev_price < op and price >= op) or (prev_price == 0 and price >= op)
            if crossed or price >= op:  # jump-through safety
                o["status"] = "FILLED"
                o["filled_ts"] = int(time.time())
                o["fill_price"] = round(float(price), 8)
                fills.append({k: o.get(k) for k in ("id", "side", "level", "price", "fill_price", "filled_ts", "qty", "usd")})
                filled_now += 1

    session["fills"] = fills[-500:]
    session["filled_now"] = filled_now
    return session


def _get_live_price_for_item(item_id: str) -> Optional[float]:
    """
    Fetch a FRESH real price for the given item_id using cached SNAPSHOTS metadata.
    - If snapshot mode == market -> CoinGecko (by id)
    - If snapshot mode == dex -> DexScreener (by contract)
    Updates SNAPSHOTS[item_id] with the new price when successful.
    Returns float price or None.
    """
    snap = SNAPSHOTS.get(item_id)
    data = snap.get("data") if isinstance(snap, dict) else None
    if not isinstance(data, dict):
        return None

    try:
        if data.get("mode") == "market" and data.get("id"):
            live = _cg_market_snapshot(str(data["id"]))
            p = live.get("price")
            if p is None:
                return None
            p = float(p)
            data["price"] = p
            SNAPSHOTS[item_id] = {"ts": now_ts(), "data": data}
            return p

        if data.get("mode") == "dex" and data.get("contract"):
            live = _dexscreener_snapshot(str(data["contract"]))
            p = live.get("price")
            if p is None:
                return None
            p = float(p)
            data["price"] = p
            SNAPSHOTS[item_id] = {"ts": now_ts(), "data": data}
            return p
    except Exception:
        return None

    return None


def _autorun_loop(item_id: str, stop_evt: threading.Event, interval: float):
    """Background loop: refresh live price and tick the simulation."""
    while not stop_evt.is_set():
        try:
            session = GRID_SESSIONS.get(item_id)
            if session:
                p = _get_live_price_for_item(item_id)
                _sim_tick(session, new_price=p)
                GRID_SESSIONS[item_id] = _trim_grid_session(session)
                _persist_grid_state()
        except Exception:
            pass
        stop_evt.wait(interval)


# --- (dedup) removed duplicate route definitions (kept first set) ---

if __name__ == "__main__":

    import os
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))
    app.run(host=host, port=port, debug=True)
