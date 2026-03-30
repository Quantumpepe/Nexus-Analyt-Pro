# backend/app.py
from __future__ import annotations
from flask import Flask, jsonify, request
from flask_cors import CORS

import os
import time
import threading
import json
import re
import sqlite3
import threading
DB_WRITE_LOCK = threading.RLock()

import secrets
import uuid
import requests
import random
import math
from typing import Optional, Dict, Any
# --- Defaults for manual orders ---
DEFAULT_SLIPPAGE_BPS = int(os.getenv("DEFAULT_SLIPPAGE_BPS", "500"))   # 500 bps = 5%
DEFAULT_DEADLINE_MINUTES = int(os.getenv("DEFAULT_DEADLINE_MINUTES", "1200"))  # actually seconds in this code (20min)

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
                return _inst.build_grid(cfg)
            except TypeError:
                continue
    raise RuntimeError("grid_sim: could not find callable build_grid(cfg)")

def _grid_step(state, cfg, snapshot):
    if hasattr(grid_sim, "step_sim") and callable(getattr(grid_sim, "step_sim")):
        try:
            return grid_sim.step_sim(state, cfg, snapshot)
        except TypeError as e:
            msg = str(e)
            if "positional argument" not in msg:
                raise
    for _name, _cls in _inspect.getmembers(grid_sim, _inspect.isclass):
        if hasattr(_cls, "step_sim") and callable(getattr(_cls, "step_sim")):
            try:
                _inst = _cls()
                return _inst.step_sim(state, cfg, snapshot)
            except TypeError:
                continue
    raise RuntimeError("grid_sim: could not find callable step_sim(state,cfg,snapshot)")

# Expose GridConfig regardless of how grid_sim defines it
GridConfig = getattr(grid_sim, "GridConfig", None)
if GridConfig is None:
    raise ImportError("grid_sim does not export GridConfig")


# -------------------------
# App init
# -------------------------
app = Flask(__name__)
app.url_map.strict_slashes = False

# Accept both /path and /path/ to avoid 404s due to trailing slashes


# Enable CORS for all API routes (UI is on a different domain)
# ---- CORS ----
# Frontend and backend are on different domains. The frontend uses fetch(..., credentials: 'include'),
# so we MUST:
#  - echo a concrete Origin (not '*')
#  - set Access-Control-Allow-Credentials: true
FRONTEND_ORIGINS = [o.strip() for o in (os.getenv("FRONTEND_ORIGINS") or "https://nexus-analyt-ui.onrender.com,http://localhost:5173,http://localhost:3000").split(",") if o.strip()]
FRONTEND_ORIGINS_SET = set(FRONTEND_ORIGINS)

CORS(
    app,
    resources={r"/api/*": {"origins": FRONTEND_ORIGINS}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization", "X-Wallet-Address"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
)

@app.get("/api/version")
def api_version():
    return {
        "status": "ok",
        "ts": int(time.time()),
        "render_git_commit": os.getenv("RENDER_GIT_COMMIT"),
        "grid_allow_anon": os.getenv("GRID_ALLOW_ANON"),
    }

from flask import request

@app.after_request
def _na_add_cors(resp):
    origin = request.headers.get("Origin")
    if origin and origin in FRONTEND_ORIGINS_SET:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Headers"] = (
           "Content-Type, Authorization, X-Wallet-Address, x-wallet-address, X-API-Key, x-api-key"
        )
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    return resp

@app.route("/api/<path:_path>", methods=["OPTIONS"])
def _na_cors_preflight(_path):
    return ("", 204)
    
import traceback
from flask import jsonify

@app.errorhandler(Exception)
def _all_errors(e):
    # TEMP: debug output (remove later)
    tb = traceback.format_exc()
    return jsonify({"status": "error", "error": str(e), "trace": tb}), 500

@app.get("/api/ping")
def ping():
    return "ok", 200

@app.get("/api/version")
def version():
    import os
    return {"version": os.getenv("RENDER_GIT_COMMIT", "unknown")}, 200


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
    "https://www.nexus-analyt-ui.onrender.com",
]

# Allow-list matcher (defensive): some proxy error paths can omit CORS headers.
# We therefore also mirror headers manually for known frontend origins.
_FRONTEND_ORIGIN_RE = re.compile(r"^(https://)?(www\.)?nexus-analyt-(ui|pro)\.onrender\.com$")

def _is_allowed_origin(origin: str) -> bool:
    if not origin:
        return False
    if origin in FRONTEND_ORIGINS:
        return True
    # Accept Render subdomains for this project (ui/pro)
    return bool(_FRONTEND_ORIGIN_RE.match(origin))

CORS(
    app,
    resources={r"/api/*": {"origins": "*"}},
    supports_credentials=False,
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type"],
    max_age=86400,
)


from flask import make_response


@app.after_request
def _add_cors_headers(resp):
    """Ensure every /api response has correct CORS headers.

    The frontend uses fetch(..., credentials: 'include'), therefore:
      - Access-Control-Allow-Origin must be the requesting origin (not '*')
      - Access-Control-Allow-Credentials must be 'true'
    """
    try:
        origin = request.headers.get("Origin")

        if origin and origin in FRONTEND_ORIGINS_SET:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Vary"] = "Origin"
        else:
            # Non-browser clients (no Origin) are fine. For unknown origins, don't enable credentials.
            if origin:
                resp.headers["Access-Control-Allow-Origin"] = origin
                resp.headers["Vary"] = "Origin"

        resp.headers.setdefault("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
        resp.headers.setdefault("Access-Control-Allow-Headers", "Content-Type, Authorization")
    except Exception:
        pass
    return resp

@app.before_request
def _handle_options_preflight():
    """Ensure preflight requests always get correct CORS headers."""
    if request.method != "OPTIONS":
        return None

    origin = request.headers.get("Origin")
    resp = make_response("", 204)

    if origin and origin in FRONTEND_ORIGINS_SET:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Vary"] = "Origin"
    else:
        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Vary"] = "Origin"

    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
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
# CoinGecko Pro (server-side only)
# -------------------------
# Use CoinGecko Pro if COINGECKO_API_KEY is set (recommended).
# Otherwise fall back to public API (may rate-limit).
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY") or os.getenv("CG_PRO_API_KEY") or ""
COINGECKO_BASE = os.getenv("COINGECKO_BASE_URL") or (
    "https://pro-api.coingecko.com/api/v3" if COINGECKO_API_KEY else "https://api.coingecko.com/api/v3"
)

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
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
    r = requests.get(url, headers=headers, timeout=12)
    r.raise_for_status()
    data = r.json()
    _CG_CACHE[url] = (now, data)
    return data


@app.route("/api/contracts", methods=["GET"])
def api_contracts():
    # Expose the active chain contract addresses (Vault/Executor/Router).
    # This helps the frontend/bot stay in sync with Render ENV after deploys.
    out = {
        "enabledEvmChains": list(_ENABLED_EVM_CHAINS),
        "chains": {}
    }

    # For UI/UX: explicit native symbols, and a backward-compatible "native" field.
    native_symbol_by_chain_id = {1: "ETH", 56: "BNB", 137: "POL"}
    for key in _ENABLED_EVM_CHAINS:
        cid = int(_CHAIN_ID_BY_KEY.get(key, 0) or 0)
        if cid <= 0:
            continue
        out["chains"][key] = {
            "chainId": cid,
            "rpc": (_RPC_URL_BY_CHAIN.get(cid) or ""),
            "usdc": (_USDC_BY_CHAIN.get(cid) or ""),
            "usdt": (_USDT_BY_CHAIN.get(cid) or ""),
            "vault": (_VAULT_BY_CHAIN.get(cid) or ""),
            "executor": (_EXECUTOR_BY_CHAIN.get(cid) or ""),
            "router": (_ROUTER_BY_CHAIN.get(cid) or ""),
            "routerV3": (_ROUTER_V3_BY_CHAIN.get(cid) or ""),
            "wnative": (_WNATIVE_BY_CHAIN.get(cid) or ""),
            "native": native_symbol_by_chain_id.get(cid, key),
            "nativeSymbol": native_symbol_by_chain_id.get(cid, key),
        }
    return jsonify(out)


def _hex_to_bool(h: str) -> bool:
    try:
        return int(str(h or "0x0"), 16) != 0
    except Exception:
        return False



def _normalize_chain_key(raw: str) -> str:
    s = str(raw or "").strip().upper()
    if ":" in s:
        s = s.split(":", 1)[0].strip().upper()
    alias = {
        "137": "POL",
        "POLYGON": "POL",
        "MATIC": "POL",
        "56": "BNB",
        "BSC": "BNB",
        "1": "ETH",
        "ETHEREUM": "ETH",
    }
    return alias.get(s, s)

def _rpc_url_for_chain(chain_id: int) -> str:
    cid = int(chain_id or 0)

    # Existing configured map first
    direct = (_RPC_URL_BY_CHAIN.get(cid) or "").strip()
    if direct:
        return direct

    # Alternate env names seen across deployments
    env_fallbacks = {
        1: [
            os.getenv("ALCHEMY_RPC_ETH"),
            os.getenv("RPC_URL_ETH"),
            os.getenv("RPC_URL_1"),
        ],
        56: [
            os.getenv("ALCHEMY_RPC_BNB"),
            os.getenv("RPC_URL_BNB"),
            os.getenv("RPC_URL_56"),
        ],
        137: [
            os.getenv("ALCHEMY_RPC_POL"),
            os.getenv("RPC_URL_POL"),
            os.getenv("RPC_URL_POLYGON"),
            os.getenv("RPC_URL_137"),
        ],
    }
    for v in env_fallbacks.get(cid, []):
        if str(v or "").strip():
            return str(v).strip()

    # Last-resort Alchemy construction from a single key
    alchemy_key = str(os.getenv("ALCHEMY_KEY") or "").strip()
    if alchemy_key:
        if cid == 1:
            return f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
        if cid == 56:
            return f"https://bnb-mainnet.g.alchemy.com/v2/{alchemy_key}"
        if cid == 137:
            return f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}"

    return ""

def _vault_balance_selector_for_chain(chain_key: str) -> str:
    ck = str(chain_key or "").strip().upper()
    if ck == "POL":
        return "0x7754e652"  # polBalance(address)
    if ck == "BNB":
        return "0x7f4d17bf"  # bnbBalance(address)
    return "0x87f38d31"      # ethBalance(address)


def _vault_state_read(wallet_address: str, chain_key: str) -> dict:
    wa = _norm_addr(wallet_address or "")
    ck = _normalize_chain_key(chain_key)
    cid = int(_CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if not wa or not _looks_like_evm_addr(wa):
        raise RuntimeError("invalid wallet")
    if cid <= 0:
        raise RuntimeError("invalid chain")

    vault_addr = (_VAULT_BY_CHAIN.get(cid) or "").strip()
    if not vault_addr:
        raise RuntimeError(f"vault not configured for {ck}")

    balance_sel = _vault_balance_selector_for_chain(ck)
    in_cycle_sel = "0x7870293e"       # inCycle(address)
    held_token_sel = "0x90ba1a44"     # heldToken(address)
    held_token_bal_sel = "0x4ad59fe9" # heldTokenBal(address)
    is_operator_for_sel = "0xd95b6371" # isOperatorFor(address,address)

    def _safe_call(data: str, default: str = "0x0") -> str:
        try:
            out = _eth_call(cid, vault_addr, data)
            return out if str(out or "").startswith("0x") else default
        except Exception:
            return default

    # IMPORTANT:
    # Vault balance must be wallet-bound, not contract-global.
    # Use the vault contract's per-wallet selector:
    #   POL -> polBalance(address)
    #   BNB -> bnbBalance(address)
    #   ETH -> ethBalance(address)
    balance_hex = _safe_call(balance_sel + _addr_to_32(wa), "0x0")
    balance_wei = _hex_to_int(balance_hex or "0x0")

    in_cycle_hex = _safe_call(in_cycle_sel + _addr_to_32(wa), "0x0")

    held_token_hex = _safe_call(
        held_token_sel + _addr_to_32(wa),
        "0x" + ("0" * 64),
    )

    held_bal_hex = _safe_call(held_token_bal_sel + _addr_to_32(wa), "0x0")

    operator_addr = (_EXECUTOR_BY_CHAIN.get(cid) or "").strip()
    operator_enabled = False
    if _looks_like_evm_addr(operator_addr):
        op_hex = _safe_call(
            is_operator_for_sel + _addr_to_32(wa) + _addr_to_32(operator_addr),
            "0x0",
        )
        operator_enabled = _hex_to_bool(op_hex)

    held_bal_raw = _hex_to_int(held_bal_hex or "0x0")

    held_token_addr = ""
    try:
        held_token_addr = _topic_to_addr(held_token_hex) if str(held_token_hex or "").startswith("0x") else ""
        if held_token_addr.lower() == "0x0000000000000000000000000000000000000000":
            held_token_addr = ""
    except Exception:
        held_token_addr = ""

    return {
        "status": "ok",
        "wallet": wa,
        "chain": ck,
        "chainId": cid,
        "vault": vault_addr,
        "operator": operator_addr if _looks_like_evm_addr(operator_addr) else "",
        "vault_balance_wei": str(balance_wei),
        "vault_balance": float(balance_wei) / 1e18,
        "inCycle": _hex_to_bool(in_cycle_hex),
        "heldToken": held_token_addr,
        "heldTokenBalWei": str(held_bal_raw),
        "heldTokenBal": float(held_bal_raw) / 1e18,
        "operatorEnabled": bool(operator_enabled),
        "ts": now_ts(),
    }

@app.route("/api/vault/state", methods=["GET"])
def api_vault_state():
    wallet = (
        request.args.get("wallet")
        or request.args.get("wallet_address")
        or request.headers.get("X-Wallet-Address")
        or ""
    )
    chain = _normalize_chain_key(request.args.get("chain") or request.args.get("chain_key") or "POL")

    if not _looks_like_evm_addr(wallet):
        return jsonify({"status": "error", "error": "invalid wallet", "wallet": _norm_addr(wallet), "chain": chain, "ts": now_ts()})

    if chain not in _CHAIN_ID_BY_KEY:
        return jsonify({"status": "error", "error": "invalid chain", "wallet": _norm_addr(wallet), "chain": chain, "ts": now_ts()})

    if _ENABLED_EVM_CHAINS and chain not in _ENABLED_EVM_CHAINS:
        return jsonify({"status": "error", "error": "chain not enabled", "wallet": _norm_addr(wallet), "chain": chain, "ts": now_ts()})

    try:
        return jsonify(_vault_state_read(wallet, chain))
    except Exception as e:
        cid = int(_CHAIN_ID_BY_KEY.get(chain, 0) or 0)
        return jsonify({
            "status": "error",
            "error": str(e),
            "wallet": _norm_addr(wallet),
            "chain": chain,
            "chainId": cid,
            "rpc_configured": bool(_rpc_url_for_chain(cid)),
            "vault_configured": bool((_VAULT_BY_CHAIN.get(cid) or "").strip()),
            "executor_configured": bool((_EXECUTOR_BY_CHAIN.get(cid) or "").strip()),
            "ts": now_ts(),
        })

@app.route("/api/coingecko/simple_price", methods=["GET"])
def coingecko_simple_price():
    # Pass-through query params (ids, vs_currencies, include_* etc.)
    # + add small compatibility aliases for assets whose CoinGecko IDs changed over time.
    qs = request.query_string.decode("utf-8", errors="ignore")
    url = f"{COINGECKO_BASE}/simple/price"
    if qs:
        url = f"{url}?{qs}"
    try:
        data = _cg_get(url)

        # --- Compatibility aliasing (prevents "0 USD" when CoinGecko ID differs) ---
        # Frontend may request POL as "polygon-pos". If CoinGecko doesn't return it,
        # fall back to other known IDs and map the USD price back onto "polygon-pos".
        try:
            ids_raw = (request.args.get("ids") or "").strip()
            if ids_raw:
                ids = [s.strip() for s in ids_raw.split(",") if s.strip()]
            else:
                ids = []

            def _ensure_alias(missing_id: str, fallbacks: list[str]):
                nonlocal data
                if missing_id not in ids:
                    return
                if isinstance(data, dict) and missing_id in data and isinstance(data.get(missing_id), dict):
                    # already present
                    return
                fb_ids = [x for x in fallbacks if x]
                if not fb_ids:
                    return
                fb_qs = request.args.to_dict(flat=True)
                fb_qs["ids"] = ",".join(fb_ids)
                fb_url = f"{COINGECKO_BASE}/simple/price"
                if fb_qs:
                    fb_url = f"{fb_url}?" + "&".join([f"{k}={requests.utils.quote(str(v))}" for k, v in fb_qs.items()])
                fb = _cg_get(fb_url) or {}
                # pick first valid USD price
                px = None
                for fid in fb_ids:
                    try:
                        v = fb.get(fid, {}).get("usd")
                        if v is not None:
                            px = float(v)
                            if px > 0:
                                break
                    except Exception:
                        continue
                if px is not None:
                    if not isinstance(data, dict):
                        data = {}
                    data[missing_id] = {"usd": px}

            # POL (Polygon) native coin ID variations
            _ensure_alias("polygon-pos", ["polygon-ecosystem-token", "matic-network"])
        except Exception:
            pass

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "coingecko_proxy_failed", "detail": str(e)}), 502

@app.route("/api/coingecko/token_price/<platform>", methods=["GET"])
def coingecko_token_price(platform: str):
    qs = request.query_string.decode("utf-8", errors="ignore")
    url = f"{COINGECKO_BASE}/simple/token_price/{platform}"
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
GRID_STATE_PATH = os.getenv('NEXUS_GRID_STATE_PATH', '/data/grid_state.json')
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
        # Ensure persistent directory exists (Render disk mounts at /data)
        dirpath = os.path.dirname(GRID_STATE_PATH) or '.'
        os.makedirs(dirpath, exist_ok=True)
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
DB_PATH = os.getenv("NEXUS_DB_PATH", "/data/nexus.db")
# Ensure DB directory exists (Render disk typically mounts at /data)
try:
    _db_dir = os.path.dirname(DB_PATH)
    if _db_dir:
        os.makedirs(_db_dir, exist_ok=True)
except Exception:
    pass
TOKEN_TTL_SEC = int(os.getenv("NEXUS_TOKEN_TTL_SEC", "604800"))  # 7 days

import sqlite3

def _db():
    # NOTE: sqlite on Render can be hit concurrently by multiple requests.
    # We use WAL + busy_timeout and a process-level lock for writes.
    conn = sqlite3.connect(
        DB_PATH,
        timeout=30,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _db_table_columns(conn, table_name: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    # row: (cid, name, type, notnull, dflt_value, pk)
    return {row[1]: row for row in cur.fetchall()}

def _db_ensure_columns(conn, table_name: str, columns_sql: dict):
    """Ensure columns exist in an existing SQLite table (safe migration for persistent /data DB)."""
    existing = _db_table_columns(conn, table_name)
    cur = conn.cursor()
    for col, col_sql in columns_sql.items():
        if col in existing:
            continue
        # SQLite supports ALTER TABLE ADD COLUMN <definition>
        try:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_sql}")
            print(f"[DB] Migrated {table_name}: added column {col}")
        except Exception as e:
            print(f"[DB] WARNING: could not add column {col} to {table_name}: {e}")

def _db_migrate_schema(conn):
    """One-time additive migrations for older DBs."""
    # grid_orders: older deployments may miss newer columns like meta_json/created_ts/updated_ts
    _db_ensure_columns(conn, "grid_orders", {
        "chain": "chain TEXT DEFAULT ''",
        "side": "side TEXT",
        "price": "price REAL",
        "qty": "qty REAL",
        "status": "status TEXT DEFAULT 'OPEN'",
        "level": "level INTEGER",
        "meta_json": "meta_json TEXT DEFAULT '{}'",
        "created_ts": "created_ts INTEGER",
        "updated_ts": "updated_ts INTEGER",
        "cancelled_ts": "cancelled_ts INTEGER",
    })
    # grid_vaults: ensure chain exists for multi-chain deployments
    _db_ensure_columns(conn, "grid_vaults", {
        "chain": "chain TEXT DEFAULT ''",
        "vault_total": "vault_total REAL DEFAULT 0",
        "updated_ts": "updated_ts INTEGER",
    })




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


    
    # Profit / Fee ledger (lifetime)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS profit_state (
            wallet_address TEXT PRIMARY KEY,
            lifetime_profit_usd REAL,
            lifetime_fee_paid_usd REAL,
            updated_ts INTEGER
        )
    """)

    # PnL events (idempotent, per fill/session)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pnl_events (
            event_id TEXT PRIMARY KEY,
            wallet_address TEXT,
            item_id TEXT,
            side TEXT,
            pnl_delta_usd REAL,
            fill_id TEXT,
            filled_ts INTEGER,
            created_ts INTEGER
        )
    """)

    # Withdraw quotes (contract-ready; can be used later for EIP-712)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS withdraw_quotes (
            quote_id TEXT PRIMARY KEY,
            wallet_address TEXT,
            amount_usd REAL,
            fee_usd REAL,
            taxable_profit_usd REAL,
            nonce TEXT,
            deadline_ts INTEGER,
            status TEXT,
            created_ts INTEGER
        )
    """)
    # --- Grid persistence (orders + vault) ---
    cur.execute('''
    CREATE TABLE IF NOT EXISTS grid_orders (
        order_id TEXT PRIMARY KEY,
        wallet_address TEXT NOT NULL,
        item_id TEXT NOT NULL,
        chain TEXT DEFAULT '',
        side TEXT NOT NULL,
        price REAL,
        qty REAL,
        status TEXT DEFAULT 'OPEN',
        level INTEGER,
        meta_json TEXT DEFAULT '{}',
        created_ts INTEGER,
        updated_ts INTEGER,
        cancelled_ts INTEGER
    )
''')
    cur.execute("CREATE INDEX IF NOT EXISTS idx_grid_orders_wallet_item ON grid_orders(wallet_address, item_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_grid_orders_wallet_item_status ON grid_orders(wallet_address, item_id, status);")

    cur.execute('''
    CREATE TABLE IF NOT EXISTS grid_vaults (
        wallet_address TEXT NOT NULL,
        item_id TEXT NOT NULL,
        chain TEXT DEFAULT '',
        vault_total REAL DEFAULT 0,
        updated_ts INTEGER,
        PRIMARY KEY (wallet_address, item_id, chain)
    )
''')

    cur.execute('''
    CREATE TABLE IF NOT EXISTS grid_ui_state (
        wallet_address TEXT PRIMARY KEY,
        active_chain TEXT DEFAULT '',
        active_item TEXT DEFAULT '',
        updated_ts INTEGER
    )
''')

    # Auto-migrate persistent DB schema on Render disk (/data)
    _db_migrate_schema(conn)

    conn.commit()
    conn.close()

def _grid_db_reserved(conn, wallet_address: str, item_id: str, chain: str = "") -> float:
    cur = conn.cursor()
    if chain:
        cur.execute(
            "SELECT COALESCE(SUM(qty),0) AS s FROM grid_orders WHERE wallet_address=? AND item_id=? AND chain=? AND status='OPEN'",
            (_norm_addr(wallet_address), item_id, chain),
        )
    else:
        cur.execute(
            "SELECT COALESCE(SUM(qty),0) AS s FROM grid_orders WHERE wallet_address=? AND item_id=? AND status='OPEN'",
            (_norm_addr(wallet_address), item_id),
        )
    row = cur.fetchone()
    return float(row["s"] if row else 0.0)

def _grid_db_vault_total(conn, wallet_address: str, item_id: str, chain: str = "") -> float:
    cur = conn.cursor()
    if chain:
        cur.execute(
            "SELECT vault_total FROM grid_vaults WHERE wallet_address=? AND item_id=? AND chain=?",
            (_norm_addr(wallet_address), item_id, chain),
        )
        row = cur.fetchone()
        return float(row["vault_total"]) if row and row["vault_total"] is not None else 0.0

    # No chain provided: prefer chain='' if present, otherwise most recently updated row for this wallet+item
    cur.execute(
        "SELECT vault_total FROM grid_vaults WHERE wallet_address=? AND item_id=? AND chain=''",
        (_norm_addr(wallet_address), item_id),
    )
    row = cur.fetchone()
    if row and row["vault_total"] is not None:
        return float(row["vault_total"])

    cur.execute(
        "SELECT vault_total FROM grid_vaults WHERE wallet_address=? AND item_id=? ORDER BY updated_ts DESC LIMIT 1",
        (_norm_addr(wallet_address), item_id),
    )
    row = cur.fetchone()
    return float(row["vault_total"]) if row and row["vault_total"] is not None else 0.0

def _grid_db_set_vault_total(conn, wallet_address: str, item_id: str, vault_total: float, chain: str = "") -> None:
    nowi = int(time.time())
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO grid_vaults(wallet_address,item_id,chain,vault_total,updated_ts) VALUES(?,?,?,?,?) "
        "ON CONFLICT(wallet_address,item_id,chain) DO UPDATE SET vault_total=excluded.vault_total, updated_ts=excluded.updated_ts",
        (_norm_addr(wallet_address), item_id, chain, float(vault_total), nowi),
    )

def _grid_chain_key(item_id: str, chain: str = "") -> str:
    ch = str(chain or "").strip().upper()
    if ch:
        return ch
    it = str(item_id or "").strip().upper()
    if ":" in it:
        pref = it.split(":", 1)[0].strip().upper()
        if pref in _CHAIN_ID_BY_KEY:
            return pref
    if it in _CHAIN_ID_BY_KEY:
        return it
    return ""

def _grid_default_item_for_chain(chain_key: str) -> str:
    ck = str(chain_key or "POL").strip().upper() or "POL"
    return f"{ck}:{ck}"

def _grid_ui_state_get(conn, wallet_address: str) -> dict:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"active_chain": "POL", "active_item": _grid_default_item_for_chain("POL")}
    cur = conn.cursor()
    cur.execute("SELECT active_chain, active_item, updated_ts FROM grid_ui_state WHERE wallet_address=?", (wa,))
    row = cur.fetchone()
    if not row:
        return {"active_chain": "POL", "active_item": _grid_default_item_for_chain("POL")}
    active_chain = str(row["active_chain"] or "").strip().upper() or "POL"
    active_item = str(row["active_item"] or "").strip() or _grid_default_item_for_chain(active_chain)
    return {
        "active_chain": active_chain,
        "active_item": active_item,
        "updated_ts": int(row["updated_ts"] or 0),
    }

def _grid_ui_state_put(conn, wallet_address: str, active_chain: str = "", active_item: str = "") -> dict:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"active_chain": "POL", "active_item": _grid_default_item_for_chain("POL")}
    ch = str(active_chain or "").strip().upper()
    it = str(active_item or "").strip()
    if not ch:
        ch = _grid_chain_key(it) or "POL"
    if not it:
        it = _grid_default_item_for_chain(ch)
    nowi = int(time.time())
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO grid_ui_state(wallet_address, active_chain, active_item, updated_ts) VALUES (?,?,?,?) "
        "ON CONFLICT(wallet_address) DO UPDATE SET active_chain=excluded.active_chain, active_item=excluded.active_item, updated_ts=excluded.updated_ts",
        (wa, ch, it, nowi),
    )
    return {"active_chain": ch, "active_item": it, "updated_ts": nowi}

def _grid_best_vault_total(conn, wallet_address: str, item_id: str, chain: str = "") -> float:
    # Prefer explicit stored grid vault total. If missing, try wallet-bound vault balance for native items.
    vt = _grid_db_vault_total(conn, wallet_address, item_id, chain=chain)
    try:
        vt_f = float(vt or 0.0)
    except Exception:
        vt_f = 0.0
    if vt_f > 0:
        return vt_f

    ck = _grid_chain_key(item_id=item_id, chain=chain)
    sym = str(item_id or "").split(":", 1)[-1].strip().upper()
    if ck in ("POL", "BNB", "ETH") and sym == ck:
        try:
            vstate = _vault_state_read(wallet_address, ck)
            wallet_vault_total = float(vstate.get("vault_balance") or 0.0)
            if wallet_vault_total >= 0:
                try:
                    with DB_WRITE_LOCK:
                        _grid_db_set_vault_total(conn, wallet_address, item_id, wallet_vault_total, chain=chain or ck)
                except Exception:
                    pass
                return wallet_vault_total
        except Exception:
            pass

    return _grid_effective_vault_total(conn, wallet_address, item_id, chain=chain)

def _native_balance_for_wallet(wallet_address: str, chain: str = "", item_id: str = "") -> float:
    wa = _norm_addr(wallet_address or "")
    if not wa or not _looks_like_evm_addr(wa):
        return 0.0
    ch = _grid_chain_key(item_id=item_id, chain=chain)
    cid = int(_CHAIN_ID_BY_KEY.get(ch, 0) or 0)
    if cid <= 0:
        return 0.0
    try:
        raw = _rpc_call(cid, "eth_getBalance", [wa, "latest"])
        wei = _hex_to_int(raw or "0x0")
        if wei <= 0:
            return 0.0
        return float(wei) / 1e18
    except Exception:
        return 0.0

def _grid_effective_vault_total(conn, wallet_address: str, item_id: str, chain: str = "") -> float:
    """Return authoritative wallet-bound vault total for grid UI.

    Priority:
      1) explicit grid_vaults row
      2) on-chain wallet-bound vault balance from the vault contract
      3) final fallback = 0.0

    IMPORTANT:
    Do NOT fall back to the wallet's native chain balance here, because the grid vault
    is supposed to reflect deposited vault funds, not the user's normal wallet balance.
    """
    vt = _grid_db_vault_total(conn, wallet_address, item_id, chain=chain)
    try:
        vt_f = float(vt or 0.0)
    except Exception:
        vt_f = 0.0
    if vt_f > 0:
        return vt_f

    ck = _grid_chain_key(item_id=item_id, chain=chain)
    sym = str(item_id or "").split(":", 1)[-1].strip().upper()
    if ck in ("POL", "BNB", "ETH") and sym == ck:
        try:
            vstate = _vault_state_read(wallet_address, ck)
            wallet_vault_total = float(vstate.get("vault_balance") or 0.0)
            if wallet_vault_total >= 0:
                try:
                    _grid_db_set_vault_total(conn, wallet_address, item_id, wallet_vault_total, chain=chain or ck)
                except Exception:
                    pass
                return wallet_vault_total
        except Exception:
            pass

    return 0.0

def _grid_db_list_orders(conn, wallet_address: str, item_id: str | None = None, chain: str = "") -> list[dict]:
    cur = conn.cursor()
    if item_id:
        if chain:
            cur.execute(
                "SELECT order_id, item_id, side, price, qty, status, level, meta_json, created_ts, updated_ts "
                "FROM grid_orders WHERE wallet_address=? AND item_id=? AND chain=? ORDER BY created_ts ASC",
                (_norm_addr(wallet_address), item_id, chain),
            )
        else:
            cur.execute(
                "SELECT order_id, item_id, side, price, qty, status, level, meta_json, created_ts, updated_ts "
                "FROM grid_orders WHERE wallet_address=? AND item_id=? ORDER BY created_ts ASC",
                (_norm_addr(wallet_address), item_id),
            )
    else:
        if chain:
            cur.execute(
                "SELECT order_id, item_id, side, price, qty, status, level, meta_json, created_ts, updated_ts "
                "FROM grid_orders WHERE wallet_address=? AND chain=? ORDER BY created_ts ASC",
                (_norm_addr(wallet_address), chain),
            )
        else:
            cur.execute(
                "SELECT order_id, item_id, side, price, qty, status, level, meta_json, created_ts, updated_ts "
                "FROM grid_orders WHERE wallet_address=? ORDER BY created_ts ASC",
                (_norm_addr(wallet_address),),
            )
    out = []
    for r in cur.fetchall():
        d = dict(r)
        d["id"] = d.pop("order_id")
        if "item_id" in d:
            d["item"] = d.pop("item_id")
        try:
            meta = json.loads(d.get("meta_json") or "{}")
        except Exception:
            meta = {}
        d.pop("meta_json", None)
        if isinstance(meta, dict):
            for k, v in meta.items():
                if k not in d:
                    d[k] = v
        out.append(d)
    return out

def _grid_db_insert_order(conn, wallet_address: str, item_id: str, order: dict, chain: str = "") -> str:
    nowi = int(time.time())
    oid = str(order.get("id") or order.get("order_id") or order.get("orderId") or uuid.uuid4().hex)
    side = str(order.get("side") or "").upper()
    price = order.get("price")
    qty = order.get("qty") if order.get("qty") is not None else order.get("amount")
    status = str(order.get("status") or "OPEN").upper()
    level = order.get("level")
    core_keys = {"id","order_id","orderId","side","price","qty","amount","status","level","item","item_id"}
    meta = {k:v for k,v in (order or {}).items() if k not in core_keys}
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO grid_orders(order_id,wallet_address,item_id,chain,side,price,qty,status,level,meta_json,created_ts,updated_ts) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT created_ts FROM grid_orders WHERE order_id=?),?),?)",
        (oid, _norm_addr(wallet_address), item_id, chain, side,
         float(price) if price is not None else None,
         float(qty) if qty is not None else None,
         status,
         int(level) if level is not None else None,
         json.dumps(meta, separators=(",",":")),
         oid, nowi, nowi),
    )
    return oid

def _grid_db_cancel_order(conn, wallet_address: str, item_id: str, oid: str, chain: str = "") -> int:
    """
    Cancel an order. Primary key is (wallet_address, order_id).
    We keep item_id/chain filters for fast-path, but fall back to order_id-only
    to tolerate UI/backend item_id mismatches.
    """
    nowi = int(time.time())
    cur = conn.cursor()
    wa = _norm_addr(wallet_address)

    # Fast-path: match the expected item_id (+ optional chain)
    if chain:
        cur.execute(
            "UPDATE grid_orders SET status='CANCELLED', cancelled_ts=COALESCE(cancelled_ts, ?), updated_ts=? "
            "WHERE order_id=? AND wallet_address=? AND item_id=? AND chain=?",
            (nowi, nowi, str(oid), wa, item_id, chain),
        )
    else:
        cur.execute(
            "UPDATE grid_orders SET status='CANCELLED', cancelled_ts=COALESCE(cancelled_ts, ?), updated_ts=? "
            "WHERE order_id=? AND wallet_address=? AND item_id=?",
            (nowi, nowi, str(oid), wa, item_id),
        )
    rc = cur.rowcount

    # Fallback: cancel by (wallet_address, order_id) only
    if rc <= 0:
        cur.execute(
            "UPDATE grid_orders SET status='CANCELLED', cancelled_ts=COALESCE(cancelled_ts, ?), updated_ts=? "
            "WHERE order_id=? AND wallet_address=?",
            (nowi, nowi, str(oid), wa),
        )
        rc = cur.rowcount

    return rc


def _grid_db_delete_order(conn, wallet_address: str, item_id: str, oid: str, chain: str = "") -> int:
    """Delete an order.

    We try the strict match (wallet+item+optional chain) first, then fall back to (wallet+order_id)
    to tolerate item_id mismatches between UI and backend.
    """
    cur = conn.cursor()
    wa = _norm_addr(wallet_address)

    if chain:
        cur.execute(
            "DELETE FROM grid_orders WHERE order_id=? AND wallet_address=? AND item_id=? AND chain=?",
            (str(oid), wa, item_id, chain),
        )
    else:
        cur.execute(
            "DELETE FROM grid_orders WHERE order_id=? AND wallet_address=? AND item_id=?",
            (str(oid), wa, item_id),
        )
    rc = cur.rowcount

    if rc <= 0:
        cur.execute(
            "DELETE FROM grid_orders WHERE order_id=? AND wallet_address=?",
            (str(oid), wa),
        )
        rc = cur.rowcount

    return rc





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

def _looks_like_evm_addr(s: str) -> bool:
    s = (s or "").strip()
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", s))

def _try_extract_wallet_from_jwt(token: str) -> Optional[str]:
    """Best-effort decode of a JWT *without* verification to extract an EVM wallet address.
    This is an interim compatibility layer for Privy access tokens.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        import base64
        def b64url_decode(seg: str) -> bytes:
            seg = seg.strip().replace("-", "+").replace("_", "/")
            seg += "=" * (-len(seg) % 4)
            return base64.b64decode(seg)

        payload_raw = b64url_decode(parts[1]).decode("utf-8", errors="ignore")
        payload = json.loads(payload_raw) if payload_raw else {}

        # Common direct fields
        for k in ("wallet_address", "walletAddress", "address"):
            v = payload.get(k)
            if isinstance(v, str) and _looks_like_evm_addr(v):
                return _norm_addr(v)

        # Some providers nest wallets in arrays/objects
        candidates = []

        def walk(obj):
            if isinstance(obj, dict):
                for kk, vv in obj.items():
                    if kk in ("wallet_address", "walletAddress", "address") and isinstance(vv, str):
                        candidates.append(vv)
                    walk(vv)
            elif isinstance(obj, list):
                for it in obj:
                    walk(it)

        walk(payload)

        for v in candidates:
            if _looks_like_evm_addr(v):
                return _norm_addr(v)

        return None
    except Exception:
        return None
        
def _extract_wallet_from_jwt_best_effort(token: str):
    """
    Best-effort: decode JWT payload WITHOUT verifying signature.
    Walk nested payload to find an EVM address (Privy-style tokens often nest wallet/address).
    """
    try:
        parts = (token or "").split(".")
        if len(parts) < 2:
            return None

        payload_b64 = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)

        import base64
        import json

        payload = json.loads(
            base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        )

        candidates = []

        def walk(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    kl = str(k).lower()
                    if kl in ("wallet", "wallet_address", "walletaddress", "address", "sub") and isinstance(v, str):
                        candidates.append(v)
                    walk(v)
            elif isinstance(obj, list):
                for it in obj:
                    walk(it)

        walk(payload)

        for v in candidates:
            if isinstance(v, str) and _looks_like_evm_addr(v):
                return _norm_addr(v)

        return None
    except Exception:
        return None

def _pick_wallet_from_request() -> Optional[str]:
    body = request.get_json(silent=True) or {}

    candidates = [
        body.get("wallet"),
        body.get("wallet_address"),
        body.get("walletAddress"),
        request.headers.get("X-Wallet-Address"),
        request.args.get("wallet"),
        request.args.get("wallet_address"),
    ]

    for c in candidates:
        if isinstance(c, str) and c.strip():
            # Robust: pick first real EVM address even if header contains commas/quotes/etc
            m = re.search(r"0x[a-fA-F0-9]{40}", c)
            if m:
                return _norm_addr(m.group(0))

    return None        

def _require_auth() -> Optional[str]:
    """Return normalized wallet address if caller is authorized, else None."""

    # ✅ DEV/SAFE bypass: allow anonymous access to /api/grid/* and /api/ai/*
    # when GRID_ALLOW_ANON=1 and a wallet is provided (header/body/query).
    allow_anon = os.getenv("GRID_ALLOW_ANON", "0") == "1"
    if allow_anon and (request.path.startswith("/api/grid") or request.path.startswith("/api/ai/")):
        wa = _pick_wallet_from_request()
        if isinstance(wa, str) and _looks_like_evm_addr(wa):
            return wa
        return None

    # --- normal auth flow below ---
    auth = (request.headers.get("Authorization") or "").strip()
    if not auth.lower().startswith("bearer "):
        return None

    token = auth.split(" ", 1)[1].strip()
    # (1) Internal server API key
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    if server_key and token == server_key:
        body = request.get_json(silent=True) or {}
        wa = (
            body.get("wallet")
            or body.get("wallet_address")
            or body.get("walletAddress")
            or request.headers.get("X-Wallet-Address")
            or request.args.get("wallet")
            or request.args.get("wallet_address")
        )
        if isinstance(wa, str) and _looks_like_evm_addr(wa):
            return _norm_addr(wa)
        return None

    # (2) Legacy signed token issued by this backend
    try:
        if _AUTH_SERIALIZER is not None:
            payload = _AUTH_SERIALIZER.loads(token, max_age=60 * 60 * 24 * 30)  # 30d
            if isinstance(payload, dict):
                wa = payload.get("wallet") or payload.get("wallet_address") or payload.get("walletAddress")
                if isinstance(wa, str) and _looks_like_evm_addr(wa):
                    return _norm_addr(wa)
    except Exception:
        pass

    # (3) Privy-style JWT (best-effort decode without verification)
    wa = _extract_wallet_from_jwt_best_effort(token)
    if isinstance(wa, str) and _looks_like_evm_addr(wa):
        return _norm_addr(wa)

    return None

    # ✅ Anonymous bypass (dev / SAFE mode) when NO bearer token
    if not auth.lower().startswith("bearer "):
        if allow_anon and (request.path.startswith("/api/grid/") or request.path.startswith("/api/ai/")):
            return _pick_wallet_from_request()
        return None

    token = auth.split(" ", 1)[1].strip()

    # ✅ Allow anonymous for Grid + AI endpoints EVEN if a Bearer token is present (Privy sends it always)
    allow_anon = os.getenv("GRID_ALLOW_ANON", "0") == "1"
    if allow_anon and (request.path.startswith("/api/grid/") or request.path.startswith("/api/ai/")):
        body = request.get_json(silent=True) or {}
        wa = (
            body.get("wallet")
            or body.get("wallet_address")
            or body.get("walletAddress")
            or request.headers.get("X-Wallet-Address")
            or request.args.get("wallet")
            or request.args.get("wallet_address")
        )
        if isinstance(wa, str) and _looks_like_evm_addr(wa):
            return _norm_addr(wa)

    # (1) Internal server API key
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    api_key_hdr = (request.headers.get("X-API-Key") or request.headers.get("x-api-key") or "").strip()
    if server_key and api_key_hdr and secrets.compare_digest(api_key_hdr, server_key):
        token = server_key

    if server_key and secrets.compare_digest(token, server_key):
        wa = _pick_wallet_from_request()
        return wa  # wa is already normalized or None

    # (2) Legacy signed token issued by this backend
    try:
        if _AUTH_SERIALIZER is not None:
            payload = _AUTH_SERIALIZER.loads(token, max_age=60 * 60 * 24 * 30)  # 30d
            if isinstance(payload, dict):
                wa = payload.get("wallet") or payload.get("wallet_address") or payload.get("walletAddress")
                if isinstance(wa, str) and _looks_like_evm_addr(wa):
                    return _norm_addr(wa)
    except Exception:
        pass

    # (3) Privy-style JWT (best-effort: decode without verification)
    wa = _extract_wallet_from_jwt_best_effort(token)
    if isinstance(wa, str) and _looks_like_evm_addr(wa):
        return _norm_addr(wa)

    # ✅ DEV/SAFE fallback even WITH bearer token (Privy token may not contain 0x wallet)
    if allow_anon and (request.path.startswith("/api/grid/") or request.path.startswith("/api/ai/")):
        return _pick_wallet_from_request()

    return None

    token = auth.split(" ", 1)[1].strip()

    # (1) Internal server API key
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    # Accept server API key either via Authorization Bearer <key> OR X-API-Key header.
    api_key_hdr = (request.headers.get("X-API-Key") or request.headers.get("x-api-key") or "").strip()
    if server_key and api_key_hdr and secrets.compare_digest(api_key_hdr, server_key):
        token = server_key

    if server_key and secrets.compare_digest(token, server_key):
        body = request.get_json(silent=True) or {}
        wa = (
            request.headers.get("X-Wallet-Address")
            or request.args.get("wallet")
            or request.args.get("wallet_address")
            or body.get("wallet")
            or body.get("wallet_address")
            or body.get("walletAddress")
        )
        if isinstance(wa, str) and _looks_like_evm_addr(wa):
            return _norm_addr(wa)
        return None

    # (2) Legacy signed token issued by this backend
    try:
        if _AUTH_SERIALIZER is not None:
            payload = _AUTH_SERIALIZER.loads(token, max_age=60 * 60 * 24 * 30)  # 30d
            if isinstance(payload, dict):
                wa = payload.get("wallet") or payload.get("wallet_address") or payload.get("walletAddress")
                if isinstance(wa, str) and _looks_like_evm_addr(wa):
                    return _norm_addr(wa)
    except Exception:
        pass

    # (3) Privy-style JWT (best-effort: decode without verification)
    wa = _extract_wallet_from_jwt_best_effort(token)
    if isinstance(wa, str) and _looks_like_evm_addr(wa):
        return _norm_addr(wa)

    return None

def issue_token(wallet_address: str) -> str:
    return _serializer.dumps({
        "wallet_address": _norm_addr(wallet_address)
    })
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
# Manual trading permission gate (must be enabled before grid/actions are allowed)
        "trading_enabled": True,  # deprecated: access (redeem/subscription) gates trading; keep True for backward compatibility
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
    # Silver chains
    "ETH", "BNB", "POL",
    # Gold adds more networks
    "BASE", "ARBITRUM", "OPTIMISM", "AVALANCHE",
    "SOL", "BTC",
]


# Single subscription plan chains (configurable via env)
# Example: NEXUS_CHAINS_PRO="ETH,BNB,POL"
def _parse_csv_list(s: str) -> list[str]:
    return [x.strip().upper() for x in (s or "").split(",") if x.strip()]

_CHAINS_PRO = _parse_csv_list(os.getenv("NEXUS_CHAINS_PRO", "ETH,BNB,POL"))
if not _CHAINS_PRO:
    _CHAINS_PRO = ["ETH", "BNB", "POL"]

# Networks that the backend treats as EVM-style chains (wallet / tx verification, etc.)
# Important: BTC and SOL are assets, not EVM chains here.
_KNOWN_NETWORKS = ["ETH", "BNB", "POL", "BASE", "ARBITRUM", "OPTIMISM", "AVALANCHE"]
# --- Chain enablement (Phase 1: only POL enabled; later enable more via env) ---
# Controls which EVM networks are ACTIVE in this deployment (affects UI exposure + tx verification).
# Example: NEXUS_ENABLED_EVM_CHAINS="POL" (Phase 1), later: "POL,BNB,BASE,ARBITRUM,ETH"
_CHAIN_ID_BY_KEY = {
    "ETH": 1,
    "BNB": 56,
    "POL": 137,
    "BASE": 8453,
    "ARBITRUM": 42161,
    "OPTIMISM": 10,
    "AVALANCHE": 43114,
}
_ENABLED_EVM_CHAINS = _parse_csv_list(os.getenv("NEXUS_ENABLED_EVM_CHAINS", "POL"))
# Always keep POL enabled by default to match Phase 1 expectations
if not _ENABLED_EVM_CHAINS:
    _ENABLED_EVM_CHAINS = ["POL"]
_ENABLED_EVM_CHAINS = [c for c in _ENABLED_EVM_CHAINS if c in _KNOWN_NETWORKS]
if "POL" not in _ENABLED_EVM_CHAINS:
    _ENABLED_EVM_CHAINS.insert(0, "POL")
_ENABLED_CHAIN_IDS = {int(_CHAIN_ID_BY_KEY[c]) for c in _ENABLED_EVM_CHAINS if c in _CHAIN_ID_BY_KEY}

# Effective PRO chains exposed/used in this deployment (intersection of plan chains and enabled chains)
_CHAINS_PRO_EFFECTIVE = [c for c in _CHAINS_PRO if c in _ENABLED_EVM_CHAINS]
if not _CHAINS_PRO_EFFECTIVE:
    _CHAINS_PRO_EFFECTIVE = list(_ENABLED_EVM_CHAINS)


# Assets/features unlocked by tiers (independent of chain/network selection)
_ASSETS_SILVER = []
_ASSETS_GOLD_EXTRA = ["BTC", "SOL"]

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
    "NEXUS-4Q7A-9K2J",
    "NEXUS-1V8D-H6PM",
    "NEXUS-Z3F1-R8CW",
    "NEXUS-6TQ9-X2LA",
    "NEXUS-P7H4-3NJD",
    "NEXUS-Y5C2-M8VK",
    "NEXUS-8LJ6-W1QF",
    "NEXUS-A2N9-7GXR",
    "NEXUS-K4P1-5DVT",
    "NEXUS-R9W3-2HLM",
    "NEXUS-3JQ8-F7YA",
    "NEXUS-M6X1-9CPT",
    "NEXUS-H2V7-L4QK",
    "NEXUS-7D3N-P8WF",
    "NEXUS-W1L9-6JXA",
    "NEXUS-C8R2-Y5PM",
    "NEXUS-9FQ6-1KVD",
    "NEXUS-T4M7-3NQH",
    "NEXUS-2XJ5-R9LA",
    "NEXUS-V7P1-8DWC",
    "NEXUS-L3H9-4QJT",
    "NEXUS-5N2A-X7RM",
    "NEXUS-Q8W6-2FVP",
    "NEXUS-D1T7-9KHC",
    "NEXUS-X6C4-L2NJ",
    "NEXUS-4P9V-7WQF",
    "NEXUS-N2J6-5YRA",
    "NEXUS-H8L1-3DVT",
    "NEXUS-R5Q7-M9XA",
    "NEXUS-7C2W-P4HJ",
    "NEXUS-Y9D1-6NQK",
    "NEXUS-K3M8-X1LA",
    "NEXUS-1QH7-R6VP",
    "NEXUS-P2V9-4FJT",
    "NEXUS-W6N3-8KHC",
    "NEXUS-9X1A-Y7RM",
    "NEXUS-T5Q2-3JXA",
    "NEXUS-2D8W-L9VP",
    "NEXUS-V4M1-6FQK",
    "NEXUS-L7P9-X2HJ",
    "NEXUS-5H3N-R8CW",
    "NEXUS-Q1J6-M4YA",
    "NEXUS-D9V2-7KVD",
    "NEXUS-X3L8-1NQH",
    "NEXUS-6F1W-P5JT",
    "NEXUS-M8Q4-Y2LA",
    "NEXUS-H5P1-9DWC",
    "NEXUS-7R2J-3KHC",
    "NEXUS-W9X6-6FVP",
    "NEXUS-C1N4-R7YA"
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

# -------------------------
# Nexus Vault / Executor (Trading Contracts)
# -------------------------
# Phase 1: Polygon only (137). Later add other chains by ENV.
_VAULT_BY_CHAIN = {
    1: (os.getenv("VAULT_ADDRESS_ETH") or os.getenv("VAULT_ADDRESS_1") or "").strip(),
    56: (os.getenv("VAULT_ADDRESS_BNB") or os.getenv("VAULT_ADDRESS_56") or "").strip(),
    137: (os.getenv("VAULT_ADDRESS_POL") or os.getenv("VAULT_ADDRESS_POLYGON") or os.getenv("VAULT_ADDRESS_137") or "").strip(),
}

_EXECUTOR_BY_CHAIN = {
    1: (os.getenv("EXECUTOR_ADDRESS_ETH") or os.getenv("EXECUTOR_ADDRESS_1") or "").strip(),
    56: (os.getenv("EXECUTOR_ADDRESS_BNB") or os.getenv("EXECUTOR_ADDRESS_56") or "").strip(),
    137: (os.getenv("EXECUTOR_ADDRESS_POL") or os.getenv("EXECUTOR_ADDRESS_POLYGON") or os.getenv("EXECUTOR_ADDRESS_137") or "").strip(),
}

# DEX Router (QuickSwap V2 for Polygon by default)
_ROUTER_BY_CHAIN = {
    1: (os.getenv("ROUTER_ADDRESS_ETH") or os.getenv("ROUTER_ADDRESS_1") or "").strip(),
    56: (os.getenv("ROUTER_ADDRESS_BNB") or os.getenv("ROUTER_ADDRESS_56") or "").strip(),
    137: (os.getenv("ROUTER_ADDRESS_POL") or os.getenv("ROUTER_ADDRESS_POLYGON") or os.getenv("ROUTER_ADDRESS_137") or "").strip(),
}

# Optional: Uniswap V3 Router (used on ETH mainly; can be empty on chains without V3 usage)
_ROUTER_V3_BY_CHAIN = {
    1: (os.getenv("ROUTER_V3_ADDRESS_ETH") or os.getenv("ROUTER_V3_ADDRESS_1") or "").strip(),
    56: (os.getenv("ROUTER_V3_ADDRESS_BNB") or os.getenv("ROUTER_V3_ADDRESS_56") or "").strip(),
    137: (os.getenv("ROUTER_V3_ADDRESS_POL") or os.getenv("ROUTER_V3_ADDRESS_POLYGON") or os.getenv("ROUTER_V3_ADDRESS_137") or "").strip(),
}

# Wrapped native token per chain (useful for auto-path building & validation)
_WNATIVE_BY_CHAIN = {
    1: (os.getenv("WNATIVE_ADDRESS_ETH") or os.getenv("WNATIVE_ADDRESS_1") or "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").strip(),
    56: (os.getenv("WNATIVE_ADDRESS_BNB") or os.getenv("WNATIVE_ADDRESS_56") or "").strip(),  # WBNB expected
    137: (os.getenv("WNATIVE_ADDRESS_POL") or os.getenv("WNATIVE_ADDRESS_POLYGON") or os.getenv("WNATIVE_ADDRESS_137") or "").strip(),  # WMATIC expected
}


_USDC_DECIMALS = int(os.getenv("USDC_DECIMALS", "6"))
_USDT_DECIMALS = int(os.getenv("USDT_DECIMALS", "6"))

PRICE_PRO_USD = float(os.getenv("PRICE_PRO_USD", os.getenv("PRICE_MONTHLY_USD", "15")))
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
    cid = int(chain_id) or 0
    if _ENABLED_CHAIN_IDS and cid not in _ENABLED_CHAIN_IDS:
        raise RuntimeError(f"chain_id not enabled: {cid}")

    url = _rpc_url_for_chain(cid)
    if not url:
        raise RuntimeError(f"rpc url not configured for chain_id={chain_id}")

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(url, json=payload, timeout=20)
    except Exception as e:
        raise RuntimeError(f"rpc request failed for chain_id={cid}: {e}")

    if not r.ok:
        body = ""
        try:
            body = (r.text or "")[:240]
        except Exception:
            body = ""
        raise RuntimeError(f"rpc http {r.status_code} for chain_id={cid}: {body}")

    try:
        j = r.json() or {}
    except Exception:
        raise RuntimeError(f"rpc returned non-json for chain_id={cid}")

    if j.get("error"):
        err = j.get("error")
        if isinstance(err, dict):
            msg = err.get("message") or json.dumps(err, ensure_ascii=False)
        else:
            msg = str(err)
        raise RuntimeError(f"rpc error for chain_id={cid}: {msg}")

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
    """
    Verify an onchain USDC/USDT payment to TREASURY_ADDRESS.

    We now use a single subscription plan ("pro") priced by PRICE_PRO_USD.
    For backwards compatibility, we accept plan values like "silver"/"gold" but
    always enforce PRICE_PRO_USD.
    """
    if not TREASURY_ADDRESS:
        raise RuntimeError("missing NEXUS_TREASURY_ADDRESS")

    plan_l = (plan or "").strip().lower()
    if plan_l not in ("pro", "silver", "gold", "basic", "premium"):
        raise RuntimeError("plan must be 'pro'")

    price = float(PRICE_PRO_USD)

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


def _access_state_put(wallet_address: str, plan: str, source: str, expires_ts: int | None, chains_allowed: list,
                      ai_limit: int, can_open_new_trades: bool, conn=None, cur=None):
    """Upsert access_state.

    If conn/cur are provided, we reuse them (important to avoid nested sqlite writes that can lock).
    """
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return

    own_conn = False
    if conn is None:
        conn = _db()
        own_conn = True
    if cur is None:
        cur = conn.cursor()

    with DB_WRITE_LOCK:
        cur.execute(
            "INSERT INTO access_state(wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades, updated_ts) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(wallet_address) DO UPDATE SET "
            "plan=excluded.plan, source=excluded.source, expires_ts=excluded.expires_ts, "
            "chains_allowed_json=excluded.chains_allowed_json, ai_limit=excluded.ai_limit, "
            "can_open_new_trades=excluded.can_open_new_trades, updated_ts=excluded.updated_ts",
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
        if own_conn:
            conn.commit()

    if own_conn:
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
        "active": False,
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

    can_open = bool(st.get("can_open_new_trades"))

    
    return {
            "plan": plan,
            "source": source,
            "expires_at": int(exp) if exp is not None else None,
            # Only expose real networks as "chains" to the UI to avoid treating assets like BTC/SOL as chains.
            "chains_allowed": [c for c in chains if (c in _KNOWN_NETWORKS and (not _ENABLED_EVM_CHAINS or c in _ENABLED_EVM_CHAINS))],
            # Extra assets/features unlocked by tier (safe to ignore by older frontends).
            "assets_allowed": (_ASSETS_GOLD_EXTRA if plan in ("gold", "unlimited") else _ASSETS_SILVER),
            "ai_limit": ai_limit,
            "can_open_new_trades": can_open,
            "can_close_trades": True,
            "active": True,
        }


def _require_access_open() -> tuple[str | None, dict | None, tuple | None]:
    """Enforce access for endpoints that OPEN new trades."""
    wa = _require_auth() or _pick_wallet_from_request()

    # -----------------------------
    # Grid trader anon/dev mode
    # -----------------------------
    # The Grid Trader UI (and Postman tests) may call grid endpoints without a Bearer token.
    # If GRID_ALLOW_ANON=1 and the request is for /api/grid/*, accept an explicit wallet
    # address from JSON/query/header and skip subscription gating.
    allow_anon = os.getenv("GRID_ALLOW_ANON", "0").strip() in ("1", "true", "True")
    if not wa and allow_anon and request.path.startswith("/api/grid/"):
        body = request.get_json(silent=True) or {}
        wa = _norm_addr(
            request.headers.get("X-Wallet-Address")
            or request.args.get("wallet")
            or request.args.get("wallet_address")
            or request.args.get("address")
            or body.get("wallet")
            or body.get("wallet_address")
            or body.get("address")
            or body.get("addr")
            or ""
        )
        if wa:
            # Minimal access object that allows opening trades during anon grid testing.
            st = _access_defaults()
            st["source"] = "grid_anon"
            st["active"] = True
            st["can_open_new_trades"] = True
            st["can_close_trades"] = True
            return wa, st, None

    if not wa:
        return None, None, err("unauthorized", 401)

    st = _compute_access_status(wa)
    if not bool(st.get("can_open_new_trades")):
        return wa, st, err("access required (no new trades allowed)", 403)
    return wa, st, None


@app.route("/api/access/status", methods=["GET"])
def api_access_status():
    wa = _require_auth()

    # Fallback: allow wallet via query param if no token
    if not wa:
        wa = _norm_addr(
            request.args.get("wallet")
            or request.args.get("addr")
            or request.args.get("address")
            or ""
        )

    st = _compute_access_status(wa)

    return jsonify({
        "status": "ok",
        "wallet_address": _norm_addr(wa) if wa else None,
        **st
    })




@app.route("/api/fees/state", methods=["GET"])
def api_fees_state():
    """Return lifetime profit + fee state for the authenticated wallet."""
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _profit_state_get(wa)
    return jsonify({
        "status": "ok",
        "wallet_address": _norm_addr(wa),
        "fee_rate": FEE_RATE,
        "fee_free_threshold_usd": FEE_FREE_THRESHOLD_USD,
        **st
    })


@app.route("/api/fees/preview", methods=["GET"])
def api_fees_preview():
    """Preview the fee for a hypothetical profit delta (no state change)."""
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    try:
        profit_delta = float(request.args.get("profit_delta") or 0.0)
    except Exception:
        profit_delta = 0.0
    st = _profit_state_get(wa)
    fee, taxable = _fee_for_profit_delta(float(st.get("lifetime_profit_usd") or 0.0), profit_delta)
    return jsonify({
        "status": "ok",
        "profit_delta_usd": profit_delta,
        "taxable_profit_usd": taxable,
        "fee_usd": fee,
        "lifetime_profit_usd_before": float(st.get("lifetime_profit_usd") or 0.0),
        "lifetime_profit_usd_after": float(st.get("lifetime_profit_usd") or 0.0) + max(0.0, profit_delta),
    })


@app.route("/api/withdraw/quote", methods=["POST"])
def api_withdraw_quote():
    """Create a withdraw quote (contract-ready).

    This does NOT move funds. Later, the vault contract will enforce this quote.
    For now it returns:
      - fee_due_usd (based on lifetime profit threshold)
      - a nonce + deadline for signing / EIP-712 later
    """
    body = request.get_json(silent=True) or {}
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)

    try:
        amount_usd = float(body.get("amount_usd") or body.get("amount") or 0.0)
    except Exception:
        amount_usd = 0.0

    # For MVP we treat 'amount_usd' as the realized profit the user is trying to withdraw.
    # Later: connect this to the vault balance + withdrawable profit accounting.
    if amount_usd <= 0:
        return err("missing/invalid amount_usd", 400)

    st = _profit_state_get(wa)
    prev_profit = float(st.get("lifetime_profit_usd") or 0.0)

    fee_usd, taxable_profit_usd = _fee_for_profit_delta(prev_profit, amount_usd)

    quote_id = str(uuid.uuid4())
    nonce = str(uuid.uuid4()).replace("-", "")
    deadline_ts = now_ts() + int(os.getenv("NEXUS_WITHDRAW_QUOTE_TTL_SEC", "900"))  # 15 min

    # Persist quote (so later the vault can use it; also helps idempotency)
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO withdraw_quotes(quote_id, wallet_address, amount_usd, fee_usd, taxable_profit_usd, nonce, deadline_ts, status, created_ts)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (quote_id, _norm_addr(wa), float(amount_usd), float(fee_usd), float(taxable_profit_usd), nonce, int(deadline_ts), "CREATED", now_ts()),
    )
    conn.commit()
    conn.close()

    return jsonify({
        "status": "ok",
        "quote_id": quote_id,
        "wallet_address": _norm_addr(wa),
        "amount_usd": float(amount_usd),
        "fee_usd": float(fee_usd),
        "taxable_profit_usd": float(taxable_profit_usd),
        "fee_rate": FEE_RATE,
        "fee_free_threshold_usd": FEE_FREE_THRESHOLD_USD,
        "nonce": nonce,
        "deadline_ts": int(deadline_ts),

        # Later (contracts):
        # "treasury": os.getenv("TREASURY_WALLET"),
        # "vault_contract": os.getenv("VAULT_CONTRACT"),
        # "signature": "0x..." (EIP-712)
    })
def _seed_unlimited_codes_if_needed(cur):
    """Ensure access_codes table contains all configured one-time codes.

    We *append* missing codes on every call (idempotent), so you can add more
    codes later without wiping the DB. Redeemed codes remain redeemed because
    they live in the DB.
    """
    codes = list(REDEEM_CODES or [])

    raw = str(os.getenv("NEXUS_UNLIMITED_CODES", "")).strip()
    if raw:
        for c in raw.split(","):
            c = (c or "").strip()
            if c and c not in codes:
                codes.append(c)

    if not codes:
        return

    # Insert any missing codes (do NOT clear existing rows)
    for c in codes[:5000]:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO access_codes(code, redeemed_by, redeemed_ts) VALUES (?,?,?)",
                (c, None, None),
            )
        except Exception:
            pass



@app.route("/api/access/redeem", methods=["POST"])
def api_access_redeem():
    """Redeem a permanent code.

    Supports:
      A) Bearer auth (if user already has a token)
      B) direct wallet in body (first-time users)

    IMPORTANT: avoid nested sqlite writes (causes "database is locked").
    """
    body = request.get_json(silent=True) or {}
    wa = _require_auth() or _norm_addr(body.get("addr") or body.get("wallet") or body.get("address") or "")
    if not wa:
        return err("missing wallet", 400)

    code = str(body.get("code") or "").strip()
    if not code:
        return err("missing code", 400)

    conn = _db()
    cur = conn.cursor()
    try:
        # best-effort seed (if env provides codes)
        try:
            _seed_unlimited_codes_if_needed(cur)
        except Exception:
            pass

        with DB_WRITE_LOCK:
            cur.execute("SELECT code, redeemed_by, redeemed_ts FROM access_codes WHERE code=?", (code,))
            row = cur.fetchone()
            if not row:
                return err("invalid code", 404)
            redeemed_by = (row[1] or "")
            if redeemed_by:
                return err("code already redeemed", 409)

            cur.execute(
                "UPDATE access_codes SET redeemed_by=?, redeemed_ts=? WHERE code=?",
                (wa, now_ts(), code),
            )

            _access_state_put(
                wallet_address=wa,
                plan="pro",
                source="code",
                expires_ts=None,
                chains_allowed=list(_CHAINS_PRO_EFFECTIVE),
                ai_limit=_AI_LIMIT_UNLIMITED,
                can_open_new_trades=True,
                conn=conn,
                cur=cur,
            )
            conn.commit()

        return jsonify({
            "status": "ok",
            "plan": "pro",
            "source": "code",
            "expires_at": None,
            "chains_allowed": list(_CHAINS_PRO_EFFECTIVE),
            "ai_limit": _AI_LIMIT_UNLIMITED,
            "can_open_new_trades": True,
            "can_close_trades": True,
            "active": True,
        })
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _require_trading_enabled() -> tuple[Optional[str], Optional[dict], Optional[tuple]]:
    """
    Returns (wallet_address, policy, error_response_tuple_or_None).

    Nexus Analyt policy:
      - NO "Trading ON/OFF" gate in the product anymore.
      - Trading is allowed if (Redeem OR Subscription) access is ACTIVE.

    Enforces:
      - valid Bearer token
      - access.can_open_new_trades == True   (Redeem / Subscription)
    """
    wa = _require_auth()
    if not wa:
        return None, None, err("unauthorized", 401)

    policy = get_policy(wa) or {}

    st = _compute_access_status(wa)
    if not bool(st.get("can_open_new_trades")):
        return wa, policy, err("access required (redeem or subscription) to open new trades", 403)

    # Backward-compat for older clients expecting this field
    policy.setdefault("trading_enabled", True)
    return wa, policy, None


def _grid_item_variants(item_id: str) -> list:
    """Return candidate item IDs to improve cross-device compatibility (with/without chain prefix)."""
    it = (item_id or "").strip()
    if not it:
        return []
    vars = []
    if it not in vars: vars.append(it)
    # Strip chain prefix if present (e.g., "POL:POL" -> "POL")
    if ":" in it:
        base = it.split(":", 1)[1].strip()
        if base and base not in vars: vars.append(base)
    else:
        # Add common chain prefixes if missing (e.g., "POL" -> "POL:POL")
        up = it.upper()
        for ch in ("POL", "BNB", "ETH"):
            if up == ch:
                cand = f"{ch}:{ch}"
                if cand not in vars: vars.append(cand)
    return vars


def _grid_sessions_set(item_id: str, sess: dict) -> None:
    for it in _grid_item_variants(item_id):
        GRID_SESSIONS[it] = sess

def _get_owned_session(item_id: str, wa: str) -> Optional[dict]:
    """Return the grid session if it belongs to wallet `wa`. Legacy sessions without owner are treated as owned.
    Also tolerates item-id variants to keep orders visible across devices/versions.
    """
    for it in _grid_item_variants(item_id):
        sess = GRID_SESSIONS.get(it)
        if not isinstance(sess, dict):
            continue
        owner = _norm_addr(sess.get("wallet_address") or "")
        if not owner or owner == _norm_addr(wa):
            return sess
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
# Profit / Fee Engine (Lifetime threshold)
# -------------------------
FEE_RATE = float(os.getenv("NEXUS_FEE_RATE", "0.03"))
FEE_FREE_THRESHOLD_USD = float(os.getenv("NEXUS_FEE_FREE_THRESHOLD_USD", "1000"))

def _profit_state_get(wallet_address: str) -> dict:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"wallet_address": "", "lifetime_profit_usd": 0.0, "lifetime_fee_paid_usd": 0.0}
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT lifetime_profit_usd, lifetime_fee_paid_usd FROM profit_state WHERE wallet_address = ?", (wa,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"wallet_address": wa, "lifetime_profit_usd": 0.0, "lifetime_fee_paid_usd": 0.0}
    return {
        "wallet_address": wa,
        "lifetime_profit_usd": float(row[0] or 0.0),
        "lifetime_fee_paid_usd": float(row[1] or 0.0),
    }

def _profit_state_upsert(wallet_address: str, lifetime_profit_usd: float, lifetime_fee_paid_usd: float):
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO profit_state(wallet_address, lifetime_profit_usd, lifetime_fee_paid_usd, updated_ts)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(wallet_address) DO UPDATE SET
          lifetime_profit_usd=excluded.lifetime_profit_usd,
          lifetime_fee_paid_usd=excluded.lifetime_fee_paid_usd,
          updated_ts=excluded.updated_ts
        """,
        (wa, float(lifetime_profit_usd or 0.0), float(lifetime_fee_paid_usd or 0.0), now_ts()),
    )
    conn.commit()
    conn.close()

def _fee_for_profit_delta(prev_lifetime_profit: float, profit_delta: float) -> tuple[float, float]:
    """Returns (fee_usd, taxable_profit_usd) for a new realized profit delta.

    Model:
      - first FEE_FREE_THRESHOLD_USD lifetime profit is free
      - after threshold, 3% fee on every additional realized profit
      - if a profit delta crosses the threshold, only the part above threshold is taxable
    """
    try:
        prev = float(prev_lifetime_profit or 0.0)
        delta = float(profit_delta or 0.0)
    except Exception:
        return (0.0, 0.0)

    if delta <= 0:
        return (0.0, 0.0)

    thr = float(FEE_FREE_THRESHOLD_USD or 1000.0)
    new_total = prev + delta

    taxable = max(0.0, new_total - thr) - max(0.0, prev - thr)
    fee = taxable * float(FEE_RATE or 0.03)
    # keep it stable
    fee = round(fee, 6)
    taxable = round(taxable, 6)
    return (fee, taxable)

def _ledger_record_pnl_event(
    wallet_address: str,
    item_id: str,
    fill: dict,
    pnl_delta_usd: float,
) -> dict:
    """Idempotently record a realized pnl event (for SELL fills).

    Returns:
      {
        ok: bool,
        event_id: str,
        already_recorded: bool,
        pnl_delta_usd: float,
        fee_usd: float,
        taxable_profit_usd: float,
        lifetime_profit_usd: float
      }
    """
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"ok": False, "error": "missing wallet"}

    # Only profit deltas affect the lifetime-profit threshold.
    try:
        delta = float(pnl_delta_usd or 0.0)
    except Exception:
        delta = 0.0

    side = str((fill or {}).get("side") or "").upper()
    fill_id = str((fill or {}).get("id") or (fill or {}).get("fill_id") or "")
    filled_ts = int((fill or {}).get("filled_ts") or now_ts())

    # Build a stable idempotency key.
    # If fill_id exists, use it; otherwise derive from (item + ts + side + delta).
    if fill_id:
        event_id = f"{wa}:{item_id}:{fill_id}"
    else:
        event_id = f"{wa}:{item_id}:{side}:{filled_ts}:{round(delta, 8)}"

    conn = _db()
    cur = conn.cursor()

    # Idempotent insert
    try:
        cur.execute(
            """
            INSERT INTO pnl_events(event_id, wallet_address, item_id, side, pnl_delta_usd, fill_id, filled_ts, created_ts)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, wa, str(item_id or ""), side, float(delta), fill_id, int(filled_ts), now_ts()),
        )
        inserted = True
    except Exception:
        inserted = False

    # Update profit_state only if newly inserted and delta > 0
    state = _profit_state_get(wa)
    prev_profit = float(state.get("lifetime_profit_usd") or 0.0)
    prev_fee_paid = float(state.get("lifetime_fee_paid_usd") or 0.0)

    fee_usd = 0.0
    taxable_profit_usd = 0.0
    if inserted and delta > 0:
        fee_usd, taxable_profit_usd = _fee_for_profit_delta(prev_profit, delta)
        _profit_state_upsert(
            wa,
            lifetime_profit_usd=prev_profit + delta,
            lifetime_fee_paid_usd=prev_fee_paid + fee_usd,
        )
        state = _profit_state_get(wa)

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "event_id": event_id,
        "already_recorded": (not inserted),
        "side": side,
        "pnl_delta_usd": float(delta),
        "fee_usd": float(fee_usd),
        "taxable_profit_usd": float(taxable_profit_usd),
        "lifetime_profit_usd": float(state.get("lifetime_profit_usd") or 0.0),
        "lifetime_fee_paid_usd": float(state.get("lifetime_fee_paid_usd") or 0.0),
    }
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
    # NFTs are disabled for the initial release (UI removed). Keeping the endpoint
    # for future re-enable without breaking old deployments.
    return err("nft access is disabled", 403)

@app.route("/api/access/subscribe/verify", methods=["POST"])
def api_access_subscribe_verify():
    """Verify an onchain USDC/USDT payment to Treasury and activate PRO subscription access.

    Body: { chain_id, tx_hash, plan?: "pro" }

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
    plan = "pro"  # single plan (15$/mo); client may still send plan but we ignore it

    try:
        chain_id = int(chain_id)
    except Exception:
        return err("invalid chain_id", 400)


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

    # activate PRO subscription (default 30 days; configurable)
    sub_seconds = int(os.getenv("NEXUS_SUBSCRIPTION_SECONDS", str(60 * 60 * 24 * 30)))
    expires_ts = now_ts() + sub_seconds
    plan = "pro"
    chains_allowed = list(_CHAINS_PRO_EFFECTIVE)
    ai_limit = _AI_LIMIT_UNLIMITED

    _access_state_put(
        wallet_address=wa,
        plan=plan,
        source=str(proof.get("token") or "payment").lower(),
        expires_ts=expires_ts,
        chains_allowed=chains_allowed,
        ai_limit=ai_limit,
        can_open_new_trades=True,
        conn=conn,
        cur=cur,
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

    # Trading ON/OFF removed: do not accept trading_enabled from clients
    policy.pop("trading_enabled", None)

    cur = get_policy(wa)
    cur.update(policy)
    # Normalize extra fields
    # Trading ON/OFF removed: ignore any client-provided trading_enabled
    cur["trading_enabled"] = True
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
    chain_id = body.get("chain_id") or 137
    pair = (body.get("pair") or "").strip()
    side = (body.get("side") or "").strip().lower()
    amount = body.get("amount")  # keep as string for precision
    max_slippage_bps = body.get("max_slippage_bps")
    deadline_ts = body.get("deadline_ts") or (now_ts() + 10 * 60)
    allowed_contracts = body.get("allowed_contracts") or []

    if not pair or side not in ("buy", "sell"):
        return err("missing pair or invalid side", 400)

    policy = get_policy(wa)

    if max_slippage_bps is None:
        max_slippage_bps = policy.get("max_slippage_bps", 75)

    # Enforce enabled chains in this deployment (Phase 1: POL only)
    if _ENABLED_CHAIN_IDS and int(chain_id) not in _ENABLED_CHAIN_IDS:
        return err("chain not enabled", 400)

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

# --- Major symbol → CoinGecko ID (fast-path, avoids search ambiguity) ---
_STATIC_CG_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    # Polygon token on CoinGecko is commonly 'matic-network' (POL rebrand)
    "POL": "matic-network",
    "MATIC": "matic-network",
}
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


def _cg_headers() -> dict:
    h = {"User-Agent": "NexusAnalyt/1.0 (+Render/Flask)"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h

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
_WATCH_SNAP_TTL_SEC = int(os.getenv("WATCH_SNAP_TTL_SEC", "120"))  # 1 min default

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
            r = requests.get(url, params=params, headers=_cg_headers(), timeout=timeout)
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
    """Search CoinGecko coins by query. Returns list of {id,name,symbol,market_cap_rank}.

    This endpoint is rate-limited. To keep the UI stable we:
      - apply a short global cooldown when we hit 429
      - cache results for a short time (per query) via the existing _cg_cache helpers
      - return [] on throttling instead of throwing (so callers can fallback to stale caches)
    """
    q = (query or "").strip()
    # fast-path: exact ticker matches (instant, avoids network on common coins)
    q_upper = q.upper()
    try:
        if q_upper in _CG_COMMON_IDS:
            quick = [{"id": _CG_COMMON_IDS[q_upper], "name": q_upper, "symbol": q_upper, "market_cap_rank": None}]
            try: _cg_cache_set(f"search|{q.lower()}", quick)
            except Exception: pass
            return quick
    except Exception:
        pass
    if not q:
        return []

    # small cache (120s) for search results (prevents hammering /search)
    cache_key = f"search|{q.lower()}"
    try:
        cached = _cg_cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        pass

    # global cooldown after we see 429
    global _CG_SEARCH_COOLDOWN_UNTIL
    try:
        if int(time.time()) < int(_CG_SEARCH_COOLDOWN_UNTIL or 0):
            stale = _cg_cache_get_any(cache_key)
            return stale if stale is not None else []
    except Exception:
        pass

    url = f"{COINGECKO_BASE}/search"
    try:
        r = requests.get(url, params={"query": q}, headers=_cg_headers(), timeout=6)

        # throttle handling
        if r.status_code == 429:
            _CG_SEARCH_COOLDOWN_UNTIL = int(time.time()) + 120
            stale = _cg_cache_get_any(cache_key)
            return stale if stale is not None else []

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

        # cache (short)
        try:
            _cg_cache_set(cache_key, out)
        except Exception:
            pass
        return out

    except Exception:
        # best-effort: stale
        try:
            stale = _cg_cache_get_any(cache_key)
            if stale is not None:
                return stale
        except Exception:
            pass
        return []


# search cooldown (seconds since epoch); set when 429 happens
_CG_SEARCH_COOLDOWN_UNTIL = 0



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
    r = requests.get(url, params={"vs_currency": "usd", "days": str(days)}, headers=_cg_headers(), timeout=15)
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


@app.route("/api/prices", methods=["GET"])
def api_prices():
    """GET /api/prices?symbols=BTC,ETH
    Returns: { status, prices: {SYM:{price,source,...}}, errors: {SYM:msg} }.
    Never returns 500; partial failures are reported in `errors`.
    """
    syms_raw = (request.args.get("symbols") or "").strip()
    symbols = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
    symbols = list(dict.fromkeys(symbols))[:25]

    prices = {}
    errors = {}
    for sym in symbols:
        try:
            p = _price_multi(sym)
            if p:
                prices[sym] = p
            else:
                errors[sym] = "price_unavailable"
        except Exception as e:
            errors[sym] = str(e)

    return jsonify({"status": "ok" if not errors else "partial", "prices": prices, "errors": errors}), 200

@app.route("/api/market/search", methods=["GET"])
def api_market_search():
    q = request.args.get("query") or ""
    cache_key = f"market-search|{q.strip().lower()}"
    try:
        results = _search_assets_multi(q, limit=25)
        resp = {"query": q, "results": results}
        _gen_cache_set(cache_key, resp)
        return jsonify(resp)
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            return jsonify(cached)
        return jsonify({"query": q, "results": [], "error": str(e)}), 200


@app.route("/api/coins/search", methods=["GET"])
def api_coins_search():
    """Coin search for the UI (like the old app).

    GET /api/coins/search?q=TON
    Returns: [{id,name,symbol,market_cap_rank}, ...]
    Never returns 500; on error returns [].
    """
    q = (request.args.get("q") or request.args.get("query") or "").strip()
    if not q:
        return jsonify([]), 200
    try:
        return jsonify(_search_assets_multi(q, limit=25)), 200
    except Exception as e:
        print("coins/search error:", e)
        return jsonify([]), 200

# --- Search alias (compat with architecture doc & newer UI) ---
# Frontend expects: GET /api/search?q=...
# Canonical implementation currently lives at /api/coins/search.
@app.route("/api/search", methods=["GET"])
def api_search_alias():
    return api_coins_search()



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
        return jsonify({"symbol": symbol.upper(), "resolved": None, "error": str(e)}), 200
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
        # Never hard-fail the UI; return stale cache or an empty-but-OK payload.
        stale = _cache_get_any(_WATCH_SNAP_CACHE, wl_cache_key)
        if stale is not None:
            stale = dict(stale)
            stale["status"] = "partial"
            stale["partial"] = True
            stale["error"] = str(e)
            return jsonify(stale), 200
        return jsonify({"status": "partial", "partial": True, "error": str(e), "results": [], "ts": int(time.time())}), 200



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
    "2Y": 730,
    "3Y": 1095,
}



def _range_to_days(range_key: str) -> int:
    """Normalize UI range keys like '30D', '30d', '7D', '1Y' into integer days."""
    rk = (range_key or "").strip()
    if not rk:
        return 30
    rk_u = rk.upper()
    # direct map
    if rk_u in _RANGE_TO_DAYS:
        return int(_RANGE_TO_DAYS[rk_u])
    # allow '30d' etc.
    m = re.match(r"^(\d{1,4})\s*D$", rk_u)
    if m:
        return int(m.group(1))
    m = re.match(r"^(\d{1,4})\s*DAY(S)?$", rk_u)
    if m:
        return int(m.group(1))
    # small hour/min buckets -> treat as 1 day
    if rk_u.endswith("H") or rk_u.endswith("M"):
        return 1
    return 30
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

def _daily_close(points):
    """Convert intraday [[ts_ms, price], ...] into daily close points (UTC)."""
    if not isinstance(points, list) or not points:
        return []
    from datetime import datetime, timezone
    by_day = {}
    for row in points:
        try:
            ts = int(row[0]); px = float(row[1])
        except Exception:
            continue
        day = datetime.fromtimestamp(ts/1000, tz=timezone.utc).date().isoformat()
        by_day[day] = [ts, px]  # overwrite => last point of day (close)
    return [by_day[k] for k in sorted(by_day.keys())]

def _get_series_for_symbol(sym: str, days: int):
    """
    Returns list[[ts_ms, price_usd], ...] for the last N days.

    Router:
      1) CryptoCompare histoday (USD) when available
      2) CoinGecko market_chart fallback
    """
    sym_u = (sym or "").strip().upper()
    try:
        days_i = int(days or 0)
    except Exception:
        days_i = 0
    if not sym_u or days_i <= 0:
        return []

    # 1) CryptoCompare daily closes
    try:
        hist = _cryptocompare_histoday(sym_u, days_i)
        if hist:
            return [[int(p["ts"]) * 1000, float(p["price"])] for p in hist if isinstance(p, dict) and p.get("ts") and p.get("price") is not None]
    except Exception:
        pass

    # 2) CoinGecko fallback (cached)
    try:
        cg_id = _cg_resolve_symbol(sym_u)
        if not cg_id:
            return []
        data = _cg_market_chart_usd(cg_id, days=days_i)
        if not data:
            return []
        prices = data.get("prices") or []
        out = []
        for row in prices:
            try:
                ts = int(row[0])
                px = float(row[1])
                out.append([ts, px])
            except Exception:
                continue
        return out
    except Exception:
        return []

def _health_for_symbol(sym: str, series):
    """
    Minimal health: last price + pct change vs first point.
    UI can ignore it if not needed.
    """
    if not series or len(series) < 2:
        return None
    p0 = float(series[0][1])
    p1 = float(series[-1][1])
    if p0 <= 0:
        return None
    pct = (p1 - p0) / p0 * 100.0
    return {"symbol": sym, "last": p1, "pct": pct}


@app.route("/api/compare", methods=["GET", "OPTIONS"])
def api_compare():
    # Preflight
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        symbols_raw = (request.args.get("symbols", "") or "").strip()
        range_key = (request.args.get("range", "30d") or "30d").strip().lower()

        # Normalize common variants
        range_alias = {
            "1d": "1d", "7d": "7d", "30d": "30d", "90d": "90d",
            "1y": "1y", "2y": "2y", "3y": "3y",
            "30": "30d", "90": "90d", "365": "1y", "730": "2y",
        }
        range_key = range_alias.get(range_key, range_key)

        # Parse days safely (never 500 because of range)
        try:
            days = _range_to_days(range_key)
        except Exception:
            range_key = "30d"
            days = _range_to_days(range_key)

        symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]

        # Instead of 400 -> return empty ok (frontend stays stable)
        if len(symbols) < 2:
            return jsonify({
                "status": "ok",
                "range": range_key,
                "days": days,
                "symbols": symbols,
                "series": {},
                "daily": {},
                "health": {},
                "errors": {"_": "select at least 2 symbols"}
            }), 200

        series_out = {}
        daily_out = {}
        errors = {}
        health_out = {}

        for sym in symbols:
            try:
                series = _get_series_for_symbol(sym, days) or []
                series_out[sym] = series

                # daily close (defensive)
                try:
                    dc = _daily_close(series)
                    daily_out[sym] = dc[-(days + 2):] if days and len(dc) > (days + 2) else dc
                except Exception:
                    daily_out[sym] = []

                # health (defensive)
                try:
                    h = _health_for_symbol(sym, series)
                    if h:
                        health_out[sym] = h
                except Exception:
                    pass

            except Exception as e:
                errors[sym] = str(e)
                series_out[sym] = []
                daily_out[sym] = []

        # If everything empty, try cache fallback
        if all(len(series_out.get(s, [])) == 0 for s in symbols):
            stale = _cache_get_any(_COMPARE_CACHE, f"{','.join(symbols)}:{range_key}")
            if stale:
                stale = dict(stale)
                stale["status"] = "partial"
                stale["partial"] = True
                stale["errors"] = errors
                return jsonify(stale), 200

        # Build response (PARTIAL vs FULL)
        out = {
            "status": "partial" if errors else "ok",
            "partial": True if errors else False,
            "range": range_key,
            "days": days,
            "symbols": symbols,
            "series": series_out,
            "daily": daily_out,
            "errors": errors,
            "updated_at": int(time.time()),
        }
        if health_out:
            out["health"] = health_out

        # Cache good/partial response (key must match fallback key!)
        _cache_set(_COMPARE_CACHE, f"{','.join(symbols)}:{range_key}", out)

        return jsonify(out), 200

    except Exception as e:
        # Last resort: return JSON error (frontend can show message)
        return err(str(e), 500)


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
    chain_key_req = str(body.get("chain") or _grid_chain_key(item_id) or "").strip().upper()

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
        # Always bind session to authenticated wallet (addr is optional)
        session["wallet_address"] = _norm_addr(addr) if addr else _norm_addr(wa)
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
        _grid_sessions_set(item_id, _trim_grid_session(session))
        _persist_grid_state()

        try:
            conn_ui = _db()
            try:
                with DB_WRITE_LOCK:
                    _grid_ui_state_put(conn_ui, wa, active_chain=(chain_key_req or _grid_chain_key(item_id) or "POL"), active_item=item_id)
                    conn_ui.commit()
            finally:
                conn_ui.close()
        except Exception:
            pass

        # Seed authoritative grid vault from on-chain native wallet balance (POL/BNB/ETH)
        # so Vault/Reserved/Free are correct immediately after Start and after refresh.
        try:
            chain_key = str(body.get("chain") or chain_key_req or "").strip()
            conn = _db()
            try:
                native_total = _native_balance_for_wallet(session.get("wallet_address") or wa, chain=chain_key, item_id=item_id)
                if native_total > 0:
                    with DB_WRITE_LOCK:
                        _grid_db_set_vault_total(conn, wa, item_id, native_total, chain=chain_key)
                        conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

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


# Frontend compatibility alias (some UIs call /api/grid/cycle/start)
@app.route("/api/grid/cycle/start", methods=["POST", "GET"])
def api_grid_cycle_start():
    return api_grid_start()

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

    _grid_sessions_set(item_id, _trim_grid_session(updated))
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

                        
                        # Ledger: record realized profit events (SELL only, idempotent)
                        try:
                            if str(_f.get("side") or "").upper() == "SELL" and float(_delta or 0.0) != 0.0:
                                # session owner wallet is attached on start; fallback to current request wallet
                                owner = session.get("wallet_address") or wa
                                _f["_ledger"] = _ledger_record_pnl_event(owner, item_id, _f, float(_delta or 0.0))
                        except Exception:
                            pass

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
    try:
        conn_ui = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_ui_state_put(conn_ui, wa, active_chain=(_grid_chain_key(item_id) or "POL"), active_item=item_id)
                conn_ui.commit()
        finally:
            conn_ui.close()
    except Exception:
        pass
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
    _grid_sessions_set(item_id, _trim_grid_session(session))
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
        _grid_sessions_set(item_id, _trim_grid_session(session))
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
        _grid_sessions_set(item_id, sess)

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

@app.route("/api/grid/ui/state", methods=["GET", "POST"])
def api_grid_ui_state():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return jsonify({"status": "error", "error": "unauthorized", "ts": now_ts()}), 401

    conn = _db()
    try:
        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            active_chain = _normalize_chain_key(body.get("chain") or body.get("active_chain") or "")
            active_item = str(body.get("item") or body.get("active_item") or "").strip()
            active_chain = _grid_chain_key(active_item, active_chain) or active_chain or "POL"
            if active_item and ":" not in active_item:
                active_item = f"{active_chain}:{str(active_item).strip().upper()}"
            with DB_WRITE_LOCK:
                state = _grid_ui_state_put(conn, wa, active_chain=active_chain, active_item=active_item)
                conn.commit()
            return jsonify({"status": "ok", **state, "wallet_address": _norm_addr(wa), "ts": now_ts()})

        state = _grid_ui_state_get(conn, wa)
        return jsonify({"status": "ok", **state, "wallet_address": _norm_addr(wa), "ts": now_ts()})
    finally:
        conn.close()

@app.route("/api/grid/init", methods=["GET"])
def api_grid_init():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return jsonify({"status": "error", "error": "unauthorized", "ts": now_ts()}), 401

    req_chain = _normalize_chain_key(request.args.get("chain") or request.args.get("active_chain") or "")
    req_item = str(request.args.get("item") or request.args.get("item_id") or request.args.get("active_item") or "").strip()

    conn = _db()
    try:
        state = _grid_ui_state_get(conn, wa)
        requested_chain = _grid_chain_key(req_item, req_chain or "") or req_chain or ""
        saved_chain = _grid_chain_key(state.get("active_item") or "", state.get("active_chain") or "") or "POL"
        active_chain = requested_chain or saved_chain or "POL"

        if req_item:
            active_item = req_item
        else:
            saved_item = str(state.get("active_item") or "").strip()
            saved_item_chain = _grid_chain_key(saved_item, state.get("active_chain") or "") or saved_chain
            # IMPORTANT:
            # If the caller explicitly requested a different chain, do NOT reuse the previous
            # active_item from another chain (e.g. active_chain=POL with active_item=BNB:BNB).
            # That stale cross-chain combination caused the UI to need a manual BNB -> POL
            # toggle before the correct vault/orders appeared after refresh.
            if requested_chain and saved_item_chain != active_chain:
                active_item = _grid_default_item_for_chain(active_chain)
            else:
                active_item = saved_item or _grid_default_item_for_chain(active_chain)

        if active_item and ":" not in active_item:
            active_item = f"{active_chain}:{str(active_item).strip().upper()}"

        with DB_WRITE_LOCK:
            state = _grid_ui_state_put(conn, wa, active_chain=active_chain, active_item=active_item)
            conn.commit()

        chain = _grid_chain_key(state["active_item"], state["active_chain"]) or "POL"
        item_id = state["active_item"]
        orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain=chain)
        if not orders:
            orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain="")
        vault_total = _grid_best_vault_total(conn, wa, item_id, chain=chain)
        reserved = _grid_db_reserved(conn, wa, item_id, chain=chain)
        if not reserved:
            reserved = _grid_db_reserved(conn, wa, item_id, chain="")
        free = max(0.0, float(vault_total) - float(reserved))

        session = _get_owned_session(item_id, wa)
        tick = int(session.get("ticks") or 0) if isinstance(session, dict) else 0
        price = float(session.get("price") or 0.0) if isinstance(session, dict) and session.get("price") is not None else None
        running = bool(session.get("running")) and not bool(session.get("stopped")) if isinstance(session, dict) else False

        vault_state = None
        try:
            vault_state = _vault_state_read(wa, chain)
        except Exception:
            vault_state = None

        return jsonify({
            "status": "ok",
            "wallet_address": _norm_addr(wa),
            "active_chain": chain,
            "active_item": item_id,
            "active_coin": str(item_id).split(":", 1)[-1].upper() if item_id else chain,
            "item": item_id,
            "orders": orders,
            "vault_total": vault_total,
            "reserved": reserved,
            "free": free,
            "tick": tick,
            "price": price,
            "running": running,
            "vault_state": vault_state,
            "loaded_item_id": item_id,
            "loaded_chain": chain,
            "orders_count": len(orders or []),
            "ts": now_ts(),
        })
    finally:
        conn.close()

@app.route("/api/grid/orders", methods=["GET"])
def api_grid_orders():
    """Return grid orders (SQLite-backed).

    - If ?item=... is provided: return orders for that item for this wallet.
    - If no item is provided: return ALL orders across items for this wallet.

    When item is provided we also return: vault_total, reserved, free.
    """
    wa = _require_auth() or _pick_wallet_from_request()

    if not wa:
        item_id = request.args.get("item") or request.args.get("item_id")
        if item_id:
            item_id = str(item_id).strip()
            return jsonify({"status": "ok", "item": item_id, "orders": [], "unauthenticated": True, "ts": now_ts()})
        return jsonify({"status": "ok", "orders": [], "unauthenticated": True, "ts": now_ts()})

    item_id = request.args.get("item") or request.args.get("item_id")
    chain = _normalize_chain_key(request.args.get("chain") or "")

    conn = _db()
    try:
        if item_id:
            item_id = str(item_id).strip()
            chain_eff = _grid_chain_key(item_id, chain) or "POL"
            if item_id and ":" not in item_id:
                item_id = f"{chain_eff}:{str(item_id).strip().upper()}"
            if item_id:
                try:
                    with DB_WRITE_LOCK:
                        _grid_ui_state_put(conn, wa, active_chain=chain_eff, active_item=item_id)
                        conn.commit()
                except Exception:
                    pass
            orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain=chain_eff)
            if not orders:
                orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain="")
            vault_total = _grid_best_vault_total(conn, wa, item_id, chain=chain_eff)
            reserved = _grid_db_reserved(conn, wa, item_id, chain=chain_eff)
            if not reserved:
                reserved = _grid_db_reserved(conn, wa, item_id, chain="")
            free = max(0.0, float(vault_total) - float(reserved))
            for o in orders:
                o["item"] = o.get("item") or item_id
            return jsonify({"status": "ok", "item": item_id, "active_chain": chain_eff, "active_item": item_id, "orders": orders, "vault_total": vault_total, "reserved": reserved, "free": free, "ts": now_ts()})

        orders = _grid_db_list_orders(conn, wa, item_id=None, chain=chain)
        return jsonify({"status": "ok", "orders": orders, "ts": now_ts()})
    finally:
        conn.close()

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



@app.route("/api/grid/budgets", methods=["GET"])
def api_grid_budgets():
    """Return per-item budget state for the authenticated wallet.

    This powers the Wallet UI split:
      - Total (on-chain) remains the vault/privy balance
      - In bots (reserved) is derived from active grid sessions (USD-based budget lock)
      - Available is informational (USD-based) and does NOT affect on-chain balances

    Response:
      { status:"ok", items:[{item, locked_usd, available_usd, initial_capital_usd, mode, order_mode}],
        totals:{locked_usd, available_usd} }
    """
    wa = _require_auth()
    if not wa:
        return ok({"items": [], "totals": {"locked_usd": 0.0, "available_usd": 0.0}})

    wa_n = _norm_addr(wa)
    items = []
    locked_total = 0.0
    avail_total = 0.0

    try:
        for item_id, sess in (GRID_SESSIONS or {}).items():
            if not isinstance(sess, dict):
                continue
            if _norm_addr(sess.get("wallet_address") or "") != wa_n:
                continue

            locked = float(sess.get("wallet_locked_usd") or 0.0)
            avail = float(sess.get("wallet_available_usd") or 0.0)
            initc = float(sess.get("initial_capital_usd") or sess.get("initial_capital") or 0.0)

            locked_total += max(0.0, locked)
            avail_total += max(0.0, avail)

            items.append({
                "item": item_id,
                "locked_usd": max(0.0, locked),
                "available_usd": max(0.0, avail),
                "initial_capital_usd": max(0.0, initc),
                "mode": sess.get("mode"),
                "order_mode": sess.get("order_mode"),
            })
    except Exception:
        items = []
        locked_total = 0.0
        avail_total = 0.0

    return ok({
        "items": items,
        "totals": {
            "locked_usd": round(locked_total, 6),
            "available_usd": round(avail_total, 6),
        }
    })


@app.route("/api/grid/budgets_by_chain", methods=["GET"])
def api_grid_budgets_by_chain():
    """Return grid budget locks grouped by chain symbol (ETH/BNB/POL).

    Response:
      { items:[...], totals:{locked_usd, available_usd}, by_chain:{ETH:{locked_usd,available_usd},...} }
    """
    wa = _require_auth()
    if not wa:
        return ok({"items": [], "totals": {"locked_usd": 0.0, "available_usd": 0.0}, "by_chain": {}})

    wa_n = _norm_addr(wa)
    items = []
    locked_total = 0.0
    avail_total = 0.0
    by_chain = {}

    def _item_chain(item_id: str) -> str:
        s = (item_id or "").strip()
        if ":" in s:
            pref = s.split(":", 1)[0].upper()
            if pref in ("ETH", "BNB", "POL"):
                return pref
        up = s.upper()
        if up in ("ETH", "BNB", "POL"):
            return up
        # Default: treat unknown as ETH (keeps UI stable). Change to "UNKNOWN" if you prefer.
        return "ETH"

    try:
        for item_id, sess in (GRID_SESSIONS or {}).items():
            if not isinstance(sess, dict):
                continue
            if _norm_addr(sess.get("wallet_address") or "") != wa_n:
                continue

            locked = float(sess.get("wallet_locked_usd") or 0.0)
            avail = float(sess.get("wallet_available_usd") or 0.0)
            initc = float(sess.get("initial_capital_usd") or sess.get("initial_capital") or 0.0)

            locked = max(0.0, locked)
            avail = max(0.0, avail)
            locked_total += locked
            avail_total += avail

            ch = _item_chain(str(item_id))
            if ch not in by_chain:
                by_chain[ch] = {"locked_usd": 0.0, "available_usd": 0.0}
            by_chain[ch]["locked_usd"] += locked
            by_chain[ch]["available_usd"] += avail

            items.append({
                "item": item_id,
                "locked_usd": locked,
                "available_usd": avail,
                "initial_capital_usd": max(0.0, initc),
                "mode": sess.get("mode"),
                "order_mode": sess.get("order_mode"),
                "chain": ch,
            })
    except Exception:
        items = []
        locked_total = 0.0
        avail_total = 0.0
        by_chain = {}

    for ch in list(by_chain.keys()):
        by_chain[ch]["locked_usd"] = round(by_chain[ch]["locked_usd"], 6)
        by_chain[ch]["available_usd"] = round(by_chain[ch]["available_usd"], 6)

    return ok({
        "items": items,
        "totals": {
            "locked_usd": round(locked_total, 6),
            "available_usd": round(avail_total, 6),
        },
        "by_chain": by_chain
    })





@app.route("/api/grid/order/stop", methods=["POST"])
def api_grid_order_stop():
    """Stop (cancel) a single order (SQLite-backed).

    Works even if the grid session is not running.
    Accepts:
      {item, id} OR {item, orderId/order_id} OR {item, side, price, level}
    """
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400

    oid = payload.get("id") or payload.get("orderId") or payload.get("order_id") or payload.get("oid")
    side = payload.get("side")
    price = payload.get("price")
    level = payload.get("level")
    chain = str(payload.get("chain") or "").strip()
    try:
        conn_ui = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_ui_state_put(conn_ui, wa, active_chain=(_grid_chain_key(item_id, chain) or chain or "POL"), active_item=item_id)
                conn_ui.commit()
        finally:
            conn_ui.close()
    except Exception:
        pass

    # Update RAM session if present (fast UI feedback)
    sess = GRID_SESSIONS.get(item_id)
    if isinstance(sess, dict):
        orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
        def _match(o: dict) -> bool:
            if not isinstance(o, dict): return False
            if o.get("status") != "OPEN": return False
            if oid is not None and str(oid).strip() != "":
                return str(o.get("id")) == str(oid)
            if side is None or level is None: return False
            if str(o.get("side")) != str(side): return False
            if o.get("level") != level: return False
            if price is None: return True
            try:
                return float(o.get("price")) == float(price)
            except Exception:
                return o.get("price") == price

        for o in orders:
            if _match(o):
                o["status"] = "CANCELLED"
                o["cancelled_ts"] = int(time.time())
                break
        sess["orders"] = orders

    if oid is None or str(oid).strip() == "":
        return jsonify({"error": "missing id"}), 400

    # Authoritative DB update
    conn = _db()
    try:
        with DB_WRITE_LOCK:
            rc = _grid_db_cancel_order(conn, wa, item_id, str(oid), chain=chain)
            conn.commit()

        orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain=chain)
        vault_total = _grid_best_vault_total(conn, wa, item_id, chain=chain)
        reserved = _grid_db_reserved(conn, wa, item_id, chain=chain)
        free = max(0.0, float(vault_total) - float(reserved))

        if rc <= 0:
            return jsonify({"error": "order not found"}), 404
        return jsonify({"status": "ok", "item": item_id, "orders": orders, "vault_total": vault_total, "reserved": reserved, "free": free, "ts": now_ts()})
    finally:
        conn.close()

@app.route("/api/grid/order/delete", methods=["POST", "DELETE"])
def api_grid_order_delete():
    """Permanently delete a single order (SQLite-backed).

    Works even if the grid session is not running.
    Accepts:
      {item, id} OR {item, orderId/order_id}
    """
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400

    oid = payload.get("id") or payload.get("orderId") or payload.get("order_id") or payload.get("oid")
    if oid is None or str(oid).strip() == "":
        return jsonify({"error": "missing id"}), 400

    chain = str(payload.get("chain") or "").strip()
    try:
        conn_ui = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_ui_state_put(conn_ui, wa, active_chain=(_grid_chain_key(item_id, chain) or chain or "POL"), active_item=item_id)
                conn_ui.commit()
        finally:
            conn_ui.close()
    except Exception:
        pass

    # Remove from RAM session if present
    sess = GRID_SESSIONS.get(item_id)
    if isinstance(sess, dict) and isinstance(sess.get("orders"), list):
        sess["orders"] = [o for o in sess["orders"] if not (isinstance(o, dict) and str(o.get("id")) == str(oid))]

    conn = _db()
    try:
        with DB_WRITE_LOCK:
            rc = _grid_db_delete_order(conn, wa, item_id, str(oid), chain=chain)
            conn.commit()

        orders = _grid_db_list_orders(conn, wa, item_id=item_id, chain=chain)
        vault_total = _grid_best_vault_total(conn, wa, item_id, chain=chain)
        reserved = _grid_db_reserved(conn, wa, item_id, chain=chain)
        free = max(0.0, float(vault_total) - float(reserved))

        if rc <= 0:
            return jsonify({"error": "order not found"}), 404
        return jsonify({"status": "ok", "item": item_id, "orders": orders, "vault_total": vault_total, "reserved": reserved, "free": free, "ts": now_ts()})
    finally:
        conn.close()


@app.route("/api/grid/order/cancel", methods=["POST"])
def api_grid_order_cancel_alias():
    return api_grid_order_stop()

@app.route("/api/grid/order/remove", methods=["POST", "DELETE"])
def api_grid_order_remove_alias():
    return api_grid_order_delete()

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
    """Add a manual order (OPEN) into an existing grid session.

    Expected JSON:
      {
        "item": "...",
        "side": "BUY"|"SELL",
        "price": number,
        "qty": number,          # token/native quantity (UI uses Qty)
        "slippage": number,
        "deadline": number
      }

    Notes:
    - This endpoint only stores the order + reserves Qty logically.
    - Execution happens asynchronously by the grid executor (backend) when cycle is running.
    """
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    _require_trading_enabled()

    try:
        payload = request.get_json(silent=True) or {}
        item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
        chain = _grid_chain_key(item_id, payload.get("chain")) or "POL"
        if item_id and ":" not in item_id:
            item_id = f"{chain}:{str(item_id).strip().upper()}"
        if not item_id:
            return jsonify({"error": "missing item"}), 400

        side = str(payload.get("side") or "").upper().strip()
        if side not in ("BUY", "SELL"):
            return jsonify({"error": "side must be BUY or SELL"}), 400

        price = payload.get("price")
        if price is None:
            return jsonify({"error": "missing price"}), 400
        try:
            price_f = float(price)
        except Exception:
            return jsonify({"error": "invalid price"}), 400
        if price_f <= 0:
            return jsonify({"error": "price must be > 0"}), 400

        qty = payload.get("qty")
        if qty is None:
            return jsonify({"error": "missing qty"}), 400
        try:
            qty_f = float(qty)
        except Exception:
            return jsonify({"error": "invalid qty"}), 400
        if qty_f <= 0:
            return jsonify({"error": "qty must be > 0"}), 400

        # Slippage: prefer slippage_bps (UI sends this), fallback to slippage (pct), else default 5%
        slippage_bps = payload.get("slippage_bps")
        slippage = payload.get("slippage")  # percent (e.g. 0.05 = 5%)

        DEFAULT_SLIPPAGE_BPS = int(os.getenv("DEFAULT_SLIPPAGE_BPS", "500"))  # 5%

        try:
            if slippage_bps is not None:
                slip_f = float(int(slippage_bps)) / 10000.0  # bps -> fraction
            elif slippage is not None:
                slip_f = float(slippage)  # already fraction
            else:
                slip_f = float(DEFAULT_SLIPPAGE_BPS) / 10000.0
        except Exception:
            slip_f = float(DEFAULT_SLIPPAGE_BPS) / 10000.0

        deadline = payload.get("deadline") or payload.get("deadline_sec")
        try:
            deadline_i = int(deadline) if deadline is not None else int(DEFAULT_DEADLINE_MINUTES)
        except Exception:
            deadline_i = int(DEFAULT_DEADLINE_MINUTES)

        # Create order
        order = {
            "id": str(uuid.uuid4()),
            "side": side,
            "price": round(price_f, 12),
            "qty": round(qty_f, 12),
            "slippage": slip_f,
            "deadline": deadline_i,
            "status": "OPEN",
            "source": "MANUAL",
            "ts": int(time.time()),
            "level": payload.get("level", None),  # optional
        }

        # Persist to DB (authoritative)
        chain = _grid_chain_key(item_id, payload.get("chain")) or "POL"

        db_saved = True
        db_error = None
        conn = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_db_insert_order(conn, wa, item_id, order, chain=chain)
                conn.commit()
        except Exception as e:
            db_saved = False
            db_error = str(e)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()

        # Optional: mirror into in-memory session if it exists (executor may use it)
        sess = GRID_SESSIONS.get(item_id)
        if isinstance(sess, dict):
            if not isinstance(sess.get("orders"), list):
                sess["orders"] = []
            sess["orders"].insert(0, order)
            _trim_grid_session(sess)
            _persist_grid_state()

        # Return DB view
        conn = _db()
        try:
            try:
                with DB_WRITE_LOCK:
                    _grid_ui_state_put(conn, wa, active_chain=(_grid_chain_key(item_id, chain) or "POL"), active_item=item_id)
                    conn.commit()
            except Exception:
                pass
            orders_db = _grid_db_list_orders(conn, wa, item_id=item_id, chain=chain)
            if not orders_db:
                orders_db = _grid_db_list_orders(conn, wa, item_id=item_id, chain="")
            vault_total = _grid_best_vault_total(conn, wa, item_id, chain=chain)
            reserved = _grid_db_reserved(conn, wa, item_id, chain=chain)
            if not reserved:
                reserved = _grid_db_reserved(conn, wa, item_id, chain="")
            free = max(0.0, float(vault_total) - float(reserved))
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()

        return jsonify({
            "status": "ok",
            "order": order,
            "orders": orders_db,
            "vault_total": vault_total,
            "reserved": reserved,
            "free": free,
            "db_saved": db_saved,
            "db_error": db_error,
            "saved_item_id": item_id,
            "saved_chain": chain,
            "orders_count": len(orders_db or []),
            "ts": now_ts(),
        })

    except Exception as e:
        print("[ERROR] manual add failed:", e)
        return jsonify({"error": "internal_error", "detail": str(e)}), 500


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
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    if st.get("plan") != "pro":
        return err("subscription required for AI", 403)

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
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    if st.get("plan") != "pro":
        return err("subscription required for AI", 403)

    wa = str(request.args.get("wallet_address") or "").strip()
    mem = _ai_mem_get(wa) if wa else []
    return jsonify({"status": "ok", "wallet_address": _norm_addr(wa), "memory": mem})


@app.route("/api/ai/memory/clear", methods=["POST"])
def api_ai_memory_clear():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    if st.get("plan") != "pro":
        return err("subscription required for AI", 403)

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
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    if st.get("plan") != "pro":
        return err("subscription required for AI", 403)

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


# -------------------------
# Multi-API market data router (Search=CoinCap, Prices=Binance, History=CryptoCompare optional, Meta/Fallback=CoinGecko)
# -------------------------
COINCAP_BASE = os.getenv("COINCAP_BASE", "https://api.coincap.io/v2")
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")
CRYPTOCOMPARE_BASE = os.getenv("CRYPTOCOMPARE_BASE", "https://min-api.cryptocompare.com")
CRYPTOCOMPARE_KEY = (os.getenv("CRYPTOCOMPARE_API_KEY") or "").strip()

def _coincap_request_json(path: str, params: dict | None = None, timeout: int = 6):
    url = COINCAP_BASE.rstrip("/") + "/" + path.lstrip("/")
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _coincap_search_assets(query: str, limit: int = 25) -> list:
    """Primary search: CoinCap assets. Normalized to [{id,name,symbol,market_cap_rank}, ...]."""
    q = (query or "").strip()
    if not q:
        return []
    cache_key = f"cc:search|{q.lower()}|{int(limit or 25)}"
    try:
        cached = _gen_cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        pass

    data = _coincap_request_json("/assets", params={"search": q, "limit": max(1, min(int(limit), 50))}, timeout=6) or {}
    items = data.get("data") if isinstance(data, dict) else None
    out = []
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = (it.get("symbol") or "").upper().strip()
            if not sym:
                continue
            out.append({
                "id": it.get("id"),
                "name": it.get("name") or sym,
                "symbol": sym,
                "market_cap_rank": int(it.get("rank")) if str(it.get("rank") or "").isdigit() else None,
            })
    try:
        _gen_cache_set(cache_key, out, ttl=120)
    except Exception:
        pass
    return out

def _binance_symbol_candidates(symbol: str) -> list[str]:
    s = (symbol or "").strip().upper()
    if not s:
        return []
    return [f"{s}USDT", f"{s}BUSD", f"{s}USD"]

def _binance_price_for_symbol(symbol: str) -> dict | None:
    """Primary CEX price: Binance ticker price. Returns {symbol,pair,price,source}."""
    s = (symbol or "").strip().upper()
    if not s:
        return None
    cache_key = f"bz:price|{s}"
    try:
        cached = _gen_cache_get(cache_key)
        if cached is not None:
            return cached
    except Exception:
        pass

    for pair in _binance_symbol_candidates(s):
        try:
            url = BINANCE_BASE.rstrip("/") + "/api/v3/ticker/price"
            r = requests.get(url, params={"symbol": pair}, headers=_cg_headers(), timeout=3)
            if r.status_code == 400:
                continue
            r.raise_for_status()
            j = r.json() or {}
            price = float(j.get("price"))
            if price > 0:
                out = {"symbol": s, "pair": pair, "price": price, "source": "binance"}
                try:
                    _gen_cache_set(cache_key, out, ttl=2)
                except Exception:
                    pass
                return out
        except Exception:
            continue
    return None

def _cryptocompare_histoday(symbol: str, days: int) -> list:
    """Optional history: CryptoCompare histoday (USD). Returns [{ts,price}] (ts seconds)."""
    sym = (symbol or "").strip().upper()
    try:
        days_i = int(days or 0)
    except Exception:
        days_i = 0
    if not sym or days_i <= 0:
        return []
    cache_key = f"ccmp:histoday|{sym}|{days_i}"
    cached = _cache_get(_COMPARE_CACHE, cache_key)
    if cached is not None:
        return cached

    headers = {}
    if CRYPTOCOMPARE_KEY:
        headers["authorization"] = f"Apikey {CRYPTOCOMPARE_KEY}"
    try:
        url = CRYPTOCOMPARE_BASE.rstrip("/") + "/data/v2/histoday"
        r = requests.get(url, params={"fsym": sym, "tsym": "USD", "limit": max(1, min(days_i, 2000))}, headers=headers, timeout=8)
        if r.status_code in (401, 403):
            return []
        r.raise_for_status()
        j = r.json() or {}
        data = (((j.get("Data") or {}).get("Data")) if isinstance(j, dict) else None) or []
        out = []
        if isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    continue
                ts = int(row.get("time") or 0)
                close = row.get("close")
                try:
                    close_f = float(close)
                except Exception:
                    continue
                if ts and close_f > 0:
                    out.append({"ts": ts, "price": close_f})
        _cache_set(_COMPARE_CACHE, cache_key, out)
        return out
    except Exception:
        return []

def _search_assets_multi(query: str, limit: int = 25) -> list:
    """Search router: CoinCap first, CoinGecko fallback."""
    try:
        out = _coincap_search_assets(query, limit=limit)
        if out:
            return out
    except Exception:
        pass
    try:
        return _cg_search(query, limit=limit)
    except Exception:
        return []

def _price_multi(symbol: str) -> dict | None:
    """Price router: Binance first, CoinGecko fallback."""
    try:
        p = _binance_price_for_symbol(symbol)
        if p:
            return p
    except Exception:
        pass
    try:
        coin_id = _cg_search_best_id_for_symbol(symbol)
        if coin_id:
            px = _cg_simple_price_usd(coin_id)
            if px is not None:
                px = float(px)
                if px > 0:
                    return {"symbol": (symbol or "").upper(), "price": px, "source": "coingecko", "id": coin_id}
    except Exception:
        pass
    return None

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
                _grid_sessions_set(item_id, _trim_grid_session(session))
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
