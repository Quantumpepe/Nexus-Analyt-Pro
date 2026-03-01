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

def init_db():
    conn = _db()
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM grid_orders WHERE order_id=? AND wallet_address=? AND item_id=?",
        (oid, _norm_addr(wa), item_id),
    )
    conn.commit()
    deleted_rows = cur.rowcount

    # Best-effort: mirror to running session if present
    try:
        sess = _get_owned_session(item_id, wa)
        if isinstance(sess, dict) and isinstance(sess.get("orders"), list):
            sess["orders"] = [o for o in sess["orders"] if not (isinstance(o, dict) and str(o.get("id")) == oid)]
            _trim_grid_session(sess)
            _persist_grid_state()
    except Exception:
        pass

    # Return DB view
    cur.execute(
        "SELECT order_id, side, price, qty, status, meta_json, created_ts, updated_ts FROM grid_orders "
        "WHERE wallet_address=? AND item_id=? AND COALESCE(status,\'\') NOT IN (\'DELETED\',\'REMOVED\') ORDER BY created_ts ASC",
        (_norm_addr(wa), item_id),
    )
    rows = cur.fetchall()
    orders = []
    for (order_id, side, price, qty, status, meta_json, created_ts, updated_ts) in rows:
        o = {"id": order_id, "order_id": order_id, "item": item_id, "side": side, "price": price, "qty": qty, "status": status, "created_ts": created_ts, "updated_ts": updated_ts}
        if meta_json:
            try: o["meta"] = json.loads(meta_json)
            except Exception: o["meta"] = meta_json
        orders.append(o)

    cur.execute("SELECT vault_total FROM grid_vaults WHERE wallet_address=? AND item_id=?", (_norm_addr(wa), item_id))
    row = cur.fetchone()
    vault_total = float(row[0]) if row and row[0] is not None else 0.0
    reserved = sum(float(o.get("qty") or 0) for o in orders if str(o.get("status","")).upper()=="OPEN")
    free = max(vault_total - reserved, 0.0)

    conn.close()
    if "deleted_rows" in locals() and deleted_rows <= 0:
        return jsonify({"status": "ok", "note": "order not found", "orders": orders, "vault_total": vault_total, "reserved": reserved, "free": free, "ts": now_ts()})

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
    """Add a manual order (persistent).

    This endpoint MUST work even if no in-memory grid session exists (after refresh / new device).
    """
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    _require_trading_enabled()

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400

    side = str(payload.get("side") or "").upper().strip()
    if side not in ("BUY", "SELL"):
        return jsonify({"error": "side must be BUY or SELL"}), 400

    price = payload.get("price")
    qty = payload.get("qty")
    if price is None or qty is None:
        return jsonify({"error": "missing price/qty"}), 400

    try:
        price_f = float(price)
        qty_f = float(qty)
    except Exception:
        return jsonify({"error": "invalid price/qty"}), 400
    if price_f <= 0 or qty_f <= 0:
        return jsonify({"error": "price/qty must be > 0"}), 400

    # Slippage/deadline optional
    slippage_bps = payload.get("slippage_bps")
    slippage = payload.get("slippage")  # fraction
    DEFAULT_SLIPPAGE_BPS = int(os.getenv("DEFAULT_SLIPPAGE_BPS", "500"))  # 5%
    try:
        if slippage_bps is not None:
            slip_f = float(int(slippage_bps)) / 10000.0
        elif slippage is not None:
            slip_f = float(slippage)
        else:
            slip_f = float(DEFAULT_SLIPPAGE_BPS) / 10000.0
    except Exception:
        slip_f = float(DEFAULT_SLIPPAGE_BPS) / 10000.0

    deadline = payload.get("deadline") or payload.get("deadline_sec")
    try:
        deadline_i = int(deadline) if deadline is not None else int(DEFAULT_DEADLINE_MINUTES)
    except Exception:
        deadline_i = int(DEFAULT_DEADLINE_MINUTES)

    order_id = str(uuid.uuid4())
    meta = {
        "slippage": slip_f,
        "deadline": deadline_i,
        "source": "MANUAL",
    }
    if payload.get("level") is not None:
        meta["level"] = payload.get("level")

    nowi = int(time.time())
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO grid_orders(order_id, wallet_address, item_id, side, price, qty, status, meta_json, created_ts, updated_ts) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (order_id, _norm_addr(wa), item_id, side, round(price_f, 12), round(qty_f, 12), "OPEN", json.dumps(meta), nowi, nowi),
    )
    conn.commit()

    # Best-effort: also inject into running session (if any) so UI sees it instantly
    try:
        sess = _get_owned_session(item_id, wa)
        if isinstance(sess, dict):
            order = {
                "id": order_id,
                "side": side,
                "price": round(price_f, 12),
                "qty": round(qty_f, 12),
                "slippage": slip_f,
                "deadline": deadline_i,
                "status": "OPEN",
                "source": "MANUAL",
                "ts": nowi,
                "level": payload.get("level", None),
                "item": item_id,
            }
            if not isinstance(sess.get("orders"), list):
                sess["orders"] = []
            sess["orders"].insert(0, order)
            _trim_grid_session(sess)
            _persist_grid_state()
    except Exception:
        pass

    # Return fresh DB view
    cur.execute(
        "SELECT order_id, side, price, qty, status, meta_json, created_ts, updated_ts FROM grid_orders "
        "WHERE wallet_address=? AND item_id=? AND COALESCE(status,\'\') NOT IN (\'DELETED\',\'REMOVED\') ORDER BY created_ts ASC",
        (_norm_addr(wa), item_id),
    )
    rows = cur.fetchall()
    orders = []
    for (oid, s, p, q, st, mj, cts, uts) in rows:
        o = {"id": oid, "order_id": oid, "item": item_id, "side": s, "price": p, "qty": q, "status": st, "created_ts": cts, "updated_ts": uts}
        if mj:
            try: o["meta"] = json.loads(mj)
            except Exception: o["meta"] = mj
        orders.append(o)

    # Vault totals
    cur.execute("SELECT vault_total FROM grid_vaults WHERE wallet_address=? AND item_id=?", (_norm_addr(wa), item_id))
    row = cur.fetchone()
    vault_total = float(row[0]) if row and row[0] is not None else 0.0
    reserved = sum(float(o.get("qty") or 0) for o in orders if str(o.get("status","")).upper()=="OPEN")
    free = max(vault_total - reserved, 0.0)

    conn.close()
    return jsonify({"status": "ok", "order": orders[-1] if orders else None, "orders": orders, "vault_total": vault_total, "reserved": reserved, "free": free, "ts": now_ts()})

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
