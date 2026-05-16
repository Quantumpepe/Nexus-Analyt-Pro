# backend/app.py
from __future__ import annotations
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS

import os
import time
import threading
import json
import re
import sqlite3
import threading
DB_WRITE_LOCK = threading.RLock()

# Short RPC cache to protect Alchemy quota. Grid execution polling is separate and remains fast.
_VAULT_STATE_CACHE: dict[str, tuple[float, dict]] = {}
_VAULT_STATE_CACHE_TTL_SEC = int(os.getenv("NEXUS_VAULT_STATE_CACHE_TTL_SEC", "8"))

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
# Frontend and backend are on different domains. The frontend uses fetch(..., credentials: "include"),
# so we MUST:
#   - echo a concrete Origin (not "*")
#   - set Access-Control-Allow-Credentials: true
#   - allow the custom headers the frontend actually sends
#
# IMPORTANT:
# Keep ONLY this CORS block. Older duplicated CORS handlers caused the browser to see
# inconsistent Access-Control-Allow-Headers, which led to "preflight 204" followed by
# a CORS error on the real request.

FRONTEND_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "https://nexus-analyt-ui.onrender.com",
    "https://www.nexus-analyt-ui.onrender.com",
    "https://nexus-analyt.com",
    "https://www.nexus-analyt.com",
]

# Allow-list matcher (defensive) for known Render subdomains of this project.
_FRONTEND_ORIGIN_RE = re.compile(r"^https://(www\.)?nexus-analyt-(ui|pro)\.onrender\.com$")
FRONTEND_ORIGINS_SET = set(FRONTEND_ORIGINS)

CORS(
    app,
    resources={r"/api/*": {"origins": FRONTEND_ORIGINS}},
    supports_credentials=True,
    allow_headers=[
        "Content-Type",
        "Authorization",
        "X-Wallet-Address",
        "x-wallet-address",
        "X-API-Key",
        "x-api-key",
    ],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    expose_headers=["Content-Type"],
    max_age=86400,
)

def _is_allowed_origin(origin: str) -> bool:
    if not origin:
        return False
    if origin in FRONTEND_ORIGINS_SET:
        return True
    return bool(_FRONTEND_ORIGIN_RE.match(origin))

@app.after_request
def add_cors_headers(resp):
    try:
        origin = request.headers.get("Origin")
        if origin and _is_allowed_origin(origin):
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = (
                "Content-Type, Authorization, X-Wallet-Address, x-wallet-address, X-API-Key, x-api-key"
            )
            resp.headers["Vary"] = "Origin"
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

    if origin and _is_allowed_origin(origin):
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Vary"] = "Origin"

    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = (
        "Content-Type, Authorization, X-Wallet-Address, x-wallet-address, X-API-Key, x-api-key"
    )
    return resp

@app.get("/api/version")
def api_version():
    return {
        "status": "ok",
        "ts": int(time.time()),
        "render_git_commit": os.getenv("RENDER_GIT_COMMIT"),
        "grid_allow_anon": os.getenv("GRID_ALLOW_ANON"),
    }

import traceback
from flask import make_response, jsonify

@app.errorhandler(Exception)
def _all_errors(e):
    # TEMP: debug output (remove later)
    tb = traceback.format_exc()
    return jsonify({"status": "error", "error": str(e), "trace": tb}), 500

@app.get("/api/ping")
def ping():
    return "ok", 200

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
# Bitquery Whale Engine
# -------------------------
# IMPORTANT: store the token only in backend ENV, never in the frontend.
BITQUERY_API_KEY = (
    os.getenv("BITQUERY_API_KEY")
    or os.getenv("BITQUERY_TOKEN")
    or os.getenv("BITQUERY_ACCESS_TOKEN")
    or ""
).strip()
BITQUERY_GRAPHQL_URL = os.getenv("BITQUERY_GRAPHQL_URL", "https://graphql.bitquery.io").strip()
BITQUERY_WHALE_CACHE_TTL_SEC = int(os.getenv("BITQUERY_WHALE_CACHE_TTL_SEC", "86400"))
BITQUERY_WHALE_LOOKBACK_MIN = int(os.getenv("BITQUERY_WHALE_LOOKBACK_MIN", "180"))
BITQUERY_WHALE_MIN_USD = float(os.getenv("BITQUERY_WHALE_MIN_USD", "10000"))
BITQUERY_WHALE_TRADE_LIMIT = int(os.getenv("BITQUERY_WHALE_TRADE_LIMIT", "40"))
BITQUERY_WHALE_MAX_USD = float(os.getenv("BITQUERY_WHALE_MAX_USD", "250000"))
BITQUERY_WHALE_VOLUME_PCT = float(os.getenv("BITQUERY_WHALE_VOLUME_PCT", "0.001"))
_WHALE_SIGNAL_CACHE: dict[str, tuple[float, dict]] = {}

# -------------------------
# CoinGecko proxy (avoid browser CORS + basic throttling)
# -------------------------
_CG_CACHE: dict[str, tuple[float, dict]] = {}
_CG_TTL_SEC = int(os.getenv("COINGECKO_CACHE_TTL_SEC", "20"))

# -------------------------
# Market Condition (Overextension + RVOL)
# -------------------------
_MARKET_CONDITION_CACHE: dict[str, tuple[float, dict]] = {}
_MARKET_CONDITION_TTL_SEC = int(os.getenv("NEXUS_MARKET_CONDITION_TTL_SEC", "900"))

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


def _market_condition_coin_id(raw: str) -> str:
    """Resolve symbol/CoinGecko id into the best CoinGecko id for market_chart."""
    s = str(raw or "").strip()
    if not s:
        return ""

    sym = s.upper()
    overrides = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "BNB": "binancecoin",
        "SOL": "solana",
        "XRP": "ripple",
        "ADA": "cardano",
        "AVAX": "avalanche-2",
        "TON": "the-open-network",
        "POL": "polygon-ecosystem-token",
        "MATIC": "matic-network",
        "LINK": "chainlink",
    }
    if sym in overrides:
        return overrides[sym]

    # Reuse the later Coin Info resolver if it exists at runtime.
    try:
        cid = _coin_info_id_from_symbol(sym)
        if cid:
            return str(cid)
    except Exception:
        pass

    return s.lower()


def _bitquery_network(chain_key: str) -> str:
    """Map Nexus chain keys to Bitquery EVM network enum values."""
    ck = _normalize_chain_key(chain_key)
    return {
        "ETH": "ethereum",
        "BNB": "bsc",
        "POL": "matic",
    }.get(ck, ck.lower())


def _bitquery_headers() -> dict:
    # Bitquery accounts differ by API generation. Support both common header names.
    return {
        "Content-Type": "application/json",
        "X-API-KEY": BITQUERY_API_KEY,
        "Authorization": f"Bearer {BITQUERY_API_KEY}",
    }


def _bitquery_post(query: str, variables: dict) -> dict:
    if not BITQUERY_API_KEY:
        raise RuntimeError("BITQUERY_API_KEY missing")
    r = requests.post(
        BITQUERY_GRAPHQL_URL,
        headers=_bitquery_headers(),
        json={"query": query, "variables": variables},
        timeout=18,
    )
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"Bitquery HTTP {r.status_code}: {str(data)[:300]}")
    if isinstance(data, dict) and data.get("errors"):
        raise RuntimeError(f"Bitquery errors: {str(data.get('errors'))[:300]}")
    return data if isinstance(data, dict) else {}


_BITQUERY_DEX_TRADE_QUERY = """
query ($network: EthereumNetwork!, $token: String!, $since: ISO8601DateTime!, $limit: Int!) {
  EVM(network: $network) {
    DEXTrades(
      limit: {count: $limit}
      orderBy: {descending: Block_Time}
      where: {
        Block: {Time: {since: $since}}
        Trade: {
          Currency: {SmartContract: {is: $token}}
        }
      }
    ) {
      Block { Time }
      Transaction { Hash }
      Trade {
        AmountInUSD
        Dex { ProtocolName ProtocolFamily SmartContract }
        Buy {
          Amount
          Buyer
          Currency { Symbol SmartContract }
        }
        Sell {
          Amount
          Seller
          Currency { Symbol SmartContract }
        }
      }
    }
  }
}
"""


def _parse_bitquery_time_to_ts(value: str) -> int:
    try:
        from datetime import datetime
        s = str(value or "").replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


def _short_addr(addr: str) -> str:
    a = str(addr or "").strip()
    if len(a) >= 12 and a.startswith("0x"):
        return f"{a[:6]}...{a[-4:]}"
    return a


def _detect_trade_side(trade: dict, token_address: str) -> str:
    """Return buy/sell from the target token perspective."""
    token = str(token_address or "").lower()
    t = trade.get("Trade") or {}
    buy_token = str(((t.get("Buy") or {}).get("Currency") or {}).get("SmartContract") or "").lower()
    sell_token = str(((t.get("Sell") or {}).get("Currency") or {}).get("SmartContract") or "").lower()
    if token and buy_token == token:
        return "buy"
    if token and sell_token == token:
        return "sell"
    return "neutral"


def _whale_strength(amount_usd: float, threshold_usd: float) -> str:
    amt = _safe_float(amount_usd)
    thr = max(_safe_float(threshold_usd), 1.0)
    if amt >= thr * 5:
        return "high"
    if amt >= thr * 2:
        return "medium"
    return "low"


def _dynamic_whale_threshold_usd(volume24h_usd: float | None) -> float:
    """Realistic whale threshold by 24h volume.

    Goal:
      - No fake NEWS: Bitquery trade data is still required.
      - Microcaps can still show real whale events.
      - Large caps stay strict enough to avoid noise.

    Examples:
      10k volume   -> ~800 USD
      50k volume   -> ~3k USD
      1.5M volume  -> ~15k USD
      10B volume   -> capped at 250k USD
    """
    try:
        vol = float(volume24h_usd) if volume24h_usd is not None else None
        if vol is None or not math.isfinite(vol) or vol <= 0:
            return float(BITQUERY_WHALE_MIN_USD)
    except Exception:
        return float(BITQUERY_WHALE_MIN_USD)

    # Microcap: SpongeV2-style whale logic. Below ~3k is usually noise;
    # 3k-5k can be a real whale if volume supports it.
    if vol < 20_000:
        return max(800.0, min(3_000.0, vol * 0.08))

    if vol < 50_000:
        return max(1_000.0, min(3_500.0, vol * 0.07))

    if vol < 500_000:
        return max(3_000.0, min(10_000.0, vol * 0.03))

    if vol < 5_000_000:
        return max(5_000.0, min(50_000.0, vol * 0.01))

    # Large caps: use the configured global min/percent/max.
    return min(
        max(float(BITQUERY_WHALE_MIN_USD), vol * float(BITQUERY_WHALE_VOLUME_PCT)),
        float(BITQUERY_WHALE_MAX_USD)
    )


def _get_whale_signal_bitquery(token_address: str, chain: str = "ETH", volume24h_usd: float | None = None, force_refresh: bool = False) -> dict:
    """Real whale buy/sell detection from Bitquery DEXTrades."""
    token = str(token_address or "").strip().lower()
    ck = _normalize_chain_key(chain)

    if not _looks_like_evm_addr(token):
        return {
            "status": "ok",
            "action": "neutral",
            "strength": "none",
            "icon": "🔥",
            "color": "neutral",
            "summary": "No token contract available for whale detection",
            "source": "bitquery",
            "chain": ck,
            "token": token,
            "ts": now_ts(),
        }

    dyn_threshold = _dynamic_whale_threshold_usd(volume24h_usd)

    cache_key = f"{ck}|{token}|{round(dyn_threshold, 2)}"
    now_f = time.time()
    if not force_refresh:
        hit = _WHALE_SIGNAL_CACHE.get(cache_key)
        if hit and (now_f - hit[0]) < BITQUERY_WHALE_CACHE_TTL_SEC:
            cached = dict(hit[1])
            cached["cached"] = True
            return cached

    since_ts = max(0, now_ts() - (int(BITQUERY_WHALE_LOOKBACK_MIN) * 60))
    since_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since_ts))

    variables = {
        "network": _bitquery_network(ck),
        "token": token,
        "since": since_iso,
        "limit": max(5, min(100, int(BITQUERY_WHALE_TRADE_LIMIT))),
    }

    out_base = {
        "status": "ok",
        "source": "bitquery",
        "chain": ck,
        "token": token,
        "thresholdUsd": round(float(dyn_threshold), 2),
        "thresholdMode": "dynamic_by_24h_volume",
        "lookbackMinutes": int(BITQUERY_WHALE_LOOKBACK_MIN),
        "cached": False,
        "ts": now_ts(),
    }

    try:
        data = _bitquery_post(_BITQUERY_DEX_TRADE_QUERY, variables)
        trades = (((data.get("data") or {}).get("EVM") or {}).get("DEXTrades") or [])
        if not isinstance(trades, list):
            trades = []

        whale_events = []
        buy_usd = 0.0
        sell_usd = 0.0

        for item in trades:
            t = item.get("Trade") or {}
            usd = _safe_float(t.get("AmountInUSD"))
            if usd < dyn_threshold:
                continue

            side = _detect_trade_side(item, token)
            if side not in ("buy", "sell"):
                continue

            if side == "buy":
                buy_usd += usd
                wallet = ((t.get("Buy") or {}).get("Buyer") or "")
            else:
                sell_usd += usd
                wallet = ((t.get("Sell") or {}).get("Seller") or "")

            dex = t.get("Dex") or {}
            whale_events.append({
                "action": side,
                "amountUsd": round(usd, 2),
                "wallet": wallet,
                "walletShort": _short_addr(wallet),
                "dex": dex.get("ProtocolName") or dex.get("ProtocolFamily") or "DEX",
                "tx": ((item.get("Transaction") or {}).get("Hash") or ""),
                "time": ((item.get("Block") or {}).get("Time") or ""),
                "timeTs": _parse_bitquery_time_to_ts(((item.get("Block") or {}).get("Time") or "")),
            })

        if not whale_events:
            out = {
                **out_base,
                "action": "neutral",
                "strength": "none",
                "icon": "🔥",
                "color": "neutral",
                "amountUsd": 0,
                "buyUsd": round(buy_usd, 2),
                "sellUsd": round(sell_usd, 2),
                "events": [],
                "summary": "No fresh whale buy/sell detected",
                "label": "No fresh whale buy/sell detected",
                "score_delta": 0,
            }
            _WHALE_SIGNAL_CACHE[cache_key] = (now_f, dict(out))
            return out

        action = "buy" if buy_usd > sell_usd else "sell" if sell_usd > buy_usd else "neutral"
        amount = buy_usd if action == "buy" else sell_usd if action == "sell" else max(buy_usd, sell_usd)
        events_sorted = sorted(whale_events, key=lambda x: int(x.get("timeTs") or 0), reverse=True)
        latest = events_sorted[0] if events_sorted else {
            "amountUsd": 0,
            "dex": "Unknown",
            "time": "",
            "walletShort": "",
        }
        strength = _whale_strength(amount, dyn_threshold) if action in ("buy", "sell") else "none"

        if action == "buy":
            summary = f"Whale bought recently · ${amount:,.0f}"
            icon = "NEWS"
            color = "green"
            score_delta = 4 if strength == "high" else 3 if strength == "medium" else 2
        elif action == "sell":
            summary = f"Whale sold recently · ${amount:,.0f}"
            icon = "NEWS"
            color = "red"
            score_delta = -4 if strength == "high" else -3 if strength == "medium" else -2
        else:
            summary = "Mixed whale activity"
            icon = "🔥"
            color = "neutral"
            score_delta = 0

        out = {
            **out_base,
            "action": action,
            "strength": strength,
            "icon": icon,
            "color": color,

            # Frontend popup data: show the latest whale trade amount directly.
            "amountUsd": round(float(latest.get("amountUsd", amount) or 0), 2),

            "buyUsd": round(buy_usd, 2),
            "sellUsd": round(sell_usd, 2),

            # Clean object for the Whale Activity popup.
            "latest": {
                "amountUsd": round(float(latest.get("amountUsd", 0) or 0), 2),
                "dex": latest.get("dex", "Unknown") or "Unknown",
                "time": latest.get("time", "") or "",
                "wallet": latest.get("walletShort", "") or "",
                "tx": latest.get("tx", "") or "",
            },

            "events": events_sorted[:10],
            "summary": summary,
            "label": summary,
            "score_delta": score_delta,
        }
        _WHALE_SIGNAL_CACHE[cache_key] = (now_f, dict(out))
        return out

    except Exception as e:
        return {
            **out_base,
            "status": "error",
            "action": "neutral",
            "strength": "none",
            "icon": "🔥",
            "color": "neutral",
            "amountUsd": 0,
            "summary": "Whale signal unavailable",
            "label": "Whale signal unavailable",
            "score_delta": 0,
            "error": str(e),
        }


def _safe_float(v, default: float = 0.0) -> float:
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return float(default)


def _classify_market_condition(oe_pct: float, rvol: float) -> dict:
    """Classify OE + RVOL into an AI-ready market condition state."""
    oe = _safe_float(oe_pct)
    rv = _safe_float(rvol)

    if oe > 40 and rv < 1.2:
        return {
            "state": "FAKE_MOVE",
            "label": "Weak / fake move risk",
            "level": "warning",
            "confidence": "HIGH",
            "score_delta": -15,
            "insight": "Price is strongly above its 20-day average, but volume does not confirm the move. Reversal risk is elevated.",
        }

    if oe > 40 and rv >= 1.5:
        return {
            "state": "REAL_BREAKOUT",
            "label": "Volume-backed breakout",
            "level": "strong",
            "confidence": "HIGH",
            "score_delta": 10,
            "insight": "Price is extended, but strong relative volume confirms real momentum. Trend continuation is more likely than in a weak-volume pump.",
        }

    if oe < 10 and rv >= 2.0:
        return {
            "state": "EARLY_ACCUMULATION",
            "label": "Early accumulation / volume build",
            "level": "positive",
            "confidence": "MEDIUM",
            "score_delta": 8,
            "insight": "Relative volume is high while price is not yet heavily extended. This can indicate early accumulation or a fresh move forming.",
        }

    if oe > 60 and rv < 1.5:
        return {
            "state": "OVEREXTENDED",
            "label": "Overextended",
            "level": "caution",
            "confidence": "MEDIUM",
            "score_delta": -8,
            "insight": "Price is far above its 20-day average. Without stronger volume support, pullback risk is increasing.",
        }

    return {
        "state": "NORMAL",
        "label": "Normal trend range",
        "level": "neutral",
        "confidence": "LOW",
        "score_delta": 0,
        "insight": "No strong overextension or relative-volume anomaly detected.",
    }


def _market_condition_for_coin(coin_or_symbol: str, days: int = 20) -> dict:
    """Calculate Overextension (OE) and Relative Volume (RVOL) from CoinGecko market_chart."""
    coin_id = _market_condition_coin_id(coin_or_symbol)
    if not coin_id:
        raise RuntimeError("missing coin id or symbol")

    days_i = max(20, min(90, int(days or 20)))
    cache_key = f"{coin_id}|{days_i}"
    now_f = time.time()
    hit = _MARKET_CONDITION_CACHE.get(cache_key)
    if hit and (now_f - hit[0]) < _MARKET_CONDITION_TTL_SEC:
        cached = dict(hit[1])
        cached["cached"] = True
        return cached

    url = f"{COINGECKO_BASE}/coins/{requests.utils.quote(coin_id)}/market_chart?vs_currency=usd&days={days_i}&interval=daily"
    data = _cg_get(url)

    prices_raw = data.get("prices") if isinstance(data, dict) else []
    volumes_raw = data.get("total_volumes") if isinstance(data, dict) else []
    if not isinstance(prices_raw, list) or not isinstance(volumes_raw, list):
        raise RuntimeError("invalid CoinGecko market_chart response")

    prices = [_safe_float(p[1]) for p in prices_raw if isinstance(p, list) and len(p) >= 2 and _safe_float(p[1]) > 0]
    volumes = [_safe_float(v[1]) for v in volumes_raw if isinstance(v, list) and len(v) >= 2 and _safe_float(v[1]) >= 0]

    # CoinGecko daily market_chart can return 21 points for 20 days. Use the latest 20 valid points.
    prices_20 = prices[-20:]
    volumes_20 = volumes[-20:]

    if len(prices_20) < 10:
        raise RuntimeError("not enough price history for market condition")
    if len(volumes_20) < 10:
        raise RuntimeError("not enough volume history for market condition")

    current_price = prices_20[-1]
    current_volume = volumes_20[-1]
    ma20 = sum(prices_20) / len(prices_20)
    avg_volume_20d = sum(volumes_20) / len(volumes_20)

    oe_pct = ((current_price - ma20) / ma20) * 100 if ma20 > 0 else 0.0
    rvol = current_volume / avg_volume_20d if avg_volume_20d > 0 else 0.0

    classification = _classify_market_condition(oe_pct, rvol)

    out = {
        "status": "ok",
        "coin_id": coin_id,
        "input": str(coin_or_symbol or "").strip(),
        "days": days_i,
        "current_price": round(current_price, 10),
        "ma20": round(ma20, 10),
        "current_volume": round(current_volume, 2),
        "avg_volume_20d": round(avg_volume_20d, 2),
        "oe_pct": round(oe_pct, 2),
        "rvol": round(rvol, 2),
        "condition": classification,
        "state": classification.get("state"),
        "label": classification.get("label"),
        "level": classification.get("level"),
        "confidence": classification.get("confidence"),
        "score_delta": classification.get("score_delta", 0),
        "ai_context": {
            "market_condition_state": classification.get("state"),
            "market_condition_label": classification.get("label"),
            "overextension_pct": round(oe_pct, 2),
            "relative_volume": round(rvol, 2),
            "interpretation": classification.get("insight"),
        },
        "cached": False,
        "ts": now_ts(),
    }

    _MARKET_CONDITION_CACHE[cache_key] = (now_f, dict(out))
    return out


@app.route("/api/market-condition", methods=["GET"])
def api_market_condition():
    coin = (
        request.args.get("coin_id")
        or request.args.get("id")
        or request.args.get("symbol")
        or request.args.get("coin")
        or ""
    )
    if not str(coin or "").strip():
        return err("missing coin_id or symbol", 400)

    try:
        days = int(request.args.get("days") or 20)
    except Exception:
        days = 20

    try:
        return jsonify(_market_condition_for_coin(coin, days=days))
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "input": str(coin or "").strip(),
            "ts": now_ts(),
        }), 502


@app.route("/api/market-condition/<coin_id>", methods=["GET"])
def api_market_condition_by_id(coin_id):
    try:
        days = int(request.args.get("days") or 20)
    except Exception:
        days = 20

    try:
        return jsonify(_market_condition_for_coin(coin_id, days=days))
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "input": str(coin_id or "").strip(),
            "ts": now_ts(),
        }), 502


@app.route("/api/onchain/whale-signal", methods=["GET"])
def api_onchain_whale_signal():
    token = (
        request.args.get("token")
        or request.args.get("token_address")
        or request.args.get("contract")
        or request.args.get("address")
        or ""
    )
    chain = _normalize_chain_key(request.args.get("chain") or request.args.get("network") or "ETH")
    force_refresh = str(request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes", "on")

    volume24h = request.args.get("volume24h") or request.args.get("volume24h_usd") or request.args.get("volume")
    volume24h_f = None
    try:
        if volume24h is not None and str(volume24h).strip() != "":
            volume24h_f = float(volume24h)
    except Exception:
        volume24h_f = None

    return jsonify(_get_whale_signal_bitquery(token, chain=chain, volume24h_usd=volume24h_f, force_refresh=force_refresh))


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
            "routers": _nexus_allowed_routers_for_chain(cid) if "_nexus_allowed_routers_for_chain" in globals() else [],
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
    wallet_accounting_wei = _hex_to_int(balance_hex or "0x0")
    balance_wei = wallet_accounting_wei

    # Also read the real native balance held by the Vault contract.
    # Some older/dev Vault deployments can hold POL in the contract while the
    # wallet-bound accounting selector still returns 0. In that case the UI
    # must not show "No vault liquidity" while the contract actually has funds.
    try:
        contract_native_hex = _rpc_call(cid, "eth_getBalance", [vault_addr, "latest"])
        contract_native_wei = _hex_to_int(contract_native_hex or "0x0")
    except Exception:
        contract_native_hex = "0x0"
        contract_native_wei = 0

    balance_source = "wallet_accounting"
    use_contract_fallback = str(os.getenv("NEXUS_VAULT_FALLBACK_CONTRACT_BALANCE", "1")).strip().lower() in ("1", "true", "yes", "on")
    if balance_wei <= 0 and contract_native_wei > 0 and use_contract_fallback:
        balance_wei = contract_native_wei
        balance_source = "contract_native_fallback"

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
        "vault_balance_source": balance_source,
        "wallet_accounting_balance_wei": str(wallet_accounting_wei),
        "wallet_accounting_balance": float(wallet_accounting_wei) / 1e18,
        "vault_contract_native_balance_wei": str(contract_native_wei),
        "vault_contract_native_balance": float(contract_native_wei) / 1e18,
        "inCycle": _hex_to_bool(in_cycle_hex),
        "heldToken": held_token_addr,
        "heldTokenBalWei": str(held_bal_raw),
        "heldTokenBal": float(held_bal_raw) / 1e18,
        "operatorEnabled": bool(operator_enabled),
        "ts": now_ts(),
    }

def _erc20_balance_of_rpc(chain_key: str, token_address: str, wallet_address: str) -> dict:
    ch = _normalize_chain_key(chain_key)
    cid = int(_CHAIN_ID_BY_KEY.get(ch, 0) or 0)
    token = str(token_address or "").strip().lower()
    wallet = _norm_addr(wallet_address)
    if cid <= 0:
        raise RuntimeError("unsupported chain")
    if not _looks_like_evm_addr(token):
        raise RuntimeError("invalid token address")
    if not _looks_like_evm_addr(wallet):
        raise RuntimeError("invalid wallet")

    # ERC20 balanceOf(address) selector = 0x70a08231
    data = "0x70a08231" + wallet.replace("0x", "").rjust(64, "0")
    raw = _rpc_call(cid, "eth_call", [{"to": token, "data": data}, "latest"])
    wei = _hex_to_int(raw or "0x0")
    return {
        "chain": ch,
        "chainId": cid,
        "address": token,
        "balance_raw": str(wei),
        "status": "ok",
    }


@app.route("/api/wallet/native-balances", methods=["GET"])
def api_wallet_native_balances():
    wallet = (
        request.args.get("wallet")
        or request.args.get("wallet_address")
        or request.headers.get("X-Wallet-Address")
        or ""
    )
    wa = _norm_addr(wallet)
    if not _looks_like_evm_addr(wa):
        return jsonify({"status": "error", "error": "invalid wallet", "wallet": wa, "ts": now_ts()}), 400

    chains_raw = str(request.args.get("chains") or ",".join(_ENABLED_EVM_CHAINS or ["POL"])).strip()
    chains = [_normalize_chain_key(x) for x in chains_raw.split(",") if str(x or "").strip()]
    if not chains:
        chains = list(_ENABLED_EVM_CHAINS or ["POL"])

    out = {}
    for ch in chains:
        cid = int(_CHAIN_ID_BY_KEY.get(ch, 0) or 0)
        if cid <= 0:
            continue
        try:
            raw = _rpc_call(cid, "eth_getBalance", [wa, "latest"])
            wei = _hex_to_int(raw or "0x0")
            out[ch] = {
                "chain": ch,
                "chainId": cid,
                "native": float(wei) / 1e18,
                "native_wei": str(wei),
                "rpc_configured": bool(_rpc_url_for_chain(cid)),
                "status": "ok",
            }
        except Exception as e:
            out[ch] = {
                "chain": ch,
                "chainId": cid,
                "native": None,
                "native_wei": None,
                "rpc_configured": bool(_rpc_url_for_chain(cid)),
                "status": "error",
                "error": str(e),
            }

    return jsonify({
        "status": "ok",
        "wallet": wa,
        "balances": out,
        "ts": now_ts(),
    })


@app.route("/api/debug/rpc-balance", methods=["GET"])
def api_debug_rpc_balance():
    wallet = (
        request.args.get("wallet")
        or request.args.get("wallet_address")
        or request.headers.get("X-Wallet-Address")
        or ""
    )
    chain = _normalize_chain_key(request.args.get("chain") or request.args.get("chain_key") or "POL")
    wa = _norm_addr(wallet)
    cid = int(_CHAIN_ID_BY_KEY.get(chain, 0) or 0)

    out = {
        "status": "ok",
        "wallet": wa,
        "chain": chain,
        "chainId": cid,
        "rpc_configured": bool(_rpc_url_for_chain(cid)),
        "enabled_chains": list(_ENABLED_EVM_CHAINS),
        "enabled_chain_ids": sorted(list(_ENABLED_CHAIN_IDS)),
        "ts": now_ts(),
    }

    rpc_url = _rpc_url_for_chain(cid)
    out["rpc_url_preview"] = (rpc_url[:42] + "...") if rpc_url else ""

    if not _looks_like_evm_addr(wa):
        out.update({"status": "error", "error": "invalid wallet"})
        return jsonify(out), 400

    try:
        raw = _rpc_call(cid, "eth_getBalance", [wa, "latest"])
        wei = _hex_to_int(raw or "0x0")
        out["wallet_native_raw"] = raw
        out["wallet_native_wei"] = str(wei)
        out["wallet_native"] = float(wei) / 1e18
    except Exception as e:
        out["wallet_native_error"] = str(e)

    vault_addr = (_VAULT_BY_CHAIN.get(cid) or "").strip()
    out["vault"] = vault_addr
    out["vault_configured"] = bool(vault_addr)
    if _looks_like_evm_addr(vault_addr):
        try:
            vraw = _rpc_call(cid, "eth_getBalance", [vault_addr, "latest"])
            vwei = _hex_to_int(vraw or "0x0")
            out["vault_contract_native_raw"] = vraw
            out["vault_contract_native_wei"] = str(vwei)
            out["vault_contract_native"] = float(vwei) / 1e18
        except Exception as e:
            out["vault_contract_native_error"] = str(e)

        try:
            selector = _vault_balance_selector_for_chain(chain)
            araw = _eth_call(cid, vault_addr, selector + _addr_to_32(wa))
            awei = _hex_to_int(araw or "0x0")
            out["vault_wallet_accounting_selector"] = selector
            out["vault_wallet_accounting_raw"] = araw
            out["vault_wallet_accounting_wei"] = str(awei)
            out["vault_wallet_accounting"] = float(awei) / 1e18
        except Exception as e:
            out["vault_wallet_accounting_error"] = str(e)

        try:
            out["vault_state"] = _vault_state_read(wa, chain)
        except Exception as e:
            out["vault_state_error"] = str(e)

    return jsonify(out)


@app.route("/api/wallet/token-balances", methods=["POST"])
def api_wallet_token_balances():
    body = request.get_json(silent=True) or {}
    wallet = (
        body.get("wallet")
        or body.get("wallet_address")
        or request.args.get("wallet")
        or request.args.get("wallet_address")
        or request.headers.get("X-Wallet-Address")
        or ""
    )
    wa = _norm_addr(wallet)
    if not _looks_like_evm_addr(wa):
        return jsonify({"status": "error", "error": "invalid wallet", "wallet": wa, "ts": now_ts()}), 400

    chain = _normalize_chain_key(body.get("chain") or request.args.get("chain") or "POL")
    tokens = body.get("tokens") or []
    if not isinstance(tokens, list):
        return jsonify({"status": "error", "error": "tokens must be a list", "ts": now_ts()}), 400

    out = {}
    for t in tokens[:80]:
        try:
            addr = str((t or {}).get("address") or t or "").strip().lower()
            if not _looks_like_evm_addr(addr):
                continue
            out[addr] = _erc20_balance_of_rpc(chain, addr, wa)
        except Exception as e:
            out[str((t or {}).get("address") or t or "").strip().lower()] = {
                "status": "error",
                "error": str(e),
            }

    return jsonify({
        "status": "ok",
        "wallet": wa,
        "chain": chain,
        "balances": out,
        "ts": now_ts(),
    })


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
        force_refresh = str(request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes", "on")
        cache_key = f"{_norm_addr(wallet)}|{chain}"
        now_f = time.time()
        if not force_refresh:
            hit = _VAULT_STATE_CACHE.get(cache_key)
            if hit and (now_f - hit[0]) < _VAULT_STATE_CACHE_TTL_SEC:
                cached = dict(hit[1])
                cached["cached"] = True
                return jsonify(cached)

        data = _vault_state_read(wallet, chain)
        _VAULT_STATE_CACHE[cache_key] = (now_f, dict(data))
        return jsonify(data)
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

# Planning / fallback capital basis for grid budgeting (USD).
# This is only used as a conservative fallback when no wallet/vault-derived budget is available yet.
INITIAL_CAPITAL_USD = float(os.getenv("NEXUS_INITIAL_CAPITAL_USD", "5000"))

# Runtime mode flags
# - GRID_LIVE_MODE keeps the public API live-first (no demo/sim labels in responses)
# - GRID_ENABLE_LEGACY_SIM keeps the existing internal grid engine available during migration
#   so the frontend and DB flows stay stable until real executor logic fully replaces it.
GRID_LIVE_MODE = str(os.getenv("GRID_LIVE_MODE", "1")).strip().lower() in ("1", "true", "yes", "on")
GRID_ENABLE_LEGACY_SIM = str(os.getenv("GRID_ENABLE_LEGACY_SIM", "1")).strip().lower() in ("1", "true", "yes", "on")

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
_GRID_EXEC_LOCK = threading.RLock()  # serialize tick/start/stop mutations so tick/order state stays monotonic

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

    # Demo AI daily usage: one combined limit across both AI endpoints.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_daily_usage (
            wallet_address TEXT NOT NULL,
            day_key TEXT NOT NULL,
            used_count INTEGER DEFAULT 0,
            updated_ts INTEGER,
            PRIMARY KEY(wallet_address, day_key)
        )
    """)

    # Adaptive Market Memory: structured snapshots for later outcome learning.
    # This stores behavior/state, not full AI text. Outcomes are intentionally nullable
    # in phase 1 and can be filled by a later background/outcome worker.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT DEFAULT '',
            pair TEXT NOT NULL,
            symbol_a TEXT DEFAULT '',
            symbol_b TEXT DEFAULT '',
            source TEXT DEFAULT 'ai_insight',
            timestamp INTEGER NOT NULL,

            regime TEXT DEFAULT '',
            liquidity_state TEXT DEFAULT '',
            tactical_state TEXT DEFAULT '',
            movement_quality TEXT DEFAULT '',

            movement_score REAL,
            confidence REAL,
            risk TEXT DEFAULT '',

            spread REAL,
            rvol REAL,
            overextension REAL,
            trap_risk REAL,

            price_a REAL,
            price_b REAL,
            meta_json TEXT DEFAULT '{}',

            outcome_1h REAL,
            outcome_4h REAL,
            outcome_24h REAL,
            outcome_json TEXT DEFAULT '{}',
            created_ts INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_market_memory_pair_ts ON market_memory(pair, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_market_memory_wallet_ts ON market_memory(wallet_address, timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_market_memory_source_ts ON market_memory(source, timestamp)")

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

    # Auto-renew subscription settings (wallet-bound).
    # Web UI stores a preference only; real recurring charges require the server-side Privy worker.
    # Safe migration: older /data SQLite DBs get these columns automatically.
    _db_ensure_columns(conn, "access_state", {
        "auto_renew_enabled": "auto_renew_enabled INTEGER DEFAULT 0",
        "preferred_token": "preferred_token TEXT DEFAULT 'USDT'",
        "preferred_chain": "preferred_chain TEXT DEFAULT 'POL'",
        "next_billing_ts": "next_billing_ts INTEGER",
        "last_auto_renew_attempt_ts": "last_auto_renew_attempt_ts INTEGER",
        "last_auto_renew_status": "last_auto_renew_status TEXT DEFAULT ''",

        # Privy Auto-Renew metadata
        "last_auto_renew_tx_hash": "last_auto_renew_tx_hash TEXT DEFAULT ''",
        "privy_wallet_id": "privy_wallet_id TEXT DEFAULT ''",
        "privy_delegation_id": "privy_delegation_id TEXT DEFAULT ''",
        "privy_policy_id": "privy_policy_id TEXT DEFAULT ''",
        "privy_consent_ts": "privy_consent_ts INTEGER",
        "auto_renew_payment_mode": "auto_renew_payment_mode TEXT DEFAULT 'manual'",
    })

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_watchlists (
            wallet_address TEXT PRIMARY KEY,
            items_json TEXT NOT NULL,
            updated_ts INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_app_state (
            wallet_address TEXT PRIMARY KEY,
            compare_json TEXT DEFAULT '[]',
            timeframe TEXT DEFAULT '90D',
            index_mode INTEGER DEFAULT 1,
            ai_selected_json TEXT DEFAULT '[]',
            updated_ts INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_coin_ratings (
            wallet_address TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rating TEXT NOT NULL,
            rating_date TEXT NOT NULL,
            created_ts INTEGER,
            updated_ts INTEGER,
            PRIMARY KEY (wallet_address, symbol, rating_date)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_coin_ratings_wallet_symbol ON user_coin_ratings(wallet_address, symbol);")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_insight_profile (
            wallet_address TEXT PRIMARY KEY,
            order_memory_json TEXT DEFAULT '{}',
            insight_profile_json TEXT DEFAULT '{}',
            updated_ts INTEGER
        )
    """)

    # Auto-migrate persistent DB schema on Render disk (/data)
    _db_migrate_schema(conn)

    # Additive migrations for app sync tables
    try:
        _db_ensure_columns(conn, "user_watchlists", {
            "items_json": "items_json TEXT DEFAULT '[]'",
            "updated_ts": "updated_ts INTEGER",
        })
        _db_ensure_columns(conn, "user_app_state", {
            "compare_json": "compare_json TEXT DEFAULT '[]'",
            "timeframe": "timeframe TEXT DEFAULT '90D'",
            "index_mode": "index_mode INTEGER DEFAULT 1",
            "ai_selected_json": "ai_selected_json TEXT DEFAULT '[]'",
            "updated_ts": "updated_ts INTEGER",
        })
        _db_ensure_columns(conn, "user_insight_profile", {
            "order_memory_json": "order_memory_json TEXT DEFAULT '{}'",
            "insight_profile_json": "insight_profile_json TEXT DEFAULT '{}'",
            "updated_ts": "updated_ts INTEGER",
        })
    except Exception:
        pass

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
        "SELECT 1 FROM grid_orders WHERE order_id=? AND wallet_address=? LIMIT 1",
        (oid, _norm_addr(wallet_address)),
    )
    if cur.fetchone():
        return oid

    cur.execute(
        "INSERT INTO grid_orders(order_id,wallet_address,item_id,chain,side,price,qty,status,level,meta_json,created_ts,updated_ts) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (oid, _norm_addr(wallet_address), item_id, chain, side,
         float(price) if price is not None else None,
         float(qty) if qty is not None else None,
         status,
         int(level) if level is not None else None,
         json.dumps(meta, separators=(",",":")),
         nowi, nowi),
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




def _grid_db_resume_order(conn, wallet_address: str, item_id: str, oid: str, chain: str = "") -> int:
    """
    Resume a previously stopped/cancelled order by setting status back to OPEN.

    Primary key is (wallet_address, order_id). We keep item_id/chain filters for the
    fast-path, but fall back to order_id-only to tolerate UI/backend item mismatches.
    """
    nowi = int(time.time())
    cur = conn.cursor()
    wa = _norm_addr(wallet_address)

    if chain:
        cur.execute(
            "UPDATE grid_orders SET status='OPEN', cancelled_ts=NULL, updated_ts=? "
            "WHERE order_id=? AND wallet_address=? AND item_id=? AND chain=?",
            (nowi, str(oid), wa, item_id, chain),
        )
    else:
        cur.execute(
            "UPDATE grid_orders SET status='OPEN', cancelled_ts=NULL, updated_ts=? "
            "WHERE order_id=? AND wallet_address=? AND item_id=?",
            (nowi, str(oid), wa, item_id),
        )
    rc = cur.rowcount

    if rc <= 0:
        cur.execute(
            "UPDATE grid_orders SET status='OPEN', cancelled_ts=NULL, updated_ts=? "
            "WHERE order_id=? AND wallet_address=?",
            (nowi, str(oid), wa),
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

def _grid_sync_session_orders_to_db(wallet_address: str, item_id: str, orders: list, chain: str = "") -> None:
    """Mirror in-memory session order statuses into SQLite so /api/grid/orders stays authoritative."""
    wa = _norm_addr(wallet_address or "")
    if not wa or not item_id or not isinstance(orders, list):
        return

    chain_eff = _grid_chain_key(item_id, chain) or chain or ""
    nowi = int(time.time())

    conn = _db()
    try:
        cur = conn.cursor()
        with DB_WRITE_LOCK:
            for o in orders:
                if not isinstance(o, dict):
                    continue
                oid = str(o.get("id") or o.get("order_id") or "").strip()
                if not oid:
                    continue
                status = str(o.get("status") or "OPEN").upper()
                meta = {
                    "fill_price": o.get("fill_price"),
                    "filled_ts": o.get("filled_ts"),
                    "cancelled_ts": o.get("cancelled_ts"),
                    "usd": o.get("usd"),
                    "source": o.get("source"),
                }
                meta_json = json.dumps({k: v for k, v in meta.items() if v is not None}, separators=(",", ":"))
                cur.execute(
                    "UPDATE grid_orders SET status=?, meta_json=CASE "
                    "WHEN COALESCE(meta_json,'')='' OR meta_json='{}' THEN ? "
                    "ELSE json_patch(meta_json, ?) END, updated_ts=?, cancelled_ts=CASE WHEN ?='CANCELLED' THEN COALESCE(cancelled_ts, ?) ELSE cancelled_ts END "
                    "WHERE order_id=? AND wallet_address=? AND item_id=? AND (?='' OR chain=?)",
                    (status, meta_json, meta_json, nowi, status, nowi, oid, wa, item_id, chain_eff, chain_eff),
                )
                if cur.rowcount <= 0:
                    # fallback without chain filter
                    cur.execute(
                        "UPDATE grid_orders SET status=?, meta_json=CASE "
                        "WHEN COALESCE(meta_json,'')='' OR meta_json='{}' THEN ? "
                        "ELSE json_patch(meta_json, ?) END, updated_ts=?, cancelled_ts=CASE WHEN ?='CANCELLED' THEN COALESCE(cancelled_ts, ?) ELSE cancelled_ts END "
                        "WHERE order_id=? AND wallet_address=? AND item_id=?",
                        (status, meta_json, meta_json, nowi, status, nowi, oid, wa, item_id),
                    )
            conn.commit()
    finally:
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


def _json_load_obj(raw: Any, default=None):
    if default is None:
        default = {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else default
        except Exception:
            return default
    return default

def _json_load_list(raw: Any, default=None):
    if default is None:
        default = []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            return data if isinstance(data, list) else default
        except Exception:
            return default
    return default

def _insight_profile_get(wallet_address: str) -> tuple[dict, dict]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {}, {}
    conn = _db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT order_memory_json, insight_profile_json FROM user_insight_profile WHERE wallet_address=?",
            (wa,),
        )
        row = cur.fetchone()
        if not row:
            return {}, {}
        return _json_load_obj(row["order_memory_json"], {}), _json_load_obj(row["insight_profile_json"], {})
    finally:
        conn.close()

def _insight_profile_save(wallet_address: str, order_memory: dict | None, insight_profile: dict | None, conn=None) -> None:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return
    own_conn = conn is None
    if own_conn:
        conn = _db()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_insight_profile(wallet_address, order_memory_json, insight_profile_json, updated_ts) VALUES (?,?,?,?) "
            "ON CONFLICT(wallet_address) DO UPDATE SET "
            "order_memory_json=excluded.order_memory_json, "
            "insight_profile_json=excluded.insight_profile_json, "
            "updated_ts=excluded.updated_ts",
            (
                wa,
                json.dumps(order_memory or {}, ensure_ascii=False, separators=(",", ":")),
                json.dumps(insight_profile or {}, ensure_ascii=False, separators=(",", ":")),
                now_ts(),
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn and conn is not None:
            conn.close()

def _derive_trading_style(avg_distance_pct: float, buy_ratio: float, manual_ratio: float) -> str:
    try:
        avg_distance_pct = float(avg_distance_pct or 0.0)
        buy_ratio = float(buy_ratio or 0.0)
        manual_ratio = float(manual_ratio or 0.0)
    except Exception:
        return "balanced"
    if avg_distance_pct >= 7.5:
        return "conservative"
    if avg_distance_pct <= 2.5 or manual_ratio >= 0.75:
        return "aggressive"
    if buy_ratio >= 0.7 and avg_distance_pct >= 4.0:
        return "accumulation-focused"
    return "balanced"

def _derive_execution_pattern(status_counts: dict, manual_ratio: float, avg_distance_pct: float, buy_ratio: float) -> str:
    open_count = int(status_counts.get("OPEN", 0) or 0)
    cancelled_count = int(status_counts.get("CANCELLED", 0) or 0)
    if manual_ratio >= 0.75:
        return "manual execution bias"
    if open_count >= 6 and avg_distance_pct >= 3.0:
        return "staggered grid structure"
    if buy_ratio >= 0.7:
        return "buy-side accumulation bias"
    if cancelled_count >= max(3, open_count):
        return "frequent reworking of orders"
    return "mixed manual/grid structure"

def _grid_order_memory_from_orders(orders: list[dict]) -> dict:
    rows = [o for o in (orders or []) if isinstance(o, dict)]
    if not rows:
        return {
            "order_count_total": 0,
            "open_order_count": 0,
            "active_item_count": 0,
            "avg_order_distance_pct": 0.0,
            "side_bias": "mixed",
            "preferred_mode": "unknown",
            "preferred_assets": [],
            "status_breakdown": {},
            "source_breakdown": {},
            "behavior_note": "No persisted order structure yet.",
        }

    side_counts = {"BUY": 0, "SELL": 0}
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    chain_counts: dict[str, int] = {}
    item_ids = set()
    prices = []
    qty_total = 0.0
    open_prices = []

    for o in rows:
        side = str(o.get("side") or "").upper()
        if side in side_counts:
            side_counts[side] += 1
        status = str(o.get("status") or "OPEN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1

        meta = _json_load_obj(o.get("meta_json"), {})
        source = str(meta.get("source") or o.get("source") or "GRID").strip().upper() or "GRID"
        source_counts[source] = source_counts.get(source, 0) + 1

        item_id = str(o.get("item_id") or o.get("item") or "").strip().upper()
        if item_id:
            item_ids.add(item_id)
            chain_guess = item_id.split(":", 1)[0] if ":" in item_id else ""
            if chain_guess:
                chain_counts[chain_guess] = chain_counts.get(chain_guess, 0) + 1

        chain = str(o.get("chain") or "").strip().upper()
        if chain:
            chain_counts[chain] = chain_counts.get(chain, 0) + 1

        try:
            px = float(o.get("price"))
            if px > 0:
                prices.append(px)
                if status == "OPEN":
                    open_prices.append(px)
        except Exception:
            pass
        try:
            qty_total += float(o.get("qty") or 0.0)
        except Exception:
            pass

    prices_sorted = sorted(set(round(p, 12) for p in prices))
    distance_pcts = []
    for i in range(1, len(prices_sorted)):
        prev_px = prices_sorted[i - 1]
        px = prices_sorted[i]
        if prev_px > 0:
            distance_pcts.append(abs(px - prev_px) / prev_px * 100.0)
    avg_distance_pct = round(sum(distance_pcts) / len(distance_pcts), 4) if distance_pcts else 0.0

    total_sides = max(1, side_counts["BUY"] + side_counts["SELL"])
    buy_ratio = side_counts["BUY"] / total_sides
    sell_ratio = side_counts["SELL"] / total_sides
    if buy_ratio >= 0.65:
        side_bias = "buy-heavy"
    elif sell_ratio >= 0.65:
        side_bias = "sell-heavy"
    else:
        side_bias = "mixed"

    total_sources = max(1, sum(source_counts.values()))
    manual_ratio = source_counts.get("MANUAL", 0) / total_sources
    if manual_ratio >= 0.65:
        preferred_mode = "manual"
    elif source_counts.get("GRID", 0) >= source_counts.get("MANUAL", 0):
        preferred_mode = "grid"
    else:
        preferred_mode = "mixed"

    preferred_assets = [k for k, _v in sorted(chain_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]]
    open_order_count = int(status_counts.get("OPEN", 0) or 0)

    if preferred_mode == "manual" and avg_distance_pct <= 2.5:
        behavior_note = "Order structure looks close to current price and manually adjusted."
    elif preferred_mode == "grid" and avg_distance_pct >= 3.0:
        behavior_note = "Order structure looks laddered and grid-like rather than single-shot."
    elif side_bias == "buy-heavy":
        behavior_note = "Order structure leans more toward accumulation than distribution."
    elif side_bias == "sell-heavy":
        behavior_note = "Order structure leans more toward distribution or profit-taking."
    else:
        behavior_note = "Order structure looks mixed without a strong single-side bias."

    return {
        "order_count_total": len(rows),
        "open_order_count": open_order_count,
        "active_item_count": len(item_ids),
        "avg_order_distance_pct": avg_distance_pct,
        "avg_order_qty": round(qty_total / max(1, len(rows)), 8),
        "side_bias": side_bias,
        "preferred_mode": preferred_mode,
        "preferred_assets": preferred_assets,
        "status_breakdown": status_counts,
        "source_breakdown": source_counts,
        "behavior_note": behavior_note,
    }

def _derive_insight_profile_from_memory(order_memory: dict) -> dict:
    om = order_memory or {}
    avg_distance_pct = float(om.get("avg_order_distance_pct") or 0.0)
    side_bias = str(om.get("side_bias") or "mixed")
    preferred_mode = str(om.get("preferred_mode") or "unknown")
    source_breakdown = om.get("source_breakdown") or {}
    status_breakdown = om.get("status_breakdown") or {}
    total_sources = max(1, sum(int(v or 0) for v in source_breakdown.values()))
    manual_ratio = float(source_breakdown.get("MANUAL", 0) or 0) / total_sources
    buy_ratio = 0.5 if side_bias == "mixed" else (0.75 if side_bias == "buy-heavy" else 0.25)

    style = _derive_trading_style(avg_distance_pct, buy_ratio, manual_ratio)
    execution_pattern = _derive_execution_pattern(status_breakdown, manual_ratio, avg_distance_pct, buy_ratio)

    if avg_distance_pct >= 7.5:
        volatility_tolerance = "low"
    elif avg_distance_pct >= 3.0:
        volatility_tolerance = "medium"
    else:
        volatility_tolerance = "high"

    if side_bias == "buy-heavy":
        bias_note = "Current wallet history leans more toward buy-side staging."
    elif side_bias == "sell-heavy":
        bias_note = "Current wallet history leans more toward sell-side staging."
    else:
        bias_note = "Current wallet history looks balanced between buy and sell orders."

    return {
        "style": style,
        "execution_pattern": execution_pattern,
        "volatility_tolerance": volatility_tolerance,
        "preferred_mode": preferred_mode,
        "bias_note": bias_note,
        "summary": (
            f"Wallet history suggests a {style} style with {execution_pattern}. "
            f"Observed side bias is {side_bias}, average order spacing is {avg_distance_pct:.2f}%."
        ),
    }

def _refresh_user_insight_profile(wallet_address: str, conn=None) -> tuple[dict, dict]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {}, {}

    own_conn = conn is None
    if own_conn:
        conn = _db()

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT order_id, item_id, chain, side, price, qty, status, meta_json, created_ts, updated_ts "
            "FROM grid_orders WHERE wallet_address=? ORDER BY updated_ts DESC, created_ts DESC",
            (wa,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        order_memory = _grid_order_memory_from_orders(rows)
        insight_profile = _derive_insight_profile_from_memory(order_memory)
        _insight_profile_save(wa, order_memory, insight_profile, conn=conn)
        if own_conn:
            conn.commit()
        return order_memory, insight_profile
    finally:
        if own_conn and conn is not None:
            conn.close()

def _norm_addr(addr: str) -> str:
    return (addr or "").strip().lower()

def _looks_like_evm_addr(s: str) -> bool:
    s = (s or "").strip()
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", s))



# -------------------------
# GoPlus token security (vault deposit gate)
# -------------------------
GOPLUS_APP_KEY = (os.getenv("GOPLUS_APP_KEY") or "").strip()
GOPLUS_APP_SECRET = (os.getenv("GOPLUS_APP_SECRET") or "").strip()
GOPLUS_TIMEOUT_SEC = float(os.getenv("GOPLUS_TIMEOUT_SEC", "8") or 8)
GOPLUS_BLOCK_HONEYPOT = str(os.getenv("GOPLUS_BLOCK_HONEYPOT", "false")).strip().lower() in ("1", "true", "yes", "on")
_GOPLUS_TOKEN_URL = "https://api.gopluslabs.io/api/v1/token_security/{chain_id}"
_GOPLUS_AUTH_URL = "https://api.gopluslabs.io/api/v1/token"
_GOPLUS_TOKEN_CACHE = {"token": None, "expires_at": 0}

def _goplus_allowlist() -> set[tuple[int, str]]:
    raw = str(os.getenv("GOPLUS_ALLOWLIST", "") or "").strip()
    out: set[tuple[int, str]] = set()
    if not raw:
        return out
    for part in raw.split(","):
        p = str(part or "").strip()
        if not p or ":" not in p:
            continue
        cid_s, addr = p.split(":", 1)
        try:
            cid = int(str(cid_s).strip())
        except Exception:
            continue
        addr_n = _norm_addr(addr)
        if cid > 0 and _looks_like_evm_addr(addr_n):
            out.add((cid, addr_n))
    return out

def _goplus_chain_id(raw_chain: Any) -> int:
    s = str(raw_chain or "").strip().upper()
    if not s:
        return 0
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return 0
    aliases = {
        "ETH": 1,
        "ETHEREUM": 1,
        "BNB": 56,
        "BSC": 56,
        "POL": 137,
        "POLYGON": 137,
        "MATIC": 137,
    }
    return int(aliases.get(s, 0) or 0)

def _goplus_native_symbols_for_chain(chain_id: int) -> set[str]:
    if int(chain_id) == 1:
        return {"ETH", "WETH"}
    if int(chain_id) == 56:
        return {"BNB", "WBNB"}
    if int(chain_id) == 137:
        return {"POL", "MATIC", "WMATIC"}
    return set()

def _goplus_is_native_asset(chain_id: int, symbol: str = "", address: str = "") -> bool:
    addr = _norm_addr(address)
    if addr and addr in ("0x0000000000000000000000000000000000000000", "native"):
        return True
    sym = str(symbol or "").strip().upper()
    return bool(sym and sym in _goplus_native_symbols_for_chain(int(chain_id)))

def _goplus_get_access_token() -> Optional[str]:
    now = int(time.time())
    cached = _GOPLUS_TOKEN_CACHE.get("token")
    exp = int(_GOPLUS_TOKEN_CACHE.get("expires_at") or 0)
    if cached and exp > now + 30:
        return str(cached)

    if not (GOPLUS_APP_KEY and GOPLUS_APP_SECRET):
        return None

    payloads = [
        {"app_key": GOPLUS_APP_KEY, "app_secret": GOPLUS_APP_SECRET},
        {"appKey": GOPLUS_APP_KEY, "appSecret": GOPLUS_APP_SECRET},
    ]
    for payload in payloads:
        try:
            r = requests.post(_GOPLUS_AUTH_URL, json=payload, timeout=GOPLUS_TIMEOUT_SEC)
            if not r.ok:
                continue
            data = r.json() or {}
            token = (
                data.get("access_token")
                or (data.get("result") or {}).get("access_token")
                or (data.get("result") or {}).get("token")
                or data.get("token")
            )
            if token:
                _GOPLUS_TOKEN_CACHE["token"] = str(token)
                _GOPLUS_TOKEN_CACHE["expires_at"] = now + 55 * 60
                return str(token)
        except Exception:
            continue
    return None

def _goplus_fetch_token_security(chain_id: int, token_address: str) -> dict:
    cid = int(chain_id or 0)
    addr = _norm_addr(token_address)
    if cid <= 0:
        raise RuntimeError("invalid chain_id")
    if not _looks_like_evm_addr(addr):
        raise RuntimeError("invalid token address")

    headers = {"Accept": "application/json", "User-Agent": "NexusAnalyt/1.0"}
    tok = _goplus_get_access_token()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"

    url = _GOPLUS_TOKEN_URL.format(chain_id=cid)
    r = requests.get(url, headers=headers, params={"contract_addresses": addr}, timeout=GOPLUS_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json() or {}
    result = data.get("result") or {}
    token_data = None
    if isinstance(result, dict):
        token_data = result.get(addr) or result.get(addr.lower()) or result.get(addr.upper())
        if token_data is None and len(result) == 1:
            token_data = next(iter(result.values()))
    if not isinstance(token_data, dict):
        token_data = {}
    return token_data

def _goplus_to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None

def _goplus_is_truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y")

def _goplus_check_token(chain_id: int, token_address: str, symbol: str = "") -> dict:
    cid = int(chain_id or 0)
    addr = _norm_addr(token_address)

    if _goplus_is_native_asset(cid, symbol=symbol, address=addr):
        return {
            "ok": True,
            "allowed": True,
            "native": True,
            "override": False,
            "blocked_by": None,
            "reason": "native asset",
            "chain_id": cid,
            "address": addr or "native",
            "symbol": str(symbol or "").strip().upper(),
            "raw": {},
        }

    if not _looks_like_evm_addr(addr):
        return {
            "ok": False,
            "allowed": False,
            "native": False,
            "override": False,
            "blocked_by": "validation",
            "reason": "invalid token address",
            "chain_id": cid,
            "address": addr,
            "symbol": str(symbol or "").strip().upper(),
            "raw": {},
        }

    if (cid, addr) in _goplus_allowlist():
        return {
            "ok": True,
            "allowed": True,
            "native": False,
            "override": True,
            "blocked_by": None,
            "reason": "allowlist override",
            "chain_id": cid,
            "address": addr,
            "symbol": str(symbol or "").strip().upper(),
            "raw": {},
        }

    raw = _goplus_fetch_token_security(cid, addr)
    is_honeypot = _goplus_is_truthy(raw.get("is_honeypot"))
    buy_tax = _goplus_to_float(raw.get("buy_tax"))
    sell_tax = _goplus_to_float(raw.get("sell_tax"))

    blocked_by = None
    reason = "ok"
    if GOPLUS_BLOCK_HONEYPOT and is_honeypot:
        blocked_by = "honeypot"
        reason = "blocked by GoPlus honeypot check"

    allowed = blocked_by is None
    return {
        "ok": True,
        "allowed": allowed,
        "native": False,
        "override": False,
        "blocked_by": blocked_by,
        "reason": reason,
        "chain_id": cid,
        "address": addr,
        "symbol": str(symbol or "").strip().upper(),
        "checks": {
            "is_honeypot": is_honeypot,
            "buy_tax": buy_tax,
            "sell_tax": sell_tax,
        },
        "raw": raw,
    }

@app.route("/api/security/token-check", methods=["POST"])
def api_security_token_check():
    body = request.get_json(silent=True) or {}

    chain_raw = body.get("chain_id") or body.get("chainId") or body.get("chain") or ""
    token_address = body.get("token_address") or body.get("address") or body.get("contract") or ""
    symbol = body.get("symbol") or body.get("asset") or ""

    chain_id = _goplus_chain_id(chain_raw)
    if chain_id <= 0:
        return jsonify({"status": "error", "error": "invalid chain", "ts": now_ts()}), 400

    try:
        result = _goplus_check_token(chain_id, str(token_address or ""), str(symbol or ""))
    except requests.HTTPError as e:
        detail = ""
        try:
            detail = e.response.text[:300]
        except Exception:
            detail = str(e)
        return jsonify({
            "status": "error",
            "error": "goplus_request_failed",
            "detail": detail,
            "chain_id": chain_id,
            "address": _norm_addr(token_address),
            "symbol": str(symbol or "").strip().upper(),
            "ts": now_ts(),
        }), 502
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "chain_id": chain_id,
            "address": _norm_addr(token_address),
            "symbol": str(symbol or "").strip().upper(),
            "ts": now_ts(),
        }), 400

    return jsonify({
        "status": "ok",
        "allowed": bool(result.get("allowed")),
        "native": bool(result.get("native")),
        "override": bool(result.get("override")),
        "blocked_by": result.get("blocked_by"),
        "reason": result.get("reason"),
        "chain_id": result.get("chain_id"),
        "address": result.get("address"),
        "symbol": result.get("symbol"),
        "checks": result.get("checks") or {},
        "ts": now_ts(),
    })

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
        body.get("addr"),
        body.get("address"),
        request.headers.get("X-Wallet-Address"),
        request.headers.get("x-wallet-address"),
        request.args.get("wallet"),
        request.args.get("wallet_address"),
        request.args.get("addr"),
        request.args.get("address"),
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

def _chain_key_from_id(chain_id: int | str) -> str:
    try:
        cid = int(chain_id)
    except Exception:
        return ""
    for k, v in _CHAIN_ID_BY_KEY.items():
        if int(v) == cid:
            return k
    return ""

def _chain_id_from_key(chain_key: str) -> int:
    return int(_CHAIN_ID_BY_KEY.get(_normalize_chain_key(chain_key), 0) or 0)


# Effective PRO chains exposed/used in this deployment (intersection of plan chains and enabled chains)
_CHAINS_PRO_EFFECTIVE = [c for c in _CHAINS_PRO if c in _ENABLED_EVM_CHAINS]
if not _CHAINS_PRO_EFFECTIVE:
    _CHAINS_PRO_EFFECTIVE = list(_ENABLED_EVM_CHAINS)


# Assets/features unlocked by tiers (independent of chain/network selection)
_ASSETS_SILVER = []
_ASSETS_GOLD_EXTRA = ["BTC", "SOL"]

# For now we model AI limit as an integer per day (free=1). Unlimited = -1.
_AI_LIMIT_FREE = int(os.getenv("NEXUS_DEMO_AI_DAILY_LIMIT", "5"))
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
    "NEXUS-NTE9-8KN0",
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
    "NEXUS-C1N4-R7YA",
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
    1: os.getenv("USDC_ADDRESS_ETH") or os.getenv("USDC_ADDRESS_1") or "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    56: os.getenv("USDC_ADDRESS_BNB") or os.getenv("USDC_ADDRESS_56") or "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    # Polygon native Circle USDC. Old bridged USDC is accepted as alternate below.
    137: os.getenv("USDC_ADDRESS_POL") or os.getenv("USDC_ADDRESS_POLYGON") or os.getenv("USDC_ADDRESS_137") or "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
}

_USDC_ALT_BY_CHAIN = {
    1: [x.strip() for x in (os.getenv("USDC_ALT_ADDRESSES_ETH") or os.getenv("USDC_ALT_ADDRESSES_1") or "").split(",") if x.strip()],
    56: [x.strip() for x in (os.getenv("USDC_ALT_ADDRESSES_BNB") or os.getenv("USDC_ALT_ADDRESSES_56") or "").split(",") if x.strip()],
    137: [x.strip() for x in (os.getenv("USDC_ALT_ADDRESSES_POL") or os.getenv("USDC_ALT_ADDRESSES_POLYGON") or os.getenv("USDC_ALT_ADDRESSES_137") or "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174").split(",") if x.strip()],
}

_USDT_BY_CHAIN = {
    1: os.getenv("USDT_ADDRESS_ETH") or os.getenv("USDT_ADDRESS_1") or "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    56: os.getenv("USDT_ADDRESS_BNB") or os.getenv("USDT_ADDRESS_56") or "0x55d398326f99059fF775485246999027B3197955",
    137: os.getenv("USDT_ADDRESS_POL") or os.getenv("USDT_ADDRESS_POLYGON") or os.getenv("USDT_ADDRESS_137") or "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
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

def _stable_decimals(chain_id: int, symbol: str, token_address: str | None = None) -> int:
    """Decimals for supported subscription tokens by chain/address."""
    cid = int(chain_id or 0)
    sym = str(symbol or "").strip().upper()
    if cid == 56:
        return 18
    if cid in (1, 137):
        return 6
    return _USDT_DECIMALS if sym == "USDT" else _USDC_DECIMALS

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

def _rpc_urls_for_chain(chain_id: int) -> list[str]:
    """Configured RPC first, then safe public fallbacks for ETH/BNB/POL."""
    cid = int(chain_id or 0)
    urls: list[str] = []
    configured = (_rpc_url_for_chain(cid) or "").strip()
    if configured:
        urls.append(configured)

    fallback = {
        1: ["https://ethereum.publicnode.com", "https://eth.llamarpc.com", "https://rpc.ankr.com/eth"],
        56: ["https://bsc-dataseed.binance.org", "https://bsc.publicnode.com", "https://rpc.ankr.com/bsc"],
        137: ["https://polygon-rpc.com", "https://polygon-bor-rpc.publicnode.com", "https://rpc.ankr.com/polygon"],
    }
    for u in fallback.get(cid, []):
        if u and u not in urls:
            urls.append(u)
    return urls


def _rpc_call(chain_id: int, method: str, params: list):
    cid = int(chain_id) or 0
    if _ENABLED_CHAIN_IDS and cid not in _ENABLED_CHAIN_IDS:
        raise RuntimeError(f"chain_id not enabled: {cid}")

    urls = _rpc_urls_for_chain(cid)
    if not urls:
        raise RuntimeError(f"rpc url not configured for chain_id={cid}")

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_error = ""
    for url in urls:
        try:
            r = requests.post(url, json=payload, timeout=20)
        except Exception as e:
            last_error = f"rpc request failed via {url} for chain_id={cid}: {e}"
            continue
        if not r.ok:
            body = (getattr(r, "text", "") or "")[:240]
            last_error = f"rpc http {r.status_code} via {url} for chain_id={cid}: {body}"
            continue
        try:
            j = r.json() or {}
        except Exception:
            body = (getattr(r, "text", "") or "")[:240]
            last_error = f"rpc returned non-json via {url} for chain_id={cid}: {body}"
            continue
        if j.get("error"):
            err = j.get("error")
            msg = err.get("message") if isinstance(err, dict) else str(err)
            last_error = f"rpc error via {url} for chain_id={cid}: {msg}"
            continue
        return j.get("result")
    raise RuntimeError(last_error or f"all rpc urls failed for chain_id={cid}")

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

    # accept USDC/USDT on that chain, including known alternates (Polygon native + bridged USDC)
    usdc_main = (_USDC_BY_CHAIN.get(int(chain_id)) or "").strip().lower()
    usdc_alts = [str(x or "").strip().lower() for x in (_USDC_ALT_BY_CHAIN.get(int(chain_id)) or []) if str(x or "").strip()]
    usdt = (_USDT_BY_CHAIN.get(int(chain_id)) or "").strip().lower()

    candidates = []
    seen = set()
    for addr in [usdc_main, *usdc_alts]:
        if addr and addr not in seen:
            candidates.append((addr, _stable_decimals(chain_id, "USDC", addr), "USDC"))
            seen.add(addr)
    if usdt and usdt not in seen:
        candidates.append((usdt, _stable_decimals(chain_id, "USDT", usdt), "USDT"))
        seen.add(usdt)
    if not candidates:
        raise RuntimeError("token addresses not configured for this chain")

    min_units_by_token = {}
    token_symbol_by_addr = {}
    for _addr, _dec, _sym in candidates:
        units = int(round(price * (10 ** int(_dec))))
        min_units_by_token[_addr] = units
        token_symbol_by_addr[_addr] = _sym

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
                sym = token_symbol_by_addr.get(addr, "USDT")
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
        "SELECT wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades, "
        "auto_renew_enabled, preferred_token, preferred_chain, next_billing_ts, last_auto_renew_attempt_ts, last_auto_renew_status, "
        "last_auto_renew_tx_hash, privy_wallet_id, privy_delegation_id, privy_policy_id, privy_consent_ts, auto_renew_payment_mode "
        "FROM access_state WHERE wallet_address=?",
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
    """Default public access state.

    Important product rule:
    - Every authenticated wallet can use DEMO mode immediately.
    - DEMO has real market data + simulations, but no live execution.
    - DEMO AI is limited per day across both AI endpoints.
    """
    demo_limit = int(os.getenv("NEXUS_DEMO_AI_DAILY_LIMIT", "5"))
    return {
        "plan": "demo",
        "source": "demo",
        "mode": "DEMO",
        "is_demo": True,
        "is_live": False,
        "is_permanent": False,
        "expires_at": None,
        # Demo can show all configured EVM markets/data in simulation.
        "chains_allowed": list(_ENABLED_EVM_CHAINS or _KNOWN_NETWORKS),
        "assets_allowed": [],
        "ai_limit": demo_limit,
        "ai_daily_limit": demo_limit,
        "ai_unlimited": False,
        "can_open_new_trades": False,
        "can_close_trades": True,
        "can_live_execute": False,
        "can_demo_simulate": True,
        "active": False,
        "auto_renew_enabled": False,
        "preferred_token": "USDT",
        "preferred_chain": "POL",
        "next_billing_ts": None,
        "last_auto_renew_attempt_ts": None,
        "last_auto_renew_status": "",
        "last_auto_renew_tx_hash": "",
        "privy_wallet_id": "",
        "privy_delegation_id": "",
        "privy_policy_id": "",
        "privy_consent_ts": None,
        "auto_renew_payment_mode": "manual",
    }

def _is_expired(expires_ts: int | None) -> bool:
    if expires_ts is None:
        return False
    try:
        return int(expires_ts) <= now_ts()
    except Exception:
        return True


def _copy_access_meta_from_row(base: dict, st: dict | None, exp=None) -> dict:
    st = st or {}
    base["expires_at"] = int(exp) if exp is not None else None
    base["auto_renew_enabled"] = bool(st.get("auto_renew_enabled"))
    base["preferred_token"] = str(st.get("preferred_token") or "USDT").upper()
    base["preferred_chain"] = _normalize_chain_key(st.get("preferred_chain") or "POL")
    base["next_billing_ts"] = int(st.get("next_billing_ts") or exp or 0) or None
    base["last_auto_renew_attempt_ts"] = int(st.get("last_auto_renew_attempt_ts") or 0) or None
    base["last_auto_renew_status"] = str(st.get("last_auto_renew_status") or "")
    base["last_auto_renew_tx_hash"] = str(st.get("last_auto_renew_tx_hash") or "")
    base["privy_wallet_id"] = str(st.get("privy_wallet_id") or "")
    base["privy_delegation_id"] = str(st.get("privy_delegation_id") or "")
    base["privy_policy_id"] = str(st.get("privy_policy_id") or "")
    base["privy_consent_ts"] = int(st.get("privy_consent_ts") or 0) or None
    base["auto_renew_payment_mode"] = str(st.get("auto_renew_payment_mode") or "manual")
    return base


def _compute_access_status(wallet_address: str | None) -> dict:
    """Central access state.

    Modes:
    - DEMO: default for every wallet; real data + simulation only; 5 AI/day.
    - LIVE: paid 30-day access; live execution allowed; AI unlimited.
    - PERMANENT: redeem-code lifetime access; live execution allowed; AI unlimited.
    - EXPIRED: previous paid plan expired; falls back to demo capabilities, but mode is EXPIRED.
    """
    if not wallet_address:
        return _access_defaults()

    wa = _norm_addr(wallet_address)
    st = _access_state_get(wa)
    if not st:
        base = _access_defaults()
        base["source"] = "auth"
        return base

    exp = st.get("expires_ts")
    plan = str(st.get("plan") or "demo").lower()
    source = str(st.get("source") or "db").lower()
    chains = st.get("chains_allowed") if isinstance(st.get("chains_allowed"), list) else []
    chains_effective = [c for c in chains if (c in _KNOWN_NETWORKS and (not _ENABLED_EVM_CHAINS or c in _ENABLED_EVM_CHAINS))]
    if not chains_effective:
        chains_effective = list(_CHAINS_PRO_EFFECTIVE or _ENABLED_EVM_CHAINS or ["ETH", "BNB", "POL"])

    if _is_expired(exp):
        base = _access_defaults()
        base["mode"] = "EXPIRED"
        base["source"] = source or "expired"
        base["is_demo"] = True
        base["is_live"] = False
        base["can_demo_simulate"] = True
        return _copy_access_meta_from_row(base, st, exp)

    ai_limit_raw = st.get("ai_limit")
    try:
        ai_limit = int(ai_limit_raw) if ai_limit_raw is not None else _AI_LIMIT_FREE
    except Exception:
        ai_limit = _AI_LIMIT_FREE

    can_open = bool(st.get("can_open_new_trades"))
    valid_plan = plan in ("pro", "gold", "unlimited", "silver")
    is_permanent = (source == "code")
    is_paid_live = (exp is not None and not _is_expired(exp) and source in ("payment", "usdc", "usdt", "subscription", "auto_renew", "auto-renew"))
    is_live = bool(valid_plan and can_open and (is_permanent or is_paid_live))

    if not is_live:
        base = _access_defaults()
        base["source"] = source or "auth"
        return _copy_access_meta_from_row(base, st, exp)

    mode = "PERMANENT" if is_permanent else "LIVE"
    return {
        "plan": plan,
        "source": source,
        "mode": mode,
        "is_demo": False,
        "is_live": True,
        "is_permanent": bool(is_permanent),
        "expires_at": int(exp) if exp is not None else None,
        "chains_allowed": chains_effective,
        "assets_allowed": (_ASSETS_GOLD_EXTRA if plan in ("gold", "unlimited") else _ASSETS_SILVER),
        "ai_limit": _AI_LIMIT_UNLIMITED,
        "ai_daily_limit": None,
        "ai_unlimited": True,
        "can_open_new_trades": True,
        "can_close_trades": True,
        "can_live_execute": True,
        "can_demo_simulate": True,
        "active": True,
        "auto_renew_enabled": bool(st.get("auto_renew_enabled")),
        "preferred_token": str(st.get("preferred_token") or "USDT").upper(),
        "preferred_chain": _normalize_chain_key(st.get("preferred_chain") or "POL"),
        "next_billing_ts": int(st.get("next_billing_ts") or exp or 0) or None,
        "last_auto_renew_attempt_ts": int(st.get("last_auto_renew_attempt_ts") or 0) or None,
        "last_auto_renew_status": str(st.get("last_auto_renew_status") or ""),
        "last_auto_renew_tx_hash": str(st.get("last_auto_renew_tx_hash") or ""),
        "privy_wallet_id": str(st.get("privy_wallet_id") or ""),
        "privy_delegation_id": str(st.get("privy_delegation_id") or ""),
        "privy_policy_id": str(st.get("privy_policy_id") or ""),
        "privy_consent_ts": int(st.get("privy_consent_ts") or 0) or None,
        "auto_renew_payment_mode": str(st.get("auto_renew_payment_mode") or "manual"),
    }


def _ai_usage_day_key(ts: int | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts or now_ts())))


def _ai_usage_get(wallet_address: str) -> dict:
    wa = _norm_addr(wallet_address or "")
    day_key = _ai_usage_day_key()
    if not wa:
        return {"used": 0, "limit": int(os.getenv("NEXUS_DEMO_AI_DAILY_LIMIT", "5")), "remaining": 0, "day": day_key}
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT used_count FROM ai_daily_usage WHERE wallet_address=? AND day_key=?", (wa, day_key))
    row = cur.fetchone()
    conn.close()
    used = int(row[0]) if row else 0
    limit = int(os.getenv("NEXUS_DEMO_AI_DAILY_LIMIT", "5"))
    return {"used": used, "limit": limit, "remaining": max(0, limit - used), "day": day_key}


def _ai_demo_consume_or_error(wallet_address: str, access_status: dict | None):
    """Allow paid/redeem AI unlimited; limit DEMO/EXPIRED to 5 total AI requests per UTC day."""
    st = access_status or {}
    if bool(st.get("ai_unlimited")) or bool(st.get("is_live")) or bool(st.get("is_permanent")):
        return None

    wa = _norm_addr(wallet_address or "")
    if not wa:
        return err("wallet required for demo AI", 401)

    limit = int(os.getenv("NEXUS_DEMO_AI_DAILY_LIMIT", "5"))
    day_key = _ai_usage_day_key()
    with DB_WRITE_LOCK:
        conn = _db()
        cur = conn.cursor()
        cur.execute("SELECT used_count FROM ai_daily_usage WHERE wallet_address=? AND day_key=?", (wa, day_key))
        row = cur.fetchone()
        used = int(row[0]) if row else 0
        if used >= limit:
            conn.close()
            return jsonify({
                "status": "error",
                "error": "daily demo AI limit reached",
                "mode": st.get("mode") or "DEMO",
                "ai_used_today": used,
                "ai_daily_limit": limit,
                "upgrade_required": True,
                "ts": now_ts(),
            }), 429
        new_used = used + 1
        cur.execute(
            "INSERT INTO ai_daily_usage(wallet_address, day_key, used_count, updated_ts) VALUES (?,?,?,?) "
            "ON CONFLICT(wallet_address, day_key) DO UPDATE SET used_count=excluded.used_count, updated_ts=excluded.updated_ts",
            (wa, day_key, new_used, now_ts()),
        )
        conn.commit()
        conn.close()
    return None


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
    ai_usage = _ai_usage_get(wa) if wa and not bool(st.get("ai_unlimited")) else {"used": 0, "limit": None, "remaining": None, "day": _ai_usage_day_key()}

    return jsonify({
        "status": "ok",
        "wallet_address": _norm_addr(wa) if wa else None,
        "ai_used_today": ai_usage.get("used"),
        "ai_daily_limit": ai_usage.get("limit"),
        "ai_remaining_today": ai_usage.get("remaining"),
        "ai_usage_day": ai_usage.get("day"),
        **st
    })





# -------------------------
# Access auto-renew settings
# -------------------------
_ALLOWED_AUTO_RENEW_TOKENS = {"USDT", "USDC"}

def _auto_renew_payload_from_row(row_or_status: dict | None) -> dict:
    d = row_or_status or {}
    return {
        "auto_renew_enabled": bool(d.get("auto_renew_enabled")),
        "preferred_token": str(d.get("preferred_token") or "USDT").upper(),
        "preferred_chain": _normalize_chain_key(d.get("preferred_chain") or "POL"),
        "next_billing_ts": int(d.get("next_billing_ts") or d.get("expires_at") or d.get("expires_ts") or 0) or None,
        "last_auto_renew_attempt_ts": int(d.get("last_auto_renew_attempt_ts") or 0) or None,
        "last_auto_renew_status": str(d.get("last_auto_renew_status") or ""),
        "amount_usd": float(os.getenv("NEXUS_SUBSCRIPTION_PRICE_USD", "15")),
        "period_days": int(int(os.getenv("NEXUS_SUBSCRIPTION_SECONDS", str(60 * 60 * 24 * 30))) / 86400),
        "supported_tokens": sorted(list(_ALLOWED_AUTO_RENEW_TOKENS)),
        "supported_chains": list(_ENABLED_EVM_CHAINS),
        "mode": str(d.get("auto_renew_payment_mode") or "manual"),
        "last_auto_renew_tx_hash": str(d.get("last_auto_renew_tx_hash") or ""),
        "privy_ready": bool(d.get("privy_wallet_id") and d.get("privy_delegation_id")),
        "privy_wallet_id": str(d.get("privy_wallet_id") or ""),
        "privy_policy_id": str(d.get("privy_policy_id") or ""),
        "privy_consent_ts": int(d.get("privy_consent_ts") or 0) or None,
    }

@app.route("/api/access/auto-renew/status", methods=["GET"])
def api_access_auto_renew_status():
    wa = _require_auth()
    if not wa:
        wa = _norm_addr(
            request.args.get("wallet")
            or request.args.get("addr")
            or request.args.get("address")
            or ""
        )
    if not wa or not _looks_like_evm_addr(wa):
        return err("missing wallet", 400)

    st = _compute_access_status(wa)
    return jsonify({
        "status": "ok",
        "wallet_address": _norm_addr(wa),
        "access": st,
        **_auto_renew_payload_from_row(st),
    })

@app.route("/api/access/auto-renew/set", methods=["POST"])
def api_access_auto_renew_set():
    wa = _require_auth()
    if not wa:
        wa = _norm_addr(
            request.args.get("wallet")
            or request.args.get("addr")
            or request.args.get("address")
            or (request.get_json(silent=True) or {}).get("wallet")
            or ""
        )
    if not wa or not _looks_like_evm_addr(wa):
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    enabled = str(body.get("enabled", body.get("auto_renew_enabled", "0"))).strip().lower() in ("1", "true", "yes", "on")
    token = str(body.get("token") or body.get("preferred_token") or "USDT").strip().upper()
    chain = _normalize_chain_key(body.get("chain") or body.get("preferred_chain") or "POL")

    if token not in _ALLOWED_AUTO_RENEW_TOKENS:
        return err("auto-renew token must be USDT or USDC", 400)
    if chain not in _ENABLED_EVM_CHAINS:
        return err(f"chain not enabled for auto-renew: {chain}", 400)

    st = _compute_access_status(wa)
    next_billing = int(st.get("expires_at") or 0) or None

    conn = _db()
    cur = conn.cursor()
    with DB_WRITE_LOCK:
        # Ensure row exists even if user is still free. This stores preference only.
        cur.execute(
            "INSERT INTO access_state(wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades, "
            "auto_renew_enabled, preferred_token, preferred_chain, next_billing_ts, last_auto_renew_status, updated_ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(wallet_address) DO UPDATE SET "
            "auto_renew_enabled=excluded.auto_renew_enabled, preferred_token=excluded.preferred_token, "
            "preferred_chain=excluded.preferred_chain, next_billing_ts=excluded.next_billing_ts, "
            "last_auto_renew_status=excluded.last_auto_renew_status, updated_ts=excluded.updated_ts",
            (
                wa,
                str(st.get("plan") or "free"),
                str(st.get("source") or "auto_renew_settings"),
                int(st.get("expires_at")) if st.get("expires_at") else None,
                json.dumps(st.get("chains_allowed") or [], ensure_ascii=False),
                int(st.get("ai_limit") if st.get("ai_limit") is not None else _AI_LIMIT_FREE),
                1 if bool(st.get("can_open_new_trades")) else 0,
                1 if enabled else 0,
                token,
                chain,
                int(next_billing) if next_billing else None,
                "web_preference_saved_server_worker_required" if enabled else "disabled",
                now_ts(),
            ),
        )
        conn.commit()
    conn.close()

    new_st = _compute_access_status(wa)
    return jsonify({
        "status": "ok",
        "wallet_address": wa,
        "access": new_st,
        **_auto_renew_payload_from_row(new_st),
    })

@app.route("/api/access/auto-renew/due", methods=["GET"])
def api_access_auto_renew_due():
    """Returns wallets that are due for renewal.

    Safe mode: this endpoint does NOT move funds. It only tells the Privy/payment worker
    which wallet preferences are due so a separate signed/delegated payment flow can run.
    """
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    if server_key and auth != f"Bearer {server_key}":
        return err("unauthorized", 401)

    now_i = now_ts()
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "SELECT wallet_address, preferred_token, preferred_chain, next_billing_ts, expires_ts, last_auto_renew_attempt_ts, "
        "last_auto_renew_status, last_auto_renew_tx_hash, privy_wallet_id, privy_delegation_id, privy_policy_id, "
        "privy_consent_ts, auto_renew_payment_mode "
        "FROM access_state WHERE auto_renew_enabled=1 AND expires_ts IS NOT NULL AND expires_ts <= ?",
        (now_i,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    return jsonify({
        "status": "ok",
        "ts": now_i,
        "amount_usd": float(os.getenv("NEXUS_SUBSCRIPTION_PRICE_USD", "15")),
        "due": rows,
        "note": "safe mode: no payment is executed by this endpoint",
    })



# -------------------------
# Privy Auto-Renew payment worker integration
# -------------------------
def _subscription_seconds() -> int:
    return int(os.getenv("NEXUS_SUBSCRIPTION_SECONDS", str(60 * 60 * 24 * 30)))

def _subscription_price_usd() -> float:
    return float(os.getenv("NEXUS_SUBSCRIPTION_PRICE_USD", "15"))

def _auto_renew_amount_units(token_symbol: str, chain_key: str) -> int:
    token = str(token_symbol or "").upper()
    chain = _normalize_chain_key(chain_key or "POL")
    specs = TOKEN_WHITELIST.get(chain) or []
    spec = next((x for x in specs if str(x.get("symbol") or "").upper() == token), None)
    if not spec:
        raise RuntimeError(f"{token} not supported on {chain}")
    decimals = int(spec.get("decimals") or 6)
    return int(_subscription_price_usd()) * (10 ** decimals)

def _mark_auto_renew_attempt(cur, wallet_address: str, status: str, tx_hash: str = ""):
    cur.execute(
        "UPDATE access_state SET last_auto_renew_attempt_ts=?, last_auto_renew_status=?, "
        "last_auto_renew_tx_hash=?, updated_ts=? WHERE wallet_address=?",
        (now_ts(), str(status or ""), str(tx_hash or ""), now_ts(), _norm_addr(wallet_address)),
    )

def _privy_auto_renew_charge(row: dict) -> dict:
    """Internal Privy auto-renew payment trigger.

    No external worker URL is required. This backend is the worker.

    Safety:
      - user must have enabled auto-renew and stored Privy consent metadata
      - token must be USDC/USDT
      - chain must be enabled
      - treasury must be configured
      - amount is fixed to the subscription price
      - this function does NOT hold private keys directly

    IMPORTANT:
      The actual Privy server-side transaction call is intentionally isolated in
      _privy_send_erc20_transfer(). Fill that function with your Privy server API
      credentials once your Privy signer/delegation setup is final.
    """
    wallet = _norm_addr(row.get("wallet_address") or "")
    token = str(row.get("preferred_token") or "USDT").upper()
    chain = _normalize_chain_key(row.get("preferred_chain") or "POL")
    chain_id = _chain_id_from_key(chain)

    if token not in _ALLOWED_AUTO_RENEW_TOKENS:
        raise RuntimeError("auto-renew token must be USDT or USDC")
    if chain not in _ENABLED_EVM_CHAINS or chain_id <= 0:
        raise RuntimeError(f"auto-renew chain not enabled: {chain}")
    if not _looks_like_evm_addr(wallet):
        raise RuntimeError("invalid wallet")
    if not _looks_like_evm_addr(TREASURY_ADDRESS):
        raise RuntimeError("treasury address not configured")
    if not row.get("privy_wallet_id") or not row.get("privy_delegation_id"):
        raise RuntimeError("Privy auto-renew wallet/delegation missing")
    # privy_policy_id is optional. Security is enforced by backend hard checks.

    specs = TOKEN_WHITELIST.get(chain) or []
    spec = next((x for x in specs if str(x.get("symbol") or "").upper() == token), None)
    if not spec or not _looks_like_evm_addr(spec.get("address")):
        raise RuntimeError(f"{token} token address not configured for {chain}")

    amount_units = _auto_renew_amount_units(token, chain)

    # HARD BACKEND SAFETY CHECKS:
    # Only the exact subscription payment is allowed.
    expected_price = int(_subscription_price_usd())
    if expected_price != 15:
        raise RuntimeError("auto-renew price must remain fixed at 15 USD")
    if token not in ("USDT", "USDC"):
        raise RuntimeError("auto-renew token rejected")
    if not _looks_like_evm_addr(TREASURY_ADDRESS):
        raise RuntimeError("treasury address invalid")
    if int(amount_units) != int(15 * (10 ** int(spec.get("decimals") or 6))):
        raise RuntimeError("auto-renew amount mismatch")

    tx_hash = _privy_send_erc20_transfer(
        wallet_address=wallet,
        privy_wallet_id=str(row.get("privy_wallet_id") or ""),
        privy_delegation_id=str(row.get("privy_delegation_id") or ""),
        privy_policy_id=str(row.get("privy_policy_id") or ""),
        chain=chain,
        chain_id=chain_id,
        token_symbol=token,
        token_address=str(spec.get("address")),
        amount_units=str(amount_units),
        to_address=TREASURY_ADDRESS,
    )

    if not tx_hash:
        return {
            "status": "needs_privy_integration",
            "error": "Privy server transfer is not configured yet",
            "wallet": wallet,
            "chain": chain,
            "token": token,
        }

    return {"status": "ok", "tx_hash": str(tx_hash), "chain_id": int(chain_id)}


def _privy_send_erc20_transfer(
    wallet_address: str,
    privy_wallet_id: str,
    privy_delegation_id: str,
    privy_policy_id: str,
    chain: str,
    chain_id: int,
    token_symbol: str,
    token_address: str,
    amount_units: str,
    to_address: str,
) -> str:
    """Send ERC20 transfer through Privy server-side wallet/delegation.

    This is the ONLY place that needs real Privy server API details.

    Return:
      tx_hash string when sent successfully.
      empty string when Privy ENV is not configured yet.

    Required ENV once activated:
      PRIVY_APP_ID
      PRIVY_APP_SECRET
      PRIVY_AUTHORIZATION_PRIVATE_KEY
      PRIVY_WALLET_API_URL  (optional override, depending on Privy endpoint)

    Policy is optional in this version. Backend hard checks enforce:
      - token = USDT/USDC only
      - amount = 15 USD equivalent
      - recipient = TREASURY_ADDRESS only
      - chain = enabled chains only
    """
    privy_app_id = (os.getenv("PRIVY_APP_ID") or "").strip()
    privy_app_secret = (os.getenv("PRIVY_APP_SECRET") or "").strip()
    if not privy_app_id or not privy_app_secret:
        return ""

    # ERC20 transfer(address,uint256)
    data = _erc20_transfer_data_for_backend(to_address, int(amount_units))

    # NOTE:
    # Privy server wallet APIs can differ depending on your enabled product
    # (delegated wallets / server wallets / policies). Keep this isolated so
    # only this payload needs adjustment to your Privy dashboard setup.
    url = (os.getenv("PRIVY_WALLET_API_URL") or "").strip()
    if not url:
        # Placeholder endpoint. Set PRIVY_WALLET_API_URL to your Privy server wallet
        # transaction endpoint from your Privy dashboard/docs.
        return ""

    payload = {
        "wallet_id": privy_wallet_id,
        "delegation_id": privy_delegation_id,
        "policy_id": privy_policy_id or "",
        "chain_id": int(chain_id),
        "to": token_address,
        "value": "0",
        "data": data,
        "metadata": {
            "purpose": "nexus_pro_auto_renew",
            "user_wallet": wallet_address,
            "token": token_symbol,
            "amount_units": str(amount_units),
            "treasury": to_address,
        },
    }

    r = requests.post(
        url,
        auth=(privy_app_id, privy_app_secret),
        json=payload,
        timeout=30,
    )
    try:
        out = r.json()
    except Exception:
        out = {"raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"Privy API HTTP {r.status_code}: {str(out)[:300]}")

    tx_hash = (
        out.get("tx_hash")
        or out.get("transaction_hash")
        or out.get("hash")
        or ((out.get("data") or {}).get("tx_hash") if isinstance(out.get("data"), dict) else "")
        or ""
    )
    return str(tx_hash or "").strip()


def _erc20_transfer_data_for_backend(to_address: str, amount_units: int) -> str:
    """Encode ERC20 transfer(address,uint256) without web3 dependency."""
    to = _norm_addr(to_address)
    if not _looks_like_evm_addr(to):
        raise RuntimeError("invalid transfer recipient")
    amount = int(amount_units)
    if amount <= 0:
        raise RuntimeError("invalid transfer amount")
    selector = "a9059cbb"  # transfer(address,uint256)
    addr_word = to.lower().replace("0x", "").rjust(64, "0")
    amount_word = hex(amount)[2:].rjust(64, "0")
    return "0x" + selector + addr_word + amount_word


@app.route("/api/access/auto-renew/consent", methods=["POST"])
def api_access_auto_renew_consent():
    """Store explicit user consent metadata for Privy auto-renew.

    The UI/Privy flow must collect user approval first. This endpoint only stores the
    resulting policy/delegation ids and enables auto-renew for this wallet.
    """
    wa = _require_auth()
    if not wa:
        wa = _norm_addr((request.get_json(silent=True) or {}).get("wallet") or request.args.get("wallet") or "")
    if not wa or not _looks_like_evm_addr(wa):
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    token = str(body.get("token") or body.get("preferred_token") or "USDT").upper()
    chain = _normalize_chain_key(body.get("chain") or body.get("preferred_chain") or "POL")
    acknowledged = bool(body.get("acknowledged") or body.get("consent") or body.get("accepted"))

    if not acknowledged:
        return err("explicit user consent acknowledgement required", 400)
    if token not in _ALLOWED_AUTO_RENEW_TOKENS:
        return err("auto-renew token must be USDT or USDC", 400)
    if chain not in _ENABLED_EVM_CHAINS:
        return err(f"chain not enabled for auto-renew: {chain}", 400)

    privy_wallet_id = str(body.get("privy_wallet_id") or "").strip()
    privy_delegation_id = str(body.get("privy_delegation_id") or body.get("delegation_id") or "").strip()
    privy_policy_id = str(body.get("privy_policy_id") or body.get("policy_id") or "").strip()
    if not privy_wallet_id or not privy_delegation_id:
        return err("missing Privy wallet/delegation id", 400)
    # privy_policy_id may be empty. Backend hard checks enforce token/amount/treasury safety.

    st = _compute_access_status(wa)
    next_billing = int(st.get("expires_at") or 0) or None

    conn = _db()
    cur = conn.cursor()
    with DB_WRITE_LOCK:
        cur.execute(
            "INSERT INTO access_state(wallet_address, plan, source, expires_ts, chains_allowed_json, ai_limit, can_open_new_trades, "
            "auto_renew_enabled, preferred_token, preferred_chain, next_billing_ts, last_auto_renew_status, "
            "privy_wallet_id, privy_delegation_id, privy_policy_id, privy_consent_ts, auto_renew_payment_mode, updated_ts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(wallet_address) DO UPDATE SET "
            "auto_renew_enabled=1, preferred_token=excluded.preferred_token, preferred_chain=excluded.preferred_chain, "
            "next_billing_ts=excluded.next_billing_ts, last_auto_renew_status=excluded.last_auto_renew_status, "
            "privy_wallet_id=excluded.privy_wallet_id, privy_delegation_id=excluded.privy_delegation_id, "
            "privy_policy_id=excluded.privy_policy_id, privy_consent_ts=excluded.privy_consent_ts, "
            "auto_renew_payment_mode=excluded.auto_renew_payment_mode, updated_ts=excluded.updated_ts",
            (
                wa,
                str(st.get("plan") or "free"),
                str(st.get("source") or "privy_consent"),
                int(st.get("expires_at")) if st.get("expires_at") else None,
                json.dumps(st.get("chains_allowed") or [], ensure_ascii=False),
                int(st.get("ai_limit") if st.get("ai_limit") is not None else _AI_LIMIT_FREE),
                1 if bool(st.get("can_open_new_trades")) else 0,
                1,
                token,
                chain,
                int(next_billing) if next_billing else None,
                "privy_consent_ready",
                privy_wallet_id,
                privy_delegation_id,
                privy_policy_id,
                now_ts(),
                "privy_delegated",
                now_ts(),
            ),
        )
        conn.commit()
    conn.close()

    new_st = _compute_access_status(wa)
    return jsonify({"status": "ok", "wallet_address": wa, "access": new_st, **_auto_renew_payload_from_row(new_st)})


@app.route("/api/access/auto-renew/test-enable", methods=["POST"])
def api_access_auto_renew_test_enable():
    """Shell/test endpoint: enable Auto Renew without real USDT/USDC.

    This is for Render Shell / staging tests only. It lets you verify:
      - DB state
      - UI display
      - renewal extension logic
      - /api/access/auto-renew/run behavior

    It does NOT move money and does NOT call Privy.
    """
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    body = request.get_json(silent=True) or {}

    # If NEXUS_API_KEY exists, protect the test endpoint for shell/admin use.
    # If no key is configured, wallet param is still required.
    if server_key and auth != f"Bearer {server_key}":
        return err("unauthorized", 401)

    wa = _norm_addr(
        body.get("wallet")
        or body.get("wallet_address")
        or request.args.get("wallet")
        or request.args.get("wallet_address")
        or ""
    )
    if not wa or not _looks_like_evm_addr(wa):
        return err("missing wallet", 400)

    token = str(body.get("token") or body.get("preferred_token") or "USDT").strip().upper()
    chain = _normalize_chain_key(body.get("chain") or body.get("preferred_chain") or "POL")
    if token not in _ALLOWED_AUTO_RENEW_TOKENS:
        return err("auto-renew token must be USDT or USDC", 400)
    if chain not in _ENABLED_EVM_CHAINS:
        return err(f"chain not enabled for auto-renew: {chain}", 400)

    now_i = now_ts()
    seconds = _subscription_seconds()
    # Optional shell controls:
    #   seconds: custom access duration
    #   due_now: true -> expires immediately so /run can extend it
    #   days: custom duration in days
    try:
        if body.get("days") is not None:
            seconds = int(float(body.get("days")) * 86400)
        elif body.get("seconds") is not None:
            seconds = int(body.get("seconds"))
    except Exception:
        seconds = _subscription_seconds()
    seconds = max(60, min(seconds, 366 * 86400))
    due_now = str(body.get("due_now") or request.args.get("due_now") or "").strip().lower() in ("1", "true", "yes", "on")
    expires_ts = now_i - 5 if due_now else now_i + seconds

    conn = _db()
    cur = conn.cursor()
    with DB_WRITE_LOCK:
        _access_state_put(
            wallet_address=wa,
            plan="pro",
            source="auto_renew_test",
            expires_ts=expires_ts,
            chains_allowed=list(_CHAINS_PRO_EFFECTIVE),
            ai_limit=_AI_LIMIT_UNLIMITED,
            can_open_new_trades=True,
            conn=conn,
            cur=cur,
        )
        cur.execute(
            "UPDATE access_state SET auto_renew_enabled=1, preferred_token=?, preferred_chain=?, "
            "next_billing_ts=?, last_auto_renew_attempt_ts=?, last_auto_renew_status=?, "
            "last_auto_renew_tx_hash='', auto_renew_payment_mode='test', updated_ts=? WHERE wallet_address=?",
            (token, chain, int(expires_ts), now_i, "test_enabled_due_now" if due_now else "test_enabled", now_i, wa),
        )
        conn.commit()
    conn.close()

    new_st = _compute_access_status(wa)
    return jsonify({
        "status": "ok",
        "wallet_address": wa,
        "message": "Auto Renew test mode enabled. No real token transfer was executed.",
        "access": new_st,
        **_auto_renew_payload_from_row(new_st),
    })

@app.route("/api/access/auto-renew/test-disable", methods=["POST"])
def api_access_auto_renew_test_disable():
    """Shell/test endpoint: disable Auto Renew for a wallet."""
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    if server_key and auth != f"Bearer {server_key}":
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    wa = _norm_addr(body.get("wallet") or body.get("wallet_address") or request.args.get("wallet") or "")
    if not wa or not _looks_like_evm_addr(wa):
        return err("missing wallet", 400)

    conn = _db()
    cur = conn.cursor()
    with DB_WRITE_LOCK:
        cur.execute(
            "UPDATE access_state SET auto_renew_enabled=0, last_auto_renew_status='test_disabled', "
            "auto_renew_payment_mode='manual', updated_ts=? WHERE wallet_address=?",
            (now_ts(), wa),
        )
        conn.commit()
    conn.close()
    new_st = _compute_access_status(wa)
    return jsonify({"status": "ok", "wallet_address": wa, "access": new_st, **_auto_renew_payload_from_row(new_st)})

@app.route("/api/access/auto-renew/run", methods=["POST"])
def api_access_auto_renew_run():
    """Server-only worker endpoint.

    Finds due wallets, uses the internal Privy signer integration to send USDC/USDT to treasury,
    verifies the tx using the existing subscription verifier, and extends access by 30 days.
    """
    server_key = (os.getenv("NEXUS_API_KEY") or "").strip()
    auth = (request.headers.get("Authorization") or "").strip()
    if server_key and auth != f"Bearer {server_key}":
        return err("unauthorized", 401)

    now_i = now_ts()
    limit = max(1, min(50, int((request.get_json(silent=True) or {}).get("limit") or 10)))

    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "SELECT wallet_address, preferred_token, preferred_chain, next_billing_ts, expires_ts, last_auto_renew_attempt_ts, "
        "last_auto_renew_status, last_auto_renew_tx_hash, privy_wallet_id, privy_delegation_id, privy_policy_id, "
        "privy_consent_ts, auto_renew_payment_mode "
        "FROM access_state WHERE auto_renew_enabled=1 AND expires_ts IS NOT NULL AND expires_ts <= ? "
        "ORDER BY expires_ts ASC LIMIT ?",
        (now_i, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    results = []

    for row in rows:
        wa = _norm_addr(row.get("wallet_address") or "")
        try:
            # TEST MODE: simulate a successful renewal without Privy and without USDT/USDC.
            # Enable it via /api/access/auto-renew/test-enable. This is the Render Shell
            # path for checking the whole subscription/renewal lifecycle safely.
            if str(row.get("auto_renew_payment_mode") or "").strip().lower() == "test":
                expires_ts = now_ts() + _subscription_seconds()
                _access_state_put(
                    wallet_address=wa,
                    plan="pro",
                    source="auto_renew_test",
                    expires_ts=expires_ts,
                    chains_allowed=list(_CHAINS_PRO_EFFECTIVE),
                    ai_limit=_AI_LIMIT_UNLIMITED,
                    can_open_new_trades=True,
                    conn=conn,
                    cur=cur,
                )
                cur.execute(
                    "UPDATE access_state SET auto_renew_enabled=1, preferred_token=?, preferred_chain=?, "
                    "next_billing_ts=?, last_auto_renew_attempt_ts=?, last_auto_renew_status=?, "
                    "last_auto_renew_tx_hash=?, auto_renew_payment_mode='test', updated_ts=? WHERE wallet_address=?",
                    (
                        str(row.get("preferred_token") or "USDT").upper(),
                        _normalize_chain_key(row.get("preferred_chain") or "POL"),
                        int(expires_ts),
                        now_ts(),
                        "test_success",
                        "test_no_tx",
                        now_ts(),
                        wa,
                    ),
                )
                results.append({
                    "wallet_address": wa,
                    "status": "test_success",
                    "tx_hash": "test_no_tx",
                    "expires_ts": expires_ts,
                    "note": "No real token transfer was executed."
                })
                continue

            charge = _privy_auto_renew_charge(row)
            if charge.get("status") != "ok":
                _mark_auto_renew_attempt(cur, wa, charge.get("error") or charge.get("status") or "privy_worker_not_ready")
                results.append({"wallet_address": wa, **charge})
                continue

            tx_hash = str(charge.get("tx_hash") or "").lower()
            chain_id = int(charge.get("chain_id") or _chain_id_from_key(row.get("preferred_chain") or "POL"))

            cur.execute("SELECT tx_hash FROM access_payments WHERE tx_hash=?", (tx_hash,))
            if cur.fetchone():
                _mark_auto_renew_attempt(cur, wa, "already_verified", tx_hash)
                results.append({"wallet_address": wa, "status": "already_verified", "tx_hash": tx_hash})
                continue

            proof = _verify_erc20_payment(chain_id=chain_id, tx_hash=tx_hash, payer=wa, plan="pro")
            cur.execute(
                "INSERT INTO access_payments(tx_hash, wallet_address, chain_id, token, amount_units, plan, created_ts) VALUES (?,?,?,?,?,?,?)",
                (tx_hash, wa, int(chain_id), str(proof.get("token") or ""), int(proof.get("amount_units") or 0), "pro", now_ts()),
            )

            expires_ts = now_ts() + _subscription_seconds()
            _access_state_put(
                wallet_address=wa,
                plan="pro",
                source=f"auto_renew_{str(proof.get('token') or 'payment').lower()}",
                expires_ts=expires_ts,
                chains_allowed=list(_CHAINS_PRO_EFFECTIVE),
                ai_limit=_AI_LIMIT_UNLIMITED,
                can_open_new_trades=True,
                conn=conn,
                cur=cur,
            )

            cur.execute(
                "UPDATE access_state SET auto_renew_enabled=1, preferred_token=?, preferred_chain=?, "
                "next_billing_ts=?, last_auto_renew_attempt_ts=?, last_auto_renew_status=?, "
                "last_auto_renew_tx_hash=?, updated_ts=? WHERE wallet_address=?",
                (
                    str(proof.get("token") or row.get("preferred_token") or "USDT").upper(),
                    _chain_key_from_id(chain_id),
                    int(expires_ts),
                    now_ts(),
                    "success",
                    tx_hash,
                    now_ts(),
                    wa,
                ),
            )
            results.append({"wallet_address": wa, "status": "ok", "tx_hash": tx_hash, "expires_ts": expires_ts})

        except Exception as e:
            _mark_auto_renew_attempt(cur, wa, f"error: {str(e)[:220]}")
            results.append({"wallet_address": wa, "status": "error", "error": str(e)})

    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "processed": len(results), "results": results, "ts": now_i})



# -------------------------
# Watchlist user rating + owner-controlled coin links
# -------------------------
_ALLOWED_USER_RATINGS = {"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "RISK"}

def _today_utc_date() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

# Automatic Coin Info links.
# No NEXUS_COIN_LINKS_JSON required anymore.
# Backend resolves Symbol -> CoinGecko ID -> Homepage / CoinGecko page and caches it.
_COIN_INFO_CACHE: dict[str, tuple[float, dict]] = {}
_COIN_INFO_TTL_SEC = int(os.getenv("NEXUS_COIN_INFO_TTL_SEC", str(24 * 60 * 60)))

_COIN_INFO_ID_OVERRIDES = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "TON": "the-open-network",
    "POL": "polygon-ecosystem-token",
    "MATIC": "matic-network",
    "LINK": "chainlink",
}

def _safe_http_url(url: str) -> str:
    u = str(url or "").strip()
    if u.startswith("https://") or u.startswith("http://"):
        return u
    return ""

def _coin_info_coingecko_page(coin_id: str, symbol: str = "") -> str:
    cid = str(coin_id or "").strip()
    if cid:
        return f"https://www.coingecko.com/en/coins/{cid}"
    sym = str(symbol or "").strip()
    if sym:
        return f"https://www.coingecko.com/en/search?query={requests.utils.quote(sym)}"
    return "https://www.coingecko.com/"

def _coin_info_id_from_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""

    # Known native / high-volume assets first.
    if sym in _COIN_INFO_ID_OVERRIDES:
        return _COIN_INFO_ID_OVERRIDES[sym]

    # If the on-chain resolver already found a CoinGecko id, reuse it.
    try:
        contract = _contract_from_coingecko_symbol(sym)
        cid = str((contract or {}).get("id") or "").strip()
        if cid:
            return cid
    except Exception:
        pass

    # Fallback: CoinGecko list search by symbol.
    try:
        coins = _cg_coin_list_with_platforms()
        candidates = []
        for c in coins if isinstance(coins, list) else []:
            try:
                if str(c.get("symbol") or "").strip().upper() == sym:
                    candidates.append(c)
            except Exception:
                continue
        # Prefer an exact/simple id if possible, otherwise first candidate.
        for c in candidates:
            cid = str(c.get("id") or "").strip()
            if cid and (cid.upper() == sym or cid.lower().replace("-", "") == sym.lower()):
                return cid
        if candidates:
            return str(candidates[0].get("id") or "").strip()
    except Exception:
        pass

    return ""

def _coin_info_for_symbol(symbol: str) -> dict:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"symbol": sym, "link": "", "link_enabled": False, "source": "none"}

    now = time.time()
    hit = _COIN_INFO_CACHE.get(sym)
    if hit and (now - hit[0]) < _COIN_INFO_TTL_SEC:
        return dict(hit[1])

    coin_id = _coin_info_id_from_symbol(sym)
    coingecko_url = _coin_info_coingecko_page(coin_id, sym)

    homepage = ""
    explorer = ""
    name = ""
    source = "coingecko_search"

    if coin_id:
        try:
            detail_url = (
                f"{COINGECKO_BASE}/coins/{requests.utils.quote(coin_id)}"
                "?localization=false&tickers=false&market_data=false"
                "&community_data=false&developer_data=false&sparkline=false"
            )
            data = _cg_get(detail_url)
            if isinstance(data, dict):
                name = str(data.get("name") or "").strip()
                links = data.get("links") or {}
                if isinstance(links, dict):
                    homes = links.get("homepage") or []
                    if isinstance(homes, list):
                        for u in homes:
                            homepage = _safe_http_url(u)
                            if homepage:
                                break
                    explorers = links.get("blockchain_site") or []
                    if isinstance(explorers, list):
                        for u in explorers:
                            explorer = _safe_http_url(u)
                            if explorer:
                                break
                source = "coingecko_detail"
        except Exception:
            pass

    link = homepage or coingecko_url
    out = {
        "symbol": sym,
        "coin_id": coin_id,
        "name": name,
        "link": link,
        "link_enabled": bool(link),
        "homepage": homepage,
        "coingecko_url": coingecko_url,
        "explorer": explorer,
        "source": source,
        "cached_for_sec": _COIN_INFO_TTL_SEC,
        "ts": now_ts(),
    }
    _COIN_INFO_CACHE[sym] = (now, dict(out))
    return out

def _ensure_rating_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_coin_ratings (
            wallet_address TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rating TEXT NOT NULL,
            rating_date TEXT NOT NULL,
            created_ts INTEGER,
            updated_ts INTEGER,
            PRIMARY KEY (wallet_address, symbol, rating_date)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_coin_ratings_wallet_symbol ON user_coin_ratings(wallet_address, symbol)")

_RATING_POINTS = {
    "AAA": 98,
    "AA": 90,
    "A": 80,
    "BBB": 70,
    "BB": 60,
    "B": 50,
    "CCC": 40,
    "CC": 30,
    "C": 20,
    "RISK": 5,
}

def _coin_rating_summary(symbol: str) -> dict:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"count": 0, "ratings": {}, "avg_score": None}
    conn = _db()
    try:
        _ensure_rating_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT rating, COUNT(*) AS c FROM user_coin_ratings WHERE symbol=? GROUP BY rating", (sym,))
        ratings = {str(r["rating"]): int(r["c"] or 0) for r in cur.fetchall()}
        count = int(sum(ratings.values()))
        total = 0.0
        for rating, c in ratings.items():
            total += float(_RATING_POINTS.get(str(rating).upper(), 0)) * int(c or 0)
        avg_score = round(total / count, 2) if count > 0 else None
        return {"count": count, "ratings": ratings, "avg_score": avg_score}
    finally:
        conn.close()

@app.route("/api/ratings/coin", methods=["GET"])
def api_rating_coin_status():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return err("unauthorized", 401)
    sym = str(request.args.get("symbol") or request.args.get("coin") or "").strip().upper()
    if not sym:
        return err("missing symbol", 400)
    today = _today_utc_date()
    conn = _db()
    try:
        _ensure_rating_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT rating, rating_date, updated_ts FROM user_coin_ratings WHERE wallet_address=? AND symbol=? AND rating_date=? LIMIT 1", (_norm_addr(wa), sym, today))
        row = cur.fetchone()
        last = None
        if not row:
            cur.execute("SELECT rating, rating_date, updated_ts FROM user_coin_ratings WHERE wallet_address=? AND symbol=? ORDER BY rating_date DESC, updated_ts DESC LIMIT 1", (_norm_addr(wa), sym))
            last = cur.fetchone()
        coin_info = _coin_info_for_symbol(sym)
        return jsonify({
            "status": "ok", "symbol": sym, "today": today,
            "can_vote": True, "already_voted_today": row is not None,
            "user_rating_today": row["rating"] if row else None,
            "last_user_rating": (row["rating"] if row else (last["rating"] if last else None)),
            "last_rating_date": (row["rating_date"] if row else (last["rating_date"] if last else None)),
            "link": coin_info.get("link") or "",
            "link_enabled": bool(coin_info.get("link_enabled")),
            "coin_info": coin_info,
            "summary": _coin_rating_summary(sym), "ts": now_ts(),
        })
    finally:
        conn.close()

@app.route("/api/coin/info", methods=["GET"])
def api_coin_info():
    sym = str(request.args.get("symbol") or request.args.get("coin") or "").strip().upper()
    if not sym:
        return err("missing symbol", 400)
    info = _coin_info_for_symbol(sym)
    return jsonify({"status": "ok", **info})


@app.route("/api/ratings/vote", methods=["POST"])
def api_rating_vote():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return err("unauthorized", 401)
    body = request.get_json(silent=True) or {}
    sym = str(body.get("symbol") or body.get("coin") or "").strip().upper()
    rating = str(body.get("rating") or "").strip().upper().replace("-", "_")
    if not sym:
        return err("missing symbol", 400)
    if rating not in _ALLOWED_USER_RATINGS:
        return err("invalid rating", 400)
    today = _today_utc_date()
    nowi = now_ts()
    conn = _db()
    try:
        _ensure_rating_table(conn)
        cur = conn.cursor()
        with DB_WRITE_LOCK:
            cur.execute("SELECT rating FROM user_coin_ratings WHERE wallet_address=? AND symbol=? AND rating_date=? LIMIT 1", (_norm_addr(wa), sym, today))
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    "UPDATE user_coin_ratings SET rating=?, updated_ts=? WHERE wallet_address=? AND symbol=? AND rating_date=?",
                    (rating, nowi, _norm_addr(wa), sym, today),
                )
            else:
                cur.execute("INSERT INTO user_coin_ratings(wallet_address, symbol, rating, rating_date, created_ts, updated_ts) VALUES (?,?,?,?,?,?)", (_norm_addr(wa), sym, rating, today, nowi, nowi))
            conn.commit()
        coin_info = _coin_info_for_symbol(sym)
        return jsonify({
            "status": "ok",
            "symbol": sym,
            "rating": rating,
            "today": today,
            "can_vote": True,
            "already_voted_today": True,
            "link": coin_info.get("link") or "",
            "link_enabled": bool(coin_info.get("link_enabled")),
            "coin_info": coin_info,
            "summary": _coin_rating_summary(sym),
            "ts": nowi,
        })
    finally:
        conn.close()



# -------------------------
# Dynamic On-Chain Signal Layer (CoinGecko -> Contract -> Moralis)
# -------------------------
MORALIS_API_KEY = (os.getenv("MORALIS_API_KEY") or "").strip()
_CG_CONTRACT_CACHE: dict[str, tuple[float, dict]] = {}
_ONCHAIN_SIGNAL_CACHE: dict[str, tuple[float, dict]] = {}
_CG_CONTRACT_TTL_SEC = int(os.getenv("NEXUS_CG_CONTRACT_TTL_SEC", str(12 * 60 * 60)))
_ONCHAIN_SIGNAL_TTL_SEC = int(os.getenv("NEXUS_ONCHAIN_SIGNAL_TTL_SEC", "900"))

_NATIVE_ONCHAIN = {
    "BTC": {"type": "native", "chain": "btc", "address": ""},
    "ETH": {"type": "native", "chain": "eth", "address": ""},
    "BNB": {"type": "native", "chain": "bsc", "address": ""},
    "SOL": {"type": "native", "chain": "sol", "address": ""},
    "XRP": {"type": "native", "chain": "xrp", "address": ""},
    "ADA": {"type": "native", "chain": "cardano", "address": ""},
    "AVAX": {"type": "native", "chain": "avalanche", "address": ""},
    "TON": {"type": "native", "chain": "ton", "address": ""},
    "POL": {"type": "native", "chain": "polygon", "address": ""},
    "MATIC": {"type": "native", "chain": "polygon", "address": ""},
}

_PLATFORM_TO_MORALIS_CHAIN = {
    "ethereum": "eth",
    "binance-smart-chain": "bsc",
    "polygon-pos": "polygon",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "base": "base",
    "avalanche": "avalanche",
}

_CHAIN_PRIORITY = ["ethereum", "polygon-pos", "binance-smart-chain", "base", "arbitrum-one", "optimistic-ethereum", "avalanche"]

def _onchain_empty_signal(symbol: str, reason: str = "") -> dict:
    sym = str(symbol or "").strip().upper()
    return {
        "symbol": sym,
        "icon": "",
        "label": "",
        "score_delta": 0,
        "signals": {
            "whale": False,
            "exchange_inflow": False,
            "accumulation": False,
            "volume_spike": False,
            "liquidity": False,
        },
        "contract": None,
        "summary": reason or "No strong on-chain signal.",
        "source": "none",
        "ts": now_ts(),
    }

def _cg_coin_list_with_platforms() -> list:
    cache_key = "__coins_list_platforms__"
    now = time.time()
    hit = _CG_CONTRACT_CACHE.get(cache_key)
    if hit and (now - hit[0]) < _CG_CONTRACT_TTL_SEC:
        data = hit[1].get("data")
        return data if isinstance(data, list) else []
    url = f"{COINGECKO_BASE}/coins/list?include_platform=true"
    data = _cg_get(url)
    if not isinstance(data, list):
        data = []
    _CG_CONTRACT_CACHE[cache_key] = (now, {"data": data})
    return data

def _contract_from_coingecko_symbol(symbol: str) -> dict | None:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None

    if sym in _NATIVE_ONCHAIN:
        d = dict(_NATIVE_ONCHAIN[sym])
        d["symbol"] = sym
        d["id"] = sym.lower()
        return d

    now = time.time()
    hit = _CG_CONTRACT_CACHE.get(sym)
    if hit and (now - hit[0]) < _CG_CONTRACT_TTL_SEC:
        return hit[1] if isinstance(hit[1], dict) else None

    # Optional owner overrides for edge cases, but no longer required.
    # Format: {"LINK":{"platform":"ethereum","address":"0x...","id":"chainlink"}}
    raw_overrides = str(os.getenv("NEXUS_TOKEN_CONTRACTS_JSON") or "").strip()
    if raw_overrides:
        try:
            overrides = json.loads(raw_overrides)
            ov = overrides.get(sym) if isinstance(overrides, dict) else None
            if isinstance(ov, str):
                if ov.lower() == "native":
                    d = {"symbol": sym, "type": "native", "chain": sym.lower(), "address": "", "id": sym.lower()}
                    _CG_CONTRACT_CACHE[sym] = (now, d)
                    return d
                if _looks_like_evm_addr(ov):
                    d = {"symbol": sym, "type": "erc20", "platform": "ethereum", "chain": "eth", "address": _norm_addr(ov), "id": sym.lower()}
                    _CG_CONTRACT_CACHE[sym] = (now, d)
                    return d
            elif isinstance(ov, dict):
                addr = _norm_addr(ov.get("address") or ov.get("contract") or "")
                platform = str(ov.get("platform") or ov.get("chain") or "ethereum").strip()
                chain = _PLATFORM_TO_MORALIS_CHAIN.get(platform, platform)
                d = {"symbol": sym, "type": "erc20" if addr else "native", "platform": platform, "chain": chain, "address": addr, "id": str(ov.get("id") or sym.lower())}
                _CG_CONTRACT_CACHE[sym] = (now, d)
                return d
        except Exception:
            pass

    coins = _cg_coin_list_with_platforms()
    candidates = []
    for c in coins:
        try:
            if str(c.get("symbol") or "").strip().upper() != sym:
                continue
            platforms = c.get("platforms") or {}
            if not isinstance(platforms, dict):
                platforms = {}
            candidates.append((c, platforms))
        except Exception:
            continue

    # Prefer known platforms with a real EVM contract.
    for platform in _CHAIN_PRIORITY:
        for c, platforms in candidates:
            addr = _norm_addr(platforms.get(platform) or "")
            if _looks_like_evm_addr(addr):
                d = {
                    "symbol": sym,
                    "id": str(c.get("id") or "").strip(),
                    "name": str(c.get("name") or "").strip(),
                    "type": "erc20",
                    "platform": platform,
                    "chain": _PLATFORM_TO_MORALIS_CHAIN.get(platform, platform),
                    "address": addr,
                }
                _CG_CONTRACT_CACHE[sym] = (now, d)
                return d

    # Fallback: first EVM-looking contract from CoinGecko.
    for c, platforms in candidates:
        for platform, addr_raw in platforms.items():
            addr = _norm_addr(addr_raw or "")
            if _looks_like_evm_addr(addr):
                d = {
                    "symbol": sym,
                    "id": str(c.get("id") or "").strip(),
                    "name": str(c.get("name") or "").strip(),
                    "type": "erc20",
                    "platform": platform,
                    "chain": _PLATFORM_TO_MORALIS_CHAIN.get(platform, platform),
                    "address": addr,
                }
                _CG_CONTRACT_CACHE[sym] = (now, d)
                return d

    _CG_CONTRACT_CACHE[sym] = (now, None)
    return None

def _moralis_get(path: str, params: dict | None = None) -> dict:
    if not MORALIS_API_KEY:
        raise RuntimeError("missing MORALIS_API_KEY")
    url = "https://deep-index.moralis.io/api/v2.2" + path
    headers = {
        "Accept": "application/json",
        "X-API-Key": MORALIS_API_KEY,
        "User-Agent": "NexusAnalyt/1.0",
    }
    r = requests.get(url, headers=headers, params=params or {}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else {"result": data}

def _onchain_signal_for_symbol(symbol: str) -> dict:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return _onchain_empty_signal(sym, "Missing symbol.")

    now = time.time()
    hit = _ONCHAIN_SIGNAL_CACHE.get(sym)
    if hit and (now - hit[0]) < _ONCHAIN_SIGNAL_TTL_SEC:
        return hit[1]

    contract = _contract_from_coingecko_symbol(sym)
    if not contract:
        out = _onchain_empty_signal(sym, "No CoinGecko contract mapping found yet.")
        _ONCHAIN_SIGNAL_CACHE[sym] = (now, out)
        return out

    if contract.get("type") == "native" or not contract.get("address"):
        out = _onchain_empty_signal(sym, "Native asset: no ERC-20 contract flow available in Phase 1.")
        out["contract"] = contract
        out["source"] = "coingecko"
        _ONCHAIN_SIGNAL_CACHE[sym] = (now, out)
        return out

    score_delta = 0
    signals = {
        "whale": False,
        "exchange_inflow": False,
        "accumulation": False,
        "volume_spike": False,
        "liquidity": False,
    }
    icon = ""
    label = ""
    summary = "Contract mapped. No strong on-chain anomaly detected."
    source = "coingecko"

    # Best-effort Moralis Phase 1:
    # Keep this conservative. If Moralis/rate-limit fails, return neutral instead of breaking the app.
    try:
        chain = str(contract.get("chain") or "eth")
        addr = str(contract.get("address") or "").lower()

        # Transfers endpoint gives a cheap activity proxy. We do NOT over-trust it.
        transfers = _moralis_get(f"/erc20/{addr}/transfers", {"chain": chain, "limit": 25})
        rows = transfers.get("result") or []
        if not isinstance(rows, list):
            rows = []

        tx_count = len(rows)
        large_count = 0
        for tx in rows:
            try:
                val = float(tx.get("value_decimal") or 0)
                if val >= 100000:
                    large_count += 1
            except Exception:
                continue

        if large_count >= 2:
            signals["whale"] = True
            score_delta += 3
            icon = "🔥"
            label = "Whale activity"
            summary = f"{sym}: whale-sized transfers detected in recent token flow."
        elif tx_count >= 20:
            signals["volume_spike"] = True
            score_delta += 1
            icon = "📊"
            label = "On-chain activity"
            summary = f"{sym}: elevated recent on-chain transfer activity."

        source = "moralis"
    except Exception:
        # Neutral fallback. The app should never fail just because the data provider is unavailable.
        pass

    score_delta = max(-5, min(5, int(score_delta)))
    out = {
        "symbol": sym,
        "icon": icon,
        "label": label,
        "score_delta": score_delta,
        "signals": signals,
        "contract": contract,
        "summary": summary,
        "source": source,
        "ts": now_ts(),
    }
    _ONCHAIN_SIGNAL_CACHE[sym] = (now, out)
    return out

@app.route("/api/onchain/signals", methods=["GET"])
def api_onchain_signals():
    raw = request.args.get("symbols") or request.args.get("symbol") or ""
    symbols = []
    for part in str(raw or "").split(","):
        s = str(part or "").strip().upper()
        if s and s not in symbols:
            symbols.append(s)
    symbols = symbols[:50]
    out = {sym: _onchain_signal_for_symbol(sym) for sym in symbols}
    return jsonify({
        "status": "ok",
        "signals": out,
        "count": len(out),
        "moralis_enabled": bool(MORALIS_API_KEY),
        "contract_mapping": "dynamic_coingecko",
        "ts": now_ts(),
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




def _symbol_from_item(item_id: str) -> str:
    """Extract tradable symbol from item id like POL:POL -> POL."""
    try:
        s = str(item_id or "").strip()
        if ":" in s:
            s = s.split(":", 1)[1]
        return s.strip().upper()
    except Exception:
        return str(item_id or "").strip().upper()

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


def _grid_canonical_item_chain(item_id: str, chain: str = "") -> tuple[str, str]:
    """Return canonical (item_id, chain) for grid DB operations.

    New visible orders are stored as:
      chain   = POL / BNB / ETH / ...
      item_id = CHAIN:SYMBOL

    Legacy variants (e.g. item_id='POL') are still read by fallback helpers,
    but writes should always use this canonical form.
    """
    raw_item = str(item_id or "").strip()
    raw_chain = str(chain or "").strip()

    chain_eff = _grid_chain_key(raw_item, raw_chain) or _normalize_chain_key(raw_chain) or "POL"
    if not chain_eff:
        chain_eff = "POL"

    if raw_item and ":" in raw_item:
        sym = raw_item.split(":", 1)[1].strip().upper()
    else:
        sym = raw_item.strip().upper()

    if not sym:
        sym = chain_eff

    return f"{chain_eff}:{sym}", chain_eff


def _grid_db_orders_payload(conn, wallet_address: str, item_id: str, chain: str = "") -> dict:
    """Authoritative visible grid payload.

    This is the single response shape for UI-visible orders.
    It intentionally reads orders only from SQLite grid_orders.
    Runtime sessions may still exist, but they do not decide visible order state.
    """
    item_eff, chain_eff = _grid_canonical_item_chain(item_id, chain)
    orders = _grid_db_list_orders_any_variant(conn, wallet_address, item_eff, chain=chain_eff)
    for o in orders or []:
        if isinstance(o, dict):
            o["item"] = o.get("item") or item_eff
            o["item_id"] = o.get("item_id") or item_eff
            o["chain"] = o.get("chain") or chain_eff

    # Fast visible-order endpoint: do NOT call on-chain vault/RPC here.
    # /api/grid/orders is used after every Add/Stop/Delete to reconcile visible orders;
    # if this endpoint performs vault reads, the Execution Preview can lag 15-30s.
    # Use only SQLite-derived values here. The frontend's dedicated vaultState reader
    # remains responsible for on-chain vault balances in the background.
    try:
        vault_total = float(_grid_db_vault_total(conn, wallet_address, item_eff, chain=chain_eff) or 0.0)
    except Exception:
        vault_total = 0.0
    try:
        reserved = float(_grid_db_reserved_any_variant(conn, wallet_address, item_eff, chain=chain_eff) or 0.0)
    except Exception:
        reserved = 0.0
    free = max(0.0, float(vault_total) - float(reserved))
    return {
        "item": item_eff,
        "active_item": item_eff,
        "active_chain": chain_eff,
        "orders": orders,
        "vault_total": vault_total,
        "reserved": reserved,
        "free": free,
    }


def _grid_refresh_session_orders_from_db(item_id: str, wallet_address: str, chain: str = "") -> Optional[dict]:
    """Refresh runtime session orders from SQLite before executor/tick usage.

    This keeps SQLite as the authoritative order source while still allowing
    GRID_SESSIONS to run executor/simulation state.
    """
    item_eff, chain_eff = _grid_canonical_item_chain(item_id, chain)
    sess = _get_owned_session(item_eff, wallet_address)
    if not isinstance(sess, dict):
        return None
    conn = _db()
    try:
        db_orders = _grid_db_list_orders_any_variant(conn, wallet_address, item_eff, chain=chain_eff)
        sess["orders"] = db_orders if isinstance(db_orders, list) else []
        sess["item"] = item_eff
        sess["item_id"] = item_eff
        sess["wallet_address"] = _norm_addr(wallet_address)
        sess["orders_source"] = "sqlite"
        _grid_sessions_set(item_eff, _trim_grid_session(sess))
        return sess
    finally:
        conn.close()


def _grid_sessions_set(item_id: str, sess: dict) -> None:
    for it in _grid_item_variants(item_id):
        GRID_SESSIONS[it] = sess

def _grid_session_variants(item_id: str) -> list[tuple[str, dict]]:
    """Return unique (key, session) pairs for all compatible item-id variants."""
    seen = set()
    out = []
    for it in _grid_item_variants(item_id):
        sess = GRID_SESSIONS.get(it)
        if not isinstance(sess, dict):
            continue
        sid = id(sess)
        if sid in seen:
            continue
        seen.add(sid)
        out.append((it, sess))
    return out

def _grid_remove_order_from_sessions(item_id: str, oid: str) -> None:
    """Remove an order id from every compatible in-memory grid session."""
    soid = str(oid)
    for it, sess in _grid_session_variants(item_id):
        orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
        sess["orders"] = [
            o for o in orders
            if not (isinstance(o, dict) and str(o.get("id") or o.get("order_id") or o.get("orderId")) == soid)
        ]
        _grid_sessions_set(it, sess)

def _grid_mark_order_in_sessions(item_id: str, oid: str, status: str, cancelled: bool = False) -> None:
    """Update order status across every compatible in-memory grid session."""
    soid = str(oid)
    nowi = int(time.time())
    for it, sess in _grid_session_variants(item_id):
        orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
        changed = False
        for o in orders:
            if not isinstance(o, dict):
                continue
            ooid = str(o.get("id") or o.get("order_id") or o.get("orderId") or "")
            if ooid != soid:
                continue
            o["status"] = str(status)
            if cancelled:
                o["cancelled_ts"] = nowi
            else:
                o.pop("cancelled_ts", None)
            changed = True
        if changed:
            sess["orders"] = orders
            _grid_sessions_set(it, sess)

def _grid_add_order_to_sessions(item_id: str, order: dict) -> None:
    """Deprecated visible mirror.

    SQLite grid_orders is now the single source of truth for visible orders.
    Runtime sessions are refreshed from SQLite before execution/tick.
    Set NEXUS_GRID_LEGACY_SESSION_ORDER_MIRROR=1 only for emergency rollback.
    """
    if str(os.getenv("NEXUS_GRID_LEGACY_SESSION_ORDER_MIRROR", "0")).strip().lower() not in ("1", "true", "yes", "on"):
        for _it, sess in _grid_session_variants(item_id):
            if isinstance(sess, dict):
                sess["orders_source"] = "sqlite"
                sess["orders_db_dirty"] = True
                _grid_sessions_set(_it, sess)
        return

    oid = str((order or {}).get("id") or (order or {}).get("order_id") or (order or {}).get("orderId") or "")
    if not oid:
        return
    for it, sess in _grid_session_variants(item_id):
        orders = sess.get("orders") if isinstance(sess.get("orders"), list) else []
        exists = any(isinstance(o, dict) and str(o.get("id") or o.get("order_id") or o.get("orderId") or "") == oid for o in orders)
        if not exists:
            orders.insert(0, dict(order))
            sess["orders"] = orders
            sess["orders_source"] = "legacy_session_mirror"
            _trim_grid_session(sess)
            _grid_sessions_set(it, sess)

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



def _grid_db_list_orders_any_variant(conn, wallet_address: str, item_id: str, chain: str = "") -> list[dict]:
    """Load orders for item_id and compatible variants (e.g. POL and POL:POL)."""
    seen = set()
    out = []
    chain_eff = _grid_chain_key(item_id, chain) or chain or ""
    for it in _grid_item_variants(item_id):
        try:
            item_eff = str(it).strip()
            if item_eff and ":" not in item_eff:
                ck = _grid_chain_key(item_eff, chain_eff) or chain_eff or "POL"
                item_eff = f"{ck}:{item_eff.upper()}"
            rows = _grid_db_list_orders(conn, wallet_address, item_id=item_eff, chain=chain_eff)
            if not rows:
                rows = _grid_db_list_orders(conn, wallet_address, item_id=item_eff, chain="")
            if not rows and ":" in item_eff:
                base = item_eff.split(":", 1)[1].strip().upper()
                rows = _grid_db_list_orders(conn, wallet_address, item_id=base, chain=chain_eff)
                if not rows:
                    rows = _grid_db_list_orders(conn, wallet_address, item_id=base, chain="")
            for o in rows or []:
                oid = str(o.get("id") or o.get("order_id") or "")
                if oid and oid in seen:
                    continue
                if oid:
                    seen.add(oid)
                out.append(o)
        except Exception:
            continue
    return out

def _grid_db_reserved_any_variant(conn, wallet_address: str, item_id: str, chain: str = "") -> float:
    """Return reserved qty for OPEN orders across compatible item-id variants without double-counting."""
    orders = _grid_db_list_orders_any_variant(conn, wallet_address, item_id, chain=chain)
    total = 0.0
    for o in orders or []:
        try:
            if str(o.get("status") or "").upper() != "OPEN":
                continue
            total += float(o.get("qty") or o.get("amount") or 0.0)
        except Exception:
            continue
    return float(total)

def _grid_db_cancel_open_orders_any_variant(conn, wallet_address: str, item_id: str, chain: str = "") -> int:
    """Cancel all OPEN visible SQLite orders for an item across compatible variants.

    Used by /api/grid/stop so stopping a grid cannot leave DB-visible orders
    active while the runtime session is stopped.
    """
    wa = _norm_addr(wallet_address)
    item_eff, chain_eff = _grid_canonical_item_chain(item_id, chain)
    nowi = int(time.time())
    total = 0
    cur = conn.cursor()
    seen_items = set()
    for it in _grid_item_variants(item_eff):
        cand = str(it or "").strip()
        if cand and ":" not in cand:
            ck = _grid_chain_key(cand, chain_eff) or chain_eff or "POL"
            cand = f"{ck}:{cand.upper()}"
        if not cand or cand in seen_items:
            continue
        seen_items.add(cand)
        cur.execute(
            "UPDATE grid_orders SET status='CANCELLED', cancelled_ts=COALESCE(cancelled_ts, ?), updated_ts=? "
            "WHERE wallet_address=? AND item_id=? AND (?='' OR chain=?) AND status='OPEN'",
            (nowi, nowi, wa, cand, chain_eff, chain_eff),
        )
        total += max(0, int(cur.rowcount or 0))
    return total


def _hydrate_grid_session_from_db(item_id: str, wa: str) -> Optional[dict]:
    """Rebuild a minimal grid session from DB orders/UI state when RAM session is missing or empty."""
    conn = _db()
    try:
        chain_eff = _grid_chain_key(item_id) or "POL"
        item_eff = str(item_id or "").strip()
        if item_eff and ":" not in item_eff:
            item_eff = f"{chain_eff}:{item_eff.upper()}"

        orders = _grid_db_list_orders_any_variant(conn, wa, item_eff, chain=chain_eff)
        existing = _get_owned_session(item_eff, wa)
        sess = existing if isinstance(existing, dict) else {}

        if not orders and not sess:
            return None

        if not isinstance(sess, dict):
            sess = {}

        # Preserve the most advanced in-memory tick/price we can find across item variants.
        best_tick = int(sess.get("ticks") or 0) if isinstance(sess, dict) else 0
        best_price = sess.get("price") if isinstance(sess, dict) else None
        for it in _grid_item_variants(item_eff):
            s2 = GRID_SESSIONS.get(it)
            if not isinstance(s2, dict):
                continue
            t2 = int(s2.get("ticks") or 0)
            if t2 >= best_tick:
                best_tick = t2
                if s2.get("price") is not None:
                    best_price = s2.get("price")

        sess["wallet_address"] = _norm_addr(wa)
        sess["item"] = item_eff
        sess["item_id"] = item_eff
        sess.setdefault("running", True)
        sess.setdefault("stopped", False)
        sess["ticks"] = max(int(sess.get("ticks") or 0), int(best_tick or 0))
        if best_price is not None and sess.get("price") is None:
            sess["price"] = best_price
        sess.setdefault("fills", [])
        sess.setdefault("filled_now", 0)
        sess.setdefault("order_mode", "MANUAL")
        sess.setdefault("initial_capital_usd", float(sess.get("initial_capital_usd") or 0.0) or float(_grid_best_vault_total(conn, wa, item_eff, chain=chain_eff) or 0.0) or 30.0)

        if orders:
            sess["orders"] = orders

        # Try to attach a live market snapshot so execute can use real prices.
        try:
            snap = SNAPSHOTS.get(item_eff)
            if not isinstance(snap, dict):
                snap = None
            if not snap:
                sym = _symbol_from_item(item_eff)
                cg_id = _STATIC_CG_IDS.get(sym) or COINGECKO_KNOWN.get(sym)
                if cg_id:
                    live = _cg_market_snapshot(str(cg_id))
                    p = float((live or {}).get("price") or 0.0)
                    if p <= 0 and isinstance(live, dict):
                        p = float(live.get("current_price") or 0.0)
                    if p > 0:
                        snap_data = {
                            "ts": now_ts(),
                            "data": {"id": cg_id, "mode": "market", "symbol": sym, "price": p}
                        }
                        for it in _grid_item_variants(item_eff):
                            SNAPSHOTS[it] = snap_data
                        if not sess.get("price"):
                            sess["price"] = p
        except Exception:
            pass

        _ensure_pnl(sess)
        _grid_sessions_set(item_eff, _trim_grid_session(sess))
        try:
            _persist_grid_state()
        except Exception:
            pass
        return sess
    finally:
        conn.close()

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
FEE_FREE_THRESHOLD_USD = float(os.getenv("NEXUS_FEE_FREE_THRESHOLD_USD", os.getenv("NEXUS_MIN_PROFIT_FEE_USD", "100")))

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

    thr = float(FEE_FREE_THRESHOLD_USD or 100.0)
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
GRID_AUTORUN: Dict[str, Dict[str, Any]] = {}  # item_id -> autorun worker state

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
# Grid runtime helpers
# -------------------------
def _grid_runtime_payload(sess: dict | None = None) -> dict:
    sess = sess if isinstance(sess, dict) else {}
    payload = {
        "mode": "live" if GRID_LIVE_MODE else "legacy-sim",
        "uses_real_market_data": True,
        "engine": "legacy-grid" if GRID_ENABLE_LEGACY_SIM else "executor",
        "simulation": False if GRID_LIVE_MODE else True,
    }
    if not GRID_LIVE_MODE:
        payload.update({
            "initial_capital_usd": float(sess.get("initial_capital_usd") or INITIAL_CAPITAL_USD),
            "equity_usd": float(sess.get("equity_usd") or 0.0),
            "pnl_pct": float(sess.get("pnl_pct") or 0.0),
        })
    return payload

def _grid_budget_baseline(wallet_address: str, item_id: str, chain: str = "") -> float:
    """Return a safe live-first budget baseline for a grid item.

    Priority:
      1) wallet-bound vault total
      2) on-chain native wallet balance
      3) configured fallback INITIAL_CAPITAL_USD
    """
    try:
        conn = _db()
        try:
            vault_total = float(_grid_effective_vault_total(conn, wallet_address, item_id, chain=chain) or 0.0)
        finally:
            conn.close()
    except Exception:
        vault_total = 0.0

    if vault_total > 0:
        return vault_total

    native_total = _native_balance_for_wallet(wallet_address, chain=chain, item_id=item_id)
    if native_total > 0:
        return float(native_total)

    return float(INITIAL_CAPITAL_USD)

def _grid_sync_session_budget_fields(sess: dict, wallet_address: str, item_id: str, chain: str = "") -> dict:
    """Keep legacy session budget fields aligned with live wallet/vault totals.

    This preserves existing frontend/backend behavior during migration while preventing
    the old demo capital defaults from leaking into live responses and budget endpoints.
    """
    if not isinstance(sess, dict):
        return {}

    budget_total = _grid_budget_baseline(wallet_address, item_id, chain=chain)

    try:
        conn = _db()
        try:
            reserved = float(_grid_db_reserved(conn, wallet_address, item_id, chain=chain) or 0.0)
            if not reserved and chain:
                reserved = float(_grid_db_reserved(conn, wallet_address, item_id, chain="") or 0.0)
        finally:
            conn.close()
    except Exception:
        reserved = 0.0

    reserved = max(0.0, reserved)
    available = max(0.0, float(budget_total) - reserved)

    sess["initial_capital_usd"] = float(budget_total)
    sess["wallet_total_usd"] = float(budget_total)
    sess["wallet_locked_usd"] = float(reserved)
    sess["wallet_available_usd"] = float(available)
    return sess

# -------------------------
# Grid PnL helpers (legacy engine compatibility)
# -------------------------

def _ensure_pnl(sess: dict) -> dict:
    # Position-based PnL tracking (legacy grid engine compatibility)
    if not isinstance(sess, dict):
        return {}
    # budget basis (live wallet/vault aligned when available)
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


@app.route("/api/access/subscribe/config", methods=["GET"])
def api_access_subscribe_config():
    """Public subscription payment config. No secrets are returned here.
    The treasury address is public because users must see/sign the payment recipient.
    """
    def _stable_decimals_safe(cid, sym):
        try:
            return _stable_decimals(cid, sym)
        except Exception:
            return 18 if int(cid) == 56 else 6

    tokens = {}
    for cid, chain in ((1, "ETH"), (56, "BNB"), (137, "POL")):
        tokens[chain] = {
            "chain_id": cid,
            "USDC": {
                "address": (_USDC_BY_CHAIN.get(cid) or "").strip(),
                "decimals": _stable_decimals_safe(cid, "USDC"),
            },
            "USDT": {
                "address": (_USDT_BY_CHAIN.get(cid) or "").strip(),
                "decimals": _stable_decimals_safe(cid, "USDT"),
            },
        }
    return jsonify({
        "status": "ok",
        "plan": "pro",
        "price_usd": float(PRICE_PRO_USD),
        "treasury": TREASURY_ADDRESS,
        "tokens": tokens,
        "subscription_seconds": int(os.getenv("NEXUS_SUBSCRIPTION_SECONDS", str(60 * 60 * 24 * 30))),
        "ts": now_ts(),
    })

@app.route("/api/config", methods=["GET"])
def api_public_config():
    return jsonify({
        "status": "ok",
        "treasury": TREASURY_ADDRESS,
        "price_usd": float(PRICE_PRO_USD),
        "ts": now_ts(),
    })

@app.route("/api/access/subscribe/verify", methods=["POST"])
def api_access_subscribe_verify():
    """Verify an onchain USDC/USDT payment to Treasury and activate PRO subscription access.

    Body: { chain_id, tx_hash, plan?: "pro" }

    Notes:
      - expects an ERC20 Transfer from the caller wallet -> TREASURY_ADDRESS
      - idempotent per tx_hash (stored in access_payments)
    """
    body = request.get_json(silent=True) or {}

    # Normal path: authenticated user.
    # Recovery path: if auth/session is missing after a wallet payment, use the
    # submitted wallet only as payer candidate. Access is activated only after
    # on-chain verification of Transfer(payer -> treasury).
    wa = _require_auth()
    if not wa:
        wa_candidate = (
            body.get("wallet")
            or body.get("wallet_address")
            or body.get("walletAddress")
            or request.headers.get("X-Wallet-Address")
            or request.args.get("wallet")
            or request.args.get("wallet_address")
        )
        if isinstance(wa_candidate, str) and _looks_like_evm_addr(wa_candidate):
            wa = _norm_addr(wa_candidate)
    if not wa:
        return err("unauthorized", 401)

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

    # Keep auto-renew schedule aligned with the paid subscription.
    # Does NOT enable auto-renew automatically; user must opt in separately.
    cur.execute(
        "UPDATE access_state SET next_billing_ts=?, preferred_token=COALESCE(NULLIF(preferred_token,''), ?), "
        "preferred_chain=COALESCE(NULLIF(preferred_chain,''), ?), last_auto_renew_status=? WHERE wallet_address=?",
        (
            int(expires_ts),
            str(proof.get("token") or "USDT").upper(),
            _chain_key_from_id(int(chain_id)),
            "subscription_verified",
            wa,
        ),
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
            "market_cap": c.get("market_cap"),
            "marketCap": c.get("market_cap"),
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
                    "market_cap": row.get("market_cap"),
                    "marketCap": row.get("market_cap"),
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


# --- CoinGecko Exchange Intelligence (Strategist context) ---
_CG_EXCHANGE_INTEL_CACHE = {}
_CG_EXCHANGE_INTEL_TTL_SEC = int(os.getenv("NEXUS_CG_EXCHANGE_INTEL_TTL_SEC", "900"))
_CG_EXCHANGE_INTEL_MAX_SYMBOLS = int(os.getenv("NEXUS_CG_EXCHANGE_INTEL_MAX_SYMBOLS", "6"))
_CG_EXCHANGE_INTEL_MAX_TICKERS = int(os.getenv("NEXUS_CG_EXCHANGE_INTEL_MAX_TICKERS", "60"))


def _cg_exchange_intelligence_for_coin(coin_id: str, symbol: str = "") -> dict:
    """Compact /coins/{id}/tickers intelligence for Strategist.

    Detects exchange price premium/discount, volume concentration, spread quality,
    stale/anomaly flags and trusted ticker quality from CoinGecko.
    """
    cid = str(coin_id or "").strip()
    sym = str(symbol or "").strip().upper()
    if not cid:
        return {"status": "missing_coin_id", "symbol": sym, "coin_id": cid}
    now_f = time.time()
    hit = _CG_EXCHANGE_INTEL_CACHE.get(cid)
    if hit and isinstance(hit, tuple) and (now_f - float(hit[0] or 0)) < _CG_EXCHANGE_INTEL_TTL_SEC:
        cached = dict(hit[1] or {})
        cached["cached"] = True
        return cached
    url = f"{COINGECKO_BASE}/coins/{requests.utils.quote(cid)}/tickers"
    try:
        r = requests.get(url, params={"include_exchange_logo":"false","depth":"false","order":"volume_desc","page":"1"}, headers=_cg_headers(), timeout=8)
        if r.status_code == 429:
            if hit:
                cached = dict(hit[1] or {})
                cached["cached"] = True
                cached["stale_fallback"] = True
                return cached
            return {"status":"rate_limited","symbol":sym,"coin_id":cid,"source":"coingecko_tickers"}
        r.raise_for_status()
        data = r.json() or {}
        raw = data.get("tickers") or []
        if not isinstance(raw, list): raw = []
        valid=[]; stale_count=0; anomaly_count=0
        for t in raw[:max(1,min(_CG_EXCHANGE_INTEL_MAX_TICKERS,100))]:
            if not isinstance(t, dict): continue
            market = t.get("market") if isinstance(t.get("market"), dict) else {}
            cl = t.get("converted_last") if isinstance(t.get("converted_last"), dict) else {}
            cv = t.get("converted_volume") if isinstance(t.get("converted_volume"), dict) else {}
            px = _safe_float(cl.get("usd"), 0.0)
            vol = _safe_float(cv.get("usd"), 0.0)
            spread_raw = t.get("bid_ask_spread_percentage")
            spread = None
            if spread_raw is not None:
                try:
                    spread = float(spread_raw)
                    if not math.isfinite(spread):
                        spread = None
                except Exception:
                    spread = None
            is_stale = bool(t.get("is_stale")); is_anomaly = bool(t.get("is_anomaly"))
            stale_count += 1 if is_stale else 0; anomaly_count += 1 if is_anomaly else 0
            if px <= 0 or vol <= 0 or is_stale or is_anomaly: continue
            valid.append({
                "exchange": str(market.get("name") or "Unknown")[:48],
                "identifier": str(market.get("identifier") or "")[:48],
                "pair": (str(t.get("base") or "")[:20] + "/" + str(t.get("target") or "")[:20]).strip("/"),
                "price_usd": round(px, 10),
                "volume_usd": round(vol, 2),
                "spread_pct": round(spread, 4) if isinstance(spread,(int,float)) and math.isfinite(spread) else None,
                "trust_score": str(t.get("trust_score") or ""),
            })
        valid.sort(key=lambda x: _safe_float(x.get("volume_usd"),0.0), reverse=True)
        top=valid[:10]
        total_vol=sum(_safe_float(x.get("volume_usd"),0.0) for x in valid)
        cheapest=highest=None; premium=None
        if valid:
            cheapest=min(valid, key=lambda x:_safe_float(x.get("price_usd"),0.0))
            highest=max(valid, key=lambda x:_safe_float(x.get("price_usd"),0.0))
            min_px=_safe_float(cheapest.get("price_usd"),0.0); max_px=_safe_float(highest.get("price_usd"),0.0)
            if min_px>0: premium=((max_px-min_px)/min_px)*100.0
        primary=top[0] if top else None
        spreads=[]
        for x in top:
            if x.get("spread_pct") is None:
                continue
            try:
                sp=float(x.get("spread_pct"))
                if math.isfinite(sp):
                    spreads.append(sp)
            except Exception:
                pass
        avg_spread=sum(spreads)/len(spreads) if spreads else None
        top_share=(_safe_float(primary.get("volume_usd"),0.0)/total_vol*100.0) if primary and total_vol>0 else None
        if premium is None:
            interp="No reliable multi-exchange price context available."
        elif premium>=2.0:
            interp="Visible exchange price dispersion; liquidity and spread confirmation required."
        elif premium>=0.7:
            interp="Small exchange premium/discount exists; useful only if volume and spread confirm it."
        else:
            interp="Exchange prices are broadly aligned; no major cross-exchange premium."
        out={"status":"ok","source":"coingecko_tickers","symbol":sym,"coin_id":cid,
             "valid_ticker_count":len(valid),"stale_ticker_count":stale_count,"anomaly_ticker_count":anomaly_count,
             "total_volume_usd_sample":round(total_vol,2),"top_exchange":primary,"cheapest_exchange":cheapest,
             "highest_exchange":highest,"exchange_premium_pct":round(float(premium),3) if premium is not None and math.isfinite(premium) else None,
             "avg_top_spread_pct":round(float(avg_spread),4) if avg_spread is not None and math.isfinite(avg_spread) else None,
             "top_exchange_volume_share_pct":round(float(top_share),2) if top_share is not None and math.isfinite(top_share) else None,
             "top_tickers":top[:5],"interpretation":interp,"cached":False,"ts":now_ts()}
        _CG_EXCHANGE_INTEL_CACHE[cid]=(now_f,dict(out))
        return out
    except Exception as e:
        if hit:
            cached=dict(hit[1] or {}); cached["cached"]=True; cached["stale_fallback"]=True; return cached
        return {"status":"error","symbol":sym,"coin_id":cid,"error":str(e),"source":"coingecko_tickers","ts":now_ts()}


def _build_exchange_intelligence_context(id_map: dict, symbols: list[str]) -> dict:
    out={}
    clean=[str(s or "").strip().upper() for s in (symbols or []) if str(s or "").strip()]
    for sym in clean[:max(1,min(_CG_EXCHANGE_INTEL_MAX_SYMBOLS,10))]:
        cid=id_map.get(sym) if isinstance(id_map,dict) else None
        if cid: out[sym]=_cg_exchange_intelligence_for_coin(cid,sym)
    return out

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

def _calc_rsi_from_prices(prices, period: int = 14):
    """Classic Wilder RSI on a close-price series (oldest -> newest)."""
    try:
        period = int(period or 14)
    except Exception:
        period = 14
    if period < 2:
        period = 14
    vals = []
    for p in (prices or []):
        try:
            fp = float(p)
            if math.isfinite(fp) and fp > 0:
                vals.append(fp)
        except Exception:
            continue
    if len(vals) < period + 1:
        return None

    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = vals[i] - vals[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += abs(diff)

    avg_gain = gains / period
    avg_loss = losses / period

    for i in range(period + 1, len(vals)):
        diff = vals[i] - vals[i - 1]
        gain = diff if diff > 0 else 0.0
        loss = abs(diff) if diff < 0 else 0.0
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(float(rsi), 2)


def _pct_change_from_series(prices, lookback: int):
    vals = []
    for p in (prices or []):
        try:
            fp = float(p)
            if math.isfinite(fp) and fp > 0:
                vals.append(fp)
        except Exception:
            continue
    if len(vals) <= lookback:
        return None
    old = vals[-(lookback + 1)]
    new = vals[-1]
    if old <= 0:
        return None
    return round(((new - old) / old) * 100.0, 4)


def _volatility_from_series(prices, lookback: int = 14):
    vals = []
    for p in (prices or []):
        try:
            fp = float(p)
            if math.isfinite(fp) and fp > 0:
                vals.append(fp)
        except Exception:
            continue
    if len(vals) < max(lookback + 1, 3):
        return None
    tail = vals[-(lookback + 1):]
    rets = []
    for i in range(1, len(tail)):
        prev = tail[i - 1]
        cur = tail[i]
        if prev > 0:
            rets.append((cur - prev) / prev)
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
    std = math.sqrt(max(0.0, var))
    return round(std * 100.0, 4)


def _rsi_state(rsi: float | None) -> str:
    if rsi is None:
        return "unknown"
    if rsi < 25:
        return "deep_oversold"
    if rsi < 35:
        return "oversold"
    if rsi < 45:
        return "weak"
    if rsi <= 55:
        return "neutral"
    if rsi <= 70:
        return "strong"
    if rsi <= 80:
        return "overbought"
    return "extreme_overbought"


def _volatility_state(vol_pct: float | None) -> str:
    if vol_pct is None:
        return "unknown"
    if vol_pct < 2.0:
        return "low"
    if vol_pct < 5.5:
        return "healthy"
    if vol_pct < 9.0:
        return "high"
    return "chaotic"


def _trend_state(ch7, ch30, ch90) -> str:
    try:
        c7 = float(ch7) if ch7 is not None else None
    except Exception:
        c7 = None
    try:
        c30 = float(ch30) if ch30 is not None else None
    except Exception:
        c30 = None
    try:
        c90 = float(ch90) if ch90 is not None else None
    except Exception:
        c90 = None

    if c30 is None and c90 is None:
        return "unknown"
    if (c30 is not None and c30 > 0) and (c90 is None or c90 > 0):
        if c7 is not None and c7 < 0:
            return "uptrend_pullback"
        return "uptrend"
    if (c30 is not None and c30 < 0) and (c90 is not None and c90 < 0):
        if c7 is not None and c7 > 0:
            return "downtrend_bounce"
        return "downtrend"
    return "mixed"


def _grid_fit_state(rsi, vol_state: str, trend_state: str) -> str:
    rs = _rsi_state(rsi)
    if vol_state == "chaotic":
        return "avoid"
    if trend_state in ("uptrend_pullback", "mixed") and vol_state in ("healthy", "high") and rs in ("deep_oversold", "oversold", "weak"):
        return "good"
    if trend_state == "uptrend" and vol_state == "healthy" and rs in ("neutral", "weak"):
        return "good"
    if trend_state in ("downtrend", "downtrend_bounce"):
        return "cautious"
    if vol_state == "low":
        return "weak"
    return "cautious"


def _indicator_summary(symbol: str, rsi, trend_state: str, vol_state: str, grid_fit: str) -> str:
    sym = str(symbol or "").upper() or "This coin"
    rs = _rsi_state(rsi)
    rsi_txt = {
        "deep_oversold": "looks deeply oversold",
        "oversold": "looks oversold",
        "weak": "shows short-term weakness",
        "neutral": "is neutral on momentum",
        "strong": "still has positive momentum",
        "overbought": "looks hot after a strong run",
        "extreme_overbought": "looks extremely stretched",
        "unknown": "has incomplete RSI data",
    }.get(rs, "has mixed momentum")
    trend_txt = {
        "uptrend": "while the broader structure remains constructive",
        "uptrend_pullback": "inside a stronger medium-term uptrend",
        "downtrend": "inside a broader downtrend",
        "downtrend_bounce": "during a bounce inside a weaker structure",
        "mixed": "with a mixed higher-timeframe structure",
        "unknown": "with limited trend context",
    }.get(trend_state, "with mixed structure")
    grid_txt = {
        "good": "Grid conditions look usable if liquidity is also acceptable.",
        "cautious": "Grid may be possible, but only with caution and tighter risk control.",
        "weak": "Grid conditions look weak because movement may be too flat.",
        "avoid": "Grid conditions look unfavorable right now.",
    }.get(grid_fit, "Grid fit is unclear.")
    vol_txt = {
        "low": "Volatility is low.",
        "healthy": "Volatility is in a healthy range.",
        "high": "Volatility is elevated.",
        "chaotic": "Volatility is chaotic.",
        "unknown": "Volatility data is limited.",
    }.get(vol_state, "Volatility is mixed.")
    return f"{sym} {rsi_txt} {trend_txt}. {vol_txt} {grid_txt}"


def _resolve_cg_id_for_indicator(symbol: str = "", coin_id: str = "") -> str | None:
    cid = str(coin_id or "").strip()
    if cid:
        return cid
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    return _STATIC_CG_IDS.get(sym) or COINGECKO_KNOWN.get(sym) or _cg_resolve_symbol(sym)


@app.route("/api/coingecko/market_chart/<coin_id>", methods=["GET"])
def coingecko_market_chart_proxy(coin_id: str):
    vs_currency = str(request.args.get("vs_currency") or "usd").strip().lower() or "usd"
    days = str(request.args.get("days") or "30").strip() or "30"
    interval = str(request.args.get("interval") or "daily").strip().lower() or "daily"
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    try:
        r = requests.get(
            url,
            params={"vs_currency": vs_currency, "days": days, "interval": interval},
            headers=_cg_headers(),
            timeout=15,
        )
        r.raise_for_status()
        return jsonify(r.json() or {})
    except Exception as e:
        return jsonify({"error": "coingecko_market_chart_failed", "detail": str(e)}), 502


@app.route("/api/ai/market-indicators", methods=["GET", "POST"])
def api_ai_market_indicators():
    body = request.get_json(silent=True) or {}
    symbol = str(body.get("symbol") or request.args.get("symbol") or "").strip().upper()
    coin_id = str(body.get("coin_id") or body.get("coinId") or request.args.get("coin_id") or request.args.get("coinId") or "").strip()
    days_raw = body.get("days") if body.get("days") is not None else request.args.get("days")
    period_raw = body.get("period") if body.get("period") is not None else request.args.get("period")
    try:
        days = max(30, min(365, int(days_raw or 120)))
    except Exception:
        days = 120
    try:
        period = max(7, min(21, int(period_raw or 14)))
    except Exception:
        period = 14

    resolved_id = _resolve_cg_id_for_indicator(symbol=symbol, coin_id=coin_id)
    if not resolved_id:
        return jsonify({"status": "error", "error": "coin_not_resolved", "symbol": symbol, "coin_id": coin_id, "ts": now_ts()}), 404

    try:
        series = _cg_price_series(str(resolved_id), days=days)
    except Exception as e:
        return jsonify({"status": "error", "error": "market_chart_fetch_failed", "detail": str(e), "coin_id": resolved_id, "ts": now_ts()}), 502

    if not series or len(series) < max(period + 1, 20):
        return jsonify({"status": "error", "error": "insufficient_price_history", "coin_id": resolved_id, "symbol": symbol, "points": len(series or []), "ts": now_ts()}), 400

    latest_price = round(float(series[-1]), 8)
    rsi = _calc_rsi_from_prices(series, period=period)
    ch7 = _pct_change_from_series(series, 7)
    ch30 = _pct_change_from_series(series, 30)
    ch90 = _pct_change_from_series(series, 90)
    vol14 = _volatility_from_series(series, lookback=14)
    trend = _trend_state(ch7, ch30, ch90)
    vol_state = _volatility_state(vol14)
    grid_fit = _grid_fit_state(rsi, vol_state, trend)
    summary = _indicator_summary(symbol or resolved_id, rsi, trend, vol_state, grid_fit)

    return jsonify({
        "status": "ok",
        "coin_id": resolved_id,
        "symbol": symbol or str(resolved_id).upper(),
        "days": int(days),
        "period": int(period),
        "latest_price": latest_price,
        "points": len(series),
        "rsi": rsi,
        "rsi_state": _rsi_state(rsi),
        "change_7d_pct": ch7,
        "change_30d_pct": ch30,
        "change_90d_pct": ch90,
        "volatility_14d_pct": vol14,
        "volatility_state": vol_state,
        "trend_state": trend,
        "grid_fit": grid_fit,
        "summary": summary,
        "ts": now_ts(),
    })


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

def _watch_items_normalize(items):
    arr = items if isinstance(items, list) else []
    out = []
    seen = set()
    for it in arr:
        if isinstance(it, str):
            sym = it.strip().upper()
            if not sym:
                continue
            row = {"symbol": sym, "mode": "market"}
        elif isinstance(it, dict):
            mode = str(it.get("mode") or "market").strip().lower()
            if mode == "dex":
                contract = str(it.get("contract") or it.get("tokenAddress") or "").strip().lower()
                if not contract:
                    continue
                row = {
                    "mode": "dex",
                    "contract": contract,
                    "tokenAddress": contract,
                    "symbol": str(it.get("symbol") or contract[:10]).strip().upper(),
                    "chain": str(it.get("chain") or "pol").strip().lower(),
                    "name": str(it.get("name") or it.get("symbol") or contract[:10]).strip(),
                }
            else:
                sym = str(it.get("symbol") or "").strip().upper()
                cid = str(it.get("coingecko_id") or it.get("id") or "").strip().lower()
                if not sym:
                    continue
                if not cid:
                    try:
                        cid = str(_cg_resolve_symbol(sym) or "").strip().lower()
                    except Exception:
                        cid = ""
                row = {
                    "mode": "market",
                    "symbol": sym,
                    "id": cid,
                    "coingecko_id": cid,
                    "name": str(it.get("name") or sym).strip(),
                }
        else:
            continue
        key = (row.get("mode"), row.get("symbol"), row.get("contract") or row.get("id") or row.get("coingecko_id") or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _db_get_user_watchlist(wallet_address: str) -> tuple[list, int]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return [], 0
    conn = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT items_json, updated_ts FROM user_watchlists WHERE wallet_address=?", (wa,))
        row = cur.fetchone()
        if not row:
            return [], 0
        try:
            items = json.loads(row["items_json"] or "[]")
        except Exception:
            items = []
        return _watch_items_normalize(items), int(row["updated_ts"] or 0)
    finally:
        conn.close()


def _db_set_user_watchlist(wallet_address: str, items: list) -> tuple[list, int]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return [], 0
    clean = _watch_items_normalize(items)
    nowi = int(time.time())
    conn = _db()
    try:
        with DB_WRITE_LOCK:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO user_watchlists(wallet_address, items_json, updated_ts) VALUES(?,?,?) "
                "ON CONFLICT(wallet_address) DO UPDATE SET items_json=excluded.items_json, updated_ts=excluded.updated_ts",
                (wa, json.dumps(clean, separators=(",", ":")), nowi),
            )
            conn.commit()
        return clean, nowi
    finally:
        conn.close()


def _db_get_user_app_state(wallet_address: str) -> tuple[dict, int]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"compare": [], "timeframe": "90D", "indexMode": True, "aiSelected": []}, 0
    conn = _db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT compare_json, timeframe, index_mode, ai_selected_json, updated_ts FROM user_app_state WHERE wallet_address=?", (wa,))
        row = cur.fetchone()
        if not row:
            return {"compare": [], "timeframe": "90D", "indexMode": True, "aiSelected": []}, 0
        try:
            compare = json.loads(row["compare_json"] or "[]")
        except Exception:
            compare = []
        try:
            ai_selected = json.loads(row["ai_selected_json"] or "[]")
        except Exception:
            ai_selected = []
        return {
            "compare": [str(x).strip().upper() for x in (compare if isinstance(compare, list) else []) if str(x).strip()],
            "timeframe": str(row["timeframe"] or "90D"),
            "indexMode": bool(int(row["index_mode"] or 0)),
            "aiSelected": [str(x).strip().upper() for x in (ai_selected if isinstance(ai_selected, list) else []) if str(x).strip()],
        }, int(row["updated_ts"] or 0)
    finally:
        conn.close()


def _db_set_user_app_state(wallet_address: str, payload: dict) -> tuple[dict, int]:
    wa = _norm_addr(wallet_address or "")
    if not wa:
        return {"compare": [], "timeframe": "90D", "indexMode": True, "aiSelected": []}, 0
    base, _ = _db_get_user_app_state(wa)
    compare = payload.get("compare") if isinstance(payload, dict) else None
    timeframe = payload.get("timeframe") if isinstance(payload, dict) else None
    index_mode = payload.get("indexMode") if isinstance(payload, dict) else None
    ai_selected = payload.get("aiSelected") if isinstance(payload, dict) else None
    if isinstance(compare, list):
        base["compare"] = [str(x).strip().upper() for x in compare if str(x).strip()][:20]
    if isinstance(timeframe, str) and timeframe.strip():
        base["timeframe"] = timeframe.strip().upper()
    if index_mode is not None:
        base["indexMode"] = bool(index_mode)
    if isinstance(ai_selected, list):
        base["aiSelected"] = [str(x).strip().upper() for x in ai_selected if str(x).strip()][:6]
    nowi = int(time.time())
    conn = _db()
    try:
        with DB_WRITE_LOCK:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO user_app_state(wallet_address, compare_json, timeframe, index_mode, ai_selected_json, updated_ts) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(wallet_address) DO UPDATE SET compare_json=excluded.compare_json, timeframe=excluded.timeframe, index_mode=excluded.index_mode, ai_selected_json=excluded.ai_selected_json, updated_ts=excluded.updated_ts",
                (wa, json.dumps(base["compare"], separators=(",", ":")), base["timeframe"], 1 if base["indexMode"] else 0, json.dumps(base["aiSelected"], separators=(",", ":")), nowi),
            )
            conn.commit()
        return base, nowi
    finally:
        conn.close()


@app.route("/api/watchlist", methods=["GET", "POST"])
def api_watchlist():
    wa = _require_auth() or _pick_wallet_from_request()
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        items = body.get("items") if isinstance(body, dict) else []
        if not wa:
            return err("wallet required", 401)
        clean, updated_ts = _db_set_user_watchlist(wa, items or [])
        resp = {"status": "ok", "wallet": wa, "items": clean, "updated_ts": updated_ts, "ts": now_ts()}
        out = jsonify(resp)
        out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return out

    if wa:
        items, updated_ts = _db_get_user_watchlist(wa)
        resp = {"status": "ok", "wallet": wa, "items": items, "updated_ts": updated_ts, "ts": now_ts()}
        out = jsonify(resp)
        out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return out

    cache_key = "watchlist"
    try:
        data = get_watchlist()
        resp = {"status": "ok", "items": data}
        out = jsonify(resp)
        out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return out
    except Exception as e:
        cached = _gen_cache_get_any(cache_key)
        if cached is not None:
            out = jsonify(cached)
            out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            return out
        return err(str(e), 500)


@app.route("/api/app-state", methods=["GET", "POST"])
def api_app_state():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return err("wallet required", 401)
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        state, updated_ts = _db_set_user_app_state(wa, body if isinstance(body, dict) else {})
        out = jsonify({"status": "ok", "wallet": wa, "state": state, "updated_ts": updated_ts, "ts": now_ts()})
        out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return out
    state, updated_ts = _db_get_user_app_state(wa)
    out = jsonify({"status": "ok", "wallet": wa, "state": state, "updated_ts": updated_ts, "ts": now_ts()})
    out.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return out


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
        body = {}
        explicit_items = False

        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            explicit_items = isinstance(body, dict) and ("items" in body)
            items = body.get("items") if isinstance(body, dict) else None
            # IMPORTANT:
            # When the client explicitly sends items: [] (for example after deleting the
            # last coin or after logout), we must respect that exact empty list and must
            # NOT fall back to the server/global watchlist.
            if (not explicit_items) or (items is not None and not isinstance(items, list)):
                wa = _require_auth() or _pick_wallet_from_request() or body.get("wallet")
                if wa:
                    items, _ = _db_get_user_watchlist(wa)

        if not isinstance(items, list):
            wa = _require_auth() or _pick_wallet_from_request()
            if wa:
                items, _ = _db_get_user_watchlist(wa)
            else:
                # Fallback to server-side watchlist for unauthenticated GET
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
                cid_raw = it.get("id") or it.get("coingecko_id")
                if cid_raw:
                    row["id"] = str(cid_raw).strip()
                    row["coingecko_id"] = str(cid_raw).strip()

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


        # Fast-path cache (avoid repeated upstream calls while user clicks quickly).
        # A forced request bypasses this cache so a second device can hydrate full price/volume/market-cap
        # data instead of receiving an early partial/pending snapshot.
        wl_cache_key = _key_from_items(ordered)
        force_refresh = str(request.args.get("force") or request.args.get("refresh") or request.args.get("_") or "").strip() not in ("", "0", "false", "False")
        fresh_cached = None if force_refresh else _cache_get_fresh(_WATCH_SNAP_CACHE, wl_cache_key, _WATCH_SNAP_TTL_SEC)
        if fresh_cached is not None:
            return jsonify(fresh_cached)

        # ---- Market batch (CoinGecko) ----
        market_items = [it for it in ordered if it.get("mode") == "market"]
        ids_by_symbol = {}
        coin_ids = []

        for it in market_items:
            sym = it["symbol"]
            cid = (it.get("id") or it.get("coingecko_id") or "").strip()
            if not cid:
                cid = _STATIC_CG_IDS.get(sym) or COINGECKO_KNOWN.get(sym) or _cg_resolve_symbol(sym)
            if cid:
                ids_by_symbol[sym] = cid
                coin_ids.append(cid)

        snaps_by_id = _cg_market_snapshots_batch(coin_ids) if coin_ids else {}

        # Backfill any misses one-by-one and finally via the generic price router.
        for it in market_items:
            sym = it.get("symbol")
            cid = ids_by_symbol.get(sym) or (it.get("id") or it.get("coingecko_id") or "").strip()
            if not cid:
                continue
            if cid not in snaps_by_id:
                try:
                    snap1 = _cg_market_snapshot(cid)
                    if isinstance(snap1, dict) and snap1.get("price") not in (None, "", 0):
                        snap1 = dict(snap1)
                        snap1.setdefault("id", cid)
                        snaps_by_id[cid] = snap1
                except Exception:
                    pass
            if cid not in snaps_by_id:
                try:
                    p = _price_multi(sym)
                    if isinstance(p, dict) and p.get("price") not in (None, "", 0):
                        snaps_by_id[cid] = {
                            "id": cid,
                            "symbol": sym,
                            "price": p.get("price"),
                            "change24": None,
                            "volume24": None,
                            "market_cap": None,
                            "marketCap": None,
                            "liquidity": None,
                            "source": p.get("source") or "market-price",
                        }
                except Exception:
                    pass

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
            cid = ids_by_symbol.get(sym) or it.get("id") or it.get("coingecko_id")
            snap = snaps_by_id.get(cid) if cid else None
            if (not snap or snap.get("price") in (None, "", 0)) and cid:
                try:
                    snap = _cg_market_snapshot(cid)
                except Exception:
                    snap = snap or None
            if (not snap or snap.get("price") in (None, "", 0)):
                try:
                    p = _price_multi(sym)
                    if isinstance(p, dict) and p.get("price") not in (None, "", 0):
                        cid = cid or p.get("id") or cid
                        snap = {
                            "price": p.get("price"),
                            "change24h": None,
                            "volume24h": None,
                            "market_cap": None,
                            "marketCap": None,
                            "source": p.get("source") or "market-price",
                        }
                except Exception:
                    pass
            if snap and snap.get("price") not in (None, "", 0):
                results.append({
                    "symbol": sym,
                    "mode": "market",
                    "id": cid,
                    "coingecko_id": cid,
                    "price": snap.get("price"),
                    "change24h": snap.get("change24") if "change24" in snap else snap.get("change24h"),
                    "volume24h": snap.get("volume24") if "volume24" in snap else snap.get("volume24h"),
                    "market_cap": snap.get("market_cap") if "market_cap" in snap else snap.get("marketCap"),
                    "marketCap": snap.get("market_cap") if "market_cap" in snap else snap.get("marketCap"),
                    "liquidity": None,
                    "source": snap.get("source") or "coingecko",
                })
            else:
                results.append({
                    "symbol": sym,
                    "mode": "market",
                    "id": cid,
                    "coingecko_id": cid,
                    "price": None,
                    "change24h": None,
                    "volume24h": None,
                    "market_cap": None,
                    "marketCap": None,
                    "liquidity": None,
                    "source": "error",
                })

        resp_payload = {"status": "ok", "results": results, "ts": int(time.time())}
        # Do not cache partial market rows that are missing price or market cap. Otherwise mobile can
        # receive a fresh-but-incomplete cache entry after desktop added a coin.
        cacheable = True
        for rr in results:
            if str(rr.get("mode") or "market").lower() == "market":
                try:
                    px_ok = float(rr.get("price") or 0) > 0
                except Exception:
                    px_ok = False
                try:
                    mc_ok = float((rr.get("marketCap") if rr.get("marketCap") is not None else rr.get("market_cap")) or 0) > 0
                except Exception:
                    mc_ok = False
                if not (px_ok and mc_ok):
                    cacheable = False
                    break
        if cacheable:
            _cache_set(_WATCH_SNAP_CACHE, wl_cache_key, resp_payload)
        return jsonify(resp_payload)
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

        if not GRID_ENABLE_LEGACY_SIM:
            return err("grid engine is temporarily disabled until real executor mode is enabled", 503)

        session = _sim_build(cfg)
        # Always bind session to authenticated wallet (addr is optional)
        session["wallet_address"] = _norm_addr(addr) if addr else _norm_addr(wa)
        session["running"] = True
        session["stopped"] = False
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

        # --- PnL init ---
        try:
            _ensure_pnl(session)
            _pnl_mark(session, session.get("price"))
        except Exception:
            pass
        # --- Wallet budget init (Available/Locked) ---
        try:
            _grid_sync_session_budget_fields(session, session.get("wallet_address") or wa, item_id, chain=(chain_key_req or _grid_chain_key(item_id) or ""))
        except Exception:
            pass

        visible_orders = []
        try:
            conn_vis = _db()
            try:
                visible_orders = (_grid_db_orders_payload(conn_vis, wa, item_id, chain_key_req or _grid_chain_key(item_id) or "POL").get("orders") or [])
            finally:
                conn_vis.close()
        except Exception:
            visible_orders = []

        return jsonify({
            "status": "ok",
            "item": item_id,
            "mode": mode,
            "config": cfg,
            "price": session.get("price"),
            "tick": int(session.get("ticks") or 0),
            "price_source": ("frontend" if (body.get("price") or body.get("start_price")) is not None else "snapshot"),
            "runtime": _grid_runtime_payload(session),
            "pnl": {
                "pos": float(session.get("position_qty") or 0.0),
                "avg_cost": float(session.get("avg_cost") or 0.0),
                "realized": float(session.get("realized_pnl") or 0.0),
                "unrealized": float(session.get("unrealized_pnl") or 0.0),
                "total": float(session.get("total_pnl") or 0.0),
            },
            "orders": visible_orders,
            "orders_source": "sqlite",
            "filled_now": int(session.get("filled_now") or 0),
            "fills": session.get("fills", []),
        })
    except Exception as e:
        return err(str(e), 500)


# Frontend compatibility alias (some UIs call /api/grid/cycle/start)
@app.route("/api/grid/cycle/start", methods=["POST", "GET"])
def api_grid_cycle_start():
    return api_grid_start()

@app.route("/api/grid/execute", methods=["POST", "GET"])
def api_grid_execute():
    # Frontend compatibility alias:
    # execute = one tick / one execution pass
    return api_grid_tick()

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

    with _GRID_EXEC_LOCK:
        item_id = str(item_id).strip()
        item_id, chain_eff = _grid_canonical_item_chain(item_id, (body.get("chain") if request.method != "GET" else request.args.get("chain")) or "")
        wa = _require_auth() or _pick_wallet_from_request()
        if not wa:
            return err("unauthorized", 401)

        session = _get_owned_session(item_id, wa)
        if isinstance(session, dict):
            session = _grid_refresh_session_orders_from_db(item_id, wa, chain_eff) or session
        if not isinstance(session, dict) or not isinstance(session.get("orders"), list) or len(session.get("orders") or []) == 0:
            session = _hydrate_grid_session_from_db(item_id, wa)
            if isinstance(session, dict):
                session = _grid_refresh_session_orders_from_db(item_id, wa, chain_eff) or session
        if not session:
            return err("grid not started (press Start first)", 404)
    
        # ✅ Prefer explicit live price from frontend; otherwise use cached snapshot/live market price.
        new_price = None
        if price is not None and price != "":
            try:
                new_price = float(price)
            except Exception:
                new_price = None
    
        if new_price is None:
            for it in _grid_item_variants(item_id):
                snap = SNAPSHOTS.get(it)
                if snap and isinstance(snap.get("data"), dict):
                    try:
                        new_price = float(snap["data"].get("price"))
                        if new_price and new_price > 0:
                            break
                    except Exception:
                        new_price = None
    
        if new_price is None:
            # Try a fresh live lookup using snapshot metadata / static CoinGecko mapping.
            for it in _grid_item_variants(item_id):
                try:
                    p = _get_live_price_for_item(it)
                    if p is not None and float(p) > 0:
                        new_price = float(p)
                        break
                except Exception:
                    pass
            if new_price is None:
                try:
                    sym = _symbol_from_item(item_id)
                    cg_id = _STATIC_CG_IDS.get(sym) or COINGECKO_KNOWN.get(sym)
                    if cg_id:
                        live = _cg_market_snapshot(str(cg_id))
                        p = float((live or {}).get("price") or 0.0)
                        if p <= 0 and isinstance(live, dict):
                            p = float(live.get("current_price") or 0.0)
                        if p > 0:
                            new_price = p
                            canonical_item = str(session.get("item_id") or item_id).strip()
                            snap_data = {
                                "ts": now_ts(),
                                "data": {"id": cg_id, "mode": "market", "symbol": sym, "price": p}
                            }
                            for it in _grid_item_variants(canonical_item):
                                SNAPSHOTS[it] = snap_data
                except Exception:
                    pass
    
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

        point = series[idx]
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            new_price = float(point[1])
        elif isinstance(point, dict):
            new_price = float(point.get("v") if point.get("v") is not None else point.get("price"))
        else:
            new_price = float(point)

        updated = _sim_tick(session, new_price=new_price)
        price_source_label = "history"
    else:
        updated = _sim_tick(session, new_price=new_price)
        if price is not None:
            price_source_label = "frontend"
        elif new_price is not None:
            price_source_label = "market"
        else:
            price_source_label = "none"

    _grid_sessions_set(item_id, _trim_grid_session(updated))
    _persist_grid_state()

    try:
        _grid_sync_session_orders_to_db(session.get("wallet_address") or wa, item_id, updated.get("orders") or [], chain=_grid_chain_key(item_id))
    except Exception:
        pass

    fills = updated.get("fills") if isinstance(updated, dict) else []
    # --- PnL update + wallet budget sync ---
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
        _grid_sync_session_budget_fields(session, session.get("wallet_address") or wa, item_id, chain=_grid_chain_key(item_id))
        _persist_grid_state()
    except Exception:
        pass

    # Return visible orders from SQLite only, even after execution/tick changed runtime state.
    visible_orders = []
    try:
        _conn_vis = _db()
        try:
            visible_orders = (_grid_db_orders_payload(_conn_vis, wa, item_id, chain_eff).get("orders") or [])
        finally:
            _conn_vis.close()
    except Exception:
        visible_orders = []

    return jsonify({
        "status": "ok",
        "item": item_id,
        "active_item": item_id,
        "active_chain": chain_eff,
        "tick": int(updated.get("ticks") or 0) if isinstance(updated, dict) else 0,
        "price": float(updated.get("price") or 0) if isinstance(updated, dict) else 0,
        "price_source": price_source_label,

        "runtime": _grid_runtime_payload(session),

        "pnl": {
            "pos": float(session.get("position_qty") or 0),
            "avg_cost": float(session.get("avg_cost") or 0),
            "realized": float(session.get("realized_pnl") or 0),
            "unrealized": float(session.get("unrealized_pnl") or 0),
            "total": float(session.get("total_pnl") or 0),
        },
        "orders": visible_orders,
        "orders_source": "sqlite",
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
    """Stop a grid session, or stop a single order when an order id is provided.

    Backward compatibility: older frontend code may call /api/grid/stop with
    order_id/id for a single order. In that case delegate to the SQLite-backed
    single-order stop route instead of stopping the whole session.
    """
    body = request.get_json(silent=True) or {}
    if body.get("order_id") or body.get("orderId") or body.get("id") or body.get("oid"):
        fn = globals().get("api_grid_order_stop")
        if callable(fn):
            return fn()

    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    item_id = body.get("item") or body.get("item_id")
    if not item_id:
        return err("missing 'item' in body", 400)

    item_id, chain_eff = _grid_canonical_item_chain(str(item_id).strip(), body.get("chain") or "")
    try:
        conn_ui = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_ui_state_put(conn_ui, wa, active_chain=(chain_eff or _grid_chain_key(item_id) or "POL"), active_item=item_id)
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
    session["running"] = False
    _grid_sessions_set(item_id, _trim_grid_session(session))
    try:
        _grid_sync_session_orders_to_db(session.get("wallet_address") or wa, item_id, session.get("orders") or [], chain=chain_eff or _grid_chain_key(item_id))
    except Exception:
        pass

    visible = {"orders": []}
    try:
        conn_vis = _db()
        try:
            with DB_WRITE_LOCK:
                _grid_db_cancel_open_orders_any_variant(conn_vis, wa, item_id, chain=chain_eff)
                conn_vis.commit()
            visible = _grid_db_orders_payload(conn_vis, wa, item_id, chain_eff)
        finally:
            conn_vis.close()
    except Exception:
        visible = {"orders": []}

    _persist_grid_state()
    return jsonify({"status": "ok", "item": item_id, "active_item": item_id, "active_chain": chain_eff, "stopped": True, **visible, "orders_source": "sqlite", "ts": now})



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
        if not isinstance(session, dict):
            try:
                session = _hydrate_grid_session_from_db(item_id, wa)
            except Exception:
                session = None
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
    """Return visible grid orders from SQLite only.

    SQLite grid_orders is the single source of truth for the UI.
    GRID_SESSIONS can still hold runtime/tick data, but never decides visible order state.
    """
    wa = _require_auth() or _pick_wallet_from_request()

    if not wa:
        item_id = request.args.get("item") or request.args.get("item_id")
        if item_id:
            item_id, chain_eff = _grid_canonical_item_chain(str(item_id).strip(), request.args.get("chain") or "")
            return jsonify({"status": "ok", "item": item_id, "active_item": item_id, "active_chain": chain_eff, "orders": [], "unauthenticated": True, "ts": now_ts()})
        return jsonify({"status": "ok", "orders": [], "unauthenticated": True, "ts": now_ts()})

    item_raw = request.args.get("item") or request.args.get("item_id")
    chain_raw = request.args.get("chain") or ""

    conn = _db()
    try:
        if item_raw:
            item_id, chain_eff = _grid_canonical_item_chain(str(item_raw).strip(), chain_raw)
            try:
                with DB_WRITE_LOCK:
                    _grid_ui_state_put(conn, wa, active_chain=chain_eff, active_item=item_id)
                    conn.commit()
            except Exception:
                pass

            payload = _grid_db_orders_payload(conn, wa, item_id, chain_eff)
            sess = _get_owned_session(item_id, wa)
            return jsonify({
                "status": "ok",
                **payload,
                "tick": int(sess.get("ticks") or 0) if isinstance(sess, dict) else 0,
                "price": float(sess.get("price") or 0.0) if isinstance(sess, dict) and sess.get("price") is not None else None,
                "running": bool(sess.get("running")) and not bool(sess.get("stopped")) if isinstance(sess, dict) else False,
                "orders_source": "sqlite",
                "ts": now_ts(),
            })

        chain = _normalize_chain_key(chain_raw)
        orders = _grid_db_list_orders(conn, wa, item_id=None, chain=chain)
        return jsonify({"status": "ok", "orders": orders, "orders_source": "sqlite", "ts": now_ts()})
    finally:
        conn.close()

@app.route("/api/grid/budgets", methods=["GET"])
def api_grid_budgets():
    """Return per-item budget state for the authenticated wallet.

    This powers the Wallet UI split:
      - Total (on-chain) remains the vault/privy balance
      - In bots (reserved) is derived from active grid orders / vault reservations
      - Available is informational and does NOT alter on-chain balances

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

            if GRID_LIVE_MODE:
                chain = _grid_chain_key(item_id)
                conn_live = _db()
                try:
                    initc = float(_grid_best_vault_total(conn_live, wa_n, item_id, chain=chain) or 0.0)
                    locked = float(_grid_db_reserved(conn_live, wa_n, item_id, chain=chain) or 0.0)
                    if not locked and chain:
                        locked = float(_grid_db_reserved(conn_live, wa_n, item_id, chain="") or 0.0)
                    avail = max(0.0, initc - locked)
                finally:
                    conn_live.close()
                if initc <= 0:
                    initc = float(sess.get("wallet_total_usd") or sess.get("initial_capital_usd") or sess.get("initial_capital") or 0.0)
            else:
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
    """Fast-path stop/cancel for a single visible SQLite order.

    Important: this endpoint intentionally does NOT rebuild vault/order summaries,
    refresh insight profiles, persist GRID_SESSIONS, or walk runtime session state.
    The UI reloads /api/grid/orders after the action.
    """
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400
    item_id, chain = _grid_canonical_item_chain(item_id, payload.get("chain") or "")

    oid = payload.get("id") or payload.get("orderId") or payload.get("order_id") or payload.get("oid")
    if oid is None or str(oid).strip() == "":
        return jsonify({"error": "missing id"}), 400

    conn = _db()
    try:
        with DB_WRITE_LOCK:
            rc = _grid_db_cancel_order(conn, wa, item_id, str(oid), chain=chain)
            try:
                _grid_ui_state_put(conn, wa, active_chain=(_grid_chain_key(item_id, chain) or chain or "POL"), active_item=item_id)
            except Exception:
                pass
            conn.commit()
        if rc <= 0:
            return jsonify({"error": "order not found", "order_id": str(oid), "item": item_id, "chain": chain}), 404
        return jsonify({
            "status": "ok",
            "action": "stopped",
            "order_id": str(oid),
            "item": item_id,
            "chain": chain,
            "orders_source": "sqlite",
            "fast_path": True,
            "ts": now_ts(),
        })
    finally:
        conn.close()

@app.route("/api/grid/order/delete", methods=["POST", "DELETE"])
def api_grid_order_delete():
    """Fast-path hard delete for a single visible SQLite order."""
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400
    item_id, chain = _grid_canonical_item_chain(item_id, payload.get("chain") or "")

    oid = payload.get("id") or payload.get("orderId") or payload.get("order_id") or payload.get("oid")
    if oid is None or str(oid).strip() == "":
        return jsonify({"error": "missing id"}), 400

    conn = _db()
    try:
        with DB_WRITE_LOCK:
            rc = _grid_db_delete_order(conn, wa, item_id, str(oid), chain=chain)
            try:
                _grid_ui_state_put(conn, wa, active_chain=(_grid_chain_key(item_id, chain) or chain or "POL"), active_item=item_id)
            except Exception:
                pass
            conn.commit()
        if rc <= 0:
            return jsonify({"error": "order not found", "order_id": str(oid), "item": item_id, "chain": chain}), 404
        return jsonify({
            "status": "ok",
            "action": "deleted",
            "order_id": str(oid),
            "item": item_id,
            "chain": chain,
            "orders_source": "sqlite",
            "fast_path": True,
            "ts": now_ts(),
        })
    finally:
        conn.close()

@app.route("/api/grid/order/resume", methods=["POST"])
@app.route("/api/grid/order/start", methods=["POST"])
@app.route("/api/grid/order/restart", methods=["POST"])
def api_grid_order_resume():
    """Fast-path resume for a single visible SQLite order."""
    wa = _require_auth()
    if not wa:
        return jsonify({"error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    item_id = str(payload.get("item") or payload.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"error": "missing item"}), 400
    item_id, chain = _grid_canonical_item_chain(item_id, payload.get("chain") or "")

    oid = payload.get("id") or payload.get("orderId") or payload.get("order_id") or payload.get("oid")
    if oid is None or str(oid).strip() == "":
        return jsonify({"error": "missing id"}), 400

    conn = _db()
    try:
        with DB_WRITE_LOCK:
            rc = _grid_db_resume_order(conn, wa, item_id, str(oid), chain=chain)
            try:
                _grid_ui_state_put(conn, wa, active_chain=(_grid_chain_key(item_id, chain) or chain or "POL"), active_item=item_id)
            except Exception:
                pass
            conn.commit()
        if rc <= 0:
            return jsonify({"error": "order not found", "order_id": str(oid), "item": item_id, "chain": chain}), 404
        return jsonify({
            "status": "ok",
            "action": "resumed",
            "order_id": str(oid),
            "item": item_id,
            "chain": chain,
            "orders_source": "sqlite",
            "fast_path": True,
            "ts": now_ts(),
        })
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
        wa = _pick_wallet_from_request()
        if not wa:
            return e

    item_id = str(body.get("item") or body.get("item_id") or "").strip()
    if not item_id:
        return err("missing 'item' in body", 400)

    session = _get_owned_session(item_id, wa)
    if not session:
        return err("forbidden", 403)

    enable = bool(body.get("enable", True))
    interval = body.get("interval", 10)
    try:
        interval = float(interval)
        if interval < 2:
            interval = 2.0
    except Exception:
        interval = 10.0

    cur = GRID_AUTORUN.pop(item_id, None)
    if cur and cur.get("stop"):
        try:
            cur["stop"].set()
        except Exception:
            pass

    try:
        session["autorun"] = bool(enable)
        session["autorun_interval"] = interval
        _grid_sessions_set(item_id, session)
        _persist_grid_state()
    except Exception:
        pass

    if not enable:
        return jsonify({"status": "ok", "item": item_id, "autorun": False, "interval": interval})

    stop_evt = threading.Event()
    th = threading.Thread(target=_autorun_loop, args=(item_id, stop_evt, interval), daemon=True)
    GRID_AUTORUN[item_id] = {"stop": stop_evt, "thread": th, "interval": interval}
    th.start()

    return jsonify({"status": "ok", "item": item_id, "autorun": True, "interval": interval})




def _nexus_funding_resolver_report(body: dict, wallet: str) -> dict:
    """Resolve whether an order has enough direct funding and suggest swap sources.

    Safe mode only: this does not execute swaps. It reports when the target/direct
    asset balance is insufficient and lists alternative assets that could fund a
    later Vault swap after explicit user approval.
    """
    body = body if isinstance(body, dict) else {}
    wa = _norm_addr(wallet or body.get("wallet") or body.get("wallet_address") or "")
    chain = _normalize_chain_key(body.get("chain") or body.get("network") or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(chain, 0) or 0)
    item_id, chain = _grid_canonical_item_chain(body.get("item") or body.get("item_id") or chain, chain)
    symbol = str(item_id.split(":", 1)[1] if ":" in item_id else item_id).upper().strip()
    side = str(body.get("side") or "BUY").upper().strip()

    price = _safe_float(body.get("price") or body.get("priceUsd") or body.get("price_usd") or 0)
    qty = _safe_float(body.get("qty") or body.get("amount") or 0)
    amount_usd = _safe_float(body.get("amountUsd") or body.get("amount_usd") or body.get("budgetUsd") or body.get("budget_usd") or 0)
    if amount_usd <= 0 and price > 0 and qty > 0:
        amount_usd = price * qty

    native_px = _safe_float(body.get("nativePriceUsd") or body.get("native_price_usd") or body.get("chainNativeUsd") or 0)
    native_required = qty if symbol == chain and qty > 0 else (amount_usd / native_px if amount_usd > 0 and native_px > 0 else 0.0)

    out = {
        "status": "ok",
        "funding_required": False,
        "requires_user_approval": False,
        "chain": chain,
        "chainId": cid,
        "symbol": symbol,
        "side": side,
        "amountUsd": round(float(amount_usd or 0), 6),
        "requiredNative": round(float(native_required or 0), 12),
        "directAsset": chain,
        "directAvailable": 0.0,
        "directAvailableUsd": 0.0,
        "shortageNative": 0.0,
        "shortageUsd": 0.0,
        "suggestions": [],
        "message": "Direct funding looks sufficient.",
        "ts": now_ts(),
    }

    if not _looks_like_evm_addr(wa) or cid <= 0 or amount_usd <= 0:
        out.update({"status": "unknown", "message": "Funding check unavailable."})
        return out

    # Direct funding source today is the wallet-bound Vault native balance for the selected chain.
    direct_native = 0.0
    try:
        vstate = _vault_state_read(wa, chain)
        direct_native = _safe_float(vstate.get("vault_balance") or 0)
    except Exception:
        # Avoid blocking UX if RPC/vault read is unavailable.
        direct_native = 0.0

    direct_usd = direct_native * native_px if native_px > 0 else 0.0
    out["directAvailable"] = round(float(direct_native), 12)
    out["directAvailableUsd"] = round(float(direct_usd), 6)

    enough = False
    if native_required > 0:
        enough = direct_native + 1e-12 >= native_required
    elif amount_usd > 0 and native_px > 0:
        enough = direct_usd + 1e-9 >= amount_usd

    if enough:
        return out

    shortage_native = max(0.0, native_required - direct_native) if native_required > 0 else 0.0
    shortage_usd = max(0.0, amount_usd - direct_usd) if amount_usd > 0 else 0.0
    if shortage_usd <= 0 and shortage_native > 0 and native_px > 0:
        shortage_usd = shortage_native * native_px
    out["funding_required"] = True
    out["requires_user_approval"] = True
    out["shortageNative"] = round(float(shortage_native), 12)
    out["shortageUsd"] = round(float(shortage_usd), 6)

    suggestions = []
    # Same-chain wallet native as a possible top-up source.
    try:
        raw = _rpc_call(cid, "eth_getBalance", [wa, "latest"])
        wallet_native = _hex_to_int(raw or "0x0") / 1e18
        wallet_native_usd = wallet_native * native_px if native_px > 0 else 0.0
        if wallet_native > 0 and (shortage_native <= 0 or wallet_native + 1e-12 >= shortage_native):
            suggestions.append({
                "asset": chain,
                "type": "native_wallet",
                "balance": round(float(wallet_native), 12),
                "balanceUsd": round(float(wallet_native_usd), 6),
                "action": "deposit_or_swap_to_vault",
                "label": f"Use wallet {chain}",
            })
    except Exception:
        pass

    # Same-chain stables as swap funding sources.
    for stable_sym, addr_map in (("USDC", _USDC_BY_CHAIN), ("USDT", _USDT_BY_CHAIN)):
        try:
            token_addr = str((addr_map or {}).get(cid) or "").strip()
            if not _looks_like_evm_addr(token_addr):
                continue
            bal = _erc20_balance_of_rpc(chain, token_addr, wa)
            raw_i = int(str(bal.get("balance_raw") or "0"))
            dec = int(_stable_decimals(cid, stable_sym, token_addr))
            bal_units = raw_i / (10 ** dec)
            if bal_units > 0 and (shortage_usd <= 0 or bal_units + 1e-9 >= shortage_usd):
                suggestions.append({
                    "asset": stable_sym,
                    "type": "stable_wallet",
                    "address": token_addr,
                    "balance": round(float(bal_units), 6),
                    "balanceUsd": round(float(bal_units), 6),
                    "action": "swap_to_target_asset",
                    "label": f"Swap {stable_sym}",
                })
        except Exception:
            continue

    out["suggestions"] = suggestions[:4]
    if suggestions:
        out["status"] = "funding_required"
        out["message"] = f"Not enough {chain} available. User approval required to fund via another asset."
    else:
        out["status"] = "blocked"
        out["message"] = f"Not enough {chain} available and no suitable same-chain funding asset was found."
    return out


@app.route("/api/nexus/funding/resolve", methods=["POST"])
def api_nexus_funding_resolve():
    body = request.get_json(silent=True) or {}
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return err("unauthorized", 401)
    try:
        return jsonify(_nexus_funding_resolver_report(body, wa))
    except Exception as e:
        return jsonify({"status": "error", "error": str(e), "ts": now_ts()}), 500

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
        if not item_id:
            return jsonify({"error": "missing item"}), 400
        item_id, chain = _grid_canonical_item_chain(item_id, payload.get("chain") or "")

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

        funding_report = _nexus_funding_resolver_report({**payload, "item": item_id, "chain": chain, "price": price_f, "qty": qty_f}, wa)
        funding_approved = str(payload.get("funding_approved") or payload.get("fundingApproved") or "").strip().lower() in ("1", "true", "yes", "on")
        if funding_report.get("funding_required") and not funding_approved:
            return jsonify({
                "status": "funding_required",
                "error": "insufficient_direct_funding",
                "funding": funding_report,
                "message": funding_report.get("message") or "Funding approval required.",
                "ts": now_ts(),
            }), 409

        # Create order. client_order_id makes retries/double-clicks idempotent when the frontend sends it.
        client_order_id = str(
            payload.get("client_order_id")
            or payload.get("clientOrderId")
            or payload.get("request_id")
            or ""
        ).strip()
        order_id = client_order_id if client_order_id else str(uuid.uuid4())

        source = str(payload.get("source") or payload.get("origin") or "MANUAL").upper().strip()
        if source not in ("MANUAL", "GRID", "ROTATION", "TRADING", "STRATEGIST"):
            source = "MANUAL"

        order = {
            "id": order_id,
            "client_order_id": client_order_id or order_id,
            "side": side,
            "price": round(price_f, 12),
            "qty": round(qty_f, 12),
            "slippage": slip_f,
            "deadline": deadline_i,
            "status": "OPEN",
            "source": source,
            "origin_module": str(payload.get("origin_module") or source.lower()).strip(),
            "session_id": str(payload.get("session_id") or "").strip(),
            "strategy_id": str(payload.get("strategy_id") or "").strip(),
            "funding_approved": bool(funding_approved),
            "funding_source_asset": str(payload.get("funding_source_asset") or payload.get("fundingSourceAsset") or "").upper().strip(),
            "funding_required": bool(funding_report.get("funding_required")),
            "funding_shortage_usd": funding_report.get("shortageUsd"),
            "ts": int(time.time()),
            "level": payload.get("level", None),  # optional
        }

        # Persist to DB (authoritative)
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

        # Fast path: do not mirror into GRID_SESSIONS, do not persist runtime state,
        # do not rebuild vault/order summaries, and do not refresh insight profiles here.
        # The UI reloads /api/grid/orders after the action; executor/tick refreshes
        # runtime orders from SQLite when needed.
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

        return jsonify({
            "status": "ok",
            "action": "added",
            "order": order,
            "order_id": order_id,
            "db_saved": db_saved,
            "db_error": db_error,
            "saved_item_id": item_id,
            "saved_chain": chain,
            "orders_source": "sqlite",
            "fast_path": True,
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


@app.route("/api/ai/insight-profile", methods=["GET"])
def api_ai_insight_profile_get():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

    wallet_q = str(request.args.get("wallet") or request.args.get("wallet_address") or wa).strip()
    wallet_q = _norm_addr(wallet_q)
    if not wallet_q:
        wallet_q = wa
    if wallet_q != wa:
        return err("forbidden", 403)

    order_memory, insight_profile = _insight_profile_get(wallet_q)
    if not order_memory and not insight_profile:
        order_memory, insight_profile = _refresh_user_insight_profile(wallet_q)

    return jsonify({
        "status": "ok",
        "wallet_address": wallet_q,
        "order_memory": order_memory,
        "insight_profile": insight_profile,
        "ts": now_ts(),
    })

@app.route("/api/ai/insight-profile/refresh", methods=["POST"])
def api_ai_insight_profile_refresh():
    wa = _require_auth() or _pick_wallet_from_request()
    if not wa:
        return err("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    wallet_q = _norm_addr(body.get("wallet") or body.get("wallet_address") or wa)
    if wallet_q != wa:
        return err("forbidden", 403)

    order_memory, insight_profile = _refresh_user_insight_profile(wallet_q)
    return jsonify({
        "status": "ok",
        "wallet_address": wallet_q,
        "order_memory": order_memory,
        "insight_profile": insight_profile,
        "ts": now_ts(),
    })

# -------------------------
# AI Run (backend-native context builder)
# -------------------------


def _hard_sanitize_ai_insight_text(value: str) -> str:
    """Remove data-dump fragments from AI Insight text.

    This is intentionally strict for /api/ai/insight only. The raw metrics are already
    visible in the UI; AI Insight should explain behavior, not repeat source data.
    """
    out = str(value or "").strip()
    if not out:
        return out

    # Remove hard data-dump tails first.
    out = re.sub(r"(?is)\bSignal context\s*:\s*.*$", "", out).strip()
    out = re.sub(r"(?is)\bObserved relative bias\s*:\s*.*$", "", out).strip()

    # Remove sentences/fragments containing forbidden source-dump terms.
    forbidden = r"(?:Votes?|Rating|CoinGecko|contract mapping|token mapping|No CoinGecko|mapping found|Signal context)"
    parts = re.split(r"(?<=[.!?])\s+", out)
    kept = []
    for part in parts:
        if re.search(forbidden, part, flags=re.I):
            continue
        kept.append(part)
    out = " ".join(kept).strip()

    # Replace raw-metric phrasing with behavior phrasing.
    out = re.sub(r"(?i)strong correlation\s+(?:of|at)\s*[-+]?\d+(?:\.\d+)?", "strong linkage", out)
    out = re.sub(r"(?i)correlation\s+(?:of|at)\s*[-+]?\d+(?:\.\d+)?", "linkage", out)
    out = re.sub(r"(?i)spread of approximately\s*[-+]?\d+(?:\.\d+)?%", "stretched spread", out)
    out = re.sub(r"(?i)spread of\s*[-+]?\d+(?:\.\d+)?%", "stretched spread", out)
    out = re.sub(r"(?i)decline of\s*[-+]?\d+(?:\.\d+)?%[^.]*", "recent weakness", out)
    out = re.sub(r"(?i)over the last\s+\d+\s*(?:days?|d)\b", "recently", out)
    out = re.sub(r"(?i)approximately\s+[-+]?\d+(?:\.\d+)?%", "noticeably", out)
    out = re.sub(r"(?i)significant decline\s+of\s*[-+]?\d+(?:\.\d+)?%", "significant recent weakness", out)

    # Remove remaining raw percentages and score-like dumps. Keep prose clean.
    out = re.sub(r"\b[-+]?\d+(?:\.\d+)?%\b", "", out)
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\s+([,.])", r"\1", out)
    out = out.strip(" ;,.-\n\t")
    return out



def _ai_intelligence_level(value: float) -> str:
    try:
        n = float(value)
    except Exception:
        n = 0.0
    if n >= 75:
        return "high"
    if n >= 55:
        return "medium"
    if n >= 35:
        return "low-medium"
    return "low"


def _dynamic_ai_confidence_from_engine(engine_ctx: dict | None = None) -> dict:
    """Compute a compact confidence profile from the internal intelligence layers.

    This does not create a buy/sell signal. It only tells AI Insight how much
    weight to put on the current read based on signal alignment, liquidity,
    confirmation quality, regime stability and risk contradictions.
    """
    ctx = engine_ctx if isinstance(engine_ctx, dict) else {}
    mb = ctx.get("market_behavior") if isinstance(ctx.get("market_behavior"), dict) else {}
    liq = ctx.get("liquidity_context") if isinstance(ctx.get("liquidity_context"), dict) else {}
    phase = ctx.get("market_phase") if isinstance(ctx.get("market_phase"), dict) else (mb.get("market_phase") if isinstance(mb.get("market_phase"), dict) else {})

    base = _safe_float(ctx.get("confidence"), 6.0) * 10.0
    continuation = _safe_float(ctx.get("continuation_quality") or mb.get("continuation_quality"), 40.0)
    volume_conf = _safe_float(ctx.get("volume_confirmation") or mb.get("volume_confirmation"), 35.0)
    trap = _safe_float(ctx.get("trap_risk") or liq.get("trap_risk"), 0.0)
    vacuum = _safe_float(ctx.get("liquidity_vacuum_risk") or liq.get("liquidity_vacuum_risk"), 0.0)
    stop_hunt = _safe_float(ctx.get("stop_hunt_risk") or liq.get("stop_hunt_risk"), 0.0)
    exhaustion = _safe_float(ctx.get("exhaustion_risk") or mb.get("exhaustion_risk"), 0.0)
    fake = _safe_float(ctx.get("fake_move_risk") or mb.get("fake_move_risk"), 0.0)
    depth = _safe_float(ctx.get("participation_depth") or liq.get("participation_depth"), 45.0)
    phase_conf = _safe_float(phase.get("confidence"), 50.0)

    score = base
    score += (continuation - 50.0) * 0.12
    score += (volume_conf - 45.0) * 0.10
    score += (depth - 45.0) * 0.08
    score += (phase_conf - 50.0) * 0.08
    score -= max(trap, vacuum, stop_hunt) * 0.10
    score -= max(exhaustion, fake) * 0.08
    score = round(max(20.0, min(95.0, score)), 1)

    label = "HIGH" if score >= 78 else "MEDIUM" if score >= 55 else "LOW"
    return {
        "score": score,
        "label": label,
        "summary": f"{label} ({score/10.0:.1f}/10)",
        "drivers": {
            "continuation_quality": continuation,
            "volume_confirmation": volume_conf,
            "liquidity_depth": depth,
            "trap_pressure": max(trap, vacuum, stop_hunt),
            "exhaustion_or_fake_move_pressure": max(exhaustion, fake),
            "phase_confidence": phase_conf,
        },
        "display_in_ui": False,
    }


def _tactical_state_from_engine(engine_ctx: dict | None = None) -> dict:
    """Classify the current pair setup into a tactical behavior window for AI wording."""
    ctx = engine_ctx if isinstance(engine_ctx, dict) else {}
    mb = ctx.get("market_behavior") if isinstance(ctx.get("market_behavior"), dict) else {}
    liq = ctx.get("liquidity_context") if isinstance(ctx.get("liquidity_context"), dict) else {}
    phase = ctx.get("market_phase") if isinstance(ctx.get("market_phase"), dict) else (mb.get("market_phase") if isinstance(mb.get("market_phase"), dict) else {})

    setup = str(ctx.get("setup_bias") or "").lower()
    phase_regime = str(phase.get("regime") or ctx.get("market_phase_regime") or "").lower()
    liq_regime = str(liq.get("regime") or ctx.get("liquidity_regime") or "").lower()
    cont = _safe_float(ctx.get("continuation_quality") or mb.get("continuation_quality"), 0.0)
    vol_conf = _safe_float(ctx.get("volume_confirmation") or mb.get("volume_confirmation"), 0.0)
    exhaustion = _safe_float(ctx.get("exhaustion_risk") or mb.get("exhaustion_risk"), 0.0)
    fake = _safe_float(ctx.get("fake_move_risk") or mb.get("fake_move_risk"), 0.0)
    trap = _safe_float(ctx.get("trap_risk") or liq.get("trap_risk"), 0.0)

    if "mean" in setup or "grid" in setup or "range" in phase_regime:
        state = "Mean-Reversion Window"
        meaning = "structure favors oscillation or rotation rather than a clean directional continuation"
    elif "accumulation" in phase_regime or _safe_float(mb.get("accumulation_signal"), 0.0) >= 55:
        state = "Accumulation Behavior"
        meaning = "participation is building before a fully confirmed expansion"
    elif exhaustion >= 65 or "euphoria" in phase_regime or "distribution" in phase_regime:
        state = "Distribution / Exhaustion Risk"
        meaning = "movement quality may be late-stage, stretched or vulnerable to reversal"
    elif trap >= 65 or fake >= 65 or "trap" in liq_regime:
        state = "Trap-Sensitive Expansion"
        meaning = "price movement can be fast, but confirmation quality remains fragile"
    elif cont >= 65 and vol_conf >= 50:
        state = "Continuation Window"
        meaning = "participation supports follow-through better than usual, while still requiring confirmation"
    elif "vacuum" in liq_regime:
        state = "Liquidity Vacuum"
        meaning = "movement may travel quickly through thin liquidity and invalidate just as fast"
    else:
        state = "Mixed Structure"
        meaning = "no clean tactical regime dominates yet"

    return {"state": state, "meaning": meaning, "display_in_ui": False}


def _liquidity_warning_labels_from_engine(engine_ctx: dict | None = None) -> list[str]:
    ctx = engine_ctx if isinstance(engine_ctx, dict) else {}
    liq = ctx.get("liquidity_context") if isinstance(ctx.get("liquidity_context"), dict) else {}
    warnings = []
    trap = _safe_float(ctx.get("trap_risk") or liq.get("trap_risk"), 0.0)
    vacuum = _safe_float(ctx.get("liquidity_vacuum_risk") or liq.get("liquidity_vacuum_risk"), 0.0)
    stop_hunt = _safe_float(ctx.get("stop_hunt_risk") or liq.get("stop_hunt_risk"), 0.0)
    depth = _safe_float(ctx.get("participation_depth") or liq.get("participation_depth"), 0.0)
    fake = _safe_float(ctx.get("fake_move_risk"), 0.0)
    vol_conf = _safe_float(ctx.get("volume_confirmation"), 0.0)

    if fake >= 60 and vol_conf < 45:
        warnings.append("LOW CONVICTION BREAKOUT")
    if trap >= 65:
        warnings.append("LIQUIDITY WARNING")
    if depth and depth < 40:
        warnings.append("THIN PARTICIPATION")
    if stop_hunt >= 65:
        warnings.append("STOP-HUNT SENSITIVITY")
    if vacuum >= 65:
        warnings.append("LIQUIDITY VACUUM")
    return warnings[:4]


def _ai_market_intelligence_sections_from_engine(engine_ctx: dict | None = None) -> str:
    """Deterministic structured AI Insight conclusion from internal engines.

    This is used as the final safety format for AI Insight so the UI receives a
    clean market-intelligence read, not raw source data. It intentionally avoids
    ratings, votes, CoinGecko mapping, direct buy/sell language and raw metrics.
    """
    ctx = engine_ctx if isinstance(engine_ctx, dict) else {}
    mb = ctx.get("market_behavior") if isinstance(ctx.get("market_behavior"), dict) else {}
    liq = ctx.get("liquidity_context") if isinstance(ctx.get("liquidity_context"), dict) else {}
    phase = ctx.get("market_phase") if isinstance(ctx.get("market_phase"), dict) else (mb.get("market_phase") if isinstance(mb.get("market_phase"), dict) else {})
    dyn_conf = _dynamic_ai_confidence_from_engine(ctx)
    tactical = _tactical_state_from_engine(ctx)
    liq_warnings = _liquidity_warning_labels_from_engine(ctx)

    verdict = str(ctx.get("verdict") or "MARKET STRUCTURE").strip()
    setup = str(ctx.get("setup_bias") or "mixed / no-clean-setup").strip()
    edge = str(ctx.get("edge") or "structure does not show a clean directional edge yet").strip()
    invalidation = str(ctx.get("invalidation") or ctx.get("risk") or "weak confirmation can invalidate the current read").strip()
    risk = str(ctx.get("risk") or "Medium").strip()
    behavior_label = str(phase.get("label") or ctx.get("market_phase_label") or mb.get("label") or ctx.get("market_behavior_label") or "mixed market behavior").strip()
    liq_label = str(liq.get("label") or ctx.get("liquidity_label") or "balanced liquidity conditions").strip()
    rel = ctx.get("relative_strength") if isinstance(ctx.get("relative_strength"), dict) else {}
    stronger = str(rel.get("stronger") or "").strip().upper()
    weaker = str(rel.get("weaker") or "").strip().upper()

    market_structure = f"{behavior_label}; current read leans {setup}."
    liquidity_state = f"{liq_label}."
    if liq_warnings:
        liquidity_state += " Warnings: " + ", ".join(liq_warnings) + "."
    risk_posture = f"Risk posture is {risk}; confidence profile reads {dyn_conf.get('summary')}."
    if stronger and weaker:
        pair_relationship = f"Relative strength currently favors {stronger} over {weaker}, but the pair still needs cleaner confirmation."
    else:
        pair_relationship = "Pair relationship remains mixed; confirmation quality matters more than isolated movement."
    tactical_read = f"{tactical.get('state')}: {tactical.get('meaning')}."
    invalidations = invalidation

    text = (
        f"Market Structure: {market_structure}\n"
        f"Liquidity State: {liquidity_state}\n"
        f"Risk Posture: {risk_posture}\n"
        f"Pair Relationship: {pair_relationship}\n"
        f"Tactical Read: {tactical_read}\n"
        f"Invalidations: {invalidations}\n\n"
        f"Edge: {edge}\n"
        f"Risk: {invalidation}\n"
        f"Setup bias: {setup}"
    )
    clean = _hard_sanitize_ai_insight_text(text)
    clean = re.sub(
        r"\s+(Liquidity State|Risk Posture|Pair Relationship|Tactical Read|Invalidations|Edge|Risk|Setup bias)\s*:",
        r"\n\1:",
        clean,
    )
    return clean.strip()


def _compact_behavior_answer_from_engine(engine_ctx: dict | None = None) -> str:
    """Deterministic fallback when the LLM still dumps raw UI/source data."""
    return _ai_market_intelligence_sections_from_engine(engine_ctx)

def _enforce_ai_insight_structure(text: str, engine_ctx: dict | None = None) -> str:
    """Guarantee AI Insight Level 2 output is concise and behavior-driven.

    For AI Insight, raw UI/source values must not leak into the final answer. If the
    model still outputs data-dump text, this function switches to a deterministic
    concise behavior summary from ai_engine_v2.
    """
    s = _hard_sanitize_ai_insight_text(str(text or "").strip())
    if not s:
        return _compact_behavior_answer_from_engine(engine_ctx)

    forbidden_hit = bool(re.search(r"(?i)\b(Votes?|Rating|CoinGecko|contract mapping|token mapping|Signal context|mapping found)\b", s))
    raw_metric_hit = bool(re.search(r"\b\d+(?:\.\d+)?%\b|\bcorrelation\s+(?:of|at)\s+\d|\bover the last\s+\d+\s*(?:days?|d)\b", s, flags=re.I))
    too_long = len(re.findall(r"\w+", s)) > 170
    lacks_market_intel_sections = bool(engine_ctx) and not re.search(r"(?i)\bMarket Structure\s*:", s)
    if forbidden_hit or raw_metric_hit or too_long or lacks_market_intel_sections:
        return _compact_behavior_answer_from_engine(engine_ctx)

    def _extract(label: str) -> str:
        pattern = rf"(?is)\b{re.escape(label)}\s*:\s*(.*?)(?=\bEdge\s*:|\bRisk\s*:|\bSetup bias\s*:|$)"
        m = re.search(pattern, s)
        if not m:
            return ""
        return _hard_sanitize_ai_insight_text(re.sub(r"\s+", " ", str(m.group(1) or "")).strip(" -:\n\t"))

    edge = _extract("Edge")
    risk = _extract("Risk")
    setup = _extract("Setup bias")

    label_positions = [
        pos for pos in [
            s.lower().find("edge:"),
            s.lower().find("risk:"),
            s.lower().find("setup bias:"),
        ] if pos >= 0
    ]
    paragraph = s[:min(label_positions)].strip() if label_positions else s.strip()
    paragraph = re.sub(r"(?is)\b(edge|risk|setup bias)\s*:.*$", "", paragraph).strip()
    paragraph = _hard_sanitize_ai_insight_text(paragraph)

    engine_ctx = engine_ctx if isinstance(engine_ctx, dict) else {}
    if not edge:
        edge = str(engine_ctx.get("edge") or "structure favors no clean edge until confirmation improves")
    if not risk:
        risk = str(engine_ctx.get("invalidation") or engine_ctx.get("risk") or "weak or missing confirmation can invalidate the setup")
    if not setup:
        setup = str(engine_ctx.get("setup_bias") or "no-clean-setup")

    edge = _hard_sanitize_ai_insight_text(re.sub(r"(?is)\b(Edge|Risk|Setup bias)\s*:\s*", "", str(edge or ""))).strip() or "not clearly defined"
    risk = _hard_sanitize_ai_insight_text(re.sub(r"(?is)\b(Edge|Risk|Setup bias)\s*:\s*", "", str(risk or ""))).strip() or "not clearly defined"
    setup = _hard_sanitize_ai_insight_text(re.sub(r"(?is)\b(Edge|Risk|Setup bias)\s*:\s*", "", str(setup or ""))).strip() or "not clearly defined"

    # Final safety pass: if paragraph is still empty or dumpy, use deterministic answer.
    final = f"{paragraph}\n\nEdge: {edge}\nRisk: {risk}\nSetup bias: {setup}".strip()
    if (not paragraph) or re.search(r"(?i)\b(Votes?|Rating|CoinGecko|contract mapping|Signal context|mapping found)\b", final):
        return _compact_behavior_answer_from_engine(engine_ctx)
    return final


# -------------------------
# AI Insight add-ons: Mode, Custom Weighting, Pair Alerts
# -------------------------
def _normalize_ai_mode(raw: Any) -> str:
    """AI Insight supports one optional extra mode: extreme.
    Standard stays the default to preserve existing behavior.
    """
    mode = str(raw or "standard").strip().lower()
    if mode in ("extreme", "x", "high_risk", "high-risk"):
        return "extreme"
    return "standard"


def _normalize_compare_weights_for_ai(raw: Any) -> dict:
    """Normalize frontend Compare weights into AI-readable 0..100 percentages."""
    defaults = {"corr": 35, "momentum": 25, "opportunity": 25, "stability": 15}
    data = raw if isinstance(raw, dict) else {}
    out = {}
    for k, default in defaults.items():
        try:
            v = float(data.get(k, default))
            if not math.isfinite(v):
                v = float(default)
        except Exception:
            v = float(default)
        out[k] = max(0.0, min(100.0, v))
    total = sum(out.values())
    if total <= 0:
        return defaults
    # Keep user percentages when total <= 100; normalize only if corrupted/over 100.
    if total > 100.0:
        out = {k: round((v / total) * 100.0, 2) for k, v in out.items()}
    return out


def _weight_focus(compare_weights: dict) -> str:
    w = _normalize_compare_weights_for_ai(compare_weights)
    top = sorted(w.items(), key=lambda kv: (-kv[1], kv[0]))[:2]
    return ", ".join([f"{k}={int(round(v))}%" for k, v in top])


def _movement_level_from_score(score: float) -> str:
    s = _safe_float(score, 0.0)
    if s >= 82:
        return "very_high"
    if s >= 68:
        return "high"
    if s >= 50:
        return "medium"
    if s >= 32:
        return "low"
    return "quiet"


def _movement_alert_label(level: str) -> str:
    lvl = str(level or "").lower()
    if lvl in ("very_high", "high"):
        return "Volatile Chance"
    if lvl == "medium":
        return "Movement Chance"
    return "Movement Watch"


def _build_movement_chance_score(pair_ctx: dict | None, coins: list, compare_weights: dict | None = None, ai_mode: str = "standard") -> dict:
    """Backend Movement Chance Score.

    This is NOT a buy/sell score and NOT a success probability.
    It measures unusual short-term movement potential from pair structure:
    spread expansion, RSI divergence, momentum gap, correlation context,
    market-condition quality, on-chain/whale confirmation, custom weights,
    and optional Extreme mode sensitivity.
    """
    p = pair_ctx if isinstance(pair_ctx, dict) else {}
    w = _normalize_compare_weights_for_ai(compare_weights or {})
    mode = _normalize_ai_mode(ai_mode)

    pair = str(p.get("pair") or "").upper().strip()
    parts = [x.strip() for x in pair.split("/") if x.strip()]

    coin_map = {}
    for c in coins if isinstance(coins, list) else []:
        if isinstance(c, dict):
            sym = str(c.get("symbol") or "").upper().strip()
            if sym:
                coin_map[sym] = c

    ca = coin_map.get(parts[0], {}) if len(parts) >= 1 else {}
    cb = coin_map.get(parts[1], {}) if len(parts) >= 2 else {}

    corr = _safe_float(p.get("corr"), 0.0)
    spread = abs(_safe_float(p.get("spread_pct"), 0.0))
    rsi_gap = abs(_safe_float(p.get("rsi_gap"), 0.0))
    pair_rank_score = _safe_float(p.get("score"), 0.0)
    ch_a = _safe_float(ca.get("change_24h_pct"), 0.0)
    ch_b = _safe_float(cb.get("change_24h_pct"), 0.0)
    momentum_gap = abs(ch_a - ch_b)

    # Component scores, each intentionally capped so one noisy metric cannot dominate.
    spread_score = min(22.0, spread * 2.75)
    rsi_score = min(22.0, rsi_gap * 1.25)
    momentum_score = min(18.0, momentum_gap * 3.0)

    if corr >= 0.82:
        corr_score = 12.0
    elif corr >= 0.70:
        corr_score = 9.0
    elif corr >= 0.55:
        corr_score = 5.0
    elif corr > 0:
        corr_score = 1.5
    else:
        corr_score = 0.0

    market_score = 0.0
    market_notes = []
    for c in (ca, cb):
        mc = c.get("market_condition") if isinstance(c.get("market_condition"), dict) else {}
        state = str(mc.get("state") or "").upper()
        if state == "REAL_BREAKOUT":
            market_score += 7.0
            market_notes.append(f"{c.get('symbol')}: volume-backed breakout")
        elif state == "EARLY_ACCUMULATION":
            market_score += 6.0
            market_notes.append(f"{c.get('symbol')}: early accumulation")
        elif state == "OVEREXTENDED":
            market_score -= 3.0
            market_notes.append(f"{c.get('symbol')}: overextended")
        elif state == "FAKE_MOVE":
            market_score -= 5.0
            market_notes.append(f"{c.get('symbol')}: weak/fake-move risk")
    market_score = max(-8.0, min(14.0, market_score))

    onchain_score = 0.0
    onchain_notes = []
    for c in (ca, cb):
        oc = c.get("onchain") if isinstance(c.get("onchain"), dict) else {}
        delta = _safe_float(c.get("onchain_delta"), _safe_float(oc.get("score_delta"), 0.0))
        if delta >= 3:
            onchain_score += 4.0
            onchain_notes.append(f"{c.get('symbol')}: positive on-chain/whale pressure")
        elif delta <= -3:
            onchain_score -= 4.0
            onchain_notes.append(f"{c.get('symbol')}: negative on-chain/whale pressure")
    onchain_score = max(-6.0, min(8.0, onchain_score))

    # Custom weighting influences the interpretation without replacing raw data.
    weight_boost = 0.0
    weight_boost += max(0.0, w.get("opportunity", 25) - 25.0) * 0.18 if spread >= 3.0 else 0.0
    weight_boost += max(0.0, w.get("momentum", 25) - 25.0) * 0.18 if (rsi_gap >= 8.0 or momentum_gap >= 2.5) else 0.0
    weight_boost += max(0.0, w.get("corr", 35) - 35.0) * 0.10 if corr >= 0.70 else 0.0
    if w.get("stability", 15) >= 30 and corr < 0.60 and corr > 0:
        weight_boost -= 4.0

    # Low-ranked pairs with strong movement receive a small scanner bonus.
    low_rank_bonus = 0.0
    if pair_rank_score and pair_rank_score < 70 and (spread >= 5.0 or rsi_gap >= 14.0 or momentum_gap >= 4.0):
        low_rank_bonus = 7.0

    raw_score = (
        spread_score + rsi_score + momentum_score + corr_score +
        market_score + onchain_score + weight_boost + low_rank_bonus
    )

    if mode == "extreme":
        raw_score *= 1.10
        if spread >= 4.0 or rsi_gap >= 12.0 or momentum_gap >= 3.5:
            raw_score += 4.0

    score = round(max(0.0, min(100.0, raw_score)), 1)
    level = _movement_level_from_score(score)

    reasons = []
    if spread >= 5.0:
        reasons.append("wide spread movement")
    elif spread >= 2.5:
        reasons.append("visible spread movement")
    if rsi_gap >= 14.0:
        reasons.append("large RSI divergence")
    elif rsi_gap >= 8.0:
        reasons.append("RSI gap building")
    if momentum_gap >= 4.0:
        reasons.append("short-term momentum gap")
    if corr >= 0.75 and spread >= 2.0:
        reasons.append("linked pair with relative imbalance")
    if low_rank_bonus > 0:
        reasons.append("lower-ranked pair showing unusual activity")
    if market_notes:
        reasons.extend(market_notes[:2])
    if onchain_notes:
        reasons.extend(onchain_notes[:2])
    if not reasons:
        reasons.append("no unusual movement structure detected")

    return {
        "pair": pair,
        "score": score,
        "level": level,
        "label": _movement_alert_label(level),
        "meaning": "Movement potential only — not a buy signal, not a quality rating, and not a success probability.",
        "components": {
            "spread": round(spread_score, 2),
            "rsi": round(rsi_score, 2),
            "momentum": round(momentum_score, 2),
            "correlation": round(corr_score, 2),
            "market_condition": round(market_score, 2),
            "onchain_whale": round(onchain_score, 2),
            "custom_weighting": round(weight_boost, 2),
            "low_rank_bonus": round(low_rank_bonus, 2),
        },
        "metrics": {
            "corr": round(corr, 4),
            "spread_pct": round(spread, 2),
            "rsi_gap": round(rsi_gap, 2),
            "momentum_gap_24h_pct": round(momentum_gap, 2),
            "pair_rank_score": round(pair_rank_score, 2),
        },
        "reasons": reasons[:6],
    }



def _behavior_level(value: float) -> str:
    v = _safe_float(value, 0.0)
    if v >= 75:
        return "high"
    if v >= 50:
        return "medium"
    if v >= 25:
        return "low"
    return "quiet"


def _market_phase_from_behavior_inputs(
    corr: float,
    spread: float,
    rsi_gap: float,
    momentum_gap: float,
    score_pair: float,
    max_rvol: float,
    max_oe: float,
    states: list,
    fake_move_risk: float,
    exhaustion_risk: float,
    continuation_quality: float,
    accumulation_signal: float,
    volatility_expansion: float,
    volume_confirmation: float,
    ai_mode: str = "standard",
) -> dict:
    """Regime / Market Phase Engine for AI Insight.

    This is an internal context layer. It classifies the market phase behind the
    movement so AI Insight can explain *what kind* of environment the pair is in.
    It is not displayed as a standalone UI badge and is not a buy/sell signal.
    """
    mode = _normalize_ai_mode(ai_mode)
    corr = _safe_float(corr, 0.0)
    spread = abs(_safe_float(spread, 0.0))
    rsi_gap = abs(_safe_float(rsi_gap, 0.0))
    momentum_gap = abs(_safe_float(momentum_gap, 0.0))
    score_pair = _safe_float(score_pair, 0.0)
    max_rvol = _safe_float(max_rvol, 0.0)
    max_oe = abs(_safe_float(max_oe, 0.0))
    fake_move_risk = _safe_float(fake_move_risk, 0.0)
    exhaustion_risk = _safe_float(exhaustion_risk, 0.0)
    continuation_quality = _safe_float(continuation_quality, 0.0)
    accumulation_signal = _safe_float(accumulation_signal, 0.0)
    volatility_expansion = _safe_float(volatility_expansion, 0.0)
    volume_confirmation = _safe_float(volume_confirmation, 0.0)
    state_set = {str(x or "").upper() for x in (states or []) if str(x or "").strip()}

    trend_strength = 0.0
    trend_strength += min(28.0, max(0.0, score_pair - 55.0) * 0.45)
    trend_strength += 18.0 if corr >= 0.72 else 8.0 if corr >= 0.55 else 0.0
    trend_strength += 18.0 if volume_confirmation >= 55 else 8.0 if volume_confirmation >= 35 else 0.0
    trend_strength += 18.0 if continuation_quality >= 60 else 8.0 if continuation_quality >= 45 else 0.0
    trend_strength += 10.0 if max_rvol >= 1.5 else 0.0
    trend_strength -= 16.0 if fake_move_risk >= 65 else 0.0
    trend_strength -= 12.0 if exhaustion_risk >= 70 else 0.0

    chop_index = 0.0
    chop_index += 18.0 if 0.45 <= corr < 0.72 else 8.0 if corr < 0.45 else 0.0
    chop_index += 16.0 if 2.0 <= spread <= 8.0 else 8.0 if spread > 8.0 else 0.0
    chop_index += 14.0 if 8.0 <= rsi_gap <= 18.0 else 8.0 if rsi_gap > 18.0 else 0.0
    chop_index += 16.0 if fake_move_risk >= 45 else 0.0
    chop_index += 10.0 if volume_confirmation < 35 and volatility_expansion >= 35 else 0.0
    chop_index += 8.0 if continuation_quality < 45 else 0.0

    range_pressure = 0.0
    range_pressure += 22.0 if corr >= 0.75 and 2.0 <= spread <= 10.0 else 0.0
    range_pressure += 18.0 if rsi_gap >= 10.0 else 0.0
    range_pressure += 14.0 if continuation_quality < 55 else 0.0
    range_pressure += 12.0 if exhaustion_risk >= 45 else 0.0

    panic_pressure = 0.0
    panic_pressure += 24.0 if volatility_expansion >= 70 else 12.0 if volatility_expansion >= 50 else 0.0
    panic_pressure += 18.0 if momentum_gap >= 8.0 else 8.0 if momentum_gap >= 4.0 else 0.0
    panic_pressure += 18.0 if max_rvol >= 2.5 else 8.0 if max_rvol >= 1.8 else 0.0
    panic_pressure += 18.0 if spread >= 14.0 else 8.0 if spread >= 8.0 else 0.0

    euphoria_pressure = 0.0
    euphoria_pressure += 26.0 if max_oe >= 60 else 14.0 if max_oe >= 35 else 0.0
    euphoria_pressure += 18.0 if max_rvol >= 2.0 else 0.0
    euphoria_pressure += 16.0 if continuation_quality >= 65 else 0.0
    euphoria_pressure += 16.0 if exhaustion_risk >= 55 else 0.0

    accumulation_pressure = 0.0
    accumulation_pressure += 32.0 if accumulation_signal >= 55 else 16.0 if accumulation_signal >= 35 else 0.0
    accumulation_pressure += 16.0 if "EARLY_ACCUMULATION" in state_set else 0.0
    accumulation_pressure += 12.0 if max_rvol >= 1.5 and max_oe < 25 else 0.0
    accumulation_pressure += 8.0 if spread < 8.0 and continuation_quality >= 45 else 0.0

    distribution_pressure = 0.0
    distribution_pressure += 24.0 if exhaustion_risk >= 65 else 12.0 if exhaustion_risk >= 45 else 0.0
    distribution_pressure += 18.0 if max_oe >= 45 and volume_confirmation < 45 else 0.0
    distribution_pressure += 14.0 if fake_move_risk >= 55 else 0.0

    if mode == "extreme":
        # Extreme mode reacts earlier to regime shifts, but still keeps risk visible.
        panic_pressure += 4.0 if volatility_expansion >= 45 else 0.0
        trend_strength += 4.0 if continuation_quality >= 55 and volume_confirmation >= 45 else 0.0
        chop_index += 4.0 if fake_move_risk >= 45 else 0.0

    scores = {
        "trend": trend_strength,
        "range": range_pressure,
        "chop": chop_index,
        "volatile": panic_pressure,
        "euphoria": euphoria_pressure,
        "accumulation": accumulation_pressure,
        "distribution": distribution_pressure,
    }
    scores = {k: round(max(0.0, min(100.0, v)), 1) for k, v in scores.items()}

    # Priority matters: some phases are more safety-critical than raw score rank.
    if scores["euphoria"] >= 58 and exhaustion_risk >= 55:
        regime = "euphoria_exhaustion"
        label = "Euphoria / exhaustion risk"
        tone = "risk_off"
    elif scores["volatile"] >= 62 and (fake_move_risk >= 50 or exhaustion_risk >= 50):
        regime = "volatile_panic"
        label = "Volatile / panic-like expansion"
        tone = "risk_off"
    elif scores["distribution"] >= 55:
        regime = "distribution"
        label = "Distribution / fading participation"
        tone = "caution"
    elif scores["accumulation"] >= 55:
        regime = "accumulation"
        label = "Accumulation / volume build"
        tone = "constructive_watch"
    elif scores["trend"] >= 60 and continuation_quality >= 55:
        regime = "trend"
        label = "Trend / continuation regime"
        tone = "constructive"
    elif scores["range"] >= 48 and corr >= 0.7:
        regime = "range"
        label = "Range / mean-reversion regime"
        tone = "mean_reversion"
    elif scores["chop"] >= 50:
        regime = "chop"
        label = "Chop / low-confirmation regime"
        tone = "caution"
    elif scores["volatile"] >= 50:
        regime = "volatile"
        label = "Volatile expansion regime"
        tone = "caution"
    else:
        regime = "mixed"
        label = "Mixed / neutral regime"
        tone = "neutral"

    confidence = max(scores.values()) if scores else 0.0
    # Reduce confidence when top regimes are too close to each other.
    ranked = sorted(scores.values(), reverse=True)
    if len(ranked) >= 2 and (ranked[0] - ranked[1]) < 8:
        confidence = max(0.0, confidence - 8.0)
    confidence = round(max(0.0, min(100.0, confidence)), 1)

    ai_hint_by_regime = {
        "trend": "Continuation can be respected more when participation stays firm; invalidation should focus on failed follow-through.",
        "range": "Mean reversion is favored over chasing direction; stretched moves should be treated as reactive until confirmed.",
        "chop": "Low-confirmation conditions increase fake-move risk; avoid overstating directional conviction.",
        "volatile": "Movement potential is high, but signal quality can change quickly; risk language should stay prominent.",
        "volatile_panic": "Volatility dominates the read; emphasize instability, fake-move risk and fast invalidation.",
        "euphoria_exhaustion": "Momentum may still move, but exhaustion and blow-off risk should be highlighted.",
        "accumulation": "Early participation build is constructive, but confirmation still matters before calling a clean trend.",
        "distribution": "Participation quality is fading; rallies can be unstable and reversal risk should be emphasized.",
        "mixed": "No dominant regime is clean enough; keep the read balanced and confirmation-dependent.",
    }

    return {
        "regime": regime,
        "label": label,
        "tone": tone,
        "confidence": confidence,
        "scores": scores,
        "ai_hint": ai_hint_by_regime.get(regime, ai_hint_by_regime["mixed"]),
        "display_in_ui": False,
        "meaning": "Internal AI Insight market-phase context only — not a buy/sell signal and not a profit guarantee.",
    }


def _liquidity_trap_context_from_behavior_inputs(
    *,
    corr: float = 0.0,
    spread: float = 0.0,
    rsi_gap: float = 0.0,
    momentum_gap: float = 0.0,
    score_pair: float = 0.0,
    max_rvol: float = 0.0,
    max_oe: float = 0.0,
    fake_move_risk: float = 0.0,
    exhaustion_risk: float = 0.0,
    continuation_quality: float = 0.0,
    volatility_expansion: float = 0.0,
    volume_confirmation: float = 0.0,
    states: list | None = None,
    ai_mode: str = "standard",
) -> dict:
    """Liquidity / Trap Detection Layer for AI Insight only.

    This does not try to read an order book. It infers liquidity fragility from the
    data Nexus already has: spread expansion, RSI divergence, momentum gap, RVOL,
    overextension, market-condition state and behavior flags. The result is internal
    context for AI wording, not a buy/sell signal and not a profit guarantee.
    """
    mode = _normalize_ai_mode(ai_mode)
    states_u = [str(x or "").upper() for x in (states or []) if str(x or "").strip()]

    corr = _safe_float(corr, 0.0)
    spread = abs(_safe_float(spread, 0.0))
    rsi_gap = abs(_safe_float(rsi_gap, 0.0))
    momentum_gap = abs(_safe_float(momentum_gap, 0.0))
    score_pair = _safe_float(score_pair, 0.0)
    max_rvol = _safe_float(max_rvol, 0.0)
    max_oe = abs(_safe_float(max_oe, 0.0))
    fake_move_risk = _safe_float(fake_move_risk, 0.0)
    exhaustion_risk = _safe_float(exhaustion_risk, 0.0)
    continuation_quality = _safe_float(continuation_quality, 0.0)
    volatility_expansion = _safe_float(volatility_expansion, 0.0)
    volume_confirmation = _safe_float(volume_confirmation, 0.0)

    trap_risk = 0.0
    stop_hunt_risk = 0.0
    liquidity_vacuum_risk = 0.0
    participation_depth = 45.0
    liquidity_quality_score = 55.0
    reasons = []
    warnings = []

    # Wide movement with weak participation often behaves like fragile liquidity.
    if spread >= 8.0:
        liquidity_vacuum_risk += 26.0
        trap_risk += 10.0
        liquidity_quality_score -= 12.0
        reasons.append("wide spread expansion suggests thin liquidity around the move")
    elif spread >= 4.0:
        liquidity_vacuum_risk += 14.0
        liquidity_quality_score -= 6.0
        reasons.append("spread expansion is building")

    if momentum_gap >= 5.0:
        liquidity_vacuum_risk += 16.0
        stop_hunt_risk += 8.0
        reasons.append("fast relative momentum gap")
    elif momentum_gap >= 2.5:
        liquidity_vacuum_risk += 8.0

    if rsi_gap >= 18.0:
        trap_risk += 14.0
        stop_hunt_risk += 12.0
        liquidity_quality_score -= 6.0
        warnings.append("large RSI imbalance can invite reversal or stop-hunt behavior")
    elif rsi_gap >= 10.0:
        trap_risk += 6.0

    if max_rvol <= 0:
        participation_depth -= 8.0
    elif max_rvol < 1.15 and (spread >= 4.0 or momentum_gap >= 3.0):
        trap_risk += 26.0
        liquidity_vacuum_risk += 12.0
        participation_depth -= 24.0
        liquidity_quality_score -= 18.0
        warnings.append("movement is not strongly participation-backed")
    elif max_rvol >= 2.0:
        participation_depth += 24.0
        liquidity_quality_score += 14.0
        trap_risk -= 8.0
        reasons.append("relative volume improves participation depth")
    elif max_rvol >= 1.4:
        participation_depth += 12.0
        liquidity_quality_score += 6.0

    if max_oe >= 55.0:
        stop_hunt_risk += 18.0
        trap_risk += 10.0
        liquidity_quality_score -= 10.0
        warnings.append("overextension raises rejection sensitivity")
    elif max_oe >= 30.0:
        stop_hunt_risk += 8.0

    if "FAKE_MOVE" in states_u:
        trap_risk += 30.0
        participation_depth -= 16.0
        liquidity_quality_score -= 16.0
        warnings.append("market condition already flags weak/fake-move behavior")
    if "OVEREXTENDED" in states_u:
        stop_hunt_risk += 14.0
        trap_risk += 8.0
    if "REAL_BREAKOUT" in states_u:
        trap_risk -= 12.0
        participation_depth += 14.0
        liquidity_quality_score += 10.0
        reasons.append("breakout has better volume support")
    if "EARLY_ACCUMULATION" in states_u:
        trap_risk -= 6.0
        participation_depth += 10.0
        reasons.append("early accumulation improves structure stability")

    if corr < 0.45 and corr > 0:
        trap_risk += 10.0
        warnings.append("weak pair linkage makes trap signals less reliable")
    elif corr >= 0.75 and spread >= 3.0:
        # Strong linkage plus spread can be tradable, but also mean-reversion sensitive.
        stop_hunt_risk += 5.0

    trap_risk += max(0.0, fake_move_risk - 50.0) * 0.45
    stop_hunt_risk += max(0.0, exhaustion_risk - 50.0) * 0.35
    liquidity_vacuum_risk += max(0.0, volatility_expansion - 50.0) * 0.35

    if continuation_quality >= 70 and volume_confirmation >= 55:
        trap_risk -= 12.0
        liquidity_quality_score += 10.0
    elif continuation_quality < 45 and volatility_expansion >= 45:
        trap_risk += 10.0
        warnings.append("movement quality is unstable versus expansion")

    if score_pair < 65 and (spread >= 5.0 or momentum_gap >= 3.0):
        trap_risk += 8.0
        warnings.append("lower-ranked pair movement may be less structurally reliable")

    if mode == "extreme":
        trap_risk += 4.0 if (spread >= 4.0 or momentum_gap >= 3.0) else 0.0
        liquidity_vacuum_risk += 4.0 if volatility_expansion >= 45.0 else 0.0

    trap_risk = round(max(0.0, min(100.0, trap_risk)), 1)
    stop_hunt_risk = round(max(0.0, min(100.0, stop_hunt_risk)), 1)
    liquidity_vacuum_risk = round(max(0.0, min(100.0, liquidity_vacuum_risk)), 1)
    participation_depth = round(max(0.0, min(100.0, participation_depth)), 1)
    liquidity_quality_score = round(max(0.0, min(100.0, liquidity_quality_score)), 1)

    if trap_risk >= 70 or liquidity_vacuum_risk >= 70:
        regime = "liquidity_trap_watch"
        label = "Liquidity trap watch"
        tone = "defensive"
    elif stop_hunt_risk >= 65:
        regime = "stop_hunt_sensitive"
        label = "Stop-hunt sensitive"
        tone = "caution"
    elif liquidity_vacuum_risk >= 55:
        regime = "liquidity_vacuum"
        label = "Liquidity vacuum risk"
        tone = "caution"
    elif participation_depth >= 65 and liquidity_quality_score >= 60:
        regime = "participation_supported"
        label = "Participation-supported movement"
        tone = "constructive"
    else:
        regime = "balanced_liquidity"
        label = "Balanced liquidity context"
        tone = "neutral"

    if not reasons:
        reasons.append("no dominant liquidity trap pattern detected")

    ai_hint_by_regime = {
        "liquidity_trap_watch": "Describe the move as fragile or trap-sensitive; emphasize confirmation quality rather than direction.",
        "stop_hunt_sensitive": "Mention rejection sensitivity or stop-hunt behavior if the setup is stretched or volatile.",
        "liquidity_vacuum": "Frame expansion as fast but potentially thin; avoid implying clean continuation without participation.",
        "participation_supported": "Acknowledge that participation quality reduces trap risk, while keeping the wording probabilistic.",
        "balanced_liquidity": "Keep liquidity wording subtle; do not overstate trap risk.",
    }

    return {
        "regime": regime,
        "label": label,
        "tone": tone,
        "trap_risk": trap_risk,
        "trap_level": _behavior_level(trap_risk),
        "stop_hunt_risk": stop_hunt_risk,
        "stop_hunt_level": _behavior_level(stop_hunt_risk),
        "liquidity_vacuum_risk": liquidity_vacuum_risk,
        "liquidity_vacuum_level": _behavior_level(liquidity_vacuum_risk),
        "participation_depth": participation_depth,
        "participation_depth_level": _behavior_level(participation_depth),
        "liquidity_quality_score": liquidity_quality_score,
        "liquidity_quality_level": _behavior_level(liquidity_quality_score),
        "reasons": reasons[:6],
        "warnings": warnings[:6],
        "ai_hint": ai_hint_by_regime.get(regime, ai_hint_by_regime["balanced_liquidity"]),
        "display_in_ui": False,
        "meaning": "Internal AI Insight liquidity/trap context only — not a buy/sell signal and not a profit guarantee.",
    }


def _market_behavior_from_pair(pair_ctx: dict | None, coins: list, ai_mode: str = "standard") -> dict:
    """Market Behavior Detection Layer for AI Insight.

    This layer classifies what the current movement may represent. It is deliberately
    informational only: it does not produce buy/sell instructions and does not replace
    the Movement Chance score. It adds context such as fake-move risk, exhaustion risk,
    continuation quality, accumulation pressure, and market regime.
    """
    p = pair_ctx if isinstance(pair_ctx, dict) else {}
    mode = _normalize_ai_mode(ai_mode)
    pair = str(p.get("pair") or "").upper().strip()
    parts = [x.strip() for x in pair.split("/") if x.strip()]

    coin_map = {}
    for c in coins if isinstance(coins, list) else []:
        if isinstance(c, dict):
            sym = str(c.get("symbol") or "").upper().strip()
            if sym:
                coin_map[sym] = c

    ca = coin_map.get(parts[0], {}) if len(parts) >= 1 else {}
    cb = coin_map.get(parts[1], {}) if len(parts) >= 2 else {}

    corr = _safe_float(p.get("corr"), 0.0)
    spread = abs(_safe_float(p.get("spread_pct"), 0.0))
    rsi_gap = abs(_safe_float(p.get("rsi_gap"), 0.0))
    score_pair = _safe_float(p.get("score"), 0.0)
    ch_a = _safe_float(ca.get("change_24h_pct"), 0.0)
    ch_b = _safe_float(cb.get("change_24h_pct"), 0.0)
    momentum_gap = abs(ch_a - ch_b)

    fake_move_risk = 0.0
    exhaustion_risk = 0.0
    continuation_quality = 35.0
    accumulation_signal = 0.0
    volatility_expansion = 0.0
    volume_confirmation = 0.0
    reasons = []
    warnings = []

    states = []
    oe_values = []
    rvol_values = []
    for c in (ca, cb):
        mc = c.get("market_condition") if isinstance(c.get("market_condition"), dict) else {}
        state = str(mc.get("state") or "").upper()
        if state:
            states.append(state)
        oe_raw = mc.get("oe_pct")
        rv_raw = mc.get("rvol")
        try:
            oe = float(oe_raw)
            if math.isfinite(oe):
                oe_values.append(oe)
        except Exception:
            pass
        try:
            rv = float(rv_raw)
            if math.isfinite(rv):
                rvol_values.append(rv)
        except Exception:
            pass

        if state == "FAKE_MOVE":
            fake_move_risk += 32.0
            continuation_quality -= 18.0
            warnings.append(f"{c.get('symbol')}: weak/fake-move structure")
        elif state == "REAL_BREAKOUT":
            continuation_quality += 22.0
            volume_confirmation += 28.0
            volatility_expansion += 12.0
            reasons.append(f"{c.get('symbol')}: volume-backed breakout")
        elif state == "EARLY_ACCUMULATION":
            accumulation_signal += 32.0
            continuation_quality += 12.0
            volume_confirmation += 18.0
            reasons.append(f"{c.get('symbol')}: early accumulation / volume build")
        elif state == "OVEREXTENDED":
            exhaustion_risk += 28.0
            fake_move_risk += 8.0
            continuation_quality -= 10.0
            warnings.append(f"{c.get('symbol')}: overextended")

    max_rvol = max(rvol_values) if rvol_values else 0.0
    max_oe = max([abs(x) for x in oe_values], default=0.0)

    if max_rvol >= 2.0:
        volume_confirmation += 18.0
        volatility_expansion += 10.0
        reasons.append("relative volume expansion")
    elif max_rvol > 0 and max_rvol < 1.2 and (spread >= 4.0 or momentum_gap >= 3.0):
        fake_move_risk += 18.0
        continuation_quality -= 10.0
        warnings.append("movement lacks strong relative-volume confirmation")

    if max_oe >= 55:
        exhaustion_risk += 20.0
        warnings.append("large overextension increases exhaustion risk")
    elif 30 <= max_oe < 55 and max_rvol >= 1.5:
        continuation_quality += 8.0
        reasons.append("extension is supported by participation")

    if spread >= 8.0:
        volatility_expansion += 24.0
        exhaustion_risk += 8.0
        reasons.append("wide spread expansion")
    elif spread >= 4.0:
        volatility_expansion += 14.0
        reasons.append("spread expansion building")

    if rsi_gap >= 18.0:
        volatility_expansion += 14.0
        exhaustion_risk += 10.0
        reasons.append("large RSI divergence")
    elif rsi_gap >= 10.0:
        volatility_expansion += 8.0
        reasons.append("RSI divergence building")

    if momentum_gap >= 5.0:
        volatility_expansion += 16.0
        continuation_quality += 6.0
        reasons.append("short-term momentum gap")
    elif momentum_gap >= 2.5:
        volatility_expansion += 8.0

    if corr >= 0.75 and spread >= 3.0:
        continuation_quality += 6.0
        reasons.append("linked pair with relative imbalance")
    elif corr > 0 and corr < 0.45:
        fake_move_risk += 10.0
        warnings.append("weak pair linkage reduces signal reliability")

    if score_pair < 65 and (spread >= 5.0 or rsi_gap >= 14.0):
        volatility_expansion += 8.0
        reasons.append("lower-ranked pair shows unusual movement")

    if mode == "extreme":
        volatility_expansion *= 1.08
        continuation_quality += 4.0 if (spread >= 4.0 or momentum_gap >= 3.0) else 0.0
        fake_move_risk += 4.0 if max_rvol < 1.2 and (spread >= 4.0 or momentum_gap >= 3.0) else 0.0

    fake_move_risk = round(max(0.0, min(100.0, fake_move_risk)), 1)
    exhaustion_risk = round(max(0.0, min(100.0, exhaustion_risk)), 1)
    continuation_quality = round(max(0.0, min(100.0, continuation_quality)), 1)
    accumulation_signal = round(max(0.0, min(100.0, accumulation_signal)), 1)
    volatility_expansion = round(max(0.0, min(100.0, volatility_expansion)), 1)
    volume_confirmation = round(max(0.0, min(100.0, volume_confirmation)), 1)

    market_phase = _market_phase_from_behavior_inputs(
        corr=corr,
        spread=spread,
        rsi_gap=rsi_gap,
        momentum_gap=momentum_gap,
        score_pair=score_pair,
        max_rvol=max_rvol,
        max_oe=max_oe,
        states=states,
        fake_move_risk=fake_move_risk,
        exhaustion_risk=exhaustion_risk,
        continuation_quality=continuation_quality,
        accumulation_signal=accumulation_signal,
        volatility_expansion=volatility_expansion,
        volume_confirmation=volume_confirmation,
        ai_mode=mode,
    )

    liquidity_context = _liquidity_trap_context_from_behavior_inputs(
        corr=corr,
        spread=spread,
        rsi_gap=rsi_gap,
        momentum_gap=momentum_gap,
        score_pair=score_pair,
        max_rvol=max_rvol,
        max_oe=max_oe,
        states=states,
        fake_move_risk=fake_move_risk,
        exhaustion_risk=exhaustion_risk,
        continuation_quality=continuation_quality,
        volatility_expansion=volatility_expansion,
        volume_confirmation=volume_confirmation,
        ai_mode=mode,
    )

    # Legacy behavior regime remains compatible, while the richer market phase
    # becomes the primary label for AI Insight interpretation.
    if fake_move_risk >= 60 and exhaustion_risk >= 55:
        behavior_regime = "overheated_fake_move_risk"
        behavior_label = "Overheated / fake-move risk"
    elif continuation_quality >= 70 and volume_confirmation >= 45:
        behavior_regime = "volume_backed_continuation"
        behavior_label = "Volume-backed continuation"
    elif accumulation_signal >= 50:
        behavior_regime = "early_accumulation"
        behavior_label = "Early accumulation"
    elif exhaustion_risk >= 60:
        behavior_regime = "momentum_exhaustion"
        behavior_label = "Momentum exhaustion risk"
    elif volatility_expansion >= 55:
        behavior_regime = "volatility_expansion"
        behavior_label = "Volatility expansion"
    elif fake_move_risk >= 50:
        behavior_regime = "fake_move_watch"
        behavior_label = "Fake-move watch"
    else:
        behavior_regime = "mixed_or_neutral"
        behavior_label = "Mixed / neutral behavior"

    regime = str(market_phase.get("regime") or behavior_regime)
    label = str(market_phase.get("label") or behavior_label)

    if not reasons:
        reasons.append("no strong behavior pattern detected")

    return {
        "pair": pair,
        "regime": regime,
        "label": label,
        "meaning": "Market behavior context only — not a buy/sell instruction and not a profit guarantee.",
        "market_phase": market_phase,
        "liquidity_context": liquidity_context,
        "liquidity_regime": liquidity_context.get("regime"),
        "liquidity_label": liquidity_context.get("label"),
        "trap_risk": liquidity_context.get("trap_risk"),
        "stop_hunt_risk": liquidity_context.get("stop_hunt_risk"),
        "liquidity_vacuum_risk": liquidity_context.get("liquidity_vacuum_risk"),
        "participation_depth": liquidity_context.get("participation_depth"),
        "liquidity_quality_score": liquidity_context.get("liquidity_quality_score"),
        "market_phase_regime": market_phase.get("regime"),
        "market_phase_label": market_phase.get("label"),
        "market_phase_confidence": market_phase.get("confidence"),
        "market_phase_tone": market_phase.get("tone"),
        "behavior_regime": behavior_regime,
        "behavior_label": behavior_label,
        "fake_move_risk": fake_move_risk,
        "fake_move_level": _behavior_level(fake_move_risk),
        "exhaustion_risk": exhaustion_risk,
        "exhaustion_level": _behavior_level(exhaustion_risk),
        "continuation_quality": continuation_quality,
        "continuation_level": _behavior_level(continuation_quality),
        "accumulation_signal": accumulation_signal,
        "accumulation_level": _behavior_level(accumulation_signal),
        "volatility_expansion": volatility_expansion,
        "volatility_level": _behavior_level(volatility_expansion),
        "volume_confirmation": volume_confirmation,
        "volume_confirmation_level": _behavior_level(volume_confirmation),
        "metrics": {
            "corr": round(corr, 4),
            "spread_pct": round(spread, 2),
            "rsi_gap": round(rsi_gap, 2),
            "momentum_gap_24h_pct": round(momentum_gap, 2),
            "max_rvol": round(max_rvol, 2),
            "max_overextension_pct": round(max_oe, 2),
            "pair_rank_score": round(score_pair, 2),
        },
        "reasons": reasons[:6],
        "warnings": warnings[:6],
    }


def _build_ai_pair_alerts(pairs: list, coins: list, compare_weights: dict | None = None, ai_mode: str = "standard") -> list[dict]:
    """Scan all Compare pairs for hidden opportunities, not only the selected/top pair.
    Deterministic and informational only; no buy/sell instructions.
    """
    if not isinstance(pairs, list):
        return []
    mode = _normalize_ai_mode(ai_mode)
    w = _normalize_compare_weights_for_ai(compare_weights or {})
    sens = 0.82 if mode == "extreme" else 1.0

    coin_map = {}
    for c in coins if isinstance(coins, list) else []:
        if isinstance(c, dict):
            sym = str(c.get("symbol") or "").upper()
            if sym:
                coin_map[sym] = c

    alerts = []
    for p in pairs[:30]:
        if not isinstance(p, dict):
            continue
        pair = str(p.get("pair") or "").upper()
        parts = [x.strip() for x in pair.split("/") if x.strip()]
        if len(parts) != 2:
            continue
        a, b = parts
        corr = _safe_float(p.get("corr"), 0.0)
        spread = abs(_safe_float(p.get("spread_pct"), 0.0))
        rsi_gap = abs(_safe_float(p.get("rsi_gap"), 0.0))
        score = _safe_float(p.get("score"), 0.0)
        ca, cb = coin_map.get(a, {}), coin_map.get(b, {})
        ch_gap = abs(_safe_float(ca.get("change_24h_pct"), 0.0) - _safe_float(cb.get("change_24h_pct"), 0.0))

        reasons = []
        kind = ""
        base = score
        if corr >= 0.72 * sens and spread >= 5.0 * sens:
            kind = "movement_chance"
            reasons.append("wide spread movement inside a linked pair")
            base += w.get("opportunity", 25) * 0.28
        if rsi_gap >= 14.0 * sens:
            kind = kind or "rsi_divergence"
            reasons.append("large RSI difference")
            base += w.get("momentum", 25) * 0.24
        if ch_gap >= 4.0 * sens:
            kind = kind or "momentum_shift"
            reasons.append("short-term momentum shift")
            base += w.get("momentum", 25) * 0.18
        if corr >= 0.82 * sens and 2.0 * sens <= spread <= 7.5 / sens:
            kind = kind or "rebound_watch"
            reasons.append("rebound / mean-reversion structure")
            base += w.get("corr", 35) * 0.14
        if score < 70 and (spread >= 6.0 * sens or rsi_gap >= 16.0 * sens):
            kind = kind or "low_rank_movement"
            reasons.append("lower-ranked pair showing unusual movement")
            base += 8

        movement = _build_movement_chance_score(p, coins, compare_weights=w, ai_mode=mode)
        movement_score = _safe_float(movement.get("score"), 0.0)

        # Keep the existing alert filter, but require at least a real movement-watch score.
        if not reasons and movement_score < 32:
            continue
        if not reasons:
            reasons = list(movement.get("reasons") or [])[:4]
            kind = kind or "movement_watch"

        strength_score = round(max(0.0, min(100.0, max(base, movement_score))), 1)
        level = str(movement.get("level") or _movement_level_from_score(strength_score))
        if level in ("very_high", "high"):
            strength = "high"
        elif level == "medium":
            strength = "medium"
        else:
            strength = "low"
        alerts.append({
            "pair": pair,
            "type": kind or "movement_alert",
            "label": movement.get("label") or _movement_alert_label(level),
            "strength": strength,
            "level": level,
            "score": strength_score,
            "movement_chance_score": movement_score,
            "movement_chance_level": level,
            "movement_chance": movement,
            "meaning": movement.get("meaning"),
            "corr": round(corr, 4),
            "spread_pct": round(spread, 2),
            "rsi_gap": round(rsi_gap, 2),
            "reasons": list(dict.fromkeys((reasons or []) + list(movement.get("reasons") or [])))[:6],
        })

    alerts.sort(key=lambda x: (x.get("movement_chance_score") or x.get("score") or 0), reverse=True)
    return alerts[:8]





def _ai_engine_v2_from_context(
    sym_norm: list[str],
    market_context: dict,
    timeframe_context: dict,
    order_memory: dict | None = None,
    insight_profile: dict | None = None,
    extra_context: dict | None = None,
) -> dict:
    """AI Engine V2 + Exit Risk + Contradiction Detection.

    Deterministic decision-support layer used by AI Insight.
    No trading instructions; it only classifies structure, risk, confidence,
    contradictions, and pre-exit warning from existing app data.
    """
    extra_context = extra_context if isinstance(extra_context, dict) else {}
    coins = extra_context.get("coins") if isinstance(extra_context.get("coins"), list) else []
    pairs = extra_context.get("relevant_pairs") if isinstance(extra_context.get("relevant_pairs"), list) else []
    all_pairs = extra_context.get("all_compare_pairs") if isinstance(extra_context.get("all_compare_pairs"), list) else pairs
    ai_mode = _normalize_ai_mode(extra_context.get("ai_mode") or extra_context.get("mode"))
    compare_weights = _normalize_compare_weights_for_ai(extra_context.get("compare_weights") or extra_context.get("weights") or {})

    def _sf(v, default=0.0):
        try:
            x = float(v)
            return x if math.isfinite(x) else float(default)
        except Exception:
            return float(default)

    def _coin(sym: str) -> dict:
        su = str(sym or "").upper()
        for c in coins:
            if isinstance(c, dict) and str(c.get("symbol") or "").upper() == su:
                return c
        return {}

    a = str((sym_norm or [""])[0] or "").upper()
    b = str((sym_norm or ["", ""])[1] if len(sym_norm or []) > 1 else "").upper()
    ca = _coin(a)
    cb = _coin(b)

    pair_ctx = {}
    if a and b:
        target = f"{a}/{b}"
        rev = f"{b}/{a}"
        for p in pairs:
            if not isinstance(p, dict):
                continue
            pp = str(p.get("pair") or "").upper()
            if pp in (target, rev):
                pair_ctx = p
                break

    corr = _sf(pair_ctx.get("corr"), 0.0) if pair_ctx else 0.0
    spread = _sf(pair_ctx.get("spread_pct"), 0.0) if pair_ctx else 0.0
    score_pair = _sf(pair_ctx.get("score"), 0.0) if pair_ctx else 0.0

    score_a = _sf(ca.get("score"), 0.0)
    score_b = _sf(cb.get("score"), 0.0)
    rating_a = str(ca.get("rating") or "n/a")
    rating_b = str(cb.get("rating") or "n/a")
    votes_a = int(_sf(ca.get("user_rating_votes"), 0))
    votes_b = int(_sf(cb.get("user_rating_votes"), 0))
    ch_a = _sf(ca.get("change_24h_pct"), 0.0)
    ch_b = _sf(cb.get("change_24h_pct"), 0.0)

    rel_strength = ""
    if a and b:
        if score_a >= score_b + 8 or ch_a >= ch_b + 2:
            rel_strength = a
        elif score_b >= score_a + 8 or ch_b >= ch_a + 2:
            rel_strength = b

    mc_notes = []
    weak_participation = False
    volume_backed = False
    accumulation = False
    overextended = False
    for c in (ca, cb):
        mc = c.get("market_condition") if isinstance(c.get("market_condition"), dict) else {}
        state = str(mc.get("state") or "").upper()
        label = str(mc.get("label") or state or "").strip()
        oe = mc.get("oe_pct")
        rv = mc.get("rvol")
        if state:
            mc_notes.append(f"{c.get('symbol')}: {label} OE={oe} RVOL={rv}")
        if state in ("FAKE_MOVE",):
            weak_participation = True
        if state in ("REAL_BREAKOUT",):
            volume_backed = True
        if state in ("EARLY_ACCUMULATION",):
            accumulation = True
        if state in ("OVEREXTENDED",):
            overextended = True

    onchain_notes = []
    onchain_positive = False
    onchain_neutral = True
    for c in (ca, cb):
        oc = c.get("onchain") if isinstance(c.get("onchain"), dict) else {}
        delta = _sf(c.get("onchain_delta"), _sf(oc.get("score_delta"), 0))
        summary = str(oc.get("summary") or "").strip()
        if abs(delta) >= 3 or summary:
            onchain_neutral = False
            onchain_notes.append(f"{c.get('symbol')}: {summary or ('on-chain delta ' + str(delta))}")
            if delta > 0:
                onchain_positive = True

    drivers = []
    warnings = []
    contradictions = []
    exit_reasons = []
    tags = []

    price_moving = abs(ch_a) >= 2 or abs(ch_b) >= 2
    divergent_24h = bool(a and b and abs(ch_a - ch_b) >= 3)
    weight_focus = _weight_focus(compare_weights)

    if compare_weights.get("momentum", 0) >= 35 and (rel_strength or divergent_24h):
        drivers.append("custom weighting emphasizes momentum")
        tags.append("weight_momentum_focus")
    if compare_weights.get("opportunity", 0) >= 35 and abs(spread) >= 3:
        drivers.append("custom weighting emphasizes opportunity spread")
        tags.append("weight_opportunity_focus")
    if compare_weights.get("stability", 0) >= 30 and corr < 0.65 and pair_ctx:
        warnings.append("custom weighting emphasizes stability, but pair linkage is not strong")
        tags.append("weight_stability_conflict")

    if corr >= 0.8:
        drivers.append("high pair linkage")
        tags.append("high_correlation")
    elif corr < 0.45 and corr != 0:
        warnings.append("weak pair linkage can cause unstable divergence")
        contradictions.append("pair relationship is weak, so spread signals are less reliable")
        tags.append("weak_correlation")

    if abs(spread) >= 8:
        drivers.append("wide relative spread")
        exit_reasons.append("spread is already stretched")
        tags.append("wide_spread")
    elif abs(spread) < 1 and pair_ctx:
        warnings.append("spread is narrow, reducing edge quality")
        tags.append("narrow_spread")

    if rel_strength:
        drivers.append(f"relative strength favors {rel_strength}")
        tags.append("relative_strength")

    if weak_participation:
        warnings.append("market condition shows weak participation / fake-move risk")
        contradictions.append("price structure is moving without strong participation")
        exit_reasons.append("participation is weak behind the move")
        tags.append("weak_participation")
    if volume_backed:
        drivers.append("market condition shows volume-backed momentum")
        tags.append("volume_backed")
    if accumulation:
        drivers.append("market condition shows early accumulation / volume build")
        tags.append("accumulation")
    if overextended:
        warnings.append("overextension increases pullback or exhaustion risk")
        exit_reasons.append("overextension increases exhaustion risk")
        tags.append("overextended")

    if price_moving and onchain_neutral:
        contradictions.append("price action is moving while on-chain confirmation is neutral")
        tags.append("price_onchain_divergence")
    if divergent_24h and corr >= 0.8:
        contradictions.append("high correlation conflicts with short-term relative divergence")
        tags.append("correlation_divergence")
    if (score_a >= 70 or score_b >= 70) and onchain_neutral:
        contradictions.append("rating quality is not strongly confirmed by on-chain data")
        tags.append("rating_onchain_mismatch")

    if onchain_positive:
        drivers.append("on-chain provides supporting confirmation")
        tags.append("onchain_support")
    elif onchain_neutral:
        warnings.append("on-chain confirmation is neutral / no strong signal")
        exit_reasons.append("on-chain does not strongly confirm continuation")
        tags.append("onchain_neutral")

    if votes_a + votes_b <= 2:
        warnings.append("community input is still thin")
        tags.append("thin_community")

    # Exit Risk / Pre-Exit warning
    exit_score = 0
    if weak_participation:
        exit_score += 2
    if overextended:
        exit_score += 2
    if abs(spread) >= 10:
        exit_score += 2
    elif abs(spread) >= 6:
        exit_score += 1
    if price_moving and onchain_neutral:
        exit_score += 1
    if divergent_24h and corr >= 0.8:
        exit_score += 1
    if volume_backed:
        exit_score -= 1
    if accumulation:
        exit_score -= 1
    exit_score = max(0, exit_score)

    if exit_score >= 5:
        exit_risk = "High"
    elif exit_score >= 3:
        exit_risk = "Medium-High"
    elif exit_score >= 1:
        exit_risk = "Medium"
    else:
        exit_risk = "Low"

    pre_exit_warning = bool(exit_score >= 3)
    if pre_exit_warning:
        tags.append("pre_exit_warning")
        warnings.append("pre-exit warning: continuation quality is weakening")

    # Behavior + setup bias
    if corr >= 0.8 and abs(spread) >= 2:
        behavior = "mean-reversion style with visible relative imbalance"
        setup_bias = "mean-reversion / grid-friendly"
        verdict = "MEAN REVERSION"
    elif rel_strength and (volume_backed or abs(ch_a - ch_b) >= 3):
        behavior = "rotation / trend-bias with continuation risk"
        setup_bias = "rotation / continuation-risk"
        verdict = "TREND BIAS"
    elif corr < 0.45 and pair_ctx:
        behavior = "unstable / choppy pair behavior"
        setup_bias = "no-clean-setup"
        verdict = "NO CLEAN SETUP"
    elif accumulation:
        behavior = "early accumulation / developing structure"
        setup_bias = "accumulation-watch / volatility-sensitive"
        verdict = "EARLY ACCUMULATION"
    else:
        behavior = "mixed / low-conviction structure"
        setup_bias = "no-clean-setup / wait-for-confirmation"
        verdict = "LOW CONVICTION"

    movement_chance = _build_movement_chance_score(pair_ctx, coins, compare_weights=compare_weights, ai_mode=ai_mode) if pair_ctx else {
        "pair": f"{a}/{b}" if a and b else "",
        "score": 0.0,
        "level": "quiet",
        "label": "Movement Watch",
        "meaning": "Movement potential only — not a buy signal, not a quality rating, and not a success probability.",
        "components": {},
        "metrics": {},
        "reasons": ["no pair context available"],
    }
    movement_score = _safe_float(movement_chance.get("score"), 0.0)
    if movement_score >= 68:
        drivers.append(f"movement chance score is elevated ({movement_score}/100)")
        tags.append("movement_chance")
    elif movement_score >= 50:
        drivers.append(f"movement chance score is building ({movement_score}/100)")
        tags.append("movement_watch")

    market_behavior = _market_behavior_from_pair(pair_ctx, coins, ai_mode=ai_mode) if pair_ctx else {
        "pair": f"{a}/{b}" if a and b else "",
        "regime": "missing_pair_context",
        "label": "No pair behavior context",
        "meaning": "Market behavior context only — not a buy/sell instruction and not a profit guarantee.",
        "fake_move_risk": 0.0,
        "fake_move_level": "quiet",
        "exhaustion_risk": 0.0,
        "exhaustion_level": "quiet",
        "continuation_quality": 0.0,
        "continuation_level": "quiet",
        "accumulation_signal": 0.0,
        "accumulation_level": "quiet",
        "volatility_expansion": 0.0,
        "volatility_level": "quiet",
        "volume_confirmation": 0.0,
        "volume_confirmation_level": "quiet",
        "liquidity_context": {
            "regime": "missing_pair_context",
            "label": "No liquidity context",
            "trap_risk": 0.0,
            "stop_hunt_risk": 0.0,
            "liquidity_vacuum_risk": 0.0,
            "participation_depth": 0.0,
            "liquidity_quality_score": 0.0,
            "display_in_ui": False,
        },
        "trap_risk": 0.0,
        "stop_hunt_risk": 0.0,
        "liquidity_vacuum_risk": 0.0,
        "participation_depth": 0.0,
        "liquidity_quality_score": 0.0,
        "metrics": {},
        "reasons": ["no pair context available"],
        "warnings": [],
    }
    behavior_regime = str(market_behavior.get("regime") or "")
    if behavior_regime and behavior_regime != "mixed_or_neutral":
        drivers.append(f"market behavior: {market_behavior.get('label')}")
        tags.append(f"behavior_{behavior_regime}")
    if _safe_float(market_behavior.get("fake_move_risk"), 0.0) >= 60:
        warnings.append("fake-move risk is elevated")
        contradictions.append("movement may be poorly confirmed by volume/participation")
        exit_reasons.append("fake-move risk is elevated")
        tags.append("fake_move_risk")
    if _safe_float(market_behavior.get("exhaustion_risk"), 0.0) >= 60:
        warnings.append("momentum exhaustion risk is elevated")
        exit_reasons.append("momentum exhaustion risk is elevated")
        tags.append("exhaustion_risk")
    if _safe_float(market_behavior.get("volume_confirmation"), 0.0) >= 55:
        drivers.append("volume confirmation supports the movement")
        tags.append("volume_confirmation")

    liquidity_context = market_behavior.get("liquidity_context") if isinstance(market_behavior.get("liquidity_context"), dict) else {}
    liquidity_regime = str(liquidity_context.get("regime") or "").strip()
    trap_risk = _safe_float(liquidity_context.get("trap_risk"), 0.0)
    stop_hunt_risk = _safe_float(liquidity_context.get("stop_hunt_risk"), 0.0)
    vacuum_risk = _safe_float(liquidity_context.get("liquidity_vacuum_risk"), 0.0)
    participation_depth = _safe_float(liquidity_context.get("participation_depth"), 0.0)
    if liquidity_regime and liquidity_regime not in ("balanced_liquidity", "missing_pair_context"):
        warnings.append(f"liquidity context: {liquidity_context.get('label')}")
        tags.append(f"liquidity_{liquidity_regime}")
    if trap_risk >= 65:
        warnings.append("liquidity trap risk is elevated")
        contradictions.append("movement may be liquidity-driven rather than participation-driven")
        exit_reasons.append("liquidity trap risk is elevated")
        tags.append("liquidity_trap_risk")
    if stop_hunt_risk >= 65:
        warnings.append("stop-hunt sensitivity is elevated")
        exit_reasons.append("stop-hunt sensitivity is elevated")
        tags.append("stop_hunt_sensitive")
    if vacuum_risk >= 65:
        warnings.append("liquidity vacuum risk is elevated")
        exit_reasons.append("liquidity vacuum risk is elevated")
        tags.append("liquidity_vacuum")
    if participation_depth >= 65:
        drivers.append("participation depth improves liquidity quality")
        tags.append("participation_depth")

    pair_alerts = _build_ai_pair_alerts(all_pairs, coins, compare_weights=compare_weights, ai_mode=ai_mode)
    if pair_alerts:
        drivers.append(f"pair scanner found {len(pair_alerts)} movement-chance alert(s)")
        tags.append("pair_alerts")
        top_alert = pair_alerts[0]
        if top_alert.get("pair") and top_alert.get("pair") != f"{a}/{b}":
            warnings.append(f"strongest movement-chance alert is {top_alert.get('pair')}")

    if ai_mode == "extreme":
        tags.append("extreme_mode")
        drivers.append("Extreme mode increases sensitivity to early momentum, rebound, and spread signals")
        # Extreme mode is more willing to read early setups, but it does not hide structural danger.
        if accumulation or volume_backed or rel_strength or pair_alerts:
            exit_score = max(0, exit_score - 1)
        if not weak_participation and not overextended:
            warnings = [w for w in warnings if "pre-exit warning" not in str(w).lower()]

    # Confidence
    confidence = 5.0
    if corr >= 0.8:
        confidence += 1.2
    elif corr < 0.45 and pair_ctx:
        confidence -= 1.0
    if abs(spread) >= 2:
        confidence += 0.7
    if score_pair >= 75:
        confidence += 0.8
    if rel_strength:
        confidence += 0.4
    if weak_participation:
        confidence -= 0.8
    if onchain_positive:
        confidence += 0.4
    if onchain_neutral:
        confidence -= 0.2
    if votes_a + votes_b <= 2:
        confidence -= 0.3
    if contradictions:
        confidence -= min(1.2, 0.3 * len(contradictions))
    if compare_weights.get("momentum", 0) >= 35 and rel_strength:
        confidence += 0.3
    if compare_weights.get("opportunity", 0) >= 35 and abs(spread) >= 3:
        confidence += 0.3
    if compare_weights.get("stability", 0) >= 30 and corr < 0.65 and pair_ctx:
        confidence -= 0.4
    if ai_mode == "extreme" and (rel_strength or accumulation or volume_backed or pair_alerts):
        confidence += 0.6
    confidence = round(max(1.0, min(10.0, confidence)), 1)

    # Risk
    risk_score = 0
    if weak_participation:
        risk_score += 2
    if overextended:
        risk_score += 2
    if corr < 0.45 and pair_ctx:
        risk_score += 2
    if abs(spread) >= 12:
        risk_score += 1
    if onchain_neutral:
        risk_score += 1
    if votes_a + votes_b <= 2:
        risk_score += 1
    if contradictions:
        risk_score += min(2, len(contradictions))
    if pre_exit_warning:
        risk_score += 1
    if confidence >= 8 and risk_score > 0:
        risk_score -= 1
    if compare_weights.get("stability", 0) >= 30 and corr < 0.65 and pair_ctx:
        risk_score += 1
    if ai_mode == "extreme" and (rel_strength or accumulation or volume_backed or pair_alerts) and risk_score > 0:
        risk_score -= 1
    if _safe_float(market_behavior.get("fake_move_risk"), 0.0) >= 70:
        risk_score += 1
    if _safe_float(market_behavior.get("exhaustion_risk"), 0.0) >= 70:
        risk_score += 1
    if _safe_float(market_behavior.get("volume_confirmation"), 0.0) >= 60 and risk_score > 0:
        risk_score -= 1
    try:
        if _safe_float((market_behavior.get("liquidity_context") or {}).get("trap_risk"), 0.0) >= 70:
            risk_score += 1
        if _safe_float((market_behavior.get("liquidity_context") or {}).get("liquidity_vacuum_risk"), 0.0) >= 70:
            risk_score += 1
        if _safe_float((market_behavior.get("liquidity_context") or {}).get("participation_depth"), 0.0) >= 70 and risk_score > 0:
            risk_score -= 1
    except Exception:
        pass

    if risk_score >= 6:
        risk = "High"
    elif risk_score >= 4:
        risk = "Medium-High"
    elif risk_score >= 2:
        risk = "Medium"
    else:
        risk = "Low-Medium"

    edge = "structure does not show a clean edge yet"
    if rel_strength and verdict == "TREND BIAS":
        edge = f"relative strength currently favors {rel_strength}"
    elif "MEAN REVERSION" in verdict:
        edge = "structure favors mean-reversion inside the correlated pair"
    elif accumulation:
        edge = "volume build favors an early accumulation read"

    invalidation = "confirmation remains weak or mixed"
    if weak_participation:
        invalidation = "low RVOL / weak participation can turn the move into a fake move"
    elif contradictions:
        invalidation = contradictions[0]
    elif onchain_neutral:
        invalidation = "neutral on-chain confirmation weakens conviction"
    elif corr < 0.45 and pair_ctx:
        invalidation = "weak correlation can break the pair relationship"

    behavior_summary_parts = []
    try:
        mb_label = str(market_behavior.get("label") or "").strip()
        mb_regime = str(market_behavior.get("regime") or "").strip()
        fake_r = _safe_float(market_behavior.get("fake_move_risk"), 0.0)
        exhaust_r = _safe_float(market_behavior.get("exhaustion_risk"), 0.0)
        cont_q = _safe_float(market_behavior.get("continuation_quality"), 0.0)
        vol_c = _safe_float(market_behavior.get("volume_confirmation"), 0.0)
        acc_s = _safe_float(market_behavior.get("accumulation_signal"), 0.0)
        market_phase = market_behavior.get("market_phase") if isinstance(market_behavior.get("market_phase"), dict) else {}
        phase_label = str(market_phase.get("label") or "").strip()
        phase_hint = str(market_phase.get("ai_hint") or "").strip()
        if phase_label:
            behavior_summary_parts.append(phase_label)
        elif mb_label:
            behavior_summary_parts.append(mb_label)
        if phase_hint:
            behavior_summary_parts.append(phase_hint)
        if fake_r >= 65:
            behavior_summary_parts.append("fake-move risk is elevated / participation may be weak")
        if exhaust_r >= 65:
            behavior_summary_parts.append("momentum exhaustion risk is elevated")
        if vol_c >= 60 and cont_q >= 55:
            behavior_summary_parts.append("volume participation supports continuation quality")
        elif vol_c < 35 and (movement_score >= 60 or abs(spread) >= 4):
            behavior_summary_parts.append("movement is not strongly volume-confirmed")
        if acc_s >= 55:
            behavior_summary_parts.append("early accumulation / volume-build behavior is present")
        liquidity_context = market_behavior.get("liquidity_context") if isinstance(market_behavior.get("liquidity_context"), dict) else {}
        liq_label = str(liquidity_context.get("label") or "").strip()
        liq_hint = str(liquidity_context.get("ai_hint") or "").strip()
        trap_r = _safe_float(liquidity_context.get("trap_risk"), 0.0)
        vacuum_r = _safe_float(liquidity_context.get("liquidity_vacuum_risk"), 0.0)
        stop_r = _safe_float(liquidity_context.get("stop_hunt_risk"), 0.0)
        depth = _safe_float(liquidity_context.get("participation_depth"), 0.0)
        if liq_label and str(liquidity_context.get("regime") or "") not in ("balanced_liquidity", "missing_pair_context"):
            behavior_summary_parts.append(liq_label)
        if liq_hint and (trap_r >= 60 or vacuum_r >= 60 or stop_r >= 60):
            behavior_summary_parts.append(liq_hint)
        if trap_r >= 65:
            behavior_summary_parts.append("liquidity trap risk is elevated")
        if vacuum_r >= 65:
            behavior_summary_parts.append("movement may be thin / liquidity-vacuum sensitive")
        if stop_r >= 65:
            behavior_summary_parts.append("stop-hunt sensitivity is elevated")
        if depth >= 65:
            behavior_summary_parts.append("participation depth improves signal quality")
        if mb_regime and mb_regime not in ("mixed_or_neutral", "missing_pair_context"):
            behavior_summary_parts.append(f"regime={mb_regime}")
    except Exception:
        behavior_summary_parts = []
    market_behavior_summary = "; ".join([x for x in behavior_summary_parts if x]) or "Market behavior context is neutral or mixed."

    summary = (
        f"{verdict}: {behavior}. "
        f"Ratings: {a} {rating_a} ({int(score_a) if score_a else 'n/a'}), {b} {rating_b} ({int(score_b) if score_b else 'n/a'}). "
        f"Risk is {risk}; exit risk is {exit_risk}; confidence {confidence}/10."
    ) if a and b else f"{verdict}: {behavior}. Risk is {risk}; exit risk is {exit_risk}; confidence {confidence}/10."

    return {
        "version": "ai_engine_v2_behavior_detection_v1",
        "ai_mode": ai_mode,
        "compare_weights": compare_weights,
        "weight_focus": weight_focus,
        "pair_alerts": pair_alerts,
        "movement_chance": movement_chance,
        "movement_chance_score": movement_score,
        "movement_chance_level": movement_chance.get("level"),
        "movement_chance_label": movement_chance.get("label"),
        "market_behavior": market_behavior,
        "liquidity_context": market_behavior.get("liquidity_context"),
        "liquidity_regime": (market_behavior.get("liquidity_context") or {}).get("regime") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "liquidity_label": (market_behavior.get("liquidity_context") or {}).get("label") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "trap_risk": (market_behavior.get("liquidity_context") or {}).get("trap_risk") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "liquidity_vacuum_risk": (market_behavior.get("liquidity_context") or {}).get("liquidity_vacuum_risk") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "stop_hunt_risk": (market_behavior.get("liquidity_context") or {}).get("stop_hunt_risk") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "participation_depth": (market_behavior.get("liquidity_context") or {}).get("participation_depth") if isinstance(market_behavior.get("liquidity_context"), dict) else None,
        "market_behavior_regime": market_behavior.get("regime"),
        "market_behavior_label": market_behavior.get("label"),
        "market_phase": market_behavior.get("market_phase"),
        "market_phase_regime": (market_behavior.get("market_phase") or {}).get("regime") if isinstance(market_behavior.get("market_phase"), dict) else None,
        "market_phase_label": (market_behavior.get("market_phase") or {}).get("label") if isinstance(market_behavior.get("market_phase"), dict) else None,
        "market_phase_confidence": (market_behavior.get("market_phase") or {}).get("confidence") if isinstance(market_behavior.get("market_phase"), dict) else None,
        "market_behavior_summary": market_behavior_summary,
        "market_behavior_context_for_ai": {
            "summary": market_behavior_summary,
            "use_in_ai_insight_only": True,
            "display_in_ui": False,
            "meaning": "Internal AI Insight behavior context only — not a buy/sell signal and not a profit guarantee.",
        },
        "fake_move_risk": market_behavior.get("fake_move_risk"),
        "exhaustion_risk": market_behavior.get("exhaustion_risk"),
        "continuation_quality": market_behavior.get("continuation_quality"),
        "volume_confirmation": market_behavior.get("volume_confirmation"),
        "verdict": verdict,
        "confidence": confidence,
        "risk": risk,
        "exit_risk": exit_risk,
        "pre_exit_warning": bool(pre_exit_warning),
        "exit_reasons": exit_reasons[:5],
        "behavior": behavior,
        "setup_bias": setup_bias,
        "edge": edge,
        "invalidation": invalidation,
        "contradictions": contradictions[:6],
        "drivers": drivers[:6],
        "warnings": warnings[:6],
        "tags": sorted(set(tags)),
        "symbols": [s for s in [a, b] if s],
        "relative_strength": rel_strength,
        "market_condition_notes": mc_notes[:4],
        "onchain_notes": onchain_notes[:4],
        "summary": summary,
    }



def _strip_forbidden_strategist_sections(ans: str, intent: str, lang: str) -> str:
    """Remove whole generic report sections that do not match the routed intent."""
    text = str(ans or "").strip()
    if not text:
        return text

    intent = str(intent or "").lower()
    if intent not in ("rotation_spread", "rotation", "risk", "grid", "trading"):
        return text

    forbidden = {
        "rotation_spread": [
            "NEXUS TRADING", "NEXUS TRADING PREPARATION", "NEXUS TRADING SUITABILITY",
            "TRADING SUITABILITY", "TRADING PREPARATION", "RUNTIME SUGGESTION",
            "NEXUS GRID", "NEXUS GRID SUITABILITY", "GRID SUITABILITY",
            "MARKET EVALUATION", "SUGGESTED BEHAVIOR"
        ],
        "rotation": [
            "NEXUS TRADING PREPARATION", "TRADING SUITABILITY", "RUNTIME SUGGESTION",
            "NEXUS GRID SUITABILITY", "GRID SUITABILITY"
        ],
        "risk": [
            "NEXUS TRADING PREPARATION", "TRADING SUITABILITY", "RUNTIME SUGGESTION",
            "NEXUS GRID SUITABILITY", "GRID SUITABILITY"
        ],
        "grid": [
            "NEXUS TRADING PREPARATION", "TRADING SUITABILITY", "RUNTIME SUGGESTION",
            "EXCHANGE / SPREAD"
        ],
        "trading": [
            "NEXUS GRID SUITABILITY", "GRID SUITABILITY"
        ],
    }.get(intent, [])

    if not forbidden:
        return text

    # Split by heading-like lines and keep allowed blocks.
    lines = text.splitlines()
    blocks = []
    cur = []
    for line in lines:
        clean = re.sub(r"^\s{0,4}(?:#{1,4}\s*)?(?:[-*]\s*)?", "", line).strip()
        normalized = re.sub(r"[:：]+\s*$", "", clean).upper()
        is_heading = bool(normalized and len(normalized) <= 70 and (normalized == clean.upper() or normalized in forbidden))
        if is_heading and cur:
            blocks.append(cur)
            cur = [line]
        else:
            cur.append(line)
    if cur:
        blocks.append(cur)

    kept = []
    for block in blocks:
        first = re.sub(r"^\s{0,4}(?:#{1,4}\s*)?(?:[-*]\s*)?", "", (block[0] if block else "")).strip().upper()
        if any(bad.upper() in first for bad in forbidden):
            continue
        kept.extend(block)
        kept.append("")

    out = "\n".join(kept).strip()
    return out or text


def _limit_strategist_number_spam(ans: str, max_numeric_lines: int = 6) -> str:
    """Keep answers readable: too many raw numeric lines makes Strategist look like a data dump."""
    lines = str(ans or "").splitlines()
    kept = []
    numeric_count = 0
    for line in lines:
        # Keep headings even if they include numbers.
        clean = line.strip()
        is_heading = clean and len(clean) <= 60 and clean.upper() == clean
        has_metric_dump = bool(re.search(r"[-+]?\d+(?:\.\d+)?\s*%|corr(?:elation)?\s*[=:]|\brsi\b|\bscore\b", clean, re.I))
        if has_metric_dump and not is_heading:
            numeric_count += 1
            if numeric_count > max_numeric_lines:
                continue
        kept.append(line)
    return "\n".join(kept).strip()


def _normalize_strategist_language_leaks(ans: str, lang: str) -> str:
    """Small deterministic heading cleanup after the model answer."""
    out = str(ans or "").strip()
    l = str(lang or "en").lower()
    if l == "de":
        repl = {
            "DIRECT VIEW": "DIREKTE EINSCHÄTZUNG",
            "DIRECT ASSESSMENT": "DIREKTE EINSCHÄTZUNG",
            "ANSWER": "ANTWORT",
            "MARKET READ": "MARKTLAGE",
            "RISK CONTEXT": "RISIKOKONTEXT",
            "NEXT CHECK": "NÄCHSTE PRÜFUNG",
            "ROTATION / RELATIVE VALUE": "ROTATION / RELATIVER WERT",
        }
        for a, b in repl.items():
            out = re.sub(rf"(?im)^(\s*(?:#{1,4}\s*)?){re.escape(a)}(\s*:?\s*)$", rf"\1{b}\2", out)
    elif l == "en":
        repl = {
            "DIREKTE EINSCHÄTZUNG": "DIRECT VIEW",
            "DIREKTE EINSCHAETZUNG": "DIRECT VIEW",
            "ANTWORT": "ANSWER",
            "MARKTLAGE": "MARKET READ",
            "RISIKOKONTEXT": "RISK CONTEXT",
            "NÄCHSTE PRÜFUNG": "NEXT CHECK",
            "NAECHSTE PRUEFUNG": "NEXT CHECK",
            "ROTATION / RELATIVER WERT": "ROTATION / RELATIVE VALUE",
            "BÖRSE / SPREAD": "EXCHANGE / SPREAD",
            "BOERSE / SPREAD": "EXCHANGE / SPREAD",
        }
        for a, b in repl.items():
            out = re.sub(rf"(?im)^(\s*(?:#{1,4}\s*)?){re.escape(a)}(\s*:?\s*)$", rf"\1{b}\2", out)
    return out.strip()

def _extract_user_intent_from_payload(user_payload: dict) -> str:
    if not isinstance(user_payload, dict):
        return ""
    for key in ("user_intent",):
        v = str(user_payload.get(key) or "").strip().lower()
        if v:
            return v
    ctx = user_payload.get("ai_signal_context") if isinstance(user_payload, dict) else None
    if isinstance(ctx, dict) and ctx.get("user_intent"):
        return str(ctx.get("user_intent") or "").strip().lower()
    q = str((user_payload or {}).get("question") or "")
    return _strategist_intent_from_payload(q, ctx if isinstance(ctx, dict) else {})


def _collect_rotation_spread_candidates(user_payload: dict) -> list[dict]:
    candidates = {}
    def add_coin(c):
        if not isinstance(c, dict):
            return
        sym = str(c.get("symbol") or "").strip().upper()
        if not sym:
            return
        cur = candidates.get(sym, {"symbol": sym})
        cur.update({k: v for k, v in c.items() if v is not None})
        candidates[sym] = cur

    mc = (user_payload or {}).get("market_context")
    if isinstance(mc, dict):
        for c in mc.get("coins") or []:
            add_coin(c)
    ctx = (user_payload or {}).get("ai_signal_context")
    if isinstance(ctx, dict):
        for c in ctx.get("coins") or []:
            add_coin(c)

    pairs = []
    if isinstance(ctx, dict):
        pairs.extend(ctx.get("relevant_pairs") or [])
        pairs.extend(ctx.get("all_compare_pairs") or [])
    out = []
    for sym, c in candidates.items():
        ex = c.get("exchange_intelligence") if isinstance(c.get("exchange_intelligence"), dict) else {}
        related = []
        for p in pairs:
            pair = str((p or {}).get("pair") or "").upper()
            if sym and sym in pair.split("/"):
                related.append(p)
        best_pair = None
        if related:
            best_pair = sorted(related, key=lambda x: float((x or {}).get("score") or 0), reverse=True)[0]
        out.append({"symbol": sym, "coin": c, "exchange": ex, "best_pair": best_pair})
    def score(x):
        ex = x.get("exchange") or {}
        bp = x.get("best_pair") or {}
        return (float(ex.get("exchange_premium_pct") or 0) * 4) + float(bp.get("score") or 0) + abs(float(bp.get("spread_pct") or 0))
    return sorted(out, key=score, reverse=True)[:5]


def _fmt_pct_value(v):
    try:
        x = float(v)
        if math.isfinite(x):
            return f"{x:.2f}%"
    except Exception:
        pass
    return None


def _deterministic_rotation_spread_answer(user_payload: dict, lang: str) -> str:
    lang = str(lang or "en").lower()
    cands = _collect_rotation_spread_candidates(user_payload)
    if lang == "de":
        if not cands:
            return ("DIREKTE EINSCHÄTZUNG\n"
                    "Aktuell sind im Kontext keine ausreichenden Coin-/Exchange-Daten vorhanden, um sauber zu sagen, wo ein Coin günstiger gekauft und wo er teurer verkauft werden könnte.\n\n"
                    "NÄCHSTE PRÜFUNG\n"
                    "Mehr verwertbar wäre ein Vergleich mit Exchange-Preis, Volumen, Spread und Stale-/Anomaly-Status pro Coin.")
        lines = ["DIREKTE EINSCHÄTZUNG"]
        top = cands[0]
        ex = top.get("exchange") or {}
        bp = top.get("best_pair") or {}
        prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
        pair_spread = _fmt_pct_value(bp.get("spread_pct"))
        if prem:
            lines.append(f"{top['symbol']} zeigt aktuell die auffälligste Exchange-Differenz im Kontext: ungefähr {prem} zwischen günstigster und teuerster Börse. Gleichzeitig sollte geprüft werden, ob genügend Volumen vorhanden ist, damit der Vorteil handelbar bleibt.")
        elif pair_spread:
            lines.append(f"{top['symbol']} ist über den Pair-/Relative-Value-Kontext auffällig. Der relevante relative Spread liegt bei etwa {pair_spread}.")
        else:
            lines.append(f"{top['symbol']} wirkt im aktuellen Kontext am interessantesten, aber es liegt kein sauberer Exchange-Prozentvorteil vor.")
        lines.append("")
        lines.append("KAUFEN / VERKAUFEN")
        any_buy_sell = False
        for item in cands[:3]:
            ex = item.get("exchange") or {}
            prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
            cheap_obj = ex.get("cheapest_exchange") or ex.get("lowest_exchange") or ex.get("cheapest") or ex.get("min_exchange")
            high_obj = ex.get("highest_exchange") or ex.get("top_exchange") or ex.get("highest") or ex.get("max_exchange")
            cheap = _extract_exchange_name(cheap_obj)
            high = _extract_exchange_name(high_obj)
            cheap_px = _extract_exchange_price(cheap_obj)
            high_px = _extract_exchange_price(high_obj)
            if cheap and high:
                price_txt = ""
                if cheap_px is not None and high_px is not None:
                    price_txt = f" ({cheap_px:g} USD → {high_px:g} USD)"

                net_edge = _estimate_net_edge(ex.get("exchange_premium_pct"))
                conf = _edge_confidence_label(
                    ex.get("exchange_premium_pct"),
                    ex.get("top_exchange_volume_share_pct"),
                    item.get("liquidity_score")
                )

                quality = "saubere Rotation"
                rank_label = "BEST CHOICE"
                if net_edge < 0.5:
                    quality = "zu kleiner Netto-Spread"
                    rank_label = "AVOID / WEAK EDGE"
                elif net_edge < 1.2:
                    quality = "schwache Edge"
                    rank_label = "SECONDARY CHOICE"
                elif net_edge > 8:
                    quality = "möglicher Spike/Fake-Move"
                    rank_label = "AVOID / WEAK EDGE"

                lines.append(
                    f"- {item['symbol']}: günstiger kaufen bei {cheap}, teurer verkaufen/prüfen bei {high}{price_txt}"
                    + (f", Differenz ca. {prem}." if prem else ".")
                    + f" Netto-Edge ~{net_edge}% | Confidence: {conf} | Ranking: {rank_label} | Bewertung: {quality}."
                )
                any_buy_sell = True
            elif cheap:
                lines.append(f"- {item['symbol']}: Kaufseite sichtbar bei {cheap}; die Verkaufsseite ist im aktuellen Kontext nicht sauber bestätigt.")
                any_buy_sell = True
        if not any_buy_sell:
            lines.append("- Kein sauberer Kaufen-/Verkaufen-Vergleich pro Exchange vorhanden. Die Pair-Rotation unten ist nur relative Stärke, keine bestätigte Arbitrage oder sichere Edge.")
        lines.append("")
        lines.append("ROTATION / RELATIVER WERT")
        added = 0
        for item in cands[:3]:
            bp = item.get("best_pair") or {}
            ptxt = f" gegen {bp.get('pair')}" if bp.get("pair") else ""
            sp = _fmt_pct_value(bp.get("spread_pct"))
            score = bp.get("score")
            detail = []
            if sp: detail.append(f"relativer Spread {sp}")
            if score is not None: detail.append(f"Pair-Score {score}")
            lines.append(f"- {item['symbol']}{ptxt}: " + (", ".join(detail) if detail else "beobachtbar, aber ohne klaren relativen Vorteil."))
            added += 1
        lines.append("")
        lines.append("BÖRSE / SPREAD")
        any_ex = False
        for item in cands[:3]:
            ex = item.get("exchange") or {}
            prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
            cheap = (ex.get("cheapest_exchange") or {}).get("exchange") if isinstance(ex.get("cheapest_exchange"), dict) else None
            high = (ex.get("highest_exchange") or {}).get("exchange") if isinstance(ex.get("highest_exchange"), dict) else None
            if prem and cheap and high:
                lines.append(f"- {item['symbol']}: günstigste Börse {cheap}, teuerste Börse {high}, Differenz ca. {prem}.")
                any_ex = True
        if not any_ex:
            lines.append("- Im aktuellen Kontext liegt keine saubere Exchange-Preisabweichung mit Börsennamen vor. Nexus sollte hier nicht so tun, als wäre echte Arbitrage bestätigt.")
        lines.append("")
        lines.append("RISIKOKONTEXT")
        lines.append("- Ein Spread ist nur verwertbar, wenn Volumen, Liquidität und Spread-Qualität bestätigen. Kleine Differenzen unter ca. 0,5% sind oft nur Rauschen oder Gebührenrisiko.")
        if top.get("coin"):
            hint = top.get("coin", {}).get("tradeability_hint")
            if hint:
                lines.append(f"- {hint}")
        return "\n".join(lines)

    # English/default. For other languages, the model should normally translate; this fallback is English only.
    if not cands:
        return "DIRECT VIEW\nNo sufficient coin/exchange context is available to identify where a coin is cheaper and where it trades higher.\n\nNEXT CHECK\nUse exchange price, volume, spread, stale and anomaly status per coin."
    top = cands[0]
    ex = top.get("exchange") or {}
    bp = top.get("best_pair") or {}
    prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
    pair_spread = _fmt_pct_value(bp.get("spread_pct"))
    lines = ["DIRECT VIEW"]
    if prem:
        lines.append(f"{top['symbol']} shows the clearest exchange difference in the current context: about {prem} between the cheapest and highest exchange. Volume quality should still confirm whether the edge is realistically tradable.")
    elif pair_spread:
        lines.append(f"{top['symbol']} stands out in the relative-value context with a relevant pair spread around {pair_spread}.")
    else:
        lines.append(f"{top['symbol']} is the most relevant candidate, but no clean exchange percentage edge is available.")
    lines += ["", "BUY / SELL"]
    any_buy_sell = False
    for item in cands[:3]:
        ex = item.get("exchange") or {}
        prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
        cheap_obj = ex.get("cheapest_exchange") or ex.get("lowest_exchange") or ex.get("cheapest") or ex.get("min_exchange")
        high_obj = ex.get("highest_exchange") or ex.get("top_exchange") or ex.get("highest") or ex.get("max_exchange")
        cheap = _extract_exchange_name(cheap_obj)
        high = _extract_exchange_name(high_obj)
        cheap_px = _extract_exchange_price(cheap_obj)
        high_px = _extract_exchange_price(high_obj)
        if cheap and high:
            price_txt = ""
            if cheap_px is not None and high_px is not None:
                price_txt = f" ({cheap_px:g} USD → {high_px:g} USD)"

            net_edge = _estimate_net_edge(ex.get("exchange_premium_pct"))
            conf = _edge_confidence_label(
                ex.get("exchange_premium_pct"),
                ex.get("top_exchange_volume_share_pct"),
                item.get("liquidity_score")
            )

            quality = "clean rotation"
            rank_label = "BEST CHOICE"
            if net_edge < 0.5:
                quality = "net edge too small"
                rank_label = "AVOID / WEAK EDGE"
            elif net_edge < 1.2:
                quality = "weak edge"
                rank_label = "SECONDARY CHOICE"
            elif net_edge > 8:
                quality = "possible spike/fake move"
                rank_label = "AVOID / WEAK EDGE"

            lines.append(
                f"- {item['symbol']}: cheaper buy side at {cheap}, higher sell/check side at {high}{price_txt}"
                + (f", difference about {prem}." if prem else ".")
                + f" Net edge ~{net_edge}% | Confidence: {conf} | Ranking: {rank_label} | Assessment: {quality}."
            )
            any_buy_sell = True
        elif cheap:
            lines.append(f"- {item['symbol']}: buy side is visible at {cheap}; the sell side is not cleanly confirmed in the current context.")
            any_buy_sell = True
    if not any_buy_sell:
        lines.append("- No clean buy/sell exchange comparison is available. The pair rotation below is relative strength only, not confirmed arbitrage.")
    lines += ["", "ROTATION / RELATIVE VALUE"]
    for item in cands[:3]:
        bp = item.get("best_pair") or {}
        sp = _fmt_pct_value(bp.get("spread_pct"))
        detail = f"relative spread {sp}" if sp else "observable, but no clean relative edge"
        lines.append(f"- {item['symbol']}: {detail}.")
    lines += ["", "EXCHANGE / SPREAD"]
    any_ex = False
    for item in cands[:3]:
        ex = item.get("exchange") or {}
        prem = _fmt_pct_value(ex.get("exchange_premium_pct"))
        cheap = (ex.get("cheapest_exchange") or {}).get("exchange") if isinstance(ex.get("cheapest_exchange"), dict) else None
        high = (ex.get("highest_exchange") or {}).get("exchange") if isinstance(ex.get("highest_exchange"), dict) else None
        if prem and cheap and high:
            lines.append(f"- {item['symbol']}: cheapest exchange {cheap}, highest exchange {high}, difference about {prem}.")
            any_ex = True
    if not any_ex:
        lines.append("- No clean exchange price deviation with exchange names is available in the current context.")
    if top.get("coin"):
        hint = top.get("coin", {}).get("tradeability_hint")
        if hint:
            lines.append(f"- {hint}")
    return "\n".join(lines)


def _answer_looks_wrong_for_intent(ans: str, lang: str, intent: str) -> bool:
    a = str(ans or "")
    if not a.strip():
        return True
    il = str(intent or "").lower()
    al = a.lower()

    forbidden_by_intent = {
        "rotation_spread": ["nexus trading preparation", "trading suitability", "runtime suggestion", "nexus grid suitability", "grid suitability", "market evaluation"],
        "rotation": ["nexus trading preparation", "runtime suggestion", "nexus grid suitability", "grid suitability"],
        "risk": ["nexus trading preparation", "runtime suggestion", "nexus grid suitability", "grid suitability"],
        "grid": ["nexus trading preparation", "runtime suggestion", "exchange / spread"],
    }
    if any(x in al for x in forbidden_by_intent.get(il, [])):
        return True

    if il == "rotation_spread" and not re.search(r"(spread|börse|boerse|exchange|rotation|relativer wert|relative value|günstig|guenstig|cheapest|premium|price difference)", a, re.I):
        return True
    if il == "rotation" and not re.search(r"(rotation|relative|stärke|staerke|strength|weakness|momentum)", a, re.I):
        return True
    if il == "risk" and not re.search(r"(risk|risiko|fake|liquid|volume|rvol|overextension|überhitzt|ueberhitzt|trap|confirmation)", a, re.I):
        return True

    if str(lang).lower() == "de":
        english_bad = ["ANSWER", "DIRECT VIEW", "MARKET READ", "NEXT CHECK", "Risk Factors", "Suggested Behavior", "Failure", "Trading Suitability"]
        if any(re.search(rf"(?im)^\s*(?:#{{1,4}}\s*)?{re.escape(x)}\s*:?", a) for x in english_bad):
            return True
    if str(lang).lower() == "en":
        german_bad = ["DIREKTE EINSCHÄTZUNG", "RISIKOKONTEXT", "NÄCHSTE PRÜFUNG", "MARKTLAGE", "BÖRSE / SPREAD"]
        if any(re.search(rf"(?im)^\s*(?:#{{1,4}}\s*)?{re.escape(x)}\s*:?", a) for x in german_bad):
            return True
    return False


def _enforce_strategist_answer_contract(ans: str, user_payload: dict) -> str:
    ctx = user_payload.get("ai_signal_context") if isinstance(user_payload, dict) else {}
    lang = _detect_user_language_from_text(str((ctx or {}).get("raw_user_question") or user_payload.get("question") or ""), (ctx or {}).get("user_language") if isinstance(ctx, dict) else None)
    intent = _extract_user_intent_from_payload(user_payload)

    cleaned = _normalize_strategist_language_leaks(str(ans or ""), lang)
    cleaned = _strip_forbidden_strategist_sections(cleaned, intent, lang)
    cleaned = _limit_strategist_number_spam(cleaned, max_numeric_lines=6)

    if _answer_looks_wrong_for_intent(cleaned, lang, intent):
        if intent == "rotation_spread":
            # Deterministic fallback prevents generic Trading/Grid reports for rotation/spread questions.
            return _deterministic_rotation_spread_answer(user_payload, lang if lang in ("de", "en") else "en")
        return cleaned.strip() or str(ans or "").strip()
    return cleaned.strip()


def _ai_call_openai(
    sys_prompt: str,
    user_payload: dict,
    wallet_address: str | None = None,
    mem_msgs: list | None = None,
    short_insight_mode: bool = False,
):
    short_insight_mode = bool(user_payload.get("short_insight_mode"))
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
        "max_output_tokens": 220 if short_insight_mode else 900,
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

        if short_insight_mode:
            try:
                ans = _enforce_ai_insight_structure(ans, user_payload.get("ai_engine_v2"))
            except Exception:
                ans = _compact_behavior_answer_from_engine(user_payload.get("ai_engine_v2"))
            try:
                ans = _hard_sanitize_ai_insight_text(ans)
                ans = _enforce_ai_insight_structure(ans, user_payload.get("ai_engine_v2"))
            except Exception:
                pass

        if not short_insight_mode:
            try:
                ans = _enforce_strategist_answer_contract(ans, user_payload if isinstance(user_payload, dict) else {})
            except Exception:
                ans = str(ans or "")

        if wallet_address:
            try:
                _ai_mem_append(wallet_address, str(user_payload.get("question") or ""), ans, max_msgs=10)
            except Exception:
                pass

        out_resp = {"status": "ok", "answer": ans, "model": model}
        if isinstance(user_payload.get("ai_engine_v2"), dict):
            out_resp["ai_engine_v2"] = user_payload.get("ai_engine_v2")
        return out_resp, None

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
    exchange_intel_by_symbol = _build_exchange_intelligence_context(id_map, symbols)

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

        # Exchange intelligence (CoinGecko tickers): price dispersion, volume concentration, spread quality.
        try:
            exi = exchange_intel_by_symbol.get(sym) if isinstance(exchange_intel_by_symbol, dict) else None
            if isinstance(exi, dict) and exi:
                item["exchange_intelligence"] = exi
        except Exception:
            pass

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

        # Market Condition (Overextension + RVOL)
        # This feeds AI Insight / AI Analyst as behavior context:
        #   OE high + RVOL weak  -> weak/fake move risk
        #   OE high + RVOL strong -> volume-backed breakout
        #   OE low + RVOL strong  -> early accumulation / volume build
        try:
            mc = _market_condition_for_coin(cid or sym, days=20)
            item["market_condition"] = {
                "state": mc.get("state"),
                "label": mc.get("label"),
                "level": mc.get("level"),
                "confidence": mc.get("confidence"),
                "oe_pct": mc.get("oe_pct"),
                "rvol": mc.get("rvol"),
                "score_delta": mc.get("score_delta"),
                "interpretation": (mc.get("ai_context") or {}).get("interpretation"),
            }
        except Exception:
            item["market_condition"] = {
                "state": "UNAVAILABLE",
                "label": "Market condition unavailable",
                "level": "unknown",
                "confidence": "LOW",
            }

        coins.append(item)

    return {
        "ts": now_ts(),
        "profile": (profile or "conservative").strip().lower(),
        "include_health": bool(include_health),
        "coins": coins,
        "note": "Numbers are snapshots; suitability is informational only.",
    }


def _normalize_ai_timeframe(raw: str) -> str:
    tf = str(raw or "90D").strip().upper()
    allowed = {"1D", "7D", "30D", "90D", "1Y", "2Y", "24H"}
    if tf == "24H":
        tf = "1D"
    return tf if tf in allowed else "90D"


def _ai_num(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _sanitize_series_stats(raw_stats: dict, allowed_symbols: list[str]) -> dict:
    out = {}
    if not isinstance(raw_stats, dict):
        return out
    allowed = {str(s or "").strip().upper() for s in (allowed_symbols or []) if str(s or "").strip()}
    for sym, stats in raw_stats.items():
        sym_u = str(sym or "").strip().upper()
        if not sym_u or sym_u not in allowed or not isinstance(stats, dict):
            continue
        clean = {
            "first": _ai_num(stats.get("first")),
            "last": _ai_num(stats.get("last")),
            "changePct": _ai_num(stats.get("changePct")),
            "volPct": _ai_num(stats.get("volPct")),
            "maxDDPct": _ai_num(stats.get("maxDDPct")),
            "min": _ai_num(stats.get("min")),
            "max": _ai_num(stats.get("max")),
        }
        try:
            pts = int(stats.get("points"))
        except Exception:
            pts = 0
        clean["points"] = max(0, pts)
        out[sym_u] = clean
    return out


def _build_ai_timeframe_context(symbols: list[str], timeframe: str, series_stats: dict | None, index_mode: bool = False) -> dict:
    requested = _normalize_ai_timeframe(timeframe)
    stats = _sanitize_series_stats(series_stats or {}, symbols)

    available_symbols = []
    missing_symbols = []
    weak_symbols = []
    for sym in symbols:
        st = stats.get(sym)
        if not st:
            missing_symbols.append(sym)
            continue
        pts = int(st.get("points") or 0)
        if pts >= 2:
            available_symbols.append(sym)
        else:
            weak_symbols.append(sym)

    complete = (len(available_symbols) == len(symbols)) and not missing_symbols and not weak_symbols and len(symbols) > 0
    if complete:
        actual = requested
        note = f"Timeframe-aligned series stats are available for all selected coins for {requested}."
    elif available_symbols:
        actual = "PARTIAL"
        note = (
            f"Requested timeframe was {requested}, but complete series stats were not available for all selected coins. "
            f"Available timeframe stats exist for: {', '.join(available_symbols)}. "
            + (f"Missing: {', '.join(missing_symbols)}. " if missing_symbols else "")
            + (f"Insufficient points: {', '.join(weak_symbols)}. " if weak_symbols else "")
            + "Do not claim a full timeframe analysis for missing or incomplete coins."
        )
    else:
        actual = "SNAPSHOT_ONLY"
        note = (
            f"Requested timeframe was {requested}, but no usable timeframe series stats were provided. "
            "Only snapshot context is available. Do not claim this is a strict timeframe analysis."
        )

    return {
        "requested_timeframe": requested,
        "actual_timeframe_used": actual,
        "timeframe_match": bool(actual == requested),
        "index_mode": bool(index_mode),
        "series_stats": stats,
        "available_symbols": available_symbols,
        "missing_symbols": missing_symbols,
        "insufficient_points_symbols": weak_symbols,
        "coverage_note": note,
    }



def _detect_user_language_from_text(text: str, explicit: str | None = None) -> str:
    """Detect the language of the latest user message.

    The frontend may send an explicit BCP-47-like short code. Prefer it when present.
    If absent, use lightweight keyword/script detection. This intentionally avoids
    treating long English system/context headers as the user's language.
    """
    e = str(explicit or "").strip().lower().replace("_", "-")
    alias = {
        "deutsch": "de", "german": "de",
        "english": "en", "englisch": "en",
        "français": "fr", "francais": "fr", "french": "fr",
        "español": "es", "espanol": "es", "spanish": "es",
        "italiano": "it", "italian": "it",
        "portuguese": "pt", "portugues": "pt", "português": "pt",
        "turkish": "tr", "türkisch": "tr", "turkce": "tr", "türkçe": "tr",
        "dutch": "nl", "nederlands": "nl",
    }
    if e and e not in ("auto", "unknown", ""):
        e = alias.get(e, e.split("-", 1)[0])
        if re.fullmatch(r"[a-z]{2,3}", e):
            return e

    t_raw = str(text or "")
    # Prefer explicit metadata when it exists.
    m = re.search(r"user_language\s*:\s*([a-zA-Z-]{2,12})", t_raw, re.I)
    if m:
        val = alias.get(m.group(1).strip().lower(), m.group(1).strip().lower().split("-", 1)[0])
        if re.fullmatch(r"[a-z]{2,3}", val):
            return val

    # If the frontend sends the raw user sentence inside the JSON, use that slice.
    raw_match = re.search(r'"raw_user_question"\s*:\s*"([^"\]*(?:\\.[^"\]*)*)"', t_raw)
    if raw_match:
        try:
            t_raw = json.loads('"' + raw_match.group(1) + '"')
        except Exception:
            t_raw = raw_match.group(1)

    t = f" {t_raw.lower()} "
    # Simple script detection for languages where Latin keyword detection is weak.
    if re.search(r"[А-Яа-яЁё]", t_raw):
        return "ru"
    if re.search(r"[أ-ي]", t_raw):
        return "ar"
    if re.search(r"[ぁ-んァ-ン一-龯]", t_raw):
        return "ja"
    if re.search(r"[가-힣]", t_raw):
        return "ko"

    lexicons = {
        "de": [" der ", " die ", " das ", " und ", " oder ", " welche", " welcher", " welches", "günstig", "guenstig", "teurer", "kaufen", "verkaufen", "börse", "boerse", " wo ", " wie ", " ist ", " sind ", "bitte", "suche"],
        "en": [" the ", " and ", " or ", " which", " what", " cheaper", " expensive", " buy", " sell", " where ", " how ", " is ", " are "],
        "fr": [" le ", " la ", " les ", " et ", " ou ", " quel", " quelle", " acheter", " vendre", " moins cher", " plus cher", " pourquoi", " comment"],
        "es": [" el ", " la ", " los ", " las ", " y ", " o ", " cuál", " cual", " comprar", " vender", " barato", " caro", " donde", " dónde"],
        "it": [" il ", " la ", " gli ", " le ", " e ", " o ", " quale", " comprare", " vendere", " economico", " caro", " dove"],
        "pt": [" o ", " a ", " os ", " as ", " e ", " ou ", " qual", " comprar", " vender", " barato", " caro", " onde"],
        "tr": [" ve ", " veya ", " hangi", " nerede", " almak", " satmak", " ucuz", " pahalı", " pahali"],
        "nl": [" de ", " het ", " en ", " of ", " welke", " kopen", " verkopen", " goedkoper", " duurder", " waar"],
    }
    scores = {lang: sum(1 for w in words if w in t) for lang, words in lexicons.items()}
    best, score = max(scores.items(), key=lambda kv: kv[1])
    if score > 0:
        return best
    return "en"


def _language_name(lang: str) -> str:
    names = {
        "de": "German", "en": "English", "fr": "French", "es": "Spanish", "it": "Italian",
        "pt": "Portuguese", "tr": "Turkish", "nl": "Dutch", "ru": "Russian", "ar": "Arabic",
        "ja": "Japanese", "ko": "Korean",
    }
    return names.get(str(lang or "").lower(), str(lang or "English"))


def _language_hard_rule(lang: str) -> str:
    lang = str(lang or "en").strip().lower()
    if lang == "de":
        return """
LANGUAGE HARD RULE (CRITICAL):
- The final answer MUST be 100% German.
- All headings, card titles, bullets, button/action labels and explanations must be German.
- Do not output English section names such as ANSWER, MARKET READ, NEXUS TRADING PREPARATION, MARKET EVALUATION, NEXT CHECK, Trading Suitability, Risk Mode, Runtime Suggestion, Suggested Behavior, Risk Factors, or Failure.
- Use German equivalents only: Antwort, Direkte Einschätzung, Rotation / relativer Wert, Börse / Spread, Risikokontext, Nächste Prüfung.
- English words are allowed only for fixed product names such as Nexus Strategist, Nexus Trading, Exchange, Grid, token symbols, exchange names and chain names.
"""
    if lang == "en":
        return """
LANGUAGE HARD RULE (CRITICAL):
- The final answer MUST be 100% English.
- All headings, card titles, bullets, button/action labels and explanations must be English.
- Do not output German section names such as Direkte Einschätzung, Risikokontext, Nächste Prüfung, or Börse / Spread.
"""
    lname = _language_name(lang)
    return f"""
LANGUAGE HARD RULE (CRITICAL):
- Detect the user's latest message language as: {lname} ({lang}).
- The final answer MUST be entirely in {lname}.
- Translate every heading, card title, bullet, warning, button/action label and explanation into {lname}.
- Do not mix English or German headings into the final answer unless they are fixed product names such as Nexus Strategist, Nexus Trading, Exchange, Grid, token symbols, exchange names or chain names.
- If a standard section is needed, translate it naturally into {lname} instead of using English templates.
"""

def _ai_kind_instructions(kind: str) -> str:
    k = (kind or "").strip().lower()

    if k in ("research", "market_research", "quick_overview", "overview"):
        return (
            "AI Analyst mode: Research. Identify rotation, relative strength, watchlist changes, market themes, "
            "unusual volume/momentum conditions, and discovery candidates. Do NOT repeat AI Insight sections. "
            "Focus on what deserves research attention and why."
        )

    if k in ("strategy_builder", "strategy", "builder", "grid_plan", "grid", "plan"):
        return (
            "AI Analyst mode: Strategy Builder. Turn the user's idea and selected context into an educational strategy framework: "
            "setup thesis, filters, entry/exit rules as logic only, risk controls, invalidation logic, alert conditions, and failure regimes. "
            "Do NOT give direct financial advice and do NOT output exact buy/sell price levels."
        )

    if k in ("backtest_review", "backtest", "review"):
        return (
            "AI Analyst mode: Backtest Review. Evaluate robustness, drawdown behavior, expectancy quality, regime dependency, "
            "parameter sensitivity, overfitting risk, and where the strategy is likely to fail. If no backtest table is provided, "
            "explain what must be checked and use available context only."
        )

    if k in ("pine_tradingview", "pine", "tradingview", "pine_script"):
        return (
            "AI Analyst mode: TradingView / Pine. Help create, explain, debug, or improve Pine Script indicators, strategies, "
            "and alert logic. Keep outputs educational and code-focused when requested. Never claim code was backtested unless results are provided."
        )

    if k in ("daily_report", "report", "daily"):
        return (
            "AI Analyst mode: Daily Report. Produce a practical report: strongest/weakest selected assets, market themes, "
            "risk conditions, movement candidates, watchlist notes, and what deserves attention next. Do not duplicate AI Insight wording."
        )

    if k in ("diagnostics", "diagnosis", "trading_diagnostics", "risk_check", "risk", "explain"):
        return (
            "AI Analyst mode: Diagnostics. Diagnose behavioral risk, execution fit, volatility tolerance, weak setups, contradictions, "
            "and common mistakes. Use coaching-style explanation, not commands. Do not repeat AI Insight sections unless needed as context."
        )

    if k in ("compare", "comparison"):
        return (
            "AI Analyst mode: Research comparison. Compare the selected coins by relative strength, risk quality, liquidity/volume, "
            "market-condition context, and suitability for the user's selected profile."
        )

    return "Answer the user's question as AI Analyst: research, diagnose, build, review, or explain using only the provided context."





def _strategist_is_followup_question(question: str) -> bool:
    q = str(question or "").strip().lower()
    if not q:
        return False
    return bool(re.search(
        r"^(und|aber|warum|wieso|wie meinst|was heißt|was heisst|erklär|erklaer|nochmal|weiter|ok|ja|nein|and|but|why|what does|explain|continue|go on|again)\b|"
        r"\b(das|dieser|diese|diesen|there|that|this|it|same|gleich)\b",
        q,
        re.I,
    ))


def _strategist_followup_rule(question: str, intent: str, lang: str) -> str:
    if not _strategist_is_followup_question(question):
        return ""
    if str(lang or "").lower() == "de":
        return """
FOLLOW-UP REGEL:
- Diese Nutzerfrage wirkt wie eine Rückfrage.
- Behalte den vorherigen Kontext und den zuletzt erkannten Intent bei.
- Antworte nicht mit einem neuen Vollreport.
- Erkläre nur den Punkt, nach dem gefragt wurde.
- Wenn der Nutzer "weiter" sagt, vertiefe die letzte Analyse um eine Ebene: Ursache, Risiko oder nächste Prüfung.
"""
    return """
FOLLOW-UP RULE:
- This user message looks like a follow-up.
- Keep the previous context and the last detected intent.
- Do not create a new full report.
- Answer only the specific point being asked about.
- If the user says "continue", deepen the last analysis by one layer: cause, risk, or next check.
"""

def _strategist_intent_from_payload(question: str, extra_context: dict | None = None) -> str:
    """Strict query router for Nexus Strategist Phase 1.

    The UI can send user_intent, but the backend re-checks the raw user question
    so long English prompt/context blocks can never override the latest user intent.
    """
    ctx = extra_context if isinstance(extra_context, dict) else {}
    explicit = str(ctx.get("user_intent") or "").strip().lower()
    allowed = {
        "rotation_spread", "rotation", "grid", "trading", "risk",
        "market", "daily_report", "pine", "backtest", "diagnostics", "general"
    }
    if explicit in allowed:
        return explicit

    q = str(ctx.get("raw_user_question") or question or "").lower()

    if re.search(r"(arbitrage|spread|exchange|börse|boerse|preisunterschied|price difference|premium|discount|günstig|guenstig|billig|cheaper|teurer|higher|wo.*kaufen|where.*buy|wo.*verkaufen|where.*sell|sell higher|buy cheaper|wo.*besser|where.*better|wo.*mehr wert|more expensive there|anderer preis|different price|lohnt|worth it)", q, re.I):
        return "rotation_spread"
    if re.search(r"(rotation|rotieren|relative\s+stärke|relative strength|weakness|strength|kapitalfluss|capital flow|outperform|underperform|welcher.*stärker|welcher.*staerker|which.*stronger|besserer coin|better coin|stärker als|staerker als|stronger than)", q, re.I):
        return "rotation"
    if re.search(r"(grid|range|seitwärts|seitwaerts|sideways|levels|raster)", q, re.I):
        return "grid"
    if re.search(r"(trading|autonom|runtime|slot|allocation|budget|position|vault|execute|execution)", q, re.I):
        return "trading"
    if re.search(r"(risiko|risk|fake|manipul|gefährlich|gefaehrlich|danger|overheat|überhitzt|ueberhitzt|trap|liquidität|liquidity)", q, re.I):
        return "risk"
    if re.search(r"(report|daily|täglich|taeglich|markt|market|overview|überblick|ueberblick)", q, re.I):
        return "market"
    return "general"


def _strategist_response_profile(intent: str, lang: str) -> str:
    """Return strict output profile so the model cannot fall back to generic reports."""
    intent = str(intent or "general").strip().lower()
    lang = str(lang or "en").strip().lower()

    de = lang == "de"
    if intent == "rotation_spread":
        return """
STRICT RESPONSE PROFILE: ROTATION_SPREAD_ANALYSIS
Allowed sections only:
- German: DIREKTE EINSCHÄTZUNG, KAUFEN / VERKAUFEN, EXCHANGE / SPREAD, ROTATION / RELATIVER WERT, RISIKOKONTEXT, NÄCHSTE PRÜFUNG.
- English: DIRECT VIEW, BUY / SELL, EXCHANGE / SPREAD, ROTATION / RELATIVE VALUE, RISK CONTEXT, NEXT CHECK.
Forbidden unless explicitly requested: Nexus Trading, Nexus Grid, Trading Suitability, Runtime Suggestion, Market Evaluation, full multi-report.
Answer logic:
1) First sentence must classify the edge: clean edge / weak edge / watch only.
2) For buy-cheap/sell-higher questions, prioritize COIN -> BUY EXCHANGE -> SELL EXCHANGE -> DIFFERENCE.
3) Do NOT answer mainly with pairs when the user asks where to buy/sell a coin.
4) Pair/relative spread is only supporting context after the coin/exchange answer.
5) Separate exchange-specific facts from pair/relative spread facts.
6) If cheapest_exchange and highest_exchange exist, name both clearly.
7) If only cheapest_exchange exists but no sell/highest exchange exists, say that the buy side is visible but the sell side is not confirmed.
8) Weight the answer by volume quality, liquidity quality, stale/anomaly risk, and spread size.
9) If exchange prices are missing, say that exchange-specific buy-cheap/sell-high data is not available in the current context.
10) Do not invent exchange names, prices, depth, orderbook data, or arbitrage execution.
11) Use at most 5 compact sections and avoid raw metric dumps.
12) When multiple candidates exist, label them clearly as BEST CHOICE, SECONDARY CHOICE, or AVOID / WEAK EDGE.
"""
    if intent == "rotation":
        return """
STRICT RESPONSE PROFILE: ROTATION_ANALYSIS
Allowed sections: Direct assessment, Relative strength / rotation, Risk context, Next check.
Forbidden unless explicitly requested: Grid suitability, Trading execution, Runtime automation.
Answer logic:
- Rank the strongest visible rotation candidates by relative strength, momentum quality, and confirmation.
- Explain why the strongest candidate matters in market-language, not just numbers.
- Mention conflicts: strong momentum but weak volume, spread without confirmation, overextension risk, or neutral on-chain.
- No direct buy/sell commands.
"""
    if intent == "grid":
        return """
STRICT RESPONSE PROFILE: GRID_ANALYSIS
Allowed sections: Grid fit, Range quality, Risk context, Next check.
Forbidden unless explicitly requested: Rotation candidates, Nexus Trading automation, exchange arbitrage.
Answer logic:
- Judge whether the market looks range-friendly, too directional, too volatile, or too thin.
- Explain range risk, expansion risk, and invalidation behavior.
- No automatic order instructions.
"""
    if intent == "trading":
        return """
STRICT RESPONSE PROFILE: TRADING_ALLOCATION_ANALYSIS
Trader Phase 1 must include budget split, slot queue, confidence, risk and safety logic when Nexus Trading is requested.
Allowed sections: Direct assessment, Budget slots, Trader queue, Allocation logic, Risk limits, Safety blocks, Next check.
Forbidden unless explicitly requested: Grid report, exchange arbitrage report.
Answer logic:
- Focus on controlled allocation, budget slots, reserve logic, READY/WAIT/BLOCKED queue state, risk state, and whether runtime should be active or defensive.
- If a budget is mentioned, propose a slot split such as 100 / 50 / 50 / 50 only as preparation, not execution.
- Nexus Trading is autonomous after budget approval. Do not ask for manual queue rebuild, manual order add, or manual start. User controls Pause and Stop only.
- Never imply the AI can execute outside user-approved limits.
- No direct buy/sell commands.
"""
    if intent == "risk":
        return """
STRICT RESPONSE PROFILE: RISK_ANALYSIS
Allowed sections: Direct assessment, Risk drivers, Confirmation quality, Next check.
Forbidden unless explicitly requested: Trading preparation, Grid setup, exchange arbitrage.
Answer logic:
- Focus on liquidity quality, fake-move risk, overextension, volume confirmation, trap/exhaustion, and invalidation.
- Explain the most important risk first.
- No direct buy/sell commands.
"""
    return """
STRICT RESPONSE PROFILE: GENERAL_MARKET_ANALYSIS
Allowed sections only if useful: Direct assessment, Market read, Risk context, Next check.
Answer logic:
- Answer the exact user question first in one direct sentence.
- Do not output every Nexus module.
- Prioritize the 2-3 most important facts and explain their meaning.
- Use narrative interpretation over raw metric lists.
"""



def _narrative_phrase_from_market_state(state: str, rvol: float | None = None, oe: float | None = None, lang: str = "en") -> str:
    s = str(state or "").lower()
    rv = _safe_float(rvol, 0.0)
    oe = _safe_float(oe, 0.0)

    if lang == "de":
        if rv >= 2.0 and oe > 8:
            return "starke Teilnahme mit bereits überdehnter Bewegung"
        if rv >= 2.0:
            return "sichtbar aggressive Marktteilnahme"
        if oe > 10:
            return "überdehnte Struktur mit erhöhtem Rücksetzungsrisiko"
        if "neutral" in s:
            return "eher neutrale Marktstruktur"
        if "risk" in s or "danger" in s:
            return "fragile Struktur mit erhöhtem Fehlbewegungsrisiko"
        return "stabile, aber noch nicht vollständig bestätigte Struktur"

    if rv >= 2.0 and oe > 8:
        return "strong participation with already extended price behavior"
    if rv >= 2.0:
        return "aggressive market participation"
    if oe > 10:
        return "extended structure with elevated pullback risk"
    if "neutral" in s:
        return "rather neutral market structure"
    if "risk" in s or "danger" in s:
        return "fragile structure with elevated fake-move risk"
    return "stable but not fully confirmed structure"


def _strategist_market_narrative(digest: dict, lang: str = "en") -> str:
    coins = digest.get("coins") if isinstance(digest, dict) else []
    if not isinstance(coins, list) or not coins:
        return ""

    ranked = sorted(
        [c for c in coins if isinstance(c, dict)],
        key=lambda c: (
            _safe_float(c.get("score"), 0.0),
            _safe_float(c.get("rvol"), 0.0),
            abs(_safe_float(c.get("change_24h_pct"), 0.0))
        ),
        reverse=True,
    )

    top = ranked[0]
    sym = top.get("symbol") or "UNKNOWN"
    phrase = _narrative_phrase_from_market_state(
        top.get("market_condition_state"),
        top.get("rvol"),
        top.get("overextension_pct"),
        lang,
    )

    if lang == "de":
        return f"{sym} zeigt aktuell {phrase}. Der Strategist sollte die Qualität der Teilnahme höher gewichten als reine Preisbewegung."
    return f"{sym} currently shows {phrase}. The Strategist should weight participation quality higher than raw price movement."



def _edge_confidence_label(spread_pct, volume_share=None, liquidity_score=None):
    try:
        s = float(spread_pct or 0)
    except Exception:
        s = 0.0

    score = 0
    if s >= 2:
        score += 1
    if s >= 5:
        score += 1
    if s >= 10:
        score += 1

    try:
        v = float(volume_share or 0)
        if v >= 15:
            score += 1
    except Exception:
        pass

    try:
        lq = float(liquidity_score or 0)
        if lq >= 60:
            score += 1
    except Exception:
        pass

    if score >= 4:
        return "HIGH"
    if score >= 2:
        return "MEDIUM"
    return "LOW"


def _estimate_net_edge(spread_pct, fee_pct=0.45, slippage_pct=0.35):
    try:
        gross = float(spread_pct or 0)
    except Exception:
        gross = 0.0

    net = gross - fee_pct - slippage_pct
    return round(net, 2)


def _extract_exchange_name(value) -> str:
    if isinstance(value, dict):
        return str(value.get("exchange") or value.get("market") or value.get("name") or value.get("identifier") or "").strip()
    if isinstance(value, str):
        return value.strip()
    return ""


def _extract_exchange_price(value):
    if isinstance(value, dict):
        for k in ("price", "last", "converted_last_usd", "usd", "value"):
            try:
                x = float(value.get(k))
                if math.isfinite(x):
                    return x
            except Exception:
                pass
    return None

def _strategist_context_digest(extra_context: dict | None) -> dict:
    """Create compact data digest for the Strategist so it can reason without dumping raw fields."""
    ctx = extra_context if isinstance(extra_context, dict) else {}
    coins = ctx.get("coins") if isinstance(ctx.get("coins"), list) else []
    pairs = ctx.get("relevant_pairs") if isinstance(ctx.get("relevant_pairs"), list) else []
    all_pairs = ctx.get("all_compare_pairs") if isinstance(ctx.get("all_compare_pairs"), list) else []

    def nf(v, default=None):
        try:
            x = float(v)
            if math.isfinite(x):
                return x
        except Exception:
            pass
        return default

    coin_digest = []
    for c in coins[:12]:
        if not isinstance(c, dict):
            continue
        mc = c.get("market_condition") if isinstance(c.get("market_condition"), dict) else {}
        ex = c.get("exchange_intelligence") if isinstance(c.get("exchange_intelligence"), dict) else {}
        coin_digest.append({
            "symbol": str(c.get("symbol") or "").upper(),
            "score": nf(c.get("score")),
            "rating": c.get("rating"),
            "change_24h_pct": nf(c.get("change_24h_pct")),
            "volume_24h": nf(c.get("volume_24h")),
            "market_condition_state": mc.get("state") or mc.get("label") or "",
            "overextension_pct": nf(mc.get("oe_pct")),
            "rvol": nf(mc.get("rvol")),
            "exchange_cheapest": _extract_exchange_name(ex.get("cheapest_exchange") or ex.get("lowest_exchange") or ex.get("cheapest") or ex.get("min_exchange")),
            "exchange_highest": _extract_exchange_name(ex.get("highest_exchange") or ex.get("top_exchange") or ex.get("highest") or ex.get("max_exchange")),
            "exchange_cheapest_price": _extract_exchange_price(ex.get("cheapest_exchange") or ex.get("lowest_exchange") or ex.get("cheapest") or ex.get("min_exchange")),
            "exchange_highest_price": _extract_exchange_price(ex.get("highest_exchange") or ex.get("top_exchange") or ex.get("highest") or ex.get("max_exchange")),
            "exchange_premium_pct": nf(ex.get("exchange_premium_pct") or ex.get("premium_pct") or ex.get("spread_pct")),
            "top_exchange_volume_share_pct": nf(ex.get("top_exchange_volume_share_pct") or ex.get("volume_share_pct")),
            "exchange_warning": ex.get("warning") or ex.get("quality") or "",
            "confidence_band": _strategist_confidence_band(
                c.get("score"),
                mc.get("rvol"),
                mc.get("oe_pct"),
            ),
            "tradeability_hint": _strategist_tradeability_hint(
                ex.get("exchange_premium_pct") or ex.get("premium_pct") or ex.get("spread_pct"),
                ex.get("top_exchange_volume_share_pct") or ex.get("volume_share_pct"),
                "de" if str(ctx.get("user_language") or "").lower() == "de" else "en",
            ),
        })

    pair_digest = []
    for p in (pairs or all_pairs)[:12]:
        if not isinstance(p, dict):
            continue
        pair_digest.append({
            "pair": p.get("pair"),
            "score": nf(p.get("score")),
            "corr": nf(p.get("corr")),
            "spread_pct": nf(p.get("spread_pct") or p.get("spreadPct")),
            "rsi_gap": nf(p.get("rsi_gap") or p.get("rsiGap")),
            "momentum_score": nf(p.get("momentum_score") or p.get("momentumScore")),
            "opportunity_score": nf(p.get("opportunity_score") or p.get("opportunityScore")),
            "stability_score": nf(p.get("stability_score") or p.get("stabilityScore")),
        })

    return {
        "user_intent": ctx.get("user_intent") or "",
        "selected_origin": ctx.get("selected_origin") or "",
        "analysis_symbols": ctx.get("analysis_symbols") or [],
        "coins": coin_digest,
        "pairs": pair_digest,
        "has_exchange_intelligence": any(bool(x.get("exchange_cheapest") or x.get("exchange_highest") or x.get("exchange_premium_pct") is not None) for x in coin_digest),
    }



def _strategist_confidence_band(score: float | None, rvol: float | None, overextension: float | None) -> str:
    s = _safe_float(score, 0.0)
    rv = _safe_float(rvol, 0.0)
    oe = abs(_safe_float(overextension, 0.0))

    confidence = 0
    if s >= 70:
        confidence += 2
    elif s >= 50:
        confidence += 1

    if rv >= 2.0:
        confidence += 2
    elif rv >= 1.2:
        confidence += 1

    if oe >= 12:
        confidence -= 2
    elif oe >= 8:
        confidence -= 1

    if confidence >= 3:
        return "high"
    if confidence >= 1:
        return "medium"
    return "low"


def _strategist_tradeability_hint(exchange_premium_pct: float | None, volume_share_pct: float | None, lang: str = "en") -> str:
    prem = abs(_safe_float(exchange_premium_pct, 0.0))
    vol = _safe_float(volume_share_pct, 0.0)

    if lang == "de":
        if prem < 0.5:
            return "Der sichtbare Preisunterschied wirkt eher klein und könnte durch Gebühren oder Slippage neutralisiert werden."
        if vol < 8:
            return "Der Vorteil existiert zwar, aber die Liquiditäts-/Volumentiefe wirkt noch schwach."
        if prem >= 1.5 and vol >= 15:
            return "Der Preisunterschied wirkt aktuell handelbarer als normale Marktgeräusche."
        return "Der Vorteil sollte zusätzlich über Liquidität und Ausführungsqualität bestätigt werden."

    if prem < 0.5:
        return "The visible price difference looks small and could be neutralized by fees or slippage."
    if vol < 8:
        return "The edge exists, but liquidity/volume depth still looks weak."
    if prem >= 1.5 and vol >= 15:
        return "The price difference currently looks more tradable than normal market noise."
    return "The edge should still be confirmed through liquidity and execution quality."

def _strategist_deterministic_overlay(intent: str, lang: str, digest: dict) -> str:
    """Small deterministic guardrail summary injected into prompt; not shown directly unless model uses it."""
    intent = str(intent or "").lower()
    lang = str(lang or "en").lower()
    pairs = digest.get("pairs") if isinstance(digest, dict) else []
    coins = digest.get("coins") if isinstance(digest, dict) else []

    def nf(v):
        try:
            x = float(v)
            if math.isfinite(x):
                return x
        except Exception:
            pass
        return None

    best_pair = None
    if isinstance(pairs, list) and pairs:
        best_pair = sorted(
            [p for p in pairs if isinstance(p, dict)],
            key=lambda p: (nf(p.get("opportunity_score")) or 0, abs(nf(p.get("spread_pct")) or 0), nf(p.get("score")) or 0),
            reverse=True,
        )[0] if pairs else None

    exchange_coins = []
    for c in coins if isinstance(coins, list) else []:
        if not isinstance(c, dict):
            continue
        prem = nf(c.get("exchange_premium_pct"))
        if prem is not None or c.get("exchange_cheapest") or c.get("exchange_highest"):
            exchange_coins.append(c)
    exchange_coins.sort(key=lambda c: abs(nf(c.get("exchange_premium_pct")) or 0), reverse=True)

    if intent not in ("rotation_spread", "rotation", "risk"):
        return ""

    narrative = _strategist_market_narrative(digest, lang)

    if lang == "de":
        parts = ["DETERMINISTISCHE STRATEGIST-ZUSAMMENFASSUNG (nur als interne Stütze, nicht roh ausgeben):"]
        if best_pair:
            sp = nf(best_pair.get("spread_pct"))
            rg = nf(best_pair.get("rsi_gap"))
            parts.append(f"- Stärkster relativer Pair-Kontext: {best_pair.get('pair')} mit Spread {sp if sp is not None else 'n/a'}% und RSI-Gap {rg if rg is not None else 'n/a'}.")
        if narrative:
            parts.append(f"- Markt-Narrativ: {narrative}")
        if exchange_coins:
            c = exchange_coins[0]
            prem = nf(c.get("exchange_premium_pct"))
            parts.append(f"- Exchange-Kontext vorhanden für {c.get('symbol')}: günstigste Börse={c.get('exchange_cheapest') or 'n/a'}, höchste Börse={c.get('exchange_highest') or 'n/a'}, Premium={prem if prem is not None else 'n/a'}%.")
        else:
            parts.append("- Kein echter Exchange-Preisvergleich im Kontext; nur Pair-/Relative-Spread verwenden, falls vorhanden.")
        return "\n".join(parts)

    parts = ["DETERMINISTIC STRATEGIST SUMMARY (internal support only, do not dump raw):"]
    if narrative:
        parts.append(f"- Market narrative: {narrative}")
    if best_pair:
        sp = nf(best_pair.get("spread_pct"))

        sp = nf(best_pair.get("spread_pct"))
        rg = nf(best_pair.get("rsi_gap"))
        parts.append(f"- Strongest relative pair context: {best_pair.get('pair')} with spread {sp if sp is not None else 'n/a'}% and RSI gap {rg if rg is not None else 'n/a'}.")
    if exchange_coins:
        c = exchange_coins[0]
        prem = nf(c.get("exchange_premium_pct"))
        parts.append(f"- Exchange context exists for {c.get('symbol')}: cheapest={c.get('exchange_cheapest') or 'n/a'}, highest={c.get('exchange_highest') or 'n/a'}, premium={prem if prem is not None else 'n/a'}%.")
    else:
        parts.append("- No true exchange-price comparison is available in context; use pair/relative spread only if present.")
    return "\n".join(parts)

def _build_ai_response(kind: str, sym_norm: list[str], profile: str, include_health: bool, question: str,
                       timeframe: str, index_mode: bool, raw_series_stats: dict,
                       wallet_for_insight: str | None = None, chat_memory_wallet: str | None = None,
                       short_insight_mode: bool = False, extra_context: dict | None = None):
    """
    Shared AI response builder.

    wallet_for_insight:
      wallet whose order_memory / insight_profile should be included.
    chat_memory_wallet:
      wallet whose ai_memory chat history should be used and updated.
      Keep this ONLY for AI Analyst / chat style endpoints.
    """
    market_context = _build_ai_market_context(sym_norm, profile=profile, include_health=include_health)
    timeframe_context = _build_ai_timeframe_context(sym_norm, timeframe, raw_series_stats, index_mode=index_mode)

    try:
        order_memory, insight_profile = _insight_profile_get(wallet_for_insight) if wallet_for_insight else ({}, {})
        if wallet_for_insight and not order_memory and not insight_profile:
            order_memory, insight_profile = _refresh_user_insight_profile(wallet_for_insight)
    except Exception:
        order_memory, insight_profile = {}, {}

    ai_mode = _normalize_ai_mode((extra_context or {}).get("ai_mode") if isinstance(extra_context, dict) else "standard")
    compare_weights_for_ai = _normalize_compare_weights_for_ai((extra_context or {}).get("compare_weights") if isinstance(extra_context, dict) else {})
    response_language = _detect_user_language_from_text(str((extra_context or {}).get("raw_user_question") or question), (extra_context or {}).get("user_language") if isinstance(extra_context, dict) else None)
    language_hard_rule = _language_hard_rule(response_language)
    strategist_intent = _strategist_intent_from_payload(question, extra_context if isinstance(extra_context, dict) else {})
    strategist_followup_rule = _strategist_followup_rule(str((extra_context or {}).get("raw_user_question") or question), strategist_intent, response_language)
    strategist_profile = _strategist_response_profile(strategist_intent, response_language)
    strategist_digest = _strategist_context_digest(extra_context if isinstance(extra_context, dict) else {})
    strategist_overlay = _strategist_deterministic_overlay(strategist_intent, response_language, strategist_digest)

    ai_engine_v2 = {}
    try:
        if short_insight_mode:
            ai_engine_v2 = _ai_engine_v2_from_context(
                sym_norm=sym_norm,
                market_context=market_context,
                timeframe_context=timeframe_context,
                order_memory=order_memory,
                insight_profile=insight_profile,
                extra_context=extra_context if isinstance(extra_context, dict) else {},
            )
    except Exception:
        ai_engine_v2 = {}

    use_order_memory = bool(wallet_for_insight)
    use_chat_memory = bool(chat_memory_wallet)
    PRO_STYLE_RULES = """
    STYLE RULES (CRITICAL):

    - Never give direct instructions to the user
    - Do NOT use: "you should", "you must", "consider buying/selling"
    - Do NOT speak directly to the user in a commanding tone

    INSTEAD:
    - Use neutral, system-level interpretation
    - Describe what the setup suggests, not what the user must do
    - Keep language calm, analytical, and professional

    TONE:
    - Calm
    - Professional
    - Analytical
    - Non-instructional
    """
    insight_length_rules = ""
    if short_insight_mode:
        insight_length_rules = """
13) This is AI Insight Level 2, not AI Analyst.
14) Keep the answer compact and trader-usable: explain consequence, not just description.
15) Prefer decision-support language over report style.
16) Do NOT dump raw stats, long metric lists, repeated timeframe blocks, or full summaries.
17) Focus on relationships between metrics, not isolated numbers.
18) Explain what the COMBINATION of correlation, spread, momentum, volatility, drawdown, rating, community rating, on-chain signal, Market Condition, and wallet-fit implies for likely behavior.
19) REQUIRED Level 2 output:
    - structure read: what the pair structure currently looks like,
    - behavior read: range-bound, mean-reversion style, trend-bias, unstable/choppy, rotation, or low-conviction,
    - strategy fit: grid-fit, rotation-style, no-clean-setup, continuation-risk, or volatility-sensitive,
    - risk reason: why the risk state exists.
20) REQUIRED when ai_signal_context is present:
    - Use ai_engine_v2 as the primary Level 2 interpretation layer when present.
    - Do not contradict ai_engine_v2.verdict, ai_engine_v2.risk, ai_engine_v2.edge, ai_engine_v2.invalidation, or ai_engine_v2.setup_bias.
    - Do NOT mention raw ratings, votes, contract mapping, CoinGecko status, or long metric lists.
    - Do NOT repeat UI-visible numbers unless one number is essential to the behavior read.
    - Use on-chain, rating, community, and market-condition data only as hidden supporting context.
    - Translate the strongest signals into market behavior, confirmation quality, strategy fit, and risk reason.
21) Market Condition interpretation rules:
    - High OE + low RVOL = weak participation / fake-move risk / unstable continuation.
    - High OE + high RVOL = stronger momentum quality / volume-backed continuation risk.
    - Low OE + rising or high RVOL = early accumulation / volume build before full extension.
    - High price extension + declining or weak volume = possible distribution / exhaustion risk.
    - Normal OE/RVOL = do not overstate; say market-condition confirmation is neutral.
22) Never tell the user what to do. Do not use buy/sell instructions. Describe what the structure favors or fails to confirm.
23) Maximum length target: about 95 to 150 words.
24) Never write like a long analyst report for AI Insight.
25) Prefer a compact structure with these labels when useful:
    - "Edge:" what the structure favors, without direct advice.
    - "Risk:" what can invalidate or weaken the read.
    - "Setup bias:" e.g. mean-reversion, rotation, continuation-risk, grid-friendly, volatility-sensitive.
26) Prefer concise behavior phrases like:
    - "confirmation quality remains weak"
    - "movement looks reactive rather than trend-supported"
    - "structure favors mean-reversion over clean continuation"
    - "participation supports continuation quality"
    - "momentum appears stretched or tiring"
    - "behavior looks range-bound / mean-reversion style / unstable"
    - "strategy fit is grid-friendly / volatility-sensitive / no-clean-setup"
27) Avoid generic filler like "monitor across multiple windows" unless it adds clear meaning.
28) Do NOT list timeframe outputs like "7D neutral, 30D neutral, 90D neutral".
29) Do NOT repeat structures already visible in the UI.
30) Always merge all signals into ONE combined interpretation.
31) Prefer one strong paragraph plus optional compact Edge/Risk/Setup bias lines.
32) Avoid breaking the answer into many titled parts.
33) Your output MUST follow this compact market-intelligence structure:

Market Structure: ...
Liquidity State: ...
Risk Posture: ...
Pair Relationship: ...
Tactical Read: ...
Invalidations: ...

Edge: ...
Risk: ...
Setup bias: ...

34) Hard rules:
- Use the section labels exactly once where possible.
- Do NOT turn this into a long report. Keep each section short.
- Do NOT skip Edge, Risk, or Setup bias.
- If liquidity context is weak/neutral, keep Liquidity State subtle.

35) Additional rules:
- Do NOT repeat raw metrics or numbers
- Do NOT restate all data points
- Do NOT mention Votes, Ratings, CoinGecko, contract mapping, or missing token mapping
- Do NOT write "Signal context" or dump source context into the answer
- Focus on interpretation, not description
- Keep it tight, clear, and trading-relevant
"""

    analyst_concise_rules = "" if short_insight_mode else """
AI ANALYST OUTPUT FORMAT — KEEP IT SHORT:
- Do not write long reports or essay-style paragraphs.
- Maximum length: usually 6-10 compact bullet lines.
- Use short section labels only when useful.
- Prefer this structure when applicable: Setup / Entry Logic / Risk / Failure / Next Check.
- Each bullet should be one clear sentence.
- Do not add long disclaimers. Use at most one short safety note at the end.
- Do not explain basic trading terms unless the user asks.
- If Pine Script/code is requested, provide code plus a very short explanation.
"""

    sys = f"""You are Nexus Strategist, the intelligent market and strategy layer inside Nexus Analyt.

{language_hard_rule}

STRATEGIST INTENT LAYER:
- Understand natural user language, even when the user does not use professional trading terms.
- Translate casual user wording into market intent internally:
  "wo ist besser" / "where is better" => compare value, liquidity and spread.
  "lohnt sich das" / "is it worth it" => edge quality and risk/reward quality.
  "ist das echt" / "is it real" => confirmation quality, fake-move risk and volume participation.
  "wo mehr gehandelt" / "where traded more" => volume/liquidity bias.
  "welcher ist stärker" / "which is stronger" => relative strength and rotation.
  "kann man das nehmen" / "can this be used" => setup quality, not a buy/sell command.
- If the user asks about "cheap", "expensive", "buy cheaper", "sell higher", "wo guenstig", "teurer verkaufen", "lohnt Rotation", "mehr gehandelt", "wo ist mehr Bewegung", interpret it as relative value / rotation / exchange premium / liquidity-confirmation analysis.
- If the user asks about danger, fake movement, manipulation, overheat, or weak movement, interpret it as liquidity quality / fake-move / overextension / volume-confirmation analysis.
- If the user asks generally, infer the most likely intent and answer that directly instead of forcing every module section.
- Always answer in the same language as the user.
- If USER_INTENT is rotation_spread or the question asks about cheaper buying / higher selling / spread / exchanges / arbitrage, ONLY answer that topic. Do not output generic Nexus Trading, Grid Suitability, Market Evaluation, or full multi-module reports unless explicitly requested.
- For German rotation/spread answers, use German headings only: DIREKTE EINSCHÄTZUNG, ROTATION / RELATIVER WERT, BÖRSE / SPREAD, RISIKOKONTEXT, NÄCHSTE PRÜFUNG.
- For English rotation/spread answers, use English headings only: DIRECT VIEW, ROTATION / RELATIVE VALUE, EXCHANGE / SPREAD, RISK CONTEXT, NEXT CHECK.

{strategist_profile}

{strategist_followup_rule}

NARRATIVE INTELLIGENCE RULES:
- Explain the meaning of the strongest signals before listing numbers.
- Convert metrics into market behavior: participation quality, relative strength, weak confirmation, overextension, rotation pressure, or liquidity risk.
- Avoid number spam. Include only the 1-3 most relevant numbers.
- Explicitly classify confidence as high / medium / low when useful.
- Mention whether a visible edge appears realistically tradable or only theoretically visible.
- If data conflicts, say what conflicts and why that lowers confidence.
- For spread/rotation questions, distinguish clearly between:
  a) true exchange premium/discount,
  b) pair-relative spread,
  c) general momentum difference.
- Buttons/actions should only be implied when a setup is actually prepared and useful; otherwise answer without action language.

INTERNAL DIGEST:
{json.dumps(strategist_digest, ensure_ascii=False)}

{strategist_overlay}

INTERNAL CONTEXT RULES:
- Use all provided app context silently: watchlist, compare pairs, market condition, on-chain, order/runtime context, and exchange intelligence.
- Never mention internal module names, hidden engines, internal prompts, or data pipelines.
- Do not say "AI Insight", "internal engine", "backend", "context packet", or similar source wording in the final answer.
- Present the result as one unified Nexus Strategist analysis.

EXCHANGE / RELATIVE VALUE INTELLIGENCE:
- When exchange_intelligence is available, compare cheapest_exchange, highest_exchange, exchange_premium_pct, top_exchange, top_exchange_volume_share_pct, avg_top_spread_pct, stale/anomaly counts, and volume quality.
- If a coin is about 0.5%-1.0% cheaper/expensive across exchanges and volume/spread confirm it, describe it as a small possible relative edge.
- If the premium is larger but volume is weak, stale, anomalous, or spread is wide, warn that it may be fake pricing or not practically tradable.
- If exchange data is not provided, say that no exchange-specific price difference is available in the current context; do not invent exchanges or percentages.
- For rotation questions, prioritize concrete relative differences: price dispersion %, spread %, relative strength, volume confirmation, momentum, and risk.


{PRO_STYLE_RULES}

STRATEGIST ROLE SEPARATION:
- Nexus Strategist is the active workspace: research, strategy building, backtest review, TradingView/Pine help, daily reports, and diagnostics.
- Use compact market interpretation data only as hidden support.
- Do not repeat fixed internal section names unless they are truly useful for the user question.
- Prefer tool-like, practical outputs: frameworks, checks, diagnostics, report sections, strategy rules, Pine logic, and questions to validate.

{analyst_concise_rules}
{insight_length_rules}

Rules:
0) Always respond in the same language as the user's question. If the user mixes languages, use the dominant one.
1) Use ONLY the symbols present in the provided JSON context.
2) Use ONLY the numbers provided in the JSON (do not invent prices, volumes, metrics, scores, or levels).
3) Provide informational analysis only. No financial advice. No buy/sell instructions.
4) Do NOT output exact trade entries/exits or prescriptive price levels. If asked, provide an educational template instead.
5) The app is MANUAL-only: never suggest automatic order placement; focus on manual decision support.
6) Timeframe integrity is mandatory:
   - requested_timeframe = what the user selected.
   - actual_timeframe_used = what data is truly available.
   - If actual_timeframe_used differs from requested_timeframe, you MUST say so clearly.
   - If timeframe stats are partial or missing, explicitly mention that the analysis is partial or snapshot-based.
   - NEVER claim a 30D/90D/1Y analysis unless the provided timeframe context says that timeframe was actually used.
7) When timeframe_context.series_stats are available, treat them as the PRIMARY source for timeframe analysis.
   Snapshot market_context is supplemental only and must not override timeframe_context.
8) Do NOT infer missing 30D values from 90D snapshots or 24h data.
9) If wallet-specific order_memory or insight_profile is present, use it only to describe the user's observed setup style, structure, and risk posture.
10) Never tell the user they must change, place, remove, or move an order. Do not use imperative trading language such as "you must", "set", "buy now", or "sell now".
11) Never mix AI Analyst chat memory with AI Insight order memory. If order_memory / insight_profile are present, treat them as wallet setup context only, not as a chat transcript.
12) Do not write as if the user asked for direct instructions. Describe, interpret, compare, and explain only.
13) When several metrics point in different directions, explain the conflict briefly instead of listing everything.
14) Prefer interpretation of structure over enumeration of values.
15) If ai_signal_context is present, use rating, community, on-chain, watchlist momentum, and pair context only as hidden support. Do not name or list those raw fields unless essential.
16) Treat on-chain signals as supporting evidence only, not as a standalone reason. Never overstate weak or missing signals.
17) If on-chain data is neutral/missing for a symbol, say it is neutral only when relevant; do not present it as a failure.
18) Market Condition is based on Overextension (distance from MA20) plus Relative Volume (RVOL). Use it as movement-quality context:
   - FAKE_MOVE = price extended but volume weak; describe possible unstable/weak move risk.
   - REAL_BREAKOUT = price extended but volume confirms; describe stronger momentum quality.
   - EARLY_ACCUMULATION = volume is high while price is not yet extended; describe early volume build.
   - OVEREXTENDED = price far above MA20; describe heat/pullback risk without sounding certain.
   - NORMAL = no strong OE/RVOL anomaly.
19) Never treat Market Condition as a direct buy/sell signal. It is probability / behavior context only.
20) For AI Insight Level 2, always translate the combined data into behavior + strategy fit + risk reason.
21) AI Insight mode: standard = balanced professional interpretation; extreme = more sensitive to early momentum, rebound, spread, and high-risk/high-reward structures, while still warning clearly about invalidation.
22) Custom Compare weights influence interpretation priority. Momentum weight increases focus on shifts/RSI gaps; opportunity weight increases focus on spread/hidden setups; stability weight increases focus on correlation and volatility quality.
23) If ai_engine_v2.pair_alerts exists, use it as movement-chance context across all Compare pairs, not only the selected pair.
24) If ai_engine_v2.market_behavior or ai_engine_v2.market_phase exists, use it as INTERNAL interpretation context only. Do not dump raw behavior fields; translate the strongest regime/phase signal into the paragraph, Edge, Risk, or Setup bias.
24b) If ai_engine_v2.liquidity_context exists, use it only as INTERNAL language guidance. Mention liquidity-trap, stop-hunt sensitivity, liquidity vacuum, thin participation, or participation-supported movement only when it is clearly relevant. Never print raw trap scores or liquidity metrics.
25) Market regime priority for AI Insight: trend = continuation can be respected; range = mean-reversion is favored; chop = low-confirmation/fake-move risk; volatile/panic = fast invalidation; euphoria = exhaustion/blow-off risk; accumulation = constructive watch; distribution = fading participation.
26) Market behavior priority for AI Insight:
   - high fake_move_risk => say the move may be poorly confirmed / unstable / fake-move risk,
   - high exhaustion_risk => say momentum may be stretched or tiring,
   - high volume_confirmation + continuation_quality => say participation supports continuation quality,
   - accumulation_signal => say behavior looks like early accumulation / volume build,
   - mixed_or_neutral => do not overstate behavior.
26) Use market_behavior_summary when present as the compact source of truth for behavior interpretation.
27) Forbidden in the final AI Insight answer: "Votes", "Rating", "CoinGecko", "contract mapping", "Signal context".
28) Preferred final answer style: one compact behavior paragraph, then Edge/Risk/Setup bias. No data dump.
29) For rotation / cheap-vs-expensive questions, answer with a clear conclusion first: "Rotation Vorteil vorhanden", "Kein sauberer Vorteil", or the equivalent in the user's language.
30) When numbers exist, include the relevant percentage difference. If the only available difference is pair spread, say it is a pair/relative spread. If exchange_intelligence exists, say it is an exchange price difference.
31) Never invent exchange names, exchange-specific prices, premiums, orderbook depth, or arbitrage edges.
{insight_length_rules}
Task:
{_ai_kind_instructions(kind)}
"""

    user_payload = {
        "kind": kind,
        "question": question,
        "profile": profile,
        "include_health": include_health,
        "requested_timeframe": timeframe_context.get("requested_timeframe"),
        "actual_timeframe_used": timeframe_context.get("actual_timeframe_used"),
        "coverage_note": timeframe_context.get("coverage_note"),
        "index_mode": bool(index_mode),
        "timeframe_context": timeframe_context,
        "market_context": market_context,
        "short_insight_mode": bool(short_insight_mode),
        "ai_mode": ai_mode,
        "compare_weights": compare_weights_for_ai,
        "user_intent": strategist_intent,
        "response_profile": strategist_profile,
        "strategist_followup": bool(strategist_followup_rule),
        "strategist_digest": strategist_digest,
    }
    if ai_engine_v2:
        user_payload["ai_engine_v2"] = ai_engine_v2

    if isinstance(extra_context, dict) and extra_context:
        user_payload["ai_signal_context"] = extra_context
        user_payload["rating_community_onchain_context"] = extra_context
        user_payload["must_use_ai_signal_context"] = True

    if use_order_memory:
        user_payload["order_memory"] = order_memory
        user_payload["insight_profile"] = insight_profile

    mem_msgs = _ai_mem_get(chat_memory_wallet) if use_chat_memory else None
    resp, err_pair = _ai_call_openai(
        sys,
        user_payload,
        wallet_address=chat_memory_wallet if use_chat_memory else None,
        mem_msgs=mem_msgs,
        short_insight_mode=bool(short_insight_mode),
    )
    if err_pair:
        msg, code = err_pair
        return None, (msg, code)

    resp["context_used"] = {
        "symbols": sym_norm,
        "profile": profile,
        "include_health": include_health,
        "requested_timeframe": timeframe_context.get("requested_timeframe"),
        "actual_timeframe_used": timeframe_context.get("actual_timeframe_used"),
        "timeframe_match": timeframe_context.get("timeframe_match"),
        "coverage_note": timeframe_context.get("coverage_note"),
        "has_order_memory": bool(use_order_memory and order_memory),
        "has_insight_profile": bool(use_order_memory and insight_profile),
        "chat_memory_used": bool(use_chat_memory),
        "insight_memory_used": bool(use_order_memory),
        "has_ai_signal_context": bool(isinstance(extra_context, dict) and extra_context),
        "has_market_condition_context": any(bool((c or {}).get("market_condition")) for c in (market_context.get("coins") or [])),
        "has_exchange_intelligence": any(bool((c or {}).get("exchange_intelligence")) for c in (market_context.get("coins") or [])),
        "has_ai_engine_v2": bool(ai_engine_v2),
        "user_intent": strategist_intent,
        "response_language": response_language,
        "has_strategist_digest": bool(strategist_digest),
        "strategist_followup": bool(strategist_followup_rule),
    }
    if ai_engine_v2:
        resp["ai_engine_v2"] = ai_engine_v2
    return resp, None



def _symbols_from_ai_context(ctx: dict, limit: int = 8) -> list[str]:
    """Extract hidden symbol scope from frontend watchlist/compare context."""
    if not isinstance(ctx, dict):
        return []
    out=[]
    def add(sym):
        s=str(sym or "").strip().upper()
        if s and re.match(r"^[A-Z0-9]{2,12}$", s) and s not in out:
            out.append(s)
    for c in ctx.get("coins") or []:
        if isinstance(c, dict): add(c.get("symbol"))
    for p in (ctx.get("relevant_pairs") or []) + (ctx.get("all_compare_pairs") or []):
        pair=str((p or {}).get("pair") or "")
        for part in pair.split("/")[:2]: add(part)
    return out[:max(1,min(int(limit or 8),12))]

@app.route("/api/ai/run", methods=["POST"])
def api_ai_run():
    """AI Analyst endpoint (chat/follow-up). Uses ai_memory only, never order_memory."""
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

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
    timeframe = _normalize_ai_timeframe(body.get("timeframe") or "90D")
    index_mode = bool(body.get("index_mode", False))
    raw_series_stats = body.get("series_stats") or {}
    ai_signal_context = body.get("ai_signal_context") or body.get("ai_context") or {}
    if isinstance(ai_signal_context, dict):
        ai_signal_context = dict(ai_signal_context)
        if body.get("user_language"):
            ai_signal_context["user_language"] = body.get("user_language")
        if body.get("user_intent"):
            ai_signal_context["user_intent"] = body.get("user_intent")
        if body.get("raw_user_question"):
            ai_signal_context["raw_user_question"] = body.get("raw_user_question")

    sym_norm = [(s or "").strip().upper() for s in symbols if (s or "").strip()]
    sym_norm = list(dict.fromkeys(sym_norm))
    if not sym_norm and isinstance(ai_signal_context, dict):
        sym_norm = _symbols_from_ai_context(ai_signal_context, limit=8)

    # New AI Analyst workspace mode:
    # The analyst is now task-based and must also run without visible coin chips / Compare symbols.
    # Research and Daily Report may still receive symbols as hidden context from the frontend,
    # but Strategy Builder, Pine Builder, Backtest Review, and Trade Review often have no symbols at all.
    if not sym_norm:
        workspace_language = _detect_user_language_from_text(str(body.get("raw_user_question") or question), body.get("user_language"))
        workspace_language_rule = _language_hard_rule(workspace_language)
        workspace_sys = f"""You are Nexus Analyt AI Analyst, an active research, strategy, Pine Script, backtest, daily report, and trade-review workspace.

{workspace_language_rule}

Rules:
- Always answer in the same language as the user's task.
- The request is task-based; do not require coin symbols.
- Do not say that no symbols were provided unless the user specifically asked for coin-specific market analysis.
- Do not invent live prices, volumes, market data, whale activity, or current market facts.
- If the task requires live/current market data that is not provided, clearly say that the answer is based only on the supplied task/context.
- Provide educational analysis, structure, diagnostics, and templates only.
- No financial advice, no direct buy/sell instruction, no exact prescriptive entry/exit levels.
- Keep the answer practical and focused on the selected AI Analyst mode.
- Do not write long reports or essay-style paragraphs.
- Maximum length: usually 6-10 compact bullet lines.
- Use short section labels only when useful.
- Prefer this structure when applicable: Setup / Entry Logic / Risk / Failure / Next Check.
- Each bullet should be one clear sentence.
- Do not add long disclaimers. Use at most one short safety note at the end.
- Do not explain basic trading terms unless the user asks.
- If Pine Script/code is requested, provide code plus a very short explanation.

Mode instructions:
{_ai_kind_instructions(kind)}
"""
        user_payload = {
            "kind": kind,
            "question": question,
            "profile": profile,
            "requested_timeframe": timeframe,
            "selected_timeframe": body.get("selected_timeframe"),
            "index_mode": bool(index_mode),
            "symbols": [],
            "task_based_workspace": True,
            "ai_signal_context": ai_signal_context if isinstance(ai_signal_context, dict) else {},
        }
        mem_msgs = _ai_mem_get(wa)
        resp, err_pair = _ai_call_openai(
            workspace_sys,
            user_payload,
            wallet_address=wa,
            mem_msgs=mem_msgs,
            short_insight_mode=False,
        )
        if err_pair:
            msg, code = err_pair
            return err(msg, code)
        if isinstance(resp, dict):
            resp["context_used"] = {
                "symbols": [],
                "profile": profile,
                "task_based_workspace": True,
                "kind": kind,
            }
        return jsonify(resp)

    resp, err_pair = _build_ai_response(
        kind=kind,
        sym_norm=sym_norm,
        profile=profile,
        include_health=include_health,
        question=question,
        timeframe=timeframe,
        index_mode=index_mode,
        raw_series_stats=raw_series_stats,
        wallet_for_insight=None,
        chat_memory_wallet=wa,
        short_insight_mode=False,
        extra_context=ai_signal_context if isinstance(ai_signal_context, dict) else {},
    )
    if err_pair:
        msg, code = err_pair
        return err(msg, code)
    return jsonify(resp)



# -------------------------
# Adaptive Market Memory (phase 1: snapshot collection)
# -------------------------
def _market_memory_as_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        x = float(value)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return default


def _market_memory_pick_text(*values) -> str:
    for v in values:
        if isinstance(v, str) and v.strip():
            return v.strip()
        if v is not None and not isinstance(v, (dict, list, tuple)):
            sv = str(v).strip()
            if sv:
                return sv
    return ""


def _market_memory_deep_get(obj, *paths, default=None):
    for path in paths:
        cur = obj
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur.get(key)
            else:
                ok = False
                break
        if ok and cur is not None:
            return cur
    return default


def _market_memory_pair_from_symbols(symbols) -> tuple[str, str, str]:
    if not isinstance(symbols, list):
        symbols = []
    clean = [str(x or "").strip().upper() for x in symbols if str(x or "").strip()]
    if len(clean) >= 2:
        return f"{clean[0]}/{clean[1]}", clean[0], clean[1]
    if len(clean) == 1:
        return clean[0], clean[0], ""
    return "UNKNOWN", "", ""


def _market_memory_extract_snapshot(source: str, pair: str, symbol_a: str = "", symbol_b: str = "", wallet_address: str = "", payload: dict | None = None, extra_context: dict | None = None) -> dict:
    """Build a compact market-memory snapshot from existing AI/pair payloads.

    The function is deliberately defensive: the app already has several AI engines,
    so fields may live under ai_engine_v2, liquidity/trap context, market_condition,
    insight, risk, spread, or frontend-provided ai_signal_context.
    """
    payload = payload if isinstance(payload, dict) else {}
    extra_context = extra_context if isinstance(extra_context, dict) else {}
    engine = payload.get("ai_engine_v2") if isinstance(payload.get("ai_engine_v2"), dict) else {}
    liq = payload.get("liquidity_behavior") if isinstance(payload.get("liquidity_behavior"), dict) else {}
    regime_obj = payload.get("market_regime") if isinstance(payload.get("market_regime"), dict) else {}
    insight = payload.get("insight") if isinstance(payload.get("insight"), dict) else {}
    risk_obj = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    spread_obj = payload.get("spread") if isinstance(payload.get("spread"), dict) else {}

    # frontend/context may contain the freshest movement/opportunity data
    ctx_engine = extra_context.get("ai_engine_v2") if isinstance(extra_context.get("ai_engine_v2"), dict) else {}
    ctx_market = extra_context.get("market_condition") if isinstance(extra_context.get("market_condition"), dict) else {}
    ctx_liq = extra_context.get("liquidity_behavior") if isinstance(extra_context.get("liquidity_behavior"), dict) else {}

    regime = _market_memory_pick_text(
        _market_memory_deep_get(engine, ("market_regime", "phase")),
        _market_memory_deep_get(engine, ("regime", "phase")),
        regime_obj.get("phase"),
        regime_obj.get("regime"),
        ctx_engine.get("regime"),
        ctx_engine.get("market_regime"),
        extra_context.get("regime"),
    )
    liquidity_state = _market_memory_pick_text(
        _market_memory_deep_get(engine, ("liquidity", "state")),
        _market_memory_deep_get(engine, ("liquidity_behavior", "liquidity_quality")),
        liq.get("liquidity_quality"),
        liq.get("liquidity_state"),
        ctx_liq.get("liquidity_quality"),
        ctx_liq.get("liquidity_state"),
        risk_obj.get("liquidity_state"),
        _market_memory_deep_get(payload, ("risk", "liquidity_state")),
        extra_context.get("liquidity_state"),
    )
    tactical_state = _market_memory_pick_text(
        engine.get("tactical_state"),
        engine.get("setup_bias"),
        insight.get("setupBias"),
        insight.get("setup_bias"),
        extra_context.get("tactical_state"),
        extra_context.get("setup_bias"),
    )
    movement_quality = _market_memory_pick_text(
        engine.get("movement_quality"),
        engine.get("verdict"),
        payload.get("verdict"),
        payload.get("ai_verdict"),
        insight.get("verdictText"),
    )

    confidence = _market_memory_as_float(
        _market_memory_deep_get(engine, ("confidence", "score")),
        None,
    )
    if confidence is None:
        confidence = _market_memory_as_float(engine.get("confidence"), None)
    if confidence is None:
        confidence = _market_memory_as_float(payload.get("confidence"), None)
    if confidence is None:
        confidence = _market_memory_as_float(extra_context.get("confidence"), None)

    movement_score = _market_memory_as_float(
        engine.get("movement_score"),
        None,
    )
    if movement_score is None:
        movement_score = _market_memory_as_float(extra_context.get("movement_score"), None)
    if movement_score is None:
        movement_score = _market_memory_as_float(extra_context.get("opportunity_score"), None)

    risk = _market_memory_pick_text(
        engine.get("risk"),
        payload.get("risk"),
        extra_context.get("risk"),
        risk_obj.get("level"),
        risk_obj.get("state"),
    )

    spread = _market_memory_as_float(
        spread_obj.get("latest") if isinstance(spread_obj, dict) else None,
        None,
    )
    if spread is None:
        spread = _market_memory_as_float(extra_context.get("spread"), None)
    if spread is None:
        spread = _market_memory_as_float(extra_context.get("spread_pct"), None)
    if spread is None:
        spread = _market_memory_as_float(engine.get("spread"), None)

    rvol = _market_memory_as_float(
        _market_memory_deep_get(engine, ("market_condition", "rvol")),
        None,
    )
    if rvol is None:
        rvol = _market_memory_as_float(ctx_market.get("rvol"), None)
    if rvol is None:
        rvol = _market_memory_as_float(ctx_market.get("relative_volume"), None)
    if rvol is None:
        rvol = _market_memory_as_float(extra_context.get("rvol"), None)

    overextension = _market_memory_as_float(
        _market_memory_deep_get(engine, ("market_condition", "overextension")),
        None,
    )
    if overextension is None:
        overextension = _market_memory_as_float(ctx_market.get("oe_pct"), None)
    if overextension is None:
        overextension = _market_memory_as_float(ctx_market.get("overextension_pct"), None)
    if overextension is None:
        overextension = _market_memory_as_float(extra_context.get("overextension"), None)

    trap_risk = _market_memory_as_float(
        _market_memory_deep_get(engine, ("liquidity_behavior", "trap_risk")),
        None,
    )
    if trap_risk is None:
        trap_risk = _market_memory_as_float(liq.get("trap_risk"), None)
    if trap_risk is None:
        trap_risk = _market_memory_as_float(ctx_liq.get("trap_risk"), None)
    if trap_risk is None:
        trap_risk = _market_memory_as_float(extra_context.get("trap_risk"), None)

    price_a = _market_memory_as_float(_market_memory_deep_get(payload, ("market", symbol_a, "price")), None)
    price_b = _market_memory_as_float(_market_memory_deep_get(payload, ("market", symbol_b, "price")), None)

    meta = {
        "source": source,
        "engine_keys": sorted(list(engine.keys()))[:40] if isinstance(engine, dict) else [],
        "liquidity_warnings": engine.get("liquidity_warnings") or engine.get("warnings") or extra_context.get("warnings"),
        "pair_relationship": engine.get("pair_relationship") or extra_context.get("pair_relationship"),
        "raw_pair": pair,
    }

    return {
        "wallet_address": _norm_addr(wallet_address or ""),
        "pair": str(pair or "UNKNOWN").strip().upper(),
        "symbol_a": str(symbol_a or "").strip().upper(),
        "symbol_b": str(symbol_b or "").strip().upper(),
        "source": str(source or "ai_insight").strip().lower(),
        "timestamp": now_ts(),
        "regime": regime,
        "liquidity_state": liquidity_state,
        "tactical_state": tactical_state,
        "movement_quality": movement_quality,
        "movement_score": movement_score,
        "confidence": confidence,
        "risk": risk,
        "spread": spread,
        "rvol": rvol,
        "overextension": overextension,
        "trap_risk": trap_risk,
        "price_a": price_a,
        "price_b": price_b,
        "meta_json": json.dumps(meta, ensure_ascii=False, separators=(",", ":")),
    }


def _market_memory_save_snapshot(snapshot: dict) -> int | None:
    if not isinstance(snapshot, dict):
        return None
    pair = str(snapshot.get("pair") or "").strip().upper()
    if not pair or pair == "UNKNOWN":
        return None
    conn = _db()
    try:
        with DB_WRITE_LOCK:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO market_memory(
                    wallet_address, pair, symbol_a, symbol_b, source, timestamp,
                    regime, liquidity_state, tactical_state, movement_quality,
                    movement_score, confidence, risk, spread, rvol, overextension,
                    trap_risk, price_a, price_b, meta_json, created_ts
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    _norm_addr(snapshot.get("wallet_address") or ""),
                    pair,
                    str(snapshot.get("symbol_a") or "").strip().upper(),
                    str(snapshot.get("symbol_b") or "").strip().upper(),
                    str(snapshot.get("source") or "ai_insight").strip().lower(),
                    int(snapshot.get("timestamp") or now_ts()),
                    str(snapshot.get("regime") or ""),
                    str(snapshot.get("liquidity_state") or ""),
                    str(snapshot.get("tactical_state") or ""),
                    str(snapshot.get("movement_quality") or ""),
                    snapshot.get("movement_score"),
                    snapshot.get("confidence"),
                    str(snapshot.get("risk") or ""),
                    snapshot.get("spread"),
                    snapshot.get("rvol"),
                    snapshot.get("overextension"),
                    snapshot.get("trap_risk"),
                    snapshot.get("price_a"),
                    snapshot.get("price_b"),
                    str(snapshot.get("meta_json") or "{}"),
                    now_ts(),
                ),
            )
            snapshot_id = int(cur.lastrowid or 0)
            conn.commit()
            return snapshot_id
    except Exception as e:
        try:
            print("[WARN] market_memory snapshot failed:", e)
        except Exception:
            pass
        return None
    finally:
        conn.close()


def _market_memory_recent(wallet_address: str = "", pair: str = "", limit: int = 25) -> list[dict]:
    lim = max(1, min(100, int(limit or 25)))
    where = []
    params = []
    wa = _norm_addr(wallet_address or "")
    if wa:
        where.append("wallet_address=?")
        params.append(wa)
    if pair:
        where.append("pair=?")
        params.append(str(pair).strip().upper())
    sql = "SELECT * FROM market_memory"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY timestamp DESC, id DESC LIMIT ?"
    params.append(lim)
    conn = _db()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            try:
                d["meta"] = json.loads(d.get("meta_json") or "{}")
            except Exception:
                d["meta"] = {}
            d.pop("meta_json", None)
            rows.append(d)
        return rows
    finally:
        conn.close()


@app.route("/api/market-memory/recent", methods=["GET"])
def api_market_memory_recent():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    pair = str(request.args.get("pair") or "").strip().upper()
    try:
        limit = int(request.args.get("limit") or 25)
    except Exception:
        limit = 25
    return jsonify({
        "status": "ok",
        "wallet_address": wa,
        "pair": pair,
        "items": _market_memory_recent(wallet_address=wa, pair=pair, limit=limit),
        "ts": now_ts(),
    })


@app.route("/api/ai/insight", methods=["POST"])
def api_ai_insight():
    """AI Insight endpoint. Uses wallet-specific order_memory / insight_profile, never ai_memory chat history."""
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

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
    timeframe = _normalize_ai_timeframe(body.get("timeframe") or "90D")
    index_mode = bool(body.get("index_mode", False))
    raw_series_stats = body.get("series_stats") or {}
    ai_signal_context = body.get("ai_signal_context") or body.get("ai_context") or {}

    sym_norm = [(s or "").strip().upper() for s in symbols if (s or "").strip()]
    sym_norm = list(dict.fromkeys(sym_norm))
    if len(sym_norm) > 6:
        return err("max 6 symbols allowed", 400)
    if not sym_norm:
        return err("no symbols provided", 400)

    resp, err_pair = _build_ai_response(
        kind=kind,
        sym_norm=sym_norm,
        profile=profile,
        include_health=include_health,
        question=question,
        timeframe=timeframe,
        index_mode=index_mode,
        raw_series_stats=raw_series_stats,
        wallet_for_insight=wa,
        chat_memory_wallet=None,
        short_insight_mode=True,
        extra_context=ai_signal_context if isinstance(ai_signal_context, dict) else {},
    )
    if err_pair:
        msg, code = err_pair
        return err(msg, code)

    # Final safety gate for AI Insight: the UI must never receive source dumps
    # like Signal context, Votes, Ratings or CoinGecko mapping in the answer.
    try:
        resp["answer"] = _enforce_ai_insight_structure(
            str(resp.get("answer") or ""),
            resp.get("ai_engine_v2") if isinstance(resp.get("ai_engine_v2"), dict) else {},
        )
    except Exception:
        try:
            resp["answer"] = _compact_behavior_answer_from_engine(
                resp.get("ai_engine_v2") if isinstance(resp.get("ai_engine_v2"), dict) else {}
            )
        except Exception:
            pass
    # Phase 1 Adaptive Market Memory: store a structured behavior snapshot.
    # This is best-effort and must never block the AI response.
    try:
        pair, symbol_a, symbol_b = _market_memory_pair_from_symbols(sym_norm)
        snap = _market_memory_extract_snapshot(
            source="ai_insight",
            pair=pair,
            symbol_a=symbol_a,
            symbol_b=symbol_b,
            wallet_address=wa,
            payload=resp if isinstance(resp, dict) else {},
            extra_context=ai_signal_context if isinstance(ai_signal_context, dict) else {},
        )
        sid = _market_memory_save_snapshot(snap)
        if sid:
            resp["market_memory_snapshot_id"] = sid
    except Exception as e:
        try:
            print("[WARN] market_memory ai_insight hook failed:", e)
        except Exception:
            pass

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
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

    wa = str(request.args.get("wallet_address") or "").strip()
    mem = _ai_mem_get(wa) if wa else []
    return jsonify({"status": "ok", "wallet_address": _norm_addr(wa), "memory": mem})


@app.route("/api/ai/memory/clear", methods=["POST"])
def api_ai_memory_clear():
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    st = _compute_access_status(wa)
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

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
    ai_gate = _ai_demo_consume_or_error(wa, st)
    if ai_gate:
        return ai_gate

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

    # Defaults kept conservative during live migration
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
    One grid-engine step using REAL price (frontend/snapshot/history).

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
    """Background loop: refresh live price and tick the current grid engine."""
    while not stop_evt.is_set():
        try:
            session = GRID_SESSIONS.get(item_id)
            if session and bool(session.get("running", True)) and not bool(session.get("stopped", False)):
                p = _get_live_price_for_item(item_id)
                _sim_tick(session, new_price=p)
                _grid_sessions_set(item_id, _trim_grid_session(session))
                try:
                    _grid_sync_session_orders_to_db(session.get("wallet_address") or "", item_id, session.get("orders") or [], chain=_grid_chain_key(item_id))
                except Exception:
                    pass
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


# -------------------------
# AI Pair Insight (backend-native)
# -------------------------
def _pair_parse_symbols(body: dict) -> tuple[str, str]:
    pair = str(body.get("pair") or body.get("selectedPair") or "").strip().upper()
    if "/" in pair:
        a, b = pair.split("/", 1)
        return a.strip().upper(), b.strip().upper()
    a = str(body.get("symbol_a") or body.get("symbolA") or body.get("a") or "").strip().upper()
    b = str(body.get("symbol_b") or body.get("symbolB") or body.get("b") or "").strip().upper()
    return a, b


def _cg_market_chart_points(cg_id: str, days: int = 365) -> list[dict]:
    j = _cg_market_chart_usd(cg_id, max(30, min(730, int(days or 365)))) or {}
    pts = []
    for p in (j.get("prices") or []):
        try:
            ts = int(float(p[0]))
            val = float(p[1])
            if ts > 0 and math.isfinite(val) and val > 0:
                pts.append({"t": ts, "v": val})
        except Exception:
            continue
    return pts


def _daily_close_map(points: list[dict]) -> dict[str, float]:
    out = {}
    for p in (points or []):
        try:
            ts = int(p.get("t"))
            v = float(p.get("v"))
            if ts <= 0 or not math.isfinite(v) or v <= 0:
                continue
            d = time.gmtime(ts / 1000.0)
            key = f"{d.tm_year:04d}-{d.tm_mon:02d}-{d.tm_mday:02d}"
            out[key] = v  # last point of the day wins
        except Exception:
            continue
    return out


def _aligned_daily_pair(points_a: list[dict], points_b: list[dict], max_days: int = 365) -> list[dict]:
    ma = _daily_close_map(points_a)
    mb = _daily_close_map(points_b)
    common = sorted(set(ma.keys()) & set(mb.keys()))
    if max_days and len(common) > int(max_days):
        common = common[-int(max_days):]
    out = []
    for day in common:
        try:
            va = float(ma[day]); vb = float(mb[day])
            if va > 0 and vb > 0:
                out.append({"day": day, "a": va, "b": vb})
        except Exception:
            continue
    return out


def _series_stats_from_points(points: list[dict]) -> dict:
    vals = []
    for p in (points or []):
        try:
            v = float(p.get("v"))
            if math.isfinite(v) and v > 0:
                vals.append(v)
        except Exception:
            continue
    if len(vals) < 2:
        return {}
    first = vals[0]
    last = vals[-1]
    change_pct = ((last - first) / first) * 100.0 if first else None
    rets = []
    for i in range(1, len(vals)):
        prev = vals[i - 1]
        cur = vals[i]
        if prev > 0 and cur > 0:
            rets.append((cur / prev) - 1.0)
    vol_pct = None
    if len(rets) >= 2:
        mean = sum(rets) / len(rets)
        var = sum((r - mean) ** 2 for r in rets) / len(rets)
        vol_pct = math.sqrt(max(0.0, var)) * 100.0
    peak = vals[0]
    max_dd = 0.0
    for v in vals:
        if v > peak:
            peak = v
        dd = ((v / peak) - 1.0) * 100.0 if peak > 0 else 0.0
        if dd < max_dd:
            max_dd = dd
    return {
        "first": round(first, 8),
        "last": round(last, 8),
        "changePct": round(change_pct, 4) if change_pct is not None else None,
        "volPct": round(vol_pct, 4) if vol_pct is not None else None,
        "maxDDPct": round(max_dd, 4),
        "min": round(min(vals), 8),
        "max": round(max(vals), 8),
        "points": len(vals),
    }


def _slice_days(points: list[dict], n_days: int) -> list[dict]:
    if not points:
        return []
    closes = _daily_close_map(points)
    keys = sorted(closes.keys())
    if n_days and len(keys) > int(n_days):
        keys = keys[-int(n_days):]
    return [{"t": idx, "v": closes[k], "day": k} for idx, k in enumerate(keys)]


def _pair_windows(points_a: list[dict], points_b: list[dict]) -> dict:
    defs = {"7D": 7, "30D": 30, "90D": 90, "1Y": 365}
    out = {}
    for label, days in defs.items():
        out[label] = {
            "a": _series_stats_from_points(_slice_days(points_a, days)),
            "b": _series_stats_from_points(_slice_days(points_b, days)),
        }
    return out


def _pair_spread_series(aligned_daily: list[dict], days: int = 30) -> list[dict]:
    rows = aligned_daily[-int(days):] if days and len(aligned_daily) > int(days) else list(aligned_daily)
    if len(rows) < 2:
        return []
    base_a = float(rows[0]["a"])
    base_b = float(rows[0]["b"])
    out = []
    for r in rows:
        try:
            ra = ((float(r["a"]) / base_a) - 1.0) * 100.0 if base_a > 0 else None
            rb = ((float(r["b"]) / base_b) - 1.0) * 100.0 if base_b > 0 else None
            d = (ra - rb) if (ra is not None and rb is not None) else None
            out.append({
                "day": r["day"],
                "a_ret": round(ra, 4) if ra is not None else None,
                "b_ret": round(rb, 4) if rb is not None else None,
                "spread": round(d, 4) if d is not None else None,
            })
        except Exception:
            continue
    return out


def _pearson_from_aligned_daily(aligned_daily: list[dict]) -> float | None:
    xs = []
    ys = []
    for i in range(1, len(aligned_daily)):
        try:
            a0 = float(aligned_daily[i - 1]["a"])
            a1 = float(aligned_daily[i]["a"])
            b0 = float(aligned_daily[i - 1]["b"])
            b1 = float(aligned_daily[i]["b"])
            if a0 > 0 and a1 > 0 and b0 > 0 and b1 > 0:
                xs.append((a1 / a0) - 1.0)
                ys.append((b1 / b0) - 1.0)
        except Exception:
            continue
    n = min(len(xs), len(ys))
    if n < 5:
        return None
    mx = sum(xs[:n]) / n
    my = sum(ys[:n]) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    denx = math.sqrt(sum((xs[i] - mx) ** 2 for i in range(n)))
    deny = math.sqrt(sum((ys[i] - my) ** 2 for i in range(n)))
    den = denx * deny
    if den <= 0:
        return None
    return round(num / den, 4)


def _pair_setup_payload(symbol_a: str, symbol_b: str, corr: float | None, windows: dict, snap_a: dict, snap_b: dict) -> dict:
    def _get_ret(sym_key: str, tf: str):
        st = ((windows.get(tf) or {}).get(sym_key) or {})
        v = st.get("changePct")
        return float(v) if isinstance(v, (int, float)) else None

    ra30 = _get_ret("a", "30D")
    rb30 = _get_ret("b", "30D")
    winner = symbol_a if (ra30 is not None and rb30 is not None and ra30 >= rb30) else (symbol_b if (ra30 is not None and rb30 is not None) else None)
    loser = symbol_b if winner == symbol_a else (symbol_a if winner == symbol_b else None)
    spread = (ra30 - rb30) if (ra30 is not None and rb30 is not None) else None

    def classify(v):
        if v is None:
            return "n/a"
        if v >= 8:
            return "bullish"
        if v <= -8:
            return "bearish"
        return "neutral"

    def avg_ret(tf: str):
        aa = _get_ret("a", tf)
        bb = _get_ret("b", tf)
        vals = [v for v in (aa, bb) if v is not None]
        return (sum(vals) / len(vals)) if vals else None

    trend_structure = " · ".join(f"{tf} {classify(avg_ret(tf))}" for tf in ("7D", "30D", "90D", "1Y"))
    short_bias = classify(avg_ret("30D"))
    long_bias = classify(avg_ret("1Y"))
    if short_bias == "bearish" and long_bias == "bullish":
        momentum_shift = "Short-term weakness inside a stronger long-term structure."
    elif short_bias == "bullish" and long_bias == "bearish":
        momentum_shift = "Short-term recovery attempt against a weaker long-term backdrop."
    elif short_bias == long_bias and short_bias != "n/a":
        momentum_shift = f"Short-term and long-term momentum are aligned {short_bias}."
    else:
        momentum_shift = "Momentum is mixed across timeframes."

    setup = "NEUTRAL"
    confidence = 5.4
    confidence_label = "MEDIUM"
    risk = "Medium"
    action = "Watch this pair and wait for a cleaner setup."
    grid_mode = "Standard"
    grid_range = "2–4%"
    verdict_text = "This pair is interesting, but the signal is not strong enough for a clear grid bias yet."
    why = []
    invalidation = []

    corr_v = float(corr) if isinstance(corr, (int, float)) else None
    vol_a = ((windows.get("30D") or {}).get("a") or {}).get("volPct")
    vol_b = ((windows.get("30D") or {}).get("b") or {}).get("volPct")
    avg_vol = None
    vv = [float(v) for v in (vol_a, vol_b) if isinstance(v, (int, float))]
    if vv:
        avg_vol = sum(vv) / len(vv)

    if spread is not None and corr_v is not None:
        if abs(spread) >= 6 and corr_v >= 0.65:
            setup = "MEAN REVERSION"
            confidence = 8.8 if abs(spread) >= 10 else 8.1
            risk = "Medium-High"
            action = f"SELL {winner} / BUY {loser}" if winner and loser else "Rebalance the outperformer against the laggard."
            grid_mode = "Wide"
            grid_range = "4–6%"
            verdict_text = f"{winner} has clearly outperformed {loser} over 30D while correlation stayed relatively high. This supports a mean-reversion style setup rather than a pure momentum chase." if winner and loser else verdict_text
            why = [
                f"30D spread is {spread:.2f}%, which is large enough to create a usable imbalance.",
                f"Correlation is {corr_v:.2f}, so both assets still move with meaningful relationship.",
                f"{winner} is the stronger side and {loser} is the weaker side over the 30D window." if winner and loser else "Relative strength is asymmetric across the pair.",
            ]
            invalidation = [
                "If the spread keeps expanding instead of stabilizing, mean reversion weakens.",
                "If correlation breaks down, pair logic becomes less reliable.",
            ]
        elif abs(spread) >= 6 and corr_v < 0.45:
            setup = "AVOID"
            confidence = 7.5
            risk = "High"
            action = "Avoid pair-based grid logic until correlation improves."
            grid_mode = "Off"
            grid_range = "—"
            verdict_text = "The spread is large, but the relationship between both assets is too weak. That makes pair reversion logic unreliable."
            why = [
                f"Spread is large ({spread:.2f}%), but correlation is only {corr_v:.2f}.",
                "Low co-movement increases breakdown risk for pair-based setups.",
            ]
            invalidation = ["Re-check only if correlation recovers and spread remains tradable."]
        elif corr_v >= 0.65 and abs(spread) < 3:
            setup = "BALANCED RANGE"
            confidence = 7.2
            risk = "Medium"
            action = "Use a tighter manual grid and smaller sizing."
            grid_mode = "Tight"
            grid_range = "2–4%"
            verdict_text = "Both assets remain closely linked and the current spread is modest. This looks more like a balanced range than a strong dislocation."
            why = [
                f"Correlation is healthy at {corr_v:.2f}.",
                f"Spread is only {spread:.2f}%, so range conditions matter more than reversion edge.",
            ]
            invalidation = ["If volatility spikes or the spread widens sharply, the setup changes."]

    if avg_vol is not None and avg_vol >= 7.5 and risk != "High":
        risk = "High"
        confidence = max(4.8, confidence - 0.7)
        why.append("30D volatility is elevated, so execution risk is higher.")

    confidence_label = "HIGH" if confidence >= 8 else ("MEDIUM" if confidence >= 6 else "LOW")

    return {
        "setup": setup,
        "confidence": round(confidence, 1),
        "confidenceLabel": confidence_label,
        "risk": risk,
        "action": action,
        "gridMode": grid_mode,
        "gridRange": grid_range,
        "winner": winner,
        "loser": loser,
        "spread30d": round(spread, 4) if spread is not None else None,
        "trendStructure": trend_structure,
        "momentumShift": momentum_shift,
        "verdictText": verdict_text,
        "why": why,
        "invalidation": invalidation,
    }


@app.route("/api/ai/pair-insight", methods=["GET", "POST"])
def api_ai_pair_insight():
    body = request.get_json(silent=True) or {}
    args = request.args or {}
    merged = {}
    merged.update({k: v for k, v in args.items()})
    if isinstance(body, dict):
        merged.update(body)

    symbol_a, symbol_b = _pair_parse_symbols(merged)
    if not symbol_a or not symbol_b:
        return jsonify({"status": "error", "error": "missing pair symbols", "ts": now_ts()}), 400

    coin_id_a = str(merged.get("coin_id_a") or merged.get("coinIdA") or merged.get("id_a") or merged.get("idA") or "").strip()
    coin_id_b = str(merged.get("coin_id_b") or merged.get("coinIdB") or merged.get("id_b") or merged.get("idB") or "").strip()
    try:
        days = max(90, min(730, int(merged.get("days") or 365)))
    except Exception:
        days = 365
    try:
        indicator_days = max(30, min(365, int(merged.get("indicator_days") or 120)))
    except Exception:
        indicator_days = 120
    try:
        period = max(7, min(21, int(merged.get("period") or 14)))
    except Exception:
        period = 14

    resolved_a = _resolve_cg_id_for_indicator(symbol=symbol_a, coin_id=coin_id_a)
    resolved_b = _resolve_cg_id_for_indicator(symbol=symbol_b, coin_id=coin_id_b)
    if not resolved_a or not resolved_b:
        return jsonify({
            "status": "error",
            "error": "coin_not_resolved",
            "symbol_a": symbol_a,
            "symbol_b": symbol_b,
            "coin_id_a": resolved_a or coin_id_a,
            "coin_id_b": resolved_b or coin_id_b,
            "ts": now_ts(),
        }), 404

    cache_key = f"pair-insight|{symbol_a}|{symbol_b}|{resolved_a}|{resolved_b}|{days}|{indicator_days}|{period}"
    fresh = _gen_cache_get_fresh(cache_key)
    if fresh is not None:
        return jsonify(fresh)

    try:
        pts_a = _cg_market_chart_points(resolved_a, days=days)
        pts_b = _cg_market_chart_points(resolved_b, days=days)
        aligned = _aligned_daily_pair(pts_a, pts_b, max_days=days)
        if len(aligned) < 20:
            return jsonify({
                "status": "error",
                "error": "insufficient_overlap_history",
                "pair": f"{symbol_a}/{symbol_b}",
                "points": len(aligned),
                "ts": now_ts(),
            }), 400

        series_a_indicator = _cg_price_series(resolved_a, days=indicator_days)
        series_b_indicator = _cg_price_series(resolved_b, days=indicator_days)
        rsi_a = _calc_rsi_from_prices(series_a_indicator, period=period)
        rsi_b = _calc_rsi_from_prices(series_b_indicator, period=period)

        windows = _pair_windows(pts_a, pts_b)
        corr = _pearson_from_aligned_daily(aligned)
        spread_series = _pair_spread_series(aligned, days=30)
        latest_spread = spread_series[-1].get("spread") if spread_series else None
        prev_spread = spread_series[-2].get("spread") if len(spread_series) >= 2 else None
        direction = None
        interpretation = "Spread is mixed."
        if latest_spread is not None and prev_spread is not None:
            if latest_spread > prev_spread:
                direction = "rising"
                interpretation = "Spread is expanding, which points more to trend continuation than mean reversion."
            elif latest_spread < prev_spread:
                direction = "falling"
                interpretation = "Spread is compressing, which supports a mean-reversion reading."
            else:
                direction = "flat"
                interpretation = "Spread is stable, so confirmation is still limited."

        snap_map = _cg_market_snapshots_batch([resolved_a, resolved_b])
        snap_a = snap_map.get(resolved_a) or _cg_market_snapshot(resolved_a)
        snap_b = snap_map.get(resolved_b) or _cg_market_snapshot(resolved_b)
        vol24_a = snap_a.get("volume24") if isinstance(snap_a, dict) else None
        vol24_b = snap_b.get("volume24") if isinstance(snap_b, dict) else None
        liq_proxy = min([v for v in (vol24_a, vol24_b) if isinstance(v, (int, float))], default=None)
        if liq_proxy is None:
            liquidity_state = "unknown"
        elif liq_proxy >= 5_000_000:
            liquidity_state = "strong"
        elif liq_proxy >= 500_000:
            liquidity_state = "usable"
        elif liq_proxy >= 100_000:
            liquidity_state = "thin"
        else:
            liquidity_state = "very_thin"

        grid_fit_a = _grid_fit_state(rsi_a, _volatility_state(((windows.get("30D") or {}).get("a") or {}).get("volPct")), _trend_state(
            ((windows.get("7D") or {}).get("a") or {}).get("changePct"),
            ((windows.get("30D") or {}).get("a") or {}).get("changePct"),
            ((windows.get("1Y") or {}).get("a") or {}).get("changePct"),
        ))
        grid_fit_b = _grid_fit_state(rsi_b, _volatility_state(((windows.get("30D") or {}).get("b") or {}).get("volPct")), _trend_state(
            ((windows.get("7D") or {}).get("b") or {}).get("changePct"),
            ((windows.get("30D") or {}).get("b") or {}).get("changePct"),
            ((windows.get("1Y") or {}).get("b") or {}).get("changePct"),
        ))

        setup = _pair_setup_payload(symbol_a, symbol_b, corr, windows, snap_a, snap_b)
        out = {
            "status": "ok",
            "pair": f"{symbol_a}/{symbol_b}",
            "symbols": [symbol_a, symbol_b],
            "coin_ids": [resolved_a, resolved_b],
            "days": days,
            "indicator_days": indicator_days,
            "period": period,
            "correlation": corr,
            "spread": {
                "series": spread_series,
                "latest": latest_spread,
                "previous": prev_spread,
                "direction": direction,
                "interpretation": interpretation,
                "zero_line": 0,
            },
            "rsi": {
                symbol_a: {"value": rsi_a, "state": _rsi_state(rsi_a)},
                symbol_b: {"value": rsi_b, "state": _rsi_state(rsi_b)},
            },
            "risk": {
                symbol_a: {
                    "volatility30d": ((windows.get("30D") or {}).get("a") or {}).get("volPct"),
                    "drawdown1y": ((windows.get("1Y") or {}).get("a") or {}).get("maxDDPct"),
                    "gridFit": grid_fit_a,
                },
                symbol_b: {
                    "volatility30d": ((windows.get("30D") or {}).get("b") or {}).get("volPct"),
                    "drawdown1y": ((windows.get("1Y") or {}).get("b") or {}).get("maxDDPct"),
                    "gridFit": grid_fit_b,
                },
                "liquidity24h_proxy": liq_proxy,
                "liquidity_state": liquidity_state,
            },
            "windows": {
                "7D": {
                    symbol_a: (windows.get("7D") or {}).get("a") or {},
                    symbol_b: (windows.get("7D") or {}).get("b") or {},
                },
                "30D": {
                    symbol_a: (windows.get("30D") or {}).get("a") or {},
                    symbol_b: (windows.get("30D") or {}).get("b") or {},
                },
                "90D": {
                    symbol_a: (windows.get("90D") or {}).get("a") or {},
                    symbol_b: (windows.get("90D") or {}).get("b") or {},
                },
                "1Y": {
                    symbol_a: (windows.get("1Y") or {}).get("a") or {},
                    symbol_b: (windows.get("1Y") or {}).get("b") or {},
                },
            },
            "market": {
                symbol_a: snap_a,
                symbol_b: snap_b,
            },
            "insight": setup,
            "ts": now_ts(),
        }
        # Phase 1 Adaptive Market Memory: pair insight snapshots are global/anonymous
        # because this endpoint can be called without wallet auth.
        try:
            snap = _market_memory_extract_snapshot(
                source="pair_insight",
                pair=out.get("pair") or f"{symbol_a}/{symbol_b}",
                symbol_a=symbol_a,
                symbol_b=symbol_b,
                wallet_address="",
                payload=out,
                extra_context={},
            )
            sid = _market_memory_save_snapshot(snap)
            if sid:
                out["market_memory_snapshot_id"] = sid
        except Exception as e:
            try:
                print("[WARN] market_memory pair_insight hook failed:", e)
            except Exception:
                pass

        _gen_cache_set(cache_key, out)
        return jsonify(out)
    except Exception as e:
        stale = _gen_cache_get_any(cache_key)
        if stale is not None:
            return jsonify(stale)
        return jsonify({
            "status": "error",
            "error": "pair_insight_failed",
            "detail": str(e),
            "pair": f"{symbol_a}/{symbol_b}",
            "coin_id_a": resolved_a,
            "coin_id_b": resolved_b,
            "ts": now_ts(),
        }), 502

# =========================================================
# Nexus Bridge Layer: Score -> Rotation -> Preview -> Vault-ready checks
# =========================================================
# Purpose:
# - Keep UI clean while exposing backend endpoints needed by Compare, Rotation and later Vault V2.
# - No trade execution here. These endpoints only score, plan, preview and validate readiness.
# - Vault V2 remains the execution/security layer and must still validate router/token/minOut/slippage on-chain.

_NEXUS_MAX_SLIPPAGE_BPS = int(os.getenv("NEXUS_MAX_SLIPPAGE_BPS", "500"))  # 5% hard backend preview cap
_NEXUS_DEFAULT_SLIPPAGE_BPS = int(os.getenv("NEXUS_DEFAULT_SLIPPAGE_BPS", str(DEFAULT_SLIPPAGE_BPS)))
_NEXUS_MIN_PROFIT_FEE_USD = float(os.getenv("NEXUS_MIN_PROFIT_FEE_USD", "100"))
_NEXUS_PERFORMANCE_FEE_BPS = int(os.getenv("NEXUS_PERFORMANCE_FEE_BPS", "300"))
_NEXUS_MAX_PERFORMANCE_FEE_BPS = int(os.getenv("NEXUS_MAX_PERFORMANCE_FEE_BPS", "500"))

# Vault V2 revenue policy:
# Performance fees must be paid to the fee receiver in a stablecoin, never in random profit tokens.
# Preferred stable can be overridden globally or per chain:
#   NEXUS_FEE_STABLE=USDC|USDT
#   NEXUS_FEE_STABLE_POL=USDC, NEXUS_FEE_STABLE_BNB=USDT, NEXUS_FEE_STABLE_ETH=USDC
_NEXUS_FEE_STABLE_DEFAULT = str(os.getenv("NEXUS_FEE_STABLE", "USDC")).strip().upper()
_NEXUS_FEE_RECEIVER = str(os.getenv("NEXUS_FEE_RECEIVER") or os.getenv("FEE_RECEIVER") or os.getenv("TREASURY_ADDRESS") or "").strip()


def _nexus_chain_key_from_id(chain_id: int) -> str:
    try:
        cid = int(chain_id or 0)
    except Exception:
        cid = 0
    for k, v in (_CHAIN_ID_BY_KEY or {}).items():
        try:
            if int(v) == cid:
                return str(k).upper()
        except Exception:
            continue
    return {1: "ETH", 56: "BNB", 137: "POL"}.get(cid, "")


def _nexus_router_env(chain_key: str, router_key: str) -> str:
    """Read a router address from flexible ENV names, e.g. ROUTER_0X_POL, ZEROX_ROUTER_ADDRESS_137."""
    ck = _normalize_chain_key(chain_key)
    cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
    rk = str(router_key or "").strip().upper().replace("-", "_")
    aliases = {
        "0X": ["0X", "ZEROX", "ZERO_X"],
        "ONEINCH": ["ONEINCH", "1INCH", "ONE_INCH"],
        "UNISWAP": ["UNISWAP", "UNI"],
        "QUICKSWAP": ["QUICKSWAP", "QUICK"],
        "PANCAKESWAP": ["PANCAKESWAP", "PANCAKE"],
        "SUSHISWAP": ["SUSHISWAP", "SUSHI"],
        "CURVE": ["CURVE"],
    }.get(rk, [rk])
    suffixes = [ck]
    if cid:
        suffixes.append(str(cid))
    if ck == "POL":
        suffixes.append("POLYGON")
    if ck == "BNB":
        suffixes.append("BSC")
    candidates = []
    for a in aliases:
        for suf in suffixes:
            candidates += [
                f"ROUTER_{a}_{suf}",
                f"{a}_ROUTER_{suf}",
                f"{a}_ROUTER_ADDRESS_{suf}",
                f"ROUTER_ADDRESS_{a}_{suf}",
            ]
    for name in candidates:
        val = str(os.getenv(name) or "").strip()
        if val:
            return val
    return ""


def _nexus_allowed_routers_for_chain(chain_id_or_key) -> list[dict]:
    """Backend router allowlist for Preview + /api/contracts.

    Addresses are ENV-driven to avoid hardcoding wrong chain router contracts.
    Existing ROUTER_ADDRESS_* and ROUTER_V3_ADDRESS_* are exposed as generic v2/v3 fallbacks.
    """
    if isinstance(chain_id_or_key, str) and not str(chain_id_or_key).isdigit():
        ck = _normalize_chain_key(chain_id_or_key)
        cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
    else:
        cid = int(chain_id_or_key or 0)
        ck = _nexus_chain_key_from_id(cid)
    native = {1: "ETH", 56: "BNB", 137: "POL"}.get(cid, ck)

    raw = [
        {"key": "primary_v2", "name": "Primary V2 Router", "type": "v2", "address": (_ROUTER_BY_CHAIN.get(cid) or "")},
        {"key": "primary_v3", "name": "Primary V3 Router", "type": "v3", "address": (_ROUTER_V3_BY_CHAIN.get(cid) or "")},
        {"key": "uniswap", "name": "Uniswap", "type": "dex", "address": _nexus_router_env(ck, "UNISWAP")},
        {"key": "quickswap", "name": "QuickSwap", "type": "dex", "address": _nexus_router_env(ck, "QUICKSWAP")},
        {"key": "pancakeswap", "name": "PancakeSwap", "type": "dex", "address": _nexus_router_env(ck, "PANCAKESWAP")},
        {"key": "sushiswap", "name": "SushiSwap", "type": "dex", "address": _nexus_router_env(ck, "SUSHISWAP")},
        {"key": "oneinch", "name": "1inch", "type": "aggregator", "address": _nexus_router_env(ck, "ONEINCH")},
        {"key": "0x", "name": "0x Exchange Proxy", "type": "aggregator", "address": _nexus_router_env(ck, "0X")},
    ]
    seen = set()
    out = []
    for r in raw:
        addr = str(r.get("address") or "").strip()
        if not addr:
            continue
        low = addr.lower()
        if low in seen:
            continue
        seen.add(low)
        rr = dict(r)
        rr["address"] = addr
        rr["chain"] = ck
        rr["chainId"] = cid
        rr["native"] = native
        rr["enabled"] = True
        out.append(rr)
    return out


def _nexus_router_allowed(chain: str, router_key: str = "", router_address: str = "") -> tuple[bool, dict | None, list[dict]]:
    ck = _normalize_chain_key(chain or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
    routers = _nexus_allowed_routers_for_chain(cid)
    rk = str(router_key or "").strip().lower()
    ra = str(router_address or "").strip().lower()
    for r in routers:
        if rk and str(r.get("key") or "").lower() == rk:
            return True, r, routers
        if ra and str(r.get("address") or "").strip().lower() == ra:
            return True, r, routers
    # If no router specified, use first configured router as default.
    if not rk and not ra and routers:
        return True, routers[0], routers
    return False, None, routers


def _nexus_stable_address_for_chain(chain: str, stable: str = "") -> dict:
    """Return the configured fee stablecoin for a chain. Fees should settle in USDT/USDC only."""
    ck = _normalize_chain_key(chain or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
    requested = str(stable or os.getenv(f"NEXUS_FEE_STABLE_{ck}") or _NEXUS_FEE_STABLE_DEFAULT or "USDC").strip().upper()
    if requested not in ("USDC", "USDT"):
        requested = "USDC"

    usdc = str((_USDC_BY_CHAIN or {}).get(cid) or "").strip()
    usdt = str((_USDT_BY_CHAIN or {}).get(cid) or "").strip()

    # Prefer requested stable, but fall back to the other configured stable if needed.
    if requested == "USDT" and usdt:
        symbol, addr = "USDT", usdt
    elif requested == "USDC" and usdc:
        symbol, addr = "USDC", usdc
    elif usdc:
        symbol, addr = "USDC", usdc
    elif usdt:
        symbol, addr = "USDT", usdt
    else:
        symbol, addr = requested, ""

    return {
        "symbol": symbol,
        "address": addr,
        "chain": ck,
        "chainId": cid,
        "configured": bool(addr),
    }


def _nexus_fee_preview(profit_usd: float, chain: str, preferred_stable: str = "") -> dict:
    """Preview performance fee. Fee is charged only on realized profit >= threshold and must settle in USDC/USDT."""
    profit = _safe_float(profit_usd)
    fee_bps = min(max(int(_NEXUS_PERFORMANCE_FEE_BPS or 0), 0), int(_NEXUS_MAX_PERFORMANCE_FEE_BPS or 500))
    threshold = float(_NEXUS_MIN_PROFIT_FEE_USD or 100.0)
    taxable = profit if profit >= threshold else 0.0
    fee_usd = taxable * fee_bps / 10_000.0
    stable = _nexus_stable_address_for_chain(chain, preferred_stable)
    return {
        "applies": bool(taxable > 0 and fee_usd > 0),
        "profitUsd": round(profit, 6),
        "taxableProfitUsd": round(taxable, 6),
        "feeUsd": round(fee_usd, 6),
        "performanceFeeBps": fee_bps,
        "maxPerformanceFeeBps": int(_NEXUS_MAX_PERFORMANCE_FEE_BPS or 500),
        "minProfitForFeeUsd": threshold,
        "settlement": {
            "mode": "stablecoin_only",
            "stableSymbol": stable.get("symbol"),
            "stableAddress": stable.get("address"),
            "stableConfigured": bool(stable.get("configured")),
            "allowedStableSymbols": ["USDC", "USDT"],
            "feeReceiver": _NEXUS_FEE_RECEIVER,
            "feeReceiverConfigured": _looks_like_evm_addr(_NEXUS_FEE_RECEIVER),
        },
        "note": "Performance fee is charged only on realized profit at/above the threshold and must be converted/sent as USDC or USDT.",
    }


def _nexus_symbol_payload(symbol_or_item: str) -> dict:
    sym = str(symbol_or_item or "").strip().upper()
    if ":" in sym:
        sym = sym.split(":", 1)[-1].strip().upper()
    out = {"symbol": sym, "price": 0.0, "change24h": 0.0, "volume24h": 0.0, "marketCap": 0.0, "coin_id": ""}
    if not sym:
        return out

    cid = ""
    try:
        cid = _cg_resolve_symbol(sym) or ""
    except Exception:
        cid = ""
    out["coin_id"] = cid

    snap = None
    if cid:
        try:
            snap = _cg_market_snapshot(cid)
        except Exception:
            snap = None
    if isinstance(snap, dict):
        out["price"] = _safe_float(snap.get("price"))
        out["change24h"] = _safe_float(snap.get("change24") if "change24" in snap else snap.get("change24h"))
        out["volume24h"] = _safe_float(snap.get("volume24") if "volume24" in snap else snap.get("volume24h"))
        out["marketCap"] = _safe_float(snap.get("market_cap") if "market_cap" in snap else snap.get("marketCap"))
    if out["price"] <= 0:
        try:
            px = _price_multi(sym)
            if isinstance(px, dict):
                out["price"] = _safe_float(px.get("price"))
                out["coin_id"] = out["coin_id"] or str(px.get("id") or "")
        except Exception:
            pass
    return out


def _nexus_rating(score: float) -> str:
    s = _safe_float(score)
    if s >= 85:
        return "AAA"
    if s >= 75:
        return "AA"
    if s >= 65:
        return "A"
    if s >= 55:
        return "B"
    if s >= 40:
        return "C"
    return "RISK"


def _nexus_action(score: float, risk: str) -> str:
    s = _safe_float(score)
    r = str(risk or "").upper()
    if r == "HIGH" or s < 40:
        return "SKIP"
    if s >= 75:
        return "INCREASE"
    if s >= 55:
        return "HOLD"
    return "REDUCE"


def _nexus_score_for_asset(asset: dict) -> dict:
    symbol = str(asset.get("symbol") or asset.get("item") or asset.get("id") or "").strip().upper()
    chain = _normalize_chain_key(asset.get("chain") or "")
    token = str(asset.get("token") or asset.get("token_address") or asset.get("address") or "").strip()
    market = _nexus_symbol_payload(symbol)
    price = _safe_float(asset.get("price"), market.get("price") or 0)
    change24h = _safe_float(asset.get("change24h"), market.get("change24h") or 0)
    volume24h = _safe_float(asset.get("volume24h"), market.get("volume24h") or 0)
    market_cap = _safe_float(asset.get("marketCap") if asset.get("marketCap") is not None else asset.get("market_cap"), market.get("marketCap") or 0)

    base = 50.0
    parts = {"base": base, "trend": 0, "volume": 0, "market_condition": 0, "whale": 0, "liquidity": 0}
    reasons = []

    if change24h > 10:
        parts["trend"] = 10; reasons.append("Strong 24h momentum")
    elif change24h > 3:
        parts["trend"] = 5; reasons.append("Positive 24h trend")
    elif change24h < -10:
        parts["trend"] = -10; reasons.append("Heavy 24h weakness")
    elif change24h < -3:
        parts["trend"] = -5; reasons.append("Weak 24h trend")

    if volume24h >= 5_000_000:
        parts["volume"] = 10; reasons.append("Strong 24h volume")
    elif volume24h >= 500_000:
        parts["volume"] = 6; reasons.append("Healthy 24h volume")
    elif 0 < volume24h < 25_000:
        parts["volume"] = -6; reasons.append("Thin 24h volume")

    if market_cap and market_cap < 1_000_000:
        parts["liquidity"] = -6; reasons.append("Micro-cap risk")
    elif market_cap and market_cap > 100_000_000:
        parts["liquidity"] = 4; reasons.append("Higher market-cap stability")

    market_condition = None
    try:
        if market.get("coin_id") or symbol:
            market_condition = _market_condition_for_coin(market.get("coin_id") or symbol, days=20)
            parts["market_condition"] = int(_safe_float(market_condition.get("score_delta")))
            if market_condition.get("label"):
                reasons.append(str(market_condition.get("label")))
    except Exception as e:
        market_condition = {"status": "error", "error": str(e)}

    whale = None
    if token and _looks_like_evm_addr(token) and chain:
        try:
            whale = _get_whale_signal_bitquery(token, chain=chain, volume24h_usd=volume24h)
            parts["whale"] = int(_safe_float(whale.get("score_delta")))
            if whale.get("summary"):
                reasons.append(str(whale.get("summary")))
        except Exception as e:
            whale = {"status": "error", "error": str(e), "score_delta": 0}

    raw_score = sum(float(v or 0) for v in parts.values())
    score = max(0, min(100, round(raw_score, 2)))
    risk = "LOW" if score >= 75 else "MEDIUM" if score >= 50 else "HIGH"
    rating = _nexus_rating(score)
    action = _nexus_action(score, risk)
    return {
        "status": "ok",
        "symbol": symbol,
        "chain": chain,
        "token": token,
        "score": score,
        "rating": rating,
        "risk": risk,
        "action": action,
        "components": parts,
        "reasons": reasons[:8],
        "market": {"price": price, "change24h": change24h, "volume24h": volume24h, "marketCap": market_cap, "coin_id": market.get("coin_id")},
        "market_condition": market_condition,
        "whale": whale,
        "ts": now_ts(),
    }


def _nexus_assets_from_request_payload(body: dict) -> list[dict]:
    assets = body.get("assets") if isinstance(body, dict) else None
    if isinstance(assets, list) and assets:
        return [x for x in assets if isinstance(x, dict)][:20]
    symbols_raw = body.get("symbols") if isinstance(body, dict) else ""
    if isinstance(symbols_raw, list):
        syms = [str(x).strip().upper() for x in symbols_raw if str(x).strip()]
    else:
        syms = [s.strip().upper() for s in str(symbols_raw or "").split(",") if s.strip()]
    chain = _normalize_chain_key(body.get("chain") or request.args.get("chain") or "")
    return [{"symbol": s, "chain": chain} for s in syms[:20]]


@app.route("/api/nexus/score", methods=["GET", "POST"])
def api_nexus_score():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
    else:
        body = request.args.to_dict(flat=True)
        body["symbol"] = body.get("symbol") or body.get("asset") or body.get("item") or ""
        body["token"] = body.get("token") or body.get("token_address") or body.get("address") or ""
    asset = dict(body)
    if not str(asset.get("symbol") or asset.get("item") or "").strip():
        return err("missing symbol", 400)
    return jsonify(_nexus_score_for_asset(asset))


@app.route("/api/nexus/compare-scores", methods=["GET", "POST"])
def api_nexus_compare_scores():
    body = request.get_json(silent=True) or {} if request.method == "POST" else request.args.to_dict(flat=True)
    assets = _nexus_assets_from_request_payload(body)
    if not assets:
        return err("missing assets or symbols", 400)
    scores = [_nexus_score_for_asset(a) for a in assets]
    scores = sorted(scores, key=lambda x: float(x.get("score") or 0), reverse=True)
    for i, row in enumerate(scores, start=1):
        row["rank"] = i
    return jsonify({"status": "ok", "items": scores, "scores": scores, "ts": now_ts()})


def _nexus_build_rotation_plan(assets: list[dict], budget_usd: float) -> dict:
    scored = [_nexus_score_for_asset(a) for a in assets]
    scored = sorted(scored, key=lambda x: float(x.get("score") or 0), reverse=True)
    eligible = [x for x in scored if x.get("action") != "SKIP" and str(x.get("risk")) != "HIGH" and float(x.get("score") or 0) >= 45]
    weight_sum = sum(max(0.0, float(x.get("score") or 0) - 40.0) for x in eligible) or 0.0
    plan = []
    for i, x in enumerate(scored, start=1):
        score = float(x.get("score") or 0)
        eligible_row = x in eligible and weight_sum > 0
        target_weight = (max(0.0, score - 40.0) / weight_sum * 100.0) if eligible_row else 0.0
        target_usd = float(budget_usd or 0) * target_weight / 100.0
        action = _nexus_action(score, x.get("risk"))
        if not eligible_row:
            action = "SKIP" if x.get("risk") == "HIGH" or score < 45 else "HOLD"
        plan.append({
            "rank": i,
            "symbol": x.get("symbol"),
            "chain": x.get("chain"),
            "token": x.get("token"),
            "score": x.get("score"),
            "rating": x.get("rating"),
            "risk": x.get("risk"),
            "action": action,
            "target_weight_pct": round(target_weight, 2),
            "target_usd": round(target_usd, 2),
            "reasons": x.get("reasons") or [],
            "score_ref": x,
        })
    return {"status": "ok", "budget_usd": round(float(budget_usd or 0), 2), "plan": plan, "items": plan, "ts": now_ts()}


@app.route("/api/nexus/rotation-plan", methods=["GET", "POST"])
def api_nexus_rotation_plan_bridge():
    body = request.get_json(silent=True) or {} if request.method == "POST" else request.args.to_dict(flat=True)
    assets = _nexus_assets_from_request_payload(body)
    if not assets:
        return err("missing assets or symbols", 400)
    budget_usd = _safe_float(body.get("budget_usd") or body.get("budgetUsd") or body.get("budget") or 0)
    return jsonify(_nexus_build_rotation_plan(assets, budget_usd))


def _nexus_order_preview(body: dict) -> dict:
    chain = _normalize_chain_key(body.get("chain") or body.get("network") or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(chain, 0) or 0)
    side = str(body.get("side") or body.get("action") or "BUY").strip().upper()
    symbol = str(body.get("symbol") or body.get("asset") or body.get("tokenSymbol") or "").strip().upper()
    token_in = str(body.get("tokenIn") or body.get("token_in") or "").strip()
    token_out = str(body.get("tokenOut") or body.get("token_out") or body.get("token") or body.get("token_address") or "").strip()
    router_key = str(body.get("router") or body.get("routerKey") or body.get("router_key") or "").strip()
    router_address = str(body.get("routerAddress") or body.get("router_address") or "").strip()
    slippage_bps = int(_safe_float(body.get("slippageBps") or body.get("slippage_bps") or _NEXUS_DEFAULT_SLIPPAGE_BPS, _NEXUS_DEFAULT_SLIPPAGE_BPS))
    amount_usd = _safe_float(body.get("amountUsd") or body.get("amount_usd") or body.get("target_usd") or body.get("usd") or 0)
    price_usd = _safe_float(body.get("priceUsd") or body.get("price_usd") or body.get("price") or 0)
    realized_profit_usd = _safe_float(
        body.get("realizedProfitUsd")
        or body.get("realized_profit_usd")
        or body.get("profitUsd")
        or body.get("profit_usd")
        or 0
    )
    preferred_fee_stable = str(body.get("feeStable") or body.get("fee_stable") or "").strip().upper()
    if price_usd <= 0 and symbol:
        price_usd = _safe_float(_nexus_symbol_payload(symbol).get("price"))

    router_ok, router_obj, routers = _nexus_router_allowed(chain, router_key=router_key, router_address=router_address)
    checks = {
        "chain_ok": cid > 0,
        "amount_ok": amount_usd > 0,
        "price_ok": price_usd > 0,
        "slippage_ok": 0 <= slippage_bps <= _NEXUS_MAX_SLIPPAGE_BPS,
        "router_ok": bool(router_ok),
        "token_in_present": bool(token_in),
        "token_out_present": bool(token_out or symbol),
    }
    blocking = []
    for k, okv in checks.items():
        if not okv:
            blocking.append(k)

    estimated_out = (amount_usd / price_usd) if price_usd > 0 and amount_usd > 0 else 0.0
    min_out = estimated_out * max(0.0, (10_000 - slippage_bps) / 10_000.0)
    fee_preview = _nexus_fee_preview(realized_profit_usd, chain, preferred_fee_stable)

    return {
        "status": "ok" if not blocking else "blocked",
        "ready_for_vault": len(blocking) == 0,
        "chain": chain,
        "chainId": cid,
        "side": side,
        "symbol": symbol,
        "tokenIn": token_in,
        "tokenOut": token_out,
        "router": router_obj,
        "allowedRouters": routers,
        "amountUsd": round(amount_usd, 2),
        "priceUsd": price_usd,
        "estimatedOut": estimated_out,
        "minAmountOut": min_out,
        "slippageBps": slippage_bps,
        "maxSlippageBps": _NEXUS_MAX_SLIPPAGE_BPS,
        "checks": checks,
        "blocking_reasons": blocking,
        "feePolicy": {
            **fee_preview,
            "rule": "Fee must always settle in USDC/USDT, never in the traded/profit token.",
        },
        "vaultPayloadDraft": {
            "chainId": cid,
            "router": (router_obj or {}).get("address") if router_obj else "",
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountUsd": round(amount_usd, 2),
            "minAmountOut": str(min_out),
            "slippageBps": slippage_bps,
            "deadlineSec": int(DEFAULT_DEADLINE_MINUTES),
            "fee": {
                "profitUsd": fee_preview.get("profitUsd"),
                "feeUsd": fee_preview.get("feeUsd"),
                "feeStableSymbol": (fee_preview.get("settlement") or {}).get("stableSymbol"),
                "feeStableAddress": (fee_preview.get("settlement") or {}).get("stableAddress"),
                "feeReceiver": (fee_preview.get("settlement") or {}).get("feeReceiver"),
                "stableOnly": True,
            },
        },
        "ts": now_ts(),
    }


@app.route("/api/nexus/order-preview", methods=["GET", "POST"])
def api_nexus_order_preview():
    body = request.get_json(silent=True) or {} if request.method == "POST" else request.args.to_dict(flat=True)
    return jsonify(_nexus_order_preview(body if isinstance(body, dict) else {}))


@app.route("/api/nexus/rotation-preview", methods=["GET", "POST"])
def api_nexus_rotation_preview():
    body = request.get_json(silent=True) or {} if request.method == "POST" else request.args.to_dict(flat=True)
    assets = _nexus_assets_from_request_payload(body)
    if not assets:
        return err("missing assets or symbols", 400)
    budget_usd = _safe_float(body.get("budget_usd") or body.get("budgetUsd") or body.get("budget") or 0)
    chain = _normalize_chain_key(body.get("chain") or "POL")
    plan = _nexus_build_rotation_plan(assets, budget_usd)
    previews = []
    for row in plan.get("plan") or []:
        if row.get("action") in ("INCREASE", "HOLD") and _safe_float(row.get("target_usd")) > 0:
            pb = dict(body)
            pb.update({
                "chain": row.get("chain") or chain,
                "symbol": row.get("symbol"),
                "token": row.get("token") or body.get("token") or body.get("tokenOut") or "",
                "side": "BUY" if row.get("action") == "INCREASE" else "HOLD",
                "amountUsd": row.get("target_usd"),
            })
            prev = _nexus_order_preview(pb)
            prev["rotationRank"] = row.get("rank")
            prev["rotationAction"] = row.get("action")
            previews.append(prev)
        else:
            previews.append({
                "status": "skipped",
                "ready_for_vault": False,
                "symbol": row.get("symbol"),
                "rotationRank": row.get("rank"),
                "rotationAction": row.get("action"),
                "blocking_reasons": ["rotation_action_not_increase_or_no_budget"],
                "ts": now_ts(),
            })
    return jsonify({"status": "ok", "plan": plan.get("plan") or [], "previews": previews, "ts": now_ts()})


# -------------------------
# Nexus Execution Safety Layer (SAFE MODE)
# -------------------------
# This layer turns previews into a final execution-readiness report for Vault V2.
# It still does NOT execute swaps. The Vault contract remains the final on-chain validator.

def _nexus_vault_state_safe(wallet: str, chain: str) -> dict:
    """Best-effort vault state read. Never raises to the execution preview layer."""
    try:
        wa = _norm_addr(wallet or "")
        if not _looks_like_evm_addr(wa):
            return {"status": "error", "error": "invalid wallet", "vaultReady": False}
        data = _vault_state_read(wa, chain)
        return {**data, "vaultReady": str(data.get("status")) == "ok"}
    except Exception as e:
        ck = _normalize_chain_key(chain or "POL")
        cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
        return {
            "status": "error",
            "error": str(e),
            "chain": ck,
            "chainId": cid,
            "vaultReady": False,
            "rpc_configured": bool(_rpc_url_for_chain(cid)),
            "vault_configured": bool((_VAULT_BY_CHAIN.get(cid) or "").strip()),
            "executor_configured": bool((_EXECUTOR_BY_CHAIN.get(cid) or "").strip()),
            "ts": now_ts(),
        }


def _nexus_token_allowed_preview(chain: str, token_addr: str, stable_ok: bool = False) -> bool:
    """Backend-side allowlist preview. Vault V2 must enforce the final allowlist on-chain.

    ENV supported:
      NEXUS_ALLOWED_TOKENS_POL=0x...,0x...
      NEXUS_ALLOWED_TOKENS_137=0x...,0x...
      NEXUS_ALLOWED_TOKENS=0x...,0x...

    Empty allowlist = preview does not block tokens yet (migration-friendly).
    USDC/USDT can be considered allowed when stable_ok=True.
    """
    ck = _normalize_chain_key(chain or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(ck, 0) or 0)
    tok = str(token_addr or "").strip().lower()
    if not tok:
        return False
    if tok in ("native", "eth", "bnb", "pol"):
        return True
    if not _looks_like_evm_addr(tok):
        return False

    stable = _nexus_stable_address_for_chain(ck)
    usdc = str((_USDC_BY_CHAIN or {}).get(cid) or "").strip().lower()
    usdt = str((_USDT_BY_CHAIN or {}).get(cid) or "").strip().lower()
    if stable_ok and tok in {x for x in [usdc, usdt, str(stable.get("address") or "").lower()] if x}:
        return True

    raw = ",".join([
        str(os.getenv(f"NEXUS_ALLOWED_TOKENS_{ck}") or ""),
        str(os.getenv(f"NEXUS_ALLOWED_TOKENS_{cid}") or ""),
        str(os.getenv("NEXUS_ALLOWED_TOKENS") or ""),
    ])
    allow = {x.strip().lower() for x in raw.split(",") if x.strip()}
    if not allow:
        return True
    return tok in allow


def _nexus_execution_safety_check(preview: dict, wallet: str = "", require_vault_balance: bool = True) -> dict:
    """Final backend safety report before a future Vault call.

    No transaction is submitted here. This produces a clear ready/blocked response.
    """
    chain = _normalize_chain_key(preview.get("chain") or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(chain, 0) or 0)
    wallet_norm = _norm_addr(wallet or preview.get("wallet") or preview.get("wallet_address") or "")
    amount_usd = _safe_float(preview.get("amountUsd") or preview.get("amount_usd") or 0)
    token_in = str(preview.get("tokenIn") or "").strip()
    token_out = str(preview.get("tokenOut") or "").strip()
    slippage_bps = int(_safe_float(preview.get("slippageBps") or 0))
    router_addr = str(((preview.get("router") or {}) if isinstance(preview.get("router"), dict) else {}).get("address") or "").strip()

    vault_state = _nexus_vault_state_safe(wallet_norm, chain) if wallet_norm else {"status": "skipped", "vaultReady": False, "error": "wallet missing"}
    vault_balance_native = _safe_float(vault_state.get("vault_balance") or 0)

    fee_policy = preview.get("feePolicy") or {}
    fee_settlement = fee_policy.get("settlement") or {}

    checks = {
        "preview_ready": bool(preview.get("ready_for_vault")),
        "chain_ok": cid > 0 and chain in (_CHAIN_ID_BY_KEY or {}),
        "wallet_ok": _looks_like_evm_addr(wallet_norm),
        "router_ok": _looks_like_evm_addr(router_addr),
        "slippage_ok": 0 <= slippage_bps <= _NEXUS_MAX_SLIPPAGE_BPS,
        "amount_ok": amount_usd > 0,
        "token_in_allowed": _nexus_token_allowed_preview(chain, token_in, stable_ok=True),
        "token_out_allowed": _nexus_token_allowed_preview(chain, token_out, stable_ok=True) if token_out else True,
        "fee_stable_ok": bool(fee_settlement.get("stableConfigured")) if bool(fee_policy.get("applies")) else True,
        "fee_receiver_ok": bool(fee_settlement.get("feeReceiverConfigured")) if bool(fee_policy.get("applies")) else True,
        "vault_configured": bool((_VAULT_BY_CHAIN.get(cid) or "").strip()),
        "executor_configured": bool((_EXECUTOR_BY_CHAIN.get(cid) or "").strip()),
        "vault_state_ok": bool(vault_state.get("vaultReady")),
    }

    # Current old/native vaults are native-balance based. Vault V2 will validate per-asset balances on-chain.
    if require_vault_balance and wallet_norm and vault_state.get("status") == "ok":
        # We cannot convert exact native vault balance to USD without a chain native price here reliably.
        # For now this check is only liquidity-present, not amount-exact.
        checks["vault_has_liquidity"] = vault_balance_native > 0
    elif require_vault_balance:
        checks["vault_has_liquidity"] = False

    blocking = [k for k, v in checks.items() if not bool(v)]

    return {
        "status": "ok" if not blocking else "blocked",
        "can_execute": len(blocking) == 0,
        "safe_mode": True,
        "message": "SAFE MODE only: no transaction was executed. Use this output to build the future Vault V2 signed call.",
        "wallet": wallet_norm,
        "chain": chain,
        "chainId": cid,
        "checks": checks,
        "blocking_reasons": blocking,
        "vaultState": vault_state,
        "preview": preview,
        "vaultCallReadyDraft": preview.get("vaultPayloadDraft") or {},
        "ts": now_ts(),
    }


@app.route("/api/nexus/execute-plan", methods=["POST"])
def api_nexus_execute_plan_safe_mode():
    """Validate a single preview or a batch/rotation preview for future Vault execution.

    SAFE MODE: returns readiness report only; does not send any transaction.
    Body examples:
      {"wallet":"0x...", "preview": {...}}
      {"wallet":"0x...", "previews": [{...}, {...}]}
      {"wallet":"0x...", "assets": [...], "budgetUsd": 500, ...}  -> builds rotation-preview first
    """
    body = request.get_json(silent=True) or {}
    wallet = body.get("wallet") or body.get("wallet_address") or request.headers.get("X-Wallet-Address") or ""
    require_vault_balance = str(body.get("requireVaultBalance", "1")).strip().lower() not in ("0", "false", "no", "off")

    if isinstance(body.get("preview"), dict):
        report = _nexus_execution_safety_check(body.get("preview") or {}, wallet=wallet, require_vault_balance=require_vault_balance)
        return jsonify(report)

    previews = body.get("previews")
    if not isinstance(previews, list):
        # Build a rotation preview from the same body if assets/symbols were provided.
        assets = _nexus_assets_from_request_payload(body)
        if assets:
            budget_usd = _safe_float(body.get("budget_usd") or body.get("budgetUsd") or body.get("budget") or 0)
            plan = _nexus_build_rotation_plan(assets, budget_usd)
            previews = []
            chain = _normalize_chain_key(body.get("chain") or "POL")
            for row in plan.get("plan") or []:
                if row.get("action") in ("INCREASE", "HOLD") and _safe_float(row.get("target_usd")) > 0:
                    pb = dict(body)
                    pb.update({
                        "chain": row.get("chain") or chain,
                        "symbol": row.get("symbol"),
                        "token": row.get("token") or body.get("token") or body.get("tokenOut") or "",
                        "side": "BUY" if row.get("action") == "INCREASE" else "HOLD",
                        "amountUsd": row.get("target_usd"),
                    })
                    prev = _nexus_order_preview(pb)
                    prev["rotationRank"] = row.get("rank")
                    prev["rotationAction"] = row.get("action")
                    previews.append(prev)
                else:
                    previews.append({
                        "status": "skipped",
                        "ready_for_vault": False,
                        "symbol": row.get("symbol"),
                        "rotationRank": row.get("rank"),
                        "rotationAction": row.get("action"),
                        "blocking_reasons": ["rotation_action_not_increase_or_no_budget"],
                        "ts": now_ts(),
                    })
        else:
            return err("missing preview, previews, assets or symbols", 400)

    reports = [_nexus_execution_safety_check(p if isinstance(p, dict) else {}, wallet=wallet, require_vault_balance=require_vault_balance) for p in (previews or [])]
    ready = [r for r in reports if r.get("can_execute")]
    blocked = [r for r in reports if not r.get("can_execute")]
    return jsonify({
        "status": "ok" if not blocked else "partial" if ready else "blocked",
        "safe_mode": True,
        "can_execute_any": bool(ready),
        "ready_count": len(ready),
        "blocked_count": len(blocked),
        "reports": reports,
        "ts": now_ts(),
    })



# -------------------------
# Nexus Vault V2 Privy Execution Package Layer
# -------------------------
# This block is intentionally isolated from the existing Grid/Rotation/AI logic.
# It prepares Vault-compatible execution packages for Privy embedded wallets and
# keeps live submission disabled unless explicitly enabled by ENV.
_NEXUS_EIP712_DOMAIN_NAME = os.getenv("NEXUS_EIP712_DOMAIN_NAME", "NexusVault")
_NEXUS_EIP712_DOMAIN_VERSION = os.getenv("NEXUS_EIP712_DOMAIN_VERSION", "2")
_NEXUS_VAULT_EXECUTION_LIVE = str(os.getenv("NEXUS_VAULT_EXECUTION_LIVE", "0")).strip().lower() in ("1", "true", "yes", "on")
_NEXUS_REQUIRE_ROUTER_CALLDATA = str(os.getenv("NEXUS_REQUIRE_ROUTER_CALLDATA", "1")).strip().lower() not in ("0", "false", "no", "off")
_NEXUS_DEFAULT_EXECUTION_DEADLINE_SEC = int(os.getenv("NEXUS_EXECUTION_DEADLINE_SEC", "1200"))
_NEXUS_MAX_ROUTER_CALLDATA_BYTES = int(os.getenv("NEXUS_MAX_ROUTER_CALLDATA_BYTES", "4096"))


def _nexus_hex_clean(v: Any) -> str:
    h = str(v or "").strip()
    if not h:
        return ""
    if not h.startswith("0x"):
        h = "0x" + h
    return h.lower()


def _nexus_is_hex_data(v: Any, allow_empty: bool = False) -> bool:
    h = str(v or "").strip()
    if not h:
        return bool(allow_empty)
    if not h.startswith("0x"):
        return False
    if len(h) == 2:
        return bool(allow_empty)
    if (len(h) - 2) % 2 != 0:
        return False
    return bool(re.fullmatch(r"0x[0-9a-fA-F]*", h))


def _nexus_bytes32(v: Any) -> str:
    """Normalize an existing bytes32 value. Does not hash names automatically."""
    h = str(v or "").strip()
    if not h:
        return ""
    if not h.startswith("0x"):
        h = "0x" + h
    if re.fullmatch(r"0x[0-9a-fA-F]{64}", h):
        return h.lower()
    return ""


def _nexus_strategy_kind_value(raw: Any) -> int:
    s = str(raw or "GRID").strip().upper()
    if s in ("0", "GRID"):
        return 0
    if s in ("1", "ROTATION", "NEXUS_ROTATION"):
        return 1
    return -1


def _nexus_default_strategy_id(kind_value: int) -> str:
    if int(kind_value) == 1:
        return _nexus_bytes32(os.getenv("NEXUS_STRATEGY_ID_ROTATION") or os.getenv("NEXUS_ROTATION_STRATEGY_ID") or "")
    return _nexus_bytes32(os.getenv("NEXUS_STRATEGY_ID_GRID") or os.getenv("NEXUS_GRID_STRATEGY_ID") or "")


def _nexus_token_address_zero_ok(token: Any) -> str:
    t = str(token or "").strip()
    if t.lower() in ("native", "eth", "bnb", "pol", "matic", "0x0000000000000000000000000000000000000000"):
        return "0x0000000000000000000000000000000000000000"
    return _norm_addr(t)


def _nexus_amount_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return max(0, int(value))
    s = str(value).strip()
    if not s:
        return 0
    if s.startswith("0x"):
        try:
            return int(s, 16)
        except Exception:
            return 0
    try:
        # Important: amounts for the Vault package must already be base units.
        return max(0, int(s))
    except Exception:
        return 0


def _nexus_vault_actions_table() -> None:
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nexus_vault_actions (
            action_id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL,
            chain TEXT NOT NULL,
            chain_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            nonce INTEGER NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT DEFAULT '{}',
            tx_hash TEXT DEFAULT '',
            error TEXT DEFAULT '',
            created_ts INTEGER,
            updated_ts INTEGER,
            UNIQUE(wallet_address, chain_id, nonce)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nexus_vault_actions_wallet_chain ON nexus_vault_actions(wallet_address, chain_id, status)")
    conn.commit()
    conn.close()


def _nexus_create_action_nonce(wallet: str, chain_id: int, action_type: str, payload: dict) -> tuple[str, int]:
    """Create a high-entropy nonce and persist it for replay/idempotency tracking."""
    _nexus_vault_actions_table()
    wallet_n = _norm_addr(wallet)
    cid = int(chain_id or 0)
    action_id = str(uuid.uuid4())
    # Solidity accepts uint256. Use a large random nonce; DB UNIQUE prevents reuse by this backend.
    for _ in range(5):
        nonce = int.from_bytes(secrets.token_bytes(24), "big")
        try:
            conn = _db()
            cur = conn.cursor()
            with DB_WRITE_LOCK:
                cur.execute(
                    "INSERT INTO nexus_vault_actions(action_id,wallet_address,chain,chain_id,action_type,nonce,status,payload_json,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (action_id, wallet_n, str(payload.get("chain") or ""), cid, str(action_type), int(nonce), "PACKAGE_CREATED", json.dumps(payload, ensure_ascii=False), now_ts(), now_ts()),
                )
                conn.commit()
            conn.close()
            return action_id, nonce
        except sqlite3.IntegrityError:
            try:
                conn.close()
            except Exception:
                pass
            continue
    raise RuntimeError("could not allocate unique vault nonce")


def _nexus_router_calldata_safety(router_call_data: str, amount_in: int, min_out: int, vault_addr: str) -> dict:
    """Conservative calldata sanity check. Full router-specific decoding is added per router later."""
    h = _nexus_hex_clean(router_call_data)
    byte_len = max(0, (len(h) - 2) // 2) if h.startswith("0x") else 0
    checks = {
        "hex_ok": _nexus_is_hex_data(h),
        "selector_present": len(h) >= 10,
        "size_ok": byte_len <= _NEXUS_MAX_ROUTER_CALLDATA_BYTES,
        "amount_in_positive": int(amount_in or 0) > 0,
        "min_out_positive": int(min_out or 0) > 0,
    }
    # Defensive recipient hint: for common ABI-encoded router calls, the vault address should appear in calldata.
    # This does not replace on-chain validation, but blocks obvious wrong-recipient payloads early.
    vault_n = _norm_addr(vault_addr).replace("0x", "")
    if vault_n and _looks_like_evm_addr(vault_addr):
        checks["vault_recipient_hint"] = vault_n in h.replace("0x", "")
    else:
        checks["vault_recipient_hint"] = False
    blocking = [k for k, v in checks.items() if not bool(v)]
    return {
        "status": "ok" if not blocking else "blocked",
        "checks": checks,
        "blocking_reasons": blocking,
        "byteLength": byte_len,
    }


def _nexus_build_execute_swap_typed_data(chain_id: int, vault_addr: str, message: dict) -> dict:
    """EIP-712 payload matching NexusVaultV2 ExecuteSwap type."""
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "ExecuteSwap": [
                {"name": "user", "type": "address"},
                {"name": "router", "type": "address"},
                {"name": "inputToken", "type": "address"},
                {"name": "outputToken", "type": "address"},
                {"name": "strategyId", "type": "bytes32"},
                {"name": "strategyKind", "type": "uint8"},
                {"name": "amountIn", "type": "uint256"},
                {"name": "minAmountOut", "type": "uint256"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
                {"name": "callHash", "type": "bytes32"},
            ],
        },
        "primaryType": "ExecuteSwap",
        "domain": {
            "name": _NEXUS_EIP712_DOMAIN_NAME,
            "version": _NEXUS_EIP712_DOMAIN_VERSION,
            "chainId": int(chain_id),
            "verifyingContract": _norm_addr(vault_addr),
        },
        "message": message,
    }


def _nexus_call_hash_placeholder(router_call_data: str) -> str:
    """Return supplied callHash when backend cannot keccak locally. Privy/client should compute if missing."""
    # The Vault signs bytes32 callHash = keccak256(routerCallData). Without a keccak dependency in this
    # legacy single-file backend, require caller/client to provide it. This avoids silently using SHA3-256.
    return ""


def _nexus_execution_package_from_body(body: dict, wallet: str) -> dict:
    chain = _normalize_chain_key(body.get("chain") or body.get("chain_key") or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(chain, 0) or 0)
    vault_addr = (_VAULT_BY_CHAIN.get(cid) or "").strip()
    wallet_n = _norm_addr(wallet or body.get("wallet") or body.get("wallet_address") or "")

    # Build or reuse preview without mutating existing preview routes.
    preview = body.get("preview") if isinstance(body.get("preview"), dict) else _nexus_order_preview(body)
    report = _nexus_execution_safety_check(preview, wallet=wallet_n, require_vault_balance=str(body.get("requireVaultBalance", "1")).strip().lower() not in ("0", "false", "no", "off"))

    token_in = _nexus_token_address_zero_ok(body.get("inputToken") or body.get("tokenIn") or preview.get("tokenIn"))
    token_out = _nexus_token_address_zero_ok(body.get("outputToken") or body.get("tokenOut") or preview.get("tokenOut"))
    router = _norm_addr(body.get("router") or body.get("routerAddress") or (((preview.get("router") or {}) if isinstance(preview.get("router"), dict) else {}).get("address") or ""))
    amount_in = _nexus_amount_int(body.get("amountIn") or body.get("amount_in") or body.get("amountInWei") or body.get("amount_raw"))
    min_out = _nexus_amount_int(body.get("minAmountOut") or body.get("min_amount_out") or body.get("minOutWei") or body.get("min_out_raw"))
    router_call_data = _nexus_hex_clean(body.get("routerCallData") or body.get("router_call_data") or "")
    call_hash = _nexus_bytes32(body.get("callHash") or body.get("call_hash") or "")

    strategy_kind = _nexus_strategy_kind_value(body.get("strategyKind") or body.get("strategy_kind") or preview.get("strategyKind") or preview.get("strategy_kind") or "GRID")
    strategy_id = _nexus_bytes32(body.get("strategyId") or body.get("strategy_id") or preview.get("strategyId") or preview.get("strategy_id") or "")
    if not strategy_id:
        strategy_id = _nexus_default_strategy_id(strategy_kind)

    deadline = _nexus_amount_int(body.get("deadline") or body.get("deadlineTs") or body.get("deadline_ts"))
    if deadline <= 0:
        deadline = now_ts() + int(_NEXUS_DEFAULT_EXECUTION_DEADLINE_SEC)

    # Nonce can be injected only for test; normally backend creates and persists it.
    supplied_nonce = body.get("nonce")
    if supplied_nonce is not None and str(supplied_nonce).strip() != "":
        nonce = _nexus_amount_int(supplied_nonce)
        action_id = str(body.get("actionId") or body.get("action_id") or uuid.uuid4())
    else:
        action_id, nonce = _nexus_create_action_nonce(wallet_n, cid, "EXECUTE_SWAP", {"chain": chain, "preview": preview})

    calldata_report = _nexus_router_calldata_safety(router_call_data, amount_in, min_out, vault_addr) if router_call_data else {
        "status": "blocked" if _NEXUS_REQUIRE_ROUTER_CALLDATA else "skipped",
        "checks": {"router_call_data_present": False},
        "blocking_reasons": ["router_call_data_missing"] if _NEXUS_REQUIRE_ROUTER_CALLDATA else [],
    }

    extra_checks = {
        "vault_address_ok": _looks_like_evm_addr(vault_addr),
        "privy_wallet_user_ok": _looks_like_evm_addr(wallet_n),
        "router_address_ok": _looks_like_evm_addr(router),
        "token_in_ok": token_in == "0x0000000000000000000000000000000000000000" or _looks_like_evm_addr(token_in),
        "token_out_ok": token_out == "0x0000000000000000000000000000000000000000" or _looks_like_evm_addr(token_out),
        "different_tokens": bool(token_in and token_out and token_in.lower() != token_out.lower()),
        "strategy_kind_ok": strategy_kind in (0, 1),
        "strategy_id_ok": bool(strategy_id),
        "amount_in_base_units_ok": amount_in > 0,
        "min_out_base_units_ok": min_out > 0,
        "deadline_future_ok": deadline > now_ts(),
        "call_hash_present": bool(call_hash),
    }
    if not call_hash:
        # Do not fake callHash. The signing side must supply keccak256(routerCallData).
        extra_checks["call_hash_present"] = False

    blocking = list(report.get("blocking_reasons") or [])
    blocking += [k for k, v in extra_checks.items() if not bool(v)]
    blocking += [f"calldata:{x}" for x in (calldata_report.get("blocking_reasons") or [])]

    message = {
        "user": wallet_n,
        "router": router,
        "inputToken": token_in,
        "outputToken": token_out,
        "strategyId": strategy_id,
        "strategyKind": int(strategy_kind if strategy_kind >= 0 else 0),
        "amountIn": str(amount_in),
        "minAmountOut": str(min_out),
        "nonce": str(nonce),
        "deadline": str(deadline),
        "callHash": call_hash,
    }
    typed_data = _nexus_build_execute_swap_typed_data(cid, vault_addr, message) if cid > 0 and _looks_like_evm_addr(vault_addr) else {}

    # Args match NexusVaultV2.ExecuteSwapParams struct order. Signature is empty until Privy returns it.
    vault_args = {
        "user": wallet_n,
        "router": router,
        "inputToken": token_in,
        "outputToken": token_out,
        "strategyId": strategy_id,
        "strategyKind": int(strategy_kind if strategy_kind >= 0 else 0),
        "amountIn": str(amount_in),
        "minAmountOut": str(min_out),
        "nonce": str(nonce),
        "deadline": str(deadline),
        "routerCallData": router_call_data,
        "signature": str(body.get("signature") or ""),
    }

    return {
        "status": "ok" if not blocking else "blocked",
        "canSign": len(blocking) == 0,
        "canSubmitLive": bool(_NEXUS_VAULT_EXECUTION_LIVE and len(blocking) == 0 and vault_args.get("signature")),
        "liveExecutionEnabled": bool(_NEXUS_VAULT_EXECUTION_LIVE),
        "message": "Vault execution package prepared for Privy signing." if not blocking else "Vault execution package blocked by safety checks.",
        "actionId": action_id,
        "wallet": wallet_n,
        "chain": chain,
        "chainId": cid,
        "vault": vault_addr,
        "executor": (_EXECUTOR_BY_CHAIN.get(cid) or "").strip(),
        "checks": {**(report.get("checks") or {}), **extra_checks, "router_calldata_ok": calldata_report.get("status") == "ok"},
        "blocking_reasons": blocking,
        "safetyReport": report,
        "calldataReport": calldata_report,
        "eip712": typed_data,
        "vaultFunction": "executeSwapWithSig((address,address,address,address,bytes32,uint8,uint256,uint256,uint256,uint256,bytes,bytes))",
        "vaultArgs": vault_args,
        "privy": {
            "walletAddress": wallet_n,
            "mode": "embedded_wallet",
            "requiresExternalWallet": False,
            "requiresPrivySignature": True,
            "signatureField": "vaultArgs.signature",
        },
        "ts": now_ts(),
    }


@app.route("/api/nexus/vault/execute-swap-package", methods=["POST"])
def api_nexus_vault_execute_swap_package():
    """Prepare a NexusVaultV2 executeSwapWithSig package for Privy embedded wallets.

    This does not replace Privy. The user wallet address remains the Vault user.
    The response contains the EIP-712 typed data that Privy/client must sign and
    the exact struct args needed by the Vault after signature is added.
    """
    body = request.get_json(silent=True) or {}
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    try:
        return jsonify(_nexus_execution_package_from_body(body, wa))
    except Exception as e:
        return jsonify({"status": "error", "error": str(e), "ts": now_ts()}), 500


@app.route("/api/nexus/vault/submit-signed-swap", methods=["POST"])
def api_nexus_vault_submit_signed_swap():
    """Accept a Privy-signed Vault package and keep live execution gated by ENV.

    By default this endpoint validates and logs the signed package but does not submit
    a transaction. Set NEXUS_VAULT_EXECUTION_LIVE=1 only after fork/testnet testing
    and after a Privy server-side signer/tx sender is connected.
    """
    body = request.get_json(silent=True) or {}
    wa = _require_auth()
    if not wa:
        return err("unauthorized", 401)
    sig = str(body.get("signature") or ((body.get("vaultArgs") or {}) if isinstance(body.get("vaultArgs"), dict) else {}).get("signature") or "").strip()
    if not _nexus_is_hex_data(sig):
        return err("missing/invalid Privy EIP-712 signature", 400)

    merged = dict(body)
    if isinstance(body.get("vaultArgs"), dict):
        for k, v in (body.get("vaultArgs") or {}).items():
            merged.setdefault(k, v)
    merged["signature"] = sig

    package = _nexus_execution_package_from_body(merged, wa)
    if package.get("status") != "ok":
        return jsonify(package), 400

    if not _NEXUS_VAULT_EXECUTION_LIVE:
        package["status"] = "ready_not_submitted"
        package["message"] = "Signed package valid, but live Vault execution is disabled. Enable NEXUS_VAULT_EXECUTION_LIVE only after tests."
        return jsonify(package)

    # Live tx sending is intentionally not implemented here because Privy server-side signing/delegated
    # transaction execution requires the project-specific Privy API credentials and policy IDs.
    return jsonify({
        "status": "blocked",
        "error": "live Privy transaction sender not connected in this backend file yet",
        "package": package,
        "ts": now_ts(),
    }), 501

@app.route("/api/nexus/routers", methods=["GET"])
def api_nexus_routers():
    chain = _normalize_chain_key(request.args.get("chain") or request.args.get("network") or "POL")
    cid = int((_CHAIN_ID_BY_KEY or {}).get(chain, 0) or 0)
    return jsonify({"status": "ok", "chain": chain, "chainId": cid, "routers": _nexus_allowed_routers_for_chain(cid), "ts": now_ts()})


@app.route("/api/nexus/fee-policy", methods=["GET"])
def api_nexus_fee_policy():
    chain = _normalize_chain_key(request.args.get("chain") or request.args.get("network") or "POL")
    profit_usd = _safe_float(request.args.get("profitUsd") or request.args.get("profit_usd") or 0)
    stable = str(request.args.get("feeStable") or request.args.get("fee_stable") or "").strip().upper()
    return jsonify({"status": "ok", "chain": chain, "feePolicy": _nexus_fee_preview(profit_usd, chain, stable), "ts": now_ts()})
