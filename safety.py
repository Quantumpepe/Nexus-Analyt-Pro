# safety.py
from dataclasses import dataclass

@dataclass
class SafetyResult:
    status: str          # SAFE / WARN / DANGER / ERROR
    reasons: list        # ["LP drop 18% in 10m", ...]
    metrics: dict        # {"lp_drop_pct": 12.3, ...}

def evaluate_safety(snapshot_now: dict, snapshot_prev: dict | None) -> SafetyResult:
    """
    snapshot_now/snapshot_prev: dict mit reserve_in_usd + volume_usd_h24 etc.
    prev kann None sein beim ersten Lauf.
    """

    # Wenn keine Daten → ERROR
    try:
        lp_now = float(snapshot_now.get("reserve_in_usd") or 0)
        vol_now = float(snapshot_now.get("volume_usd_h24") or 0)
    except Exception:
        return SafetyResult("ERROR", ["bad snapshot format"], {})

    reasons = []
    metrics = {"lp_now": lp_now, "vol_h24_now": vol_now}

    if lp_now <= 0:
        return SafetyResult("DANGER", ["no liquidity"], metrics)

    # Ohne vorherigen Snapshot können wir nur BASIC checks
    if snapshot_prev is None:
        # Basic: extrem low volume → WARN (optional)
        if vol_now < 1000:
            reasons.append("low volume (no baseline yet)")
            return SafetyResult("WARN", reasons, metrics)
        return SafetyResult("SAFE", ["baseline created"], metrics)

    lp_prev = float(snapshot_prev.get("reserve_in_usd") or 0)
    vol_prev = float(snapshot_prev.get("volume_usd_h24") or 0)
    metrics["lp_prev"] = lp_prev
    metrics["vol_h24_prev"] = vol_prev

    # LP Drop %
    lp_drop_pct = 0.0
    if lp_prev > 0:
        lp_drop_pct = max(0.0, (lp_prev - lp_now) / lp_prev * 100.0)
    metrics["lp_drop_pct"] = lp_drop_pct

    # Volume Drop (H24) – grob (nur als Hinweis)
    vol_drop_pct = 0.0
    if vol_prev > 0:
        vol_drop_pct = max(0.0, (vol_prev - vol_now) / vol_prev * 100.0)
    metrics["vol_drop_pct"] = vol_drop_pct

    # Regeln (einfach + sicher)
    # DANGER: LP drop >= 25%
    if lp_drop_pct >= 25.0:
        reasons.append(f"LP drop {lp_drop_pct:.1f}%")
        return SafetyResult("DANGER", reasons, metrics)

    # WARN: LP drop >= 10% oder sehr niedriges Volumen
    if lp_drop_pct >= 10.0:
        reasons.append(f"LP drop {lp_drop_pct:.1f}%")
    if vol_now < 5000:
        reasons.append("very low volume")

    if reasons:
        return SafetyResult("WARN", reasons, metrics)

    return SafetyResult("SAFE", ["stable"], metrics)
