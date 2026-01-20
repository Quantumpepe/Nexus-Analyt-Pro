import time
from dataclasses import dataclass, asdict


@dataclass
class GridConfig:
    invest_usd: float = 100.0
    grid_step_pct: float = 2.0
    grid_levels_each_side: int = 5
    take_profit_pct: float = 50.0
    stop_loss_pct: float = 20.0
    fee_bps: float = 30.0          # 0.30% fees simulation
    mode: str = "SAFE"             # SAFE / AGGRESSIVE


@dataclass
class GridState:
    started_at: float
    start_price: float
    last_price: float

    cash_usd: float
    asset_qty: float
    cost_basis_usd: float
    realized_pnl_usd: float

    invested_usd: float = 0.0      # <<< NEU: hart verbrauchtes Budget

    active: bool = True
    closed_reason: str = ""

    orders: list = None            # list of dict fills/opens


def build_grid(start_price: float, cfg: GridConfig):
    step = cfg.grid_step_pct / 100.0
    orders = []

    for i in range(1, cfg.grid_levels_each_side + 1):
        orders.append({"side": "BUY", "price": start_price * (1.0 - step * i), "filled": False})
    for i in range(1, cfg.grid_levels_each_side + 1):
        orders.append({"side": "SELL", "price": start_price * (1.0 + step * i), "filled": False})

    buys = sorted([o for o in orders if o["side"] == "BUY"], key=lambda x: x["price"], reverse=True)
    sells = sorted([o for o in orders if o["side"] == "SELL"], key=lambda x: x["price"])
    return buys + sells


def _fee_multiplier(cfg: GridConfig):
    return max(0.0, 1.0 - (cfg.fee_bps / 10000.0))


def equity_usd(state: GridState):
    return float(state.cash_usd) + float(state.asset_qty) * float(state.last_price)


def avg_cost(state: GridState):
    if state.asset_qty <= 0:
        return 0.0
    return state.cost_basis_usd / state.asset_qty


def _close_all(state: GridState, cfg: GridConfig, price: float, reason: str):
    if not state.active:
        return

    state.last_price = price

    if state.asset_qty > 0:
        fee_mul = _fee_multiplier(cfg)
        received = state.asset_qty * price * fee_mul

        pnl = received - state.cost_basis_usd
        state.realized_pnl_usd += pnl

        state.cash_usd += received
        state.asset_qty = 0.0
        state.cost_basis_usd = 0.0

    state.active = False
    state.closed_reason = reason


def step_sim(state: GridState, cfg: GridConfig, price: float):
    state.last_price = price
    if not state.active:
        return state

    # TP / SL relativ zum Startpreis
    tp_price = state.start_price * (1.0 + cfg.take_profit_pct / 100.0)
    sl_price = state.start_price * (1.0 - cfg.stop_loss_pct / 100.0)

    if price >= tp_price:
        _close_all(state, cfg, price, f"TAKE_PROFIT hit ({cfg.take_profit_pct}%)")
        return state

    if price <= sl_price:
        _close_all(state, cfg, price, f"STOP_LOSS hit ({cfg.stop_loss_pct}%)")
        return state

    # Portionsgröße
    portion = 0.33 if cfg.mode == "AGGRESSIVE" else 0.20
    buy_budget = max(0.0, state.cash_usd) * portion
    fee_mul = _fee_multiplier(cfg)

    for o in state.orders:
        if o.get("filled"):
            continue

        # -----------------
        # BUY (mit HARTem Budget-Limit)
        # -----------------
        if o["side"] == "BUY" and price <= o["price"] and state.cash_usd > 1.0:
            remaining_budget = max(0.0, cfg.invest_usd - state.invested_usd)
            if remaining_budget <= 1.0:
                continue  # <<< Budget komplett verbraucht

            spend = min(buy_budget, state.cash_usd, remaining_budget)
            if spend < 1.0:
                continue

            qty = (spend * fee_mul) / price

            state.cash_usd -= spend
            state.asset_qty += qty
            state.cost_basis_usd += spend
            state.invested_usd += spend   # <<< Budget wird hier fest gebucht

            o["filled"] = True
            o["fill_price"] = price
            o["fill_ts"] = time.time()
            o["qty"] = qty
            o["spent_usd"] = spend

        # -----------------
        # SELL
        # -----------------
        elif o["side"] == "SELL" and price >= o["price"] and state.asset_qty > 0:
            qty = state.asset_qty * portion
            if qty * price < 1.0:
                continue

            received = qty * price * fee_mul

            cost_removed = state.cost_basis_usd * (qty / state.asset_qty) if state.asset_qty > 0 else 0.0
            pnl = received - cost_removed

            state.realized_pnl_usd += pnl
            state.asset_qty -= qty
            state.cost_basis_usd -= cost_removed
            state.cash_usd += received

            o["filled"] = True
            o["fill_price"] = price
            o["fill_ts"] = time.time()
            o["qty"] = qty
            o["received_usd"] = received
            o["pnl_usd"] = pnl

    return state


def snapshot(state: GridState, cfg: GridConfig):
    snap = asdict(state)
    snap["equity_usd"] = equity_usd(state)
    snap["avg_cost_usd"] = avg_cost(state)
    snap["mode"] = cfg.mode
    return snap
