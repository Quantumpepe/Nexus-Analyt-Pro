import requests

GECKO_BASE = "https://api.geckoterminal.com/api/v2"

def get_pair_data(network: str, pool_address: str):
    url = f"{GECKO_BASE}/networks/{network}/pools/{pool_address}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    attrs = r.json()["data"]["attributes"]

    return {
        "base_token_price_usd": attrs.get("base_token_price_usd"),
        "reserve_in_usd": attrs.get("reserve_in_usd"),
        "volume_usd_h24": (attrs.get("volume_usd") or {}).get("h24"),
        "price_change_pct_h24": (attrs.get("price_change_percentage") or {}).get("h24"),
    }
