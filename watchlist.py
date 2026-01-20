# watchlist.py
# Eine kleine Start-Watchlist. Später kommt DB/CRUD dazu.

WATCHLIST = [
    {
        "id": "polygon_weth_usdc_quickswap",
        "label": "WETH/USDC (QuickSwap)",
        "network": "polygon_pos",
        "pool": "0x853ee4b2a13f8a742d64c8f088be7ba2131f670d",
        "min_lp_usd": 150_000,
        "min_vol_h24_usd": 50_000
    },
    # Beispiel-Meme Slot (später ersetzt du pool + label)
    {
        "id": "polygon_meme_example",
        "label": "MEME-EXAMPLE/USDC",
        "network": "polygon_pos",
        "pool": "0x0000000000000000000000000000000000000000",
        "min_lp_usd": 150_000,
        "min_vol_h24_usd": 25_000
    }
]

def get_watchlist():
    return WATCHLIST
