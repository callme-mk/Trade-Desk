"""
TradeDesk MT5 Sync Script
==========================
Runs on YOUR PC where MT5 is installed.
Connects to MT5, pulls closed trades and live balance,
sends them to your TradeDesk backend automatically.
"""

import time
import requests
from datetime import datetime, timedelta

try:
    import MetaTrader5 as mt5
except ImportError:
    print("ERROR: Run: pip install MetaTrader5")
    exit(1)

# ═══════════════════════════════════════════════
#  YOUR SETTINGS
# ═══════════════════════════════════════════════
API_URL      = "https://trade-desk-production.up.railway.app"  # your deployed backend URL
USERNAME     = "mustafa"      # your TradeDesk username
PASSWORD     = "mk21"      # your TradeDesk password

MT5_LOGIN    = 20202544                    # your MT5 account number (0 = use current)
MT5_PASSWORD = "(iNVICTUS-2025)"                   # leave empty if already logged in
MT5_SERVER   = "WindsorBrokers1-Demo"                   # leave empty if already logged in

SYNC_EVERY   = 60                   # seconds between syncs
SYNC_DAYS    = 365                  # how many days back to pull trades
# ═══════════════════════════════════════════════


def get_token():
    """Login to TradeDesk API and get JWT token."""
    resp = requests.post(f"{API_URL}/auth/login", json={
        "username": USERNAME,
        "password": PASSWORD
    })
    if resp.status_code != 200:
        print(f"Login failed: {resp.text}")
        return None
    return resp.json()["token"]


def connect_mt5():
    """Initialize MT5 connection."""
    if not mt5.initialize():
        print("MT5 initialize failed:", mt5.last_error())
        return False

    if MT5_LOGIN != 0:
        if not mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
            print("MT5 login failed:", mt5.last_error())
            return False

    info = mt5.account_info()
    if info:
        print(f"MT5 Connected: {info.name} | {info.server} | Balance: {info.balance} {info.currency}")
    return True


def pull_closed_trades(days_back=365):
    """Pull closed trades from MT5 history."""
    date_from = datetime.now() - timedelta(days=days_back)
    date_to   = datetime.now() + timedelta(days=1)

    deals = mt5.history_deals_get(date_from, date_to)
    if deals is None:
        print("No deals found:", mt5.last_error())
        return []

    # MT5 deals come in pairs: IN (open) and OUT (close)
    out_deals = [d for d in deals if d.entry == mt5.DEAL_ENTRY_OUT]

    trades = []
    for deal in out_deals:
        if deal.type not in [mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL]:
            continue

        position_id = deal.position_id
        in_deals = [d for d in deals
                    if d.position_id == position_id
                    and d.entry == mt5.DEAL_ENTRY_IN]

        if not in_deals:
            continue

        open_deal = in_deals[0]
        direction = "BUY" if open_deal.type == mt5.DEAL_TYPE_BUY else "SELL"

        sl = 0.0
        tp = 0.0
        try:
            history_orders = mt5.history_orders_get(position=position_id)
            if history_orders:
                for order in history_orders:
                    if order.sl > 0: sl = order.sl
                    if order.tp > 0: tp = order.tp
        except Exception:
            pass

        open_time  = datetime.fromtimestamp(open_deal.time)
        close_time = datetime.fromtimestamp(deal.time)

        trades.append({
            "ticket":      str(position_id),
            "symbol":      deal.symbol,
            "direction":   direction,
            "open_time":   open_time.strftime("%Y-%m-%d %H:%M:%S"),
            "close_time":  close_time.strftime("%Y-%m-%d %H:%M:%S"),
            "open_price":  open_deal.price,
            "close_price": deal.price,
            "sl":          sl,
            "tp":          tp,
            "volume":      deal.volume,
            "profit":      round(deal.profit, 2),
            "swap":        round(deal.swap, 2),
            "commission":  round(deal.commission, 2),
        })

    return trades


def sync(token: str, trades: list, balance: float, equity: float) -> dict:
    """Send trades and account info to TradeDesk backend."""
    # Modified to allow sending data even if trades list is empty
    payload = {
        "trades": trades,
        "balance": balance,
        "equity": equity
    }

    resp = requests.post(
        f"{API_URL}/trades/sync",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30
    )

    if resp.status_code != 200:
        print(f"Sync failed: {resp.status_code} {resp.text}")
        return {}

    return resp.json()


def main():
    print("=" * 50)
    print("  TradeDesk MT5 Sync")
    print("=" * 50)
    print(f"  API: {API_URL}")
    print(f"  Syncing every {SYNC_EVERY} seconds")
    print(f"  Pulling last {SYNC_DAYS} days of trades")
    print()

    if not connect_mt5():
        print("Failed to connect to MT5. Is it open?")
        return

    print("Logging into TradeDesk API...")
    token = get_token()
    if not token:
        print("Could not login. Check USERNAME and PASSWORD.")
        return

    print("Logged in. Starting sync loop...\n")
    token_refresh = datetime.now() + timedelta(days=1)

    while True:
        try:
            now = datetime.now().strftime("%H:%M:%S")

            if datetime.now() > token_refresh:
                token = get_token()
                token_refresh = datetime.now() + timedelta(days=1)

            # Pull current account info
            account_info = mt5.account_info()
            balance = round(account_info.balance, 2) if account_info else None
            equity = round(account_info.equity, 2) if account_info else None

            # Pull trades
            trades = pull_closed_trades(SYNC_DAYS)
            
            # Print status depending on if there are trades or just a balance update
            if trades:
                print(f"[{now}] Found {len(trades)} trades | Balance: {balance} | Equity: {equity}")
            else:
                print(f"[{now}] Syncing Balance: {balance} | Equity: {equity}")

            # Send to backend (syncs balance even if trades is empty)
            if balance is not None:
                result = sync(token, trades, balance, equity)
                if result and trades:
                    print(f"[{now}] Synced Trades — Added: {result.get('added',0)} | Updated: {result.get('updated',0)}")

            time.sleep(SYNC_EVERY)

        except KeyboardInterrupt:
            print("\nSync stopped.")
            mt5.shutdown()
            break
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
