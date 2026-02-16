from .binance import BinanceAdapter
from .hyperliquid import HyperliquidAdapter
from .bybit import BybitAdapter
from .okx import OKXAdapter
from .bitget import BitgetAdapter
from .lighter import LighterAdapter
from .hyperliquid_hip3 import HIP3Adapter, discover_hip3_deployers

EXCHANGE_MAP = {
    'binance': BinanceAdapter,
    'hyperliquid': HyperliquidAdapter,
    'bybit': BybitAdapter,
    'okx': OKXAdapter,
    'bitget': BitgetAdapter,
    'lighter': LighterAdapter,
}


def get_hip3_adapters() -> dict:
    """Discover HIP3 deployers and return a map of exchange_name -> adapter instance."""
    deployers = discover_hip3_deployers()
    adapters = {}
    for dep in deployers:
        name = f"hl-{dep['name']}"
        adapters[name] = HIP3Adapter(dep['name'], dep['markets'])
    return adapters
