"""Streamlit dashboard for funding rates — matrix view."""

import sys
import os
import time
import math
import json
from itertools import combinations
import streamlit as st
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.database import Database
from core.normalizer import TimeNormalizer
from core.calculator import calculate_apr, rate_sum_to_percent
# Collector import removed for cloud deployment

import streamlit as st

# Hide Streamlit banner
st.markdown("""
<style>
header {visibility: hidden;}
.stDeployButton {display: none;}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)



# Load config (keep for exchange settings but not database)
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Database connection from environment variables or Streamlit secrets
db = Database()
normalizer = TimeNormalizer(db)

st.set_page_config(page_title="Funding Rates", layout="wide", initial_sidebar_state="collapsed")

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""<style>
    /* dark theme overrides */
    .block-container { padding-top: 1rem; max-width: 100%; }
    .stApp { background-color: #0d1117; color: #c9d1d9; }
    
    /* timeframe buttons */
    .tf-bar { display: flex; gap: 6px; margin-bottom: 12px; flex-wrap: wrap; }
    .tf-btn { padding: 6px 14px; border-radius: 6px; background: #161b22; color: #8b949e;
              border: 1px solid #30363d; cursor: pointer; font-size: 13px; font-weight: 500;
              text-decoration: none; }
    .tf-btn:hover { background: #1f2937; color: #e6edf3; }
    .tf-btn.active { background: #1f6feb; color: #fff; border-color: #1f6feb; }
    
    /* header bar */
    .hdr { display: flex; justify-content: space-between; align-items: center;
           margin-bottom: 8px; flex-wrap: wrap; gap: 8px; }
    .hdr-left { font-size: 22px; font-weight: 700; color: #e6edf3; }
    .hdr-meta { font-size: 12px; color: #8b949e; }
    
    /* matrix table */
    .mtx th a { color: #8b949e; text-decoration: none; }
    .mtx th a:hover { color: #e6edf3; }
    .mtx { width: 100%; border-collapse: collapse; font-size: 13px; }
    .mtx th { position: sticky; top: 0; background: #161b22; color: #8b949e;
              padding: 8px 10px; text-align: center; border-bottom: 2px solid #30363d;
              font-weight: 600; font-size: 12px; text-transform: uppercase; cursor: pointer; }
    .mtx th:first-child { text-align: left; min-width: 80px; }
    .mtx td { padding: 6px 10px; text-align: center; border-bottom: 1px solid #21262d; }
    .mtx td:first-child { text-align: left; font-weight: 600; color: #e6edf3; }
    .mtx tr:hover { background: #161b22; }
    
    /* cell styling */
    .cell { border-radius: 4px; padding: 4px 6px; display: inline-block; min-width: 80px; }
    .apr-val { font-size: 14px; font-weight: 700; }
    .raw-val { font-size: 10px; opacity: 0.7; margin-top: 1px; }
    .cell-best { outline: 2px solid #3fb950; outline-offset: -1px; }
    .cell-worst { outline: 2px solid #f85149; outline-offset: -1px; }
    .na-cell { color: #484f58; font-size: 12px; }
    
    /* scrollable wrapper */
    .tbl-wrap { overflow-x: auto; max-height: 80vh; overflow-y: auto; }
    
    /* search */
    div[data-testid="stTextInput"] { max-width: 300px; }

    /* arbitrage table */
    .arb-tbl { width: 100%; border-collapse: collapse; font-size: 13px; }
    .arb-tbl th { position: sticky; top: 0; background: #161b22; color: #8b949e;
                  padding: 8px 10px; text-align: center; border-bottom: 2px solid #30363d;
                  font-weight: 600; font-size: 12px; text-transform: uppercase; }
    .arb-tbl th:first-child { text-align: center; }
    .arb-tbl td { padding: 6px 10px; text-align: center; border-bottom: 1px solid #21262d;
                  color: #c9d1d9; }
    .arb-tbl tr:hover { background: #161b22; }
    .arb-action { font-size: 11px; font-weight: 600; color: #58a6ff; white-space: nowrap; }
    .arb-long { color: #3fb950; font-weight: 600; }
    .arb-short { color: #f85149; font-weight: 600; }
</style>""", unsafe_allow_html=True)

# ── Timeframe ────────────────────────────────────────────────────────────────
TIMEFRAMES = [1, 3, 7, 15, 30, 60, 90, 180, 270, 360]

qp = st.query_params
selected_days = int(qp.get("days", "7"))
if selected_days not in TIMEFRAMES:
    selected_days = 7

# Read sort params
sort_param = qp.get("sort", "")
sort_dir = qp.get("dir", "desc")

tf_html = '<div class="tf-bar">'
for d in TIMEFRAMES:
    cls = "tf-btn active" if d == selected_days else "tf-btn"
    # Preserve sort params in timeframe links
    extra = ""
    if sort_param:
        extra += f"&sort={sort_param}"
    if sort_dir and sort_param:
        extra += f"&dir={sort_dir}"
    tf_html += f'<a class="{cls}" href="?days={d}{extra}" target="_self">{d}d</a>'
tf_html += '</div>'
st.markdown(tf_html, unsafe_allow_html=True)

# ── Search Control ──────────────────────────────────────────────────────────
search = st.text_input("🔍 Search asset", "", label_visibility="collapsed",
                       placeholder="Search asset...")

# ── Database Update Status ───────────────────────────────────────────────────
def _human_time_ago(ts_ms):
    """Convert millisecond timestamp to human-readable time ago."""
    if not ts_ms:
        return "never"
    diff_s = (time.time() * 1000 - ts_ms) / 1000
    if diff_s < 60:
        return f"{int(diff_s)}s ago"
    if diff_s < 3600:
        return f"{int(diff_s / 60)} min ago"
    if diff_s < 86400:
        h = diff_s / 3600
        return f"{h:.1f} hours ago"
    return f"{diff_s / 86400:.1f} days ago"

now_status_ms = int(time.time() * 1000)
exchange_status = []
try:
    # Get exchange status using the new RPC method
    status_data = db.get_exchange_status()
    for status_item in status_data:
        age_h = status_item.get('age_ms', 0) / 3_600_000
        typical_h = status_item.get('typical_interval_ms', 28800000) / 3_600_000
        
        if status_item.get('status') == 'failed':
            status = "❌"
            level = "error"
        elif status_item.get('status') == 'stale':
            status = "⚠️"
            level = "warn"
        else:
            status = "✅"
            level = "ok"

        exchange_status.append({
            'name': status_item.get('exchange'),
            'last_ts': status_item.get('last_update_ts'),
            'status': status,
            'level': level,
            'age_h': age_h,
            'typical_h': typical_h,
            'recs_1h': status_item.get('records_last_hour', 0),
            'recs_1d': status_item.get('records_last_day', 0),
        })
except Exception:
    pass

if exchange_status:
    errors = sum(1 for e in exchange_status if e['level'] == 'error')
    warns = sum(1 for e in exchange_status if e['level'] == 'warn')
    if errors:
        overall = '🔴 Error'
        overall_color = '#f85149'
    elif warns:
        overall = '🟡 Warning'
        overall_color = '#d29922'
    else:
        overall = '🟢 Healthy'
        overall_color = '#3fb950'

    newest_ts = max(e['last_ts'] for e in exchange_status if e['last_ts'])
    # Next expected update based on shortest typical interval
    _min_typical_h = min(e['typical_h'] for e in exchange_status)
    _next_ms = newest_ts + int(_min_typical_h * 3_600_000)
    _next_diff = (_next_ms - now_status_ms) / 60_000
    _next_str = f"in {int(_next_diff)} min" if _next_diff > 0 else f"{int(-_next_diff)} min overdue"
    status_line = (f'<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;'
                   f'padding:12px 16px;margin-bottom:12px;">'
                   f'<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;">'
                   f'<span><span style="font-weight:700;color:{overall_color};font-size:15px;">'
                   f'{overall}</span>'
                   f'<span style="color:#8b949e;font-size:12px;margin-left:16px;">'
                   f'Last update: {_human_time_ago(newest_ts)} · '
                   f'Next expected: {_next_str} · '
                   f'{len(exchange_status)} exchanges</span></span>'
                   f'<span style="color:#484f58;font-size:11px;">Auto-refreshes with page reload</span>'
                   f'</div></div>')
    st.markdown(status_line, unsafe_allow_html=True)

    with st.expander("Exchange details", expanded=False):
        detail_html = '<table style="width:100%;font-size:13px;color:#c9d1d9;border-collapse:collapse;">'
        detail_html += ('<tr style="border-bottom:1px solid #30363d;">'
                        '<th style="text-align:left;padding:4px 8px;color:#8b949e;">Exchange</th>'
                        '<th style="padding:4px 8px;color:#8b949e;">Status</th>'
                        '<th style="padding:4px 8px;color:#8b949e;">Last Update</th>'
                        '<th style="padding:4px 8px;color:#8b949e;">Recs (1h)</th>'
                        '<th style="padding:4px 8px;color:#8b949e;">Recs (24h)</th></tr>')
        for e in exchange_status:
            _n = e['name']
            dn = f"HL-{_n[3:]}" if _n.startswith('hl-') else _n.capitalize()
            detail_html += (f'<tr style="border-bottom:1px solid #21262d;">'
                            f'<td style="padding:4px 8px;font-weight:600;">{dn}</td>'
                            f'<td style="padding:4px 8px;text-align:center;">{e["status"]}</td>'
                            f'<td style="padding:4px 8px;text-align:center;">{_human_time_ago(e["last_ts"])}</td>'
                            f'<td style="padding:4px 8px;text-align:center;">{e["recs_1h"]}</td>'
                            f'<td style="padding:4px 8px;text-align:center;">{e["recs_1d"]}</td></tr>')
        detail_html += '</table>'
        st.markdown(detail_html, unsafe_allow_html=True)

# ── Gather data ──────────────────────────────────────────────────────────────
all_symbols = db.get_available_symbols()
if not all_symbols:
    st.warning("No data yet. Run: `python scripts/bootstrap.py --symbols ALL --days 30`")
    st.stop()

# Filter by search
if search:
    all_symbols = [s for s in all_symbols if search.upper() in s.upper()]

now_ms = int(time.time() * 1000)
start_ms = now_ms - (selected_days * 86400 * 1000)

# Get all exchanges that have data
all_exchanges = set()
# Build matrix: symbol -> exchange -> {apr, rate_sum}
matrix = {}

for symbol in all_symbols:
    rates = normalizer.get_normalized_rates(symbol, start_ms, now_ms)
    if not rates:
        continue
    row = {}
    for exch, data in rates.items():
        apr = calculate_apr(data['rate_sum'], selected_days)
        raw_pct = rate_sum_to_percent(data['rate_sum'])
        row[exch] = {'apr': apr, 'raw': raw_pct}
        all_exchanges.add(exch)
    if row:
        matrix[symbol] = row

exchanges = sorted(all_exchanges)


def exchange_display(name: str) -> str:
    """Pretty display name for exchange columns."""
    if name.startswith('hl-'):
        return f"HL-{name[3:]}"
    return name.capitalize()


# ── Header ───────────────────────────────────────────────────────────────────
last_updated = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
st.markdown(f'''<div class="hdr">
  <span class="hdr-left">Funding Rates Matrix</span>
  <span class="hdr-meta">{len(matrix)} pairs · {len(exchanges)} exchanges · {selected_days}d · Updated {last_updated}</span>
</div>''', unsafe_allow_html=True)

if not matrix:
    st.info("No data for the selected filter/timeframe.")
    st.stop()

# ── Sort ─────────────────────────────────────────────────────────────────────
def sort_key(item):
    sym, row = item
    if not sort_param or sort_param == "asset":
        return sym
    # sort_param is an exchange name
    v = row.get(sort_param, {}).get('apr', None)
    if v is None:
        return float('-inf') if sort_dir == 'desc' else float('inf')
    return v

reverse = (sort_dir == 'desc') if sort_param and sort_param != 'asset' else False
if sort_param == 'asset' and sort_dir == 'desc':
    reverse = True

sorted_matrix = sorted(matrix.items(), key=sort_key, reverse=reverse)


# ── Color helpers ────────────────────────────────────────────────────────────
def apr_color(apr: float) -> str:
    """Return CSS color + bg for an APR value."""
    if apr >= 0:
        intensity = min(abs(apr) / 100, 1.0)
        r = int(22 + intensity * 10)
        g = int(27 + intensity * 50)
        b = int(34 + intensity * 15)
        text = f"rgb({int(60 + intensity * 140)}, {int(180 + intensity * 75)}, {int(80 + intensity * 50)})"
    else:
        intensity = min(abs(apr) / 100, 1.0)
        r = int(22 + intensity * 60)
        g = int(27 + intensity * 5)
        b = int(34 + intensity * 5)
        text = f"rgb({int(200 + intensity * 48)}, {int(80 + intensity * 20)}, {int(70 + intensity * 20)})"
    bg = f"rgb({r},{g},{b})"
    return f"background:{bg}; color:{text};"


# ── Helper: build sort link for column header ────────────────────────────────
def _header_link(col_key: str, display: str) -> str:
    """Build an <a> tag for a sortable column header."""
    # Determine new direction
    if sort_param == col_key:
        new_dir = 'asc' if sort_dir == 'desc' else 'desc'
        indicator = ' ▼' if sort_dir == 'desc' else ' ▲'
    else:
        new_dir = 'desc'
        indicator = ''
    # For asset column, default sort is asc
    if col_key == 'asset' and sort_param != col_key:
        new_dir = 'asc'
    href = f"?days={selected_days}&sort={col_key}&dir={new_dir}"
    return f'<a href="{href}" target="_self">{display}{indicator}</a>'


# ── Build HTML table ─────────────────────────────────────────────────────────
html = '<div class="tbl-wrap"><table class="mtx"><thead><tr>'
html += f'<th>{_header_link("asset", "Asset")}</th>'
for e in exchanges:
    html += f'<th>{_header_link(e, exchange_display(e))}</th>'
html += '</tr></thead><tbody>'

for symbol, row in sorted_matrix:
    html += f'<tr><td>{symbol}</td>'
    aprs = {e: row[e]['apr'] for e in row}
    best_e = max(aprs, key=aprs.get) if aprs else None
    worst_e = min(aprs, key=aprs.get) if aprs and len(aprs) > 1 else None
    if best_e == worst_e:
        worst_e = None

    for e in exchanges:
        if e not in row:
            html += '<td><span class="na-cell">—</span></td>'
            continue
        apr = row[e]['apr']
        raw = row[e]['raw']
        style = apr_color(apr)
        extra_cls = ""
        if e == best_e:
            extra_cls = " cell-best"
        elif e == worst_e:
            extra_cls = " cell-worst"
        html += (f'<td><div class="cell{extra_cls}" style="{style}">'
                 f'<div class="apr-val">{apr:+.2f}%</div>'
                 f'<div class="raw-val">{raw:+.4f}%</div>'
                 f'</div></td>')
    html += '</tr>'

html += '</tbody></table></div>'
st.markdown(html, unsafe_allow_html=True)

# ── Arbitrage Opportunities ──────────────────────────────────────────────────
st.markdown('<div style="margin-top: 32px;"></div>', unsafe_allow_html=True)
st.markdown(f'''<div class="hdr">
  <span class="hdr-left">Top Arbitrage Opportunities</span>
  <span class="hdr-meta">{selected_days}d APR · Min 0.5% net collection</span>
</div>''', unsafe_allow_html=True)

# Exchange filter checkboxes
st.markdown("**Filter exchanges:**")
n_ex = len(exchanges)
chk_cols = st.columns(min(n_ex, 8))
selected_exchanges = []
for i, e in enumerate(exchanges):
    with chk_cols[i % min(n_ex, 8)]:
        if st.checkbox(exchange_display(e), value=True, key=f"arb_ex_{e}"):
            selected_exchanges.append(e)

# Stocks Only filter
stock_symbols = set()
try:
    # Use the RPC function to get stock symbols
    response = db.client.rpc('get_stock_symbols').execute()
    if response.data:
        stock_symbols = {row['symbol'] for row in response.data}
except Exception:
    pass

stocks_only = st.checkbox("📈 Stocks Only", value=False, key="arb_stocks_only")

# OI filter — load from database
oi_data = []
oi_symbols = {}  # symbol -> rank
oi_stale = False
oi_available = False
try:
    oi_data = db.get_latest_oi_data(10)
    if oi_data:
        oi_available = True
        oi_symbols = db.get_oi_symbols_map()
        # Check staleness
        from datetime import datetime, timezone
        if oi_data[0].get("timestamp"):
            latest_ts = oi_data[0]["timestamp"]
            if isinstance(latest_ts, str):
                oi_ts = datetime.fromisoformat(latest_ts).replace(tzinfo=timezone.utc)
            else:
                oi_ts = latest_ts if latest_ts.tzinfo else latest_ts.replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - oi_ts).total_seconds() / 3600
            if age_h > 24:
                oi_stale = True
except Exception:
    pass

if oi_available:
    preview = ", ".join(item["symbol"] for item in oi_data[:3])
    more = len(oi_data) - 3
    label = f"💧 Top 10 OI Coins ({preview}... +{more} more)"
    top_oi_checked = st.checkbox(label, value=False, key="arb_top_oi",
                                  help="\n".join(
                                      f"{i+1}. {item['symbol']} — ${item['oi_usd']/1e9:.1f}B OI"
                                      for i, item in enumerate(oi_data)
                                  ) + (f"\n\n⚠️ Data is {age_h:.0f}h old" if oi_stale else ""))
    if oi_stale:
        st.caption(f"⚠️ OI data is {age_h:.0f} hours old — may not reflect current liquidity")
else:
    top_oi_checked = False
    st.caption("💧 OI data unavailable — run `python scripts/update_oi.py`")

# Combine filters
if stocks_only and top_oi_checked:
    st.info("No stocks in crypto OI rankings — showing empty results.")
    arb_source = []
elif stocks_only:
    arb_source = [(s, r) for s, r in matrix.items() if s in stock_symbols]
elif top_oi_checked:
    arb_source = [(s, r) for s, r in matrix.items() if s in oi_symbols]
else:
    arb_source = list(matrix.items())

# Calculate arbitrage opportunities
arb_rows = []
for symbol, row in arb_source:
    # Only exchanges that are selected AND have data for this symbol
    avail = [e for e in selected_exchanges if e in row]
    if len(avail) < 2:
        continue
    for e_a, e_b in combinations(avail, 2):
        rate_a = row[e_a]['apr']
        rate_b = row[e_b]['apr']
        diff = abs(rate_b - rate_a)
        if diff < 0.5:
            continue
        # Determine direction: long the lower rate, short the higher rate
        if rate_b > rate_a:
            long_ex, short_ex = e_a, e_b
            long_rate, short_rate = rate_a, rate_b
        else:
            long_ex, short_ex = e_b, e_a
            long_rate, short_rate = rate_b, rate_a
        net = short_rate - long_rate
        arb_rows.append({
            'symbol': symbol,
            'long_ex': long_ex,
            'short_ex': short_ex,
            'long_rate': long_rate,
            'short_rate': short_rate,
            'net': net,
        })

# Sort and take top 20
arb_rows.sort(key=lambda x: x['net'], reverse=True)
arb_rows = arb_rows[:20]

if arb_rows:
    def net_color(net: float) -> str:
        if net >= 5:
            return '#56d364'
        elif net >= 2:
            return '#2ea043'
        else:
            return '#1a7f37'

    arb_html = '<div class="tbl-wrap"><table class="arb-tbl"><thead><tr>'
    arb_html += '<th>#</th><th>Asset</th><th>Exchange 1</th><th>Rate 1 (APR)</th><th>Position</th>'
    arb_html += '<th>Exchange 2</th><th>Rate 2 (APR)</th><th>Position</th>'
    arb_html += '<th>Net Collection (%)</th><th>Action</th></tr></thead><tbody>'

    for i, r in enumerate(arb_rows, 1):
        nc = net_color(r['net'])
        long_name = exchange_display(r['long_ex'])
        short_name = exchange_display(r['short_ex'])
        oi_rank = oi_symbols.get(r["symbol"])
        badge = f' <span style="font-size:11px;color:#58a6ff;">💧 #{oi_rank}</span>' if oi_rank else ""
        arb_html += (
            f'<tr>'
            f'<td>{i}</td>'
            f'<td style="font-weight:600; color:#e6edf3;">{r["symbol"]}{badge}</td>'
            f'<td>{long_name}</td>'
            f'<td>{r["long_rate"]:+.2f}%</td>'
            f'<td><span class="arb-long">Long</span></td>'
            f'<td>{short_name}</td>'
            f'<td>{r["short_rate"]:+.2f}%</td>'
            f'<td><span class="arb-short">Short</span></td>'
            f'<td style="color:{nc}; font-weight:700;">{r["net"]:.2f}%</td>'
            f'<td><span class="arb-action">Long {long_name} / Short {short_name}</span></td>'
            f'</tr>'
        )

    arb_html += '</tbody></table></div>'
    st.markdown(arb_html, unsafe_allow_html=True)
else:
    st.info("No arbitrage opportunities above 0.5% net collection for the selected exchanges.")
