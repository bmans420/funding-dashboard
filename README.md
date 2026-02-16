# 📊 Funding Rates Dashboard

A comprehensive multi-exchange cryptocurrency funding rates comparison dashboard with cloud deployment and automated data collection.

## ✨ Features

- **Multi-exchange support**: Binance, Hyperliquid, Bybit, OKX, Bitget, and more
- **Real-time data**: Automated hourly funding rate updates
- **Arbitrage opportunities**: Identify profitable funding rate spreads
- **Open Interest integration**: Top 10 OI coins for liquidity insights
- **Cloud-ready**: Deployed on Streamlit Cloud with PostgreSQL backend
- **Dark theme**: Professional GitHub-inspired design

## 🚀 Quick Start (Cloud Deployment)

The easiest way to get started is with our cloud deployment:

### 1. Set up Database (Supabase)
1. Create a [Supabase](https://supabase.com) account and new project
2. Get your PostgreSQL connection string from project settings

### 2. Migrate Existing Data (if you have SQLite data)
```bash
export DATABASE_URL="your-supabase-connection-string"
python scripts/migrate_to_supabase.py
```

### 3. Deploy to Streamlit Cloud
1. Fork this repository to your GitHub account
2. Connect to [Streamlit Cloud](https://share.streamlit.io)
3. Add your database URL in app secrets
4. Deploy! 🎉

📖 **Full deployment guide**: See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions.

## 🛠 Local Development

### Requirements
- Python 3.11+
- PostgreSQL database (or SQLite for development)

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/funding-dashboard.git
cd funding-dashboard

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your database URL

# Initialize database (automatic on first run)
python -c "from db.database import Database; Database()"
```

### Bootstrap Data (First Time)
```bash
# Collect 30 days of historical data for major symbols
python scripts/bootstrap.py --days 30 --symbols BTC ETH SOL --discover

# Or collect data for all discovered symbols
python scripts/bootstrap.py --days 7 --discover
```

### Run Dashboard
```bash
streamlit run app.py
```
Visit http://localhost:8501

## 📈 Data Updates

### Automated (Cloud Deployment)
- **Funding rates**: Every hour via GitHub Actions
- **Open Interest**: Every 4 hours via GitHub Actions

### Manual
```bash
# Update funding rates
python scripts/update.py

# Update open interest data
python scripts/update_oi.py
```

## 🏢 Supported Exchanges

| Exchange | Funding Interval | Asset Types |
|----------|------------------|-------------|
| Binance | 8 hours | Crypto perpetuals |
| Hyperliquid | 1 hour | Crypto + Stocks |
| Bybit | 8 hours | Crypto perpetuals |
| OKX | 8 hours | Crypto perpetuals |
| Bitget | 8 hours | Crypto perpetuals |
| Lighter | 1 hour | Hyperliquid-based |

**Special Features:**
- **Hyperliquid Stocks**: Trade funding rates on AAPL, TSLA, etc.
- **HIP3 Protocol**: Automated detection of new Hyperliquid deployers
- **Dynamic intervals**: Automatically adapts to exchange-specific funding cycles

## ⚙️ Configuration

### Database Connection
Set via environment variable or Streamlit secrets:
```bash
# Environment variable
export DATABASE_URL="postgresql://user:pass@host:port/db"

# Or Streamlit secrets (.streamlit/secrets.toml)
[database]
url = "postgresql://user:pass@host:port/db"
```

### Exchange Settings
Edit `config.yaml` to enable/disable exchanges:
```yaml
exchanges:
  binance:
    enabled: true
    funding_interval_hours: 8
  hyperliquid:
    enabled: true
    funding_interval_hours: 1
```

## 🔧 Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GitHub Actions│───▶│  PostgreSQL DB  │◀───│  Streamlit App  │
│   (Data Updates)│    │   (Supabase)    │    │  (Frontend)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
  ┌──────────────┐    ┌──────────────────┐    ┌─────────────────┐
  │ Exchange APIs│    │ Funding Rates +  │    │ Users worldwide │
  │ (8 different)│    │ Open Interest    │    │                 │
  └──────────────┘    └──────────────────┘    └─────────────────┘
```

### Key Components
- **`app.py`**: Main Streamlit dashboard
- **`db/database.py`**: PostgreSQL operations
- **`scripts/update.py`**: Automated funding rate collector
- **`scripts/update_oi.py`**: Open interest data collector
- **`exchanges/`**: Exchange-specific API adapters

## 🐛 Troubleshooting

### Common Issues

**Database connection failed**
```bash
# Test connection
python -c "from db.database import Database; Database().get_total_records()"
```

**No data showing**
```bash
# Check if bootstrap was run
python -c "from db.database import Database; print(f'Records: {Database().get_total_records()}')"
```

**GitHub Actions failing**
- Check repository secrets contain `DATABASE_URL`
- Verify Supabase project is not paused
- Check Actions logs for specific errors

### Getting Help
1. Check [DEPLOYMENT.md](DEPLOYMENT.md) for detailed setup
2. Review GitHub Issues for common problems
3. Check exchange API documentation for rate limits

## 📊 Data Sources

- **Funding Rates**: Direct from exchange APIs
- **Open Interest**: Binance Futures API
- **Price Data**: Real-time from each exchange
- **Historical Data**: Backfilled from exchange APIs

## 🔒 Security & Privacy

- No API keys stored (read-only public endpoints)
- Database credentials via environment variables
- No user data collection
- Open source and auditable

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add exchange adapters in `exchanges/`
4. Test with local development setup
5. Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🎯 Roadmap

- [ ] More exchanges (dYdX, GMX, etc.)
- [ ] Historical arbitrage tracking
- [ ] Telegram/Discord notifications
- [ ] Mobile-responsive improvements
- [ ] Advanced filtering and sorting
- [ ] Export functionality for data analysis

---

**⭐ Star this repo** if you find it useful for your crypto trading!
