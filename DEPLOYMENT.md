# Deployment Guide: Funding Dashboard

This guide walks you through deploying the Funding Dashboard to the cloud using Supabase PostgreSQL and Streamlit Cloud.

## Overview

The dashboard uses Supabase's REST API for all database operations with these components:
- **Frontend**: Streamlit app hosted on Streamlit Cloud
- **Database**: Supabase PostgreSQL (accessed via REST API only)
- **Updates**: GitHub Actions for automated data collection
- **OI Data**: Stored in PostgreSQL instead of JSON files

## Prerequisites

- GitHub account
- Supabase account
- Streamlit Cloud account (free)
- Local development environment with Python 3.11+

## Step 1: Set up Supabase Database

### 1.1 Create Supabase Project
1. Go to [supabase.com](https://supabase.com) and sign up/log in
2. Click "New Project"
3. Choose organization and fill in project details:
   - **Name**: `funding-dashboard` (or your preferred name)
   - **Database Password**: Generate a strong password (save it!)
   - **Region**: Choose closest to your users
4. Wait for project to be created (~2 minutes)

### 1.2 Get Supabase API Credentials
1. In your Supabase project, go to **Settings** → **API**
2. Copy these two values:
   - **Project URL**: `https://your-project.supabase.co`
   - **Anon/Public Key**: `eyJhbGciOiJIUzI1NiIsInR5cCI6...` (long string)
3. Save these - you'll need them for all configurations

### 1.3 Set up Database Schema
The application uses REST API exclusively, so you must create tables manually:

1. Go to **SQL Editor** in your Supabase dashboard
2. Copy the entire content of `db/setup.sql` from this repository
3. Paste it into the SQL Editor
4. Click **Run** to execute
5. Verify you see "✅ Database setup completed successfully!" message

This creates:
- Tables: `funding_rates`, `fetch_log`, `oi_data`
- Performance indexes
- RPC functions for complex queries
- Proper constraints and relationships

## Step 2: Migrate Your Existing Data (Optional)

If you have existing SQLite data to migrate:

### 2.1 Set Environment Variables
```bash
# In your project directory
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-anon-key-here"
```

### 2.2 Run Migration Script
```bash
python scripts/migrate_to_supabase.py
```

**Important**: Make sure you've run `db/setup.sql` in Supabase first!

**Expected output:**
```
✅ Connected to Supabase
📊 SQLite contains:
   - 150,000 funding_rates records
   - 5,000 fetch_log records
🚀 Starting migration...
✅ Migration complete!
```

## Step 3: Set up GitHub Repository

### 3.1 Create GitHub Repository
1. Go to GitHub and create a new repository (public or private)
2. Push your code:
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Supabase REST API migration"
   git branch -M main
   git remote add origin https://github.com/yourusername/funding-dashboard.git
   git push -u origin main
   ```

### 3.2 Add Repository Secrets
1. In your GitHub repo, go to **Settings** → **Secrets and Variables** → **Actions**
2. Click **New repository secret**
3. Add these secrets:
   - **Name**: `SUPABASE_URL` | **Value**: `https://your-project.supabase.co`
   - **Name**: `SUPABASE_KEY` | **Value**: Your anon/public key

## Step 4: Configure GitHub Actions

The workflows are already updated in `.github/workflows/`:
- `update_funding.yml`: Updates funding rates every hour
- `update_oi.yml`: Updates open interest data every 4 hours

### 4.1 Enable Actions
1. Go to your repo's **Actions** tab
2. Enable workflows if prompted
3. The first run will happen according to the schedule, or you can trigger manually

### 4.2 Test Manual Run
1. Go to **Actions** tab
2. Click on "Update Funding Rates"
3. Click "Run workflow" to test

## Step 5: Deploy to Streamlit Cloud

### 5.1 Connect Repository
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click "Deploy an app"
3. Connect your GitHub account
4. Select your repository and main branch
5. Set main file path: `app.py`

### 5.2 Configure Secrets
1. In the deployment settings, click **Advanced settings**
2. Add your secrets in the **Secrets** section:
   ```toml
   [supabase]
   url = "https://your-project.supabase.co"
   key = "your-anon-key-here"
   ```
3. Click **Deploy**

### 5.3 Custom Domain (Optional)
Once deployed, you can:
1. Go to app settings
2. Add a custom domain
3. Set up CNAME records

## Step 6: Verify Everything Works

### 6.1 Check Database Connection
1. Visit your deployed app
2. You should see data loading without errors
3. Check the "Exchange details" section for recent updates

### 6.2 Verify GitHub Actions
1. Check **Actions** tab in GitHub
2. Both workflows should be running successfully
3. Check workflow logs for any errors

### 6.3 Monitor Updates
- Funding rates update hourly
- OI data updates every 4 hours
- Check logs in GitHub Actions for any issues

## Troubleshooting

### Database Connection Issues
```python
# Test connection locally
python -c "
import os
from db.database import Database
os.environ['SUPABASE_URL'] = 'https://your-project.supabase.co'
os.environ['SUPABASE_KEY'] = 'your-anon-key'
db = Database()
print('Connected successfully!')
print(f'Total records: {db.get_total_records()}')
"
```

### GitHub Actions Failing
1. Check the **Actions** tab for error logs
2. Common issues:
   - `SUPABASE_URL` or `SUPABASE_KEY` secrets not set
   - Incorrect Supabase credentials
   - Supabase project paused (free tier limitation)
   - Database schema not set up (run `db/setup.sql`)

### Streamlit App Not Loading Data
1. Check Streamlit logs for errors
2. Verify `SUPABASE_URL` and `SUPABASE_KEY` in secrets
3. Make sure Supabase project is not paused
4. Verify database schema is created (check tables in Supabase dashboard)

### OI Data Not Showing
The OI data now comes from the database. If it's not showing:
1. Run the OI update manually: `python scripts/update_oi.py`
2. Check if the GitHub Actions workflow is working
3. Verify the `oi_data` table has recent data in Supabase

### RPC Function Errors
If you get "function does not exist" errors:
1. Go to Supabase SQL Editor
2. Re-run the `db/setup.sql` script
3. Check the Functions section in Supabase dashboard to verify they exist

## Performance Tips

### 1. Database Optimization
- Supabase free tier: 500MB storage, 2GB data transfer/month
- Consider upgrading if you have large datasets
- Use RPC functions for complex queries (already implemented)
- Regular data cleanup for old records if needed

### 2. Streamlit Cloud
- Free tier: unlimited public apps
- Apps go to sleep after inactivity
- Consider caching for better performance

### 3. GitHub Actions
- Free tier: 2,000 minutes/month
- Each workflow run takes ~2-5 minutes
- Monitor usage in GitHub Settings

## Architecture Notes

### REST API Only
This deployment uses **Supabase REST API exclusively**:
- ✅ No direct PostgreSQL connections needed
- ✅ Works with just `SUPABASE_URL` and `SUPABASE_KEY`
- ✅ All complex queries use PostgreSQL functions via `.rpc()`
- ✅ Simple CRUD operations use table API (`.select()`, `.insert()`, `.upsert()`)
- ✅ Automatic batching for large datasets (500 records per batch)

### Database Schema Management
- Tables and functions created via SQL Editor (one-time setup)
- No runtime schema management
- Uses PostgreSQL functions for aggregations and complex queries
- Proper indexes for performance optimization

## Support

### Supabase Issues
- [Supabase Documentation](https://supabase.com/docs)
- [Supabase Support](https://supabase.com/support)

### Streamlit Issues
- [Streamlit Documentation](https://docs.streamlit.io)
- [Streamlit Community Forum](https://discuss.streamlit.io)

### Application Issues
- Check GitHub Issues in your repository
- Review application logs in Streamlit Cloud
- Monitor GitHub Actions for data update issues

## Security Notes

1. **Never commit secrets** to your repository
2. Use environment variables or Streamlit secrets
3. Use anon/public key (not service role key) for security
4. Monitor Supabase usage and access logs
5. Consider enabling RLS (Row Level Security) for production

## Cost Considerations

### Free Tier Limits
- **Supabase**: 500MB storage, 2GB bandwidth/month
- **Streamlit Cloud**: Unlimited public apps
- **GitHub Actions**: 2,000 minutes/month

### Scaling Up
When you outgrow free tiers:
- Supabase Pro: $25/month for 8GB storage
- GitHub Pro: $4/month for private repos + more Actions minutes
- Consider optimizing data retention policies

---

🎉 **Congratulations!** Your funding dashboard is now running in the cloud with Supabase REST API and automated updates!