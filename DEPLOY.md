# Scalable Deployment Guide

## Overview
This guide helps you deploy the Privacy Chat app with Redis (shared state) and PostgreSQL (persistent messages) for high scalability.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Static Site    │────▶│  WebSocket      │────▶│     Redis       │
│  (Frontend)     │     │  Backend        │     │  (State)        │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  PostgreSQL     │
                       │  (Messages)     │
                       └─────────────────┘
```

## Prerequisites

1. **Render Account**: Sign up at https://render.com
2. **GitHub Repository**: Your code should be pushed to GitHub
3. **Credit Card**: Required on Render for paid plans (Starter plan recommended)

## Deployment Steps

### Option A: Blueprint Deploy (Recommended)

The `render.yaml` file automatically provisions all services:

1. **Push code to GitHub**:
   ```bash
   git add .
   git commit -m "Add scalable architecture with Redis and PostgreSQL"
   git push origin main
   ```

2. **Go to Render Dashboard** → Click "Blueprints" in left sidebar

3. **Click "New Blueprint Instance"**

4. **Select your GitHub repository**

5. **Click "Apply"** — Render will automatically create:
   - Redis instance (shared state)
   - PostgreSQL database (persistent messages)
   - WebSocket backend service
   - Static frontend site

6. **Wait for all services to deploy** (~5-10 minutes)

### Option B: Manual Deploy (Alternative)

If Blueprint doesn't work, create services manually:

#### Step 1: Create Redis
1. Dashboard → "New" → "Redis"
2. Name: `chat-redis`
3. Region: Singapore
4. Plan: Starter
5. Click "Create"

#### Step 2: Create PostgreSQL
1. Dashboard → "New" → "PostgreSQL"
2. Name: `chat-postgres`
3. Region: Singapore  
4. Plan: Starter
5. Click "Create"

#### Step 3: Create WebSocket Backend
1. Dashboard → "New" → "Web Service"
2. Connect your GitHub repo
3. Settings:
   - **Name**: `privacy-chat-backend`
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python server.py`
   - **Region**: Singapore
   - **Plan**: Starter (or higher for production)
4. Environment Variables:
   - `REDIS_URL`: Copy from your Redis dashboard
   - `DATABASE_URL`: Copy from your PostgreSQL dashboard
5. Click "Deploy"

#### Step 4: Create Static Site
1. Dashboard → "New" → "Static Site"
2. Connect same GitHub repo
3. Settings:
   - **Name**: `privacy-chat-frontend`
   - **Publish Directory**: `.`
   - **Region**: Singapore
   - **Plan**: Starter
4. Environment Variable:
   - Key: `WS_URL`
   - Value: `wss://privacy-chat-backend-XXXX.onrender.com` (copy from your backend URL)
5. Click "Deploy"

## Post-Deployment

### Access Your App
- **Frontend URL**: `https://privacy-chat-frontend-XXXX.onrender.com`
- **Backend URL**: `wss://privacy-chat-backend-XXXX.onrender.com`

### Scale Your App

On Render Dashboard:
1. Go to your **WebSocket Backend** service
2. Click "Settings" → "Scaling"
3. Increase number of instances (each gets load-balanced)
4. With Redis, all instances share state seamlessly

### Monitor
- **Logs**: Each service has a "Logs" tab
- **Metrics**: Available on paid plans
- **Health**: Check service status on dashboard

## Architecture Benefits

| Feature | Before | After |
|---------|--------|-------|
| **State** | In-memory (lost on restart) | Redis (persistent) |
| **Messages** | Lost on server restart | PostgreSQL (persistent) |
| **Scale** | Single server | Multiple instances |
| **Users** | Max ~1000 concurrent | Thousands+ with scaling |
| **Recovery** | None | Automatic failover |

## Environment Variables Reference

| Variable | Source | Purpose |
|----------|--------|---------|
| `PORT` | Auto-set by Render | Server listen port |
| `REDIS_URL` | Redis service | Shared state storage |
| `DATABASE_URL` | PostgreSQL service | Persistent messages |
| `WS_URL` | Backend service | Frontend connects here |
| `RENDER_INSTANCE_ID` | Auto-set | Instance identification |

## Troubleshooting

### Connection Issues
- Check `WS_URL` matches backend URL exactly
- Ensure WebSocket uses `wss://` (secure), not `ws://`

### Redis Connection Failures
- Verify `REDIS_URL` environment variable is set
- Check Redis service is "Available" (not "Creating")

### Database Errors
- Verify `DATABASE_URL` is correct
- Check PostgreSQL status on dashboard

### Scaling Problems
- Ensure plan supports multiple instances (Starter+)
- Check Redis memory usage (upgrade if >80%)

## Cost Estimate (Singapore Region)

| Component | Plan | Monthly Cost |
|-----------|------|--------------|
| Static Site | Starter | $0 (free tier available) |
| WebSocket Backend | Starter | $7 |
| Redis | Starter | $12 |
| PostgreSQL | Starter | $15 |
| **Total** | | **~$34/month** |

For production with 2 backend instances: ~$41/month

## Local Development

To test locally with Redis/PostgreSQL:

```bash
# 1. Start Redis locally
redis-server

# 2. Start PostgreSQL locally (or use Docker)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15

# 3. Set environment variables
export REDIS_URL=redis://localhost:6379
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres

# 4. Run server
python server.py
```

## Security Notes

- All connections use TLS/SSL on Render
- PostgreSQL connections are encrypted
- Redis connections use TLS on paid plans
- End-to-end encryption still applies to messages
- No changes needed to frontend crypto logic

## Next Steps

1. **Add monitoring**: Integrate Sentry or DataDog
2. **Rate limiting**: Add Redis-based rate limiting
3. **Message retention**: Add cleanup job for old messages
4. **CDN**: Use Cloudflare in front of static site
5. **Backups**: PostgreSQL auto-backups on paid plans
