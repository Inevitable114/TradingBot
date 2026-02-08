# VPS Deployment Runbook

## 1. Server Prerequisites
- **OS**: Ubuntu 22.04 LTS (Recommended)
- **RAM**: 2GB Minimum
- **Disk**: 20GB+ SSD

## 2. Initial Setup
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker & Compose
sudo apt install docker.io docker-compose -y
sudo systemctl enable --now docker
```

## 3. Security (UFW)
```bash
# Allow SSH
sudo ufw allow 22
# Allow Database (Warning: Only allow trusted IPs in production!)
# sudo ufw allow 5432 
sudo ufw enable
```

## 4. Application Deployment
1. **Transfer Files**:
   Use `scp` or `git` to copy the project to `/opt/signal-engine`.

2. **Configure Environment**:
   ```bash
   cd /opt/signal-engine
   cp .env.example .env
   nano .env
   # Set POSTGRES_PASSWORD, TELEGRAM_BOT_TOKEN
   ```

3. **Start Service**:
   ```bash
   docker-compose up --build -d
   ```

4. **Verify Running**:
   ```bash
   docker-compose ps
   # Status should be "Up" for all 3 containers (app, db, redis)
   ```

## 5. Maintenance & Monitoring

### Check Logs
```bash
docker-compose logs -f --tail=100 app
```

### Disk Usage
The application manages its own snapshots (limited to 200MB).
To clean up Docker artifacts:
```bash
docker system prune -a --volumes
```

### Database Backup
```bash
docker exec -t signal_engine_db pg_dumpall -c -U admin > dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql
```

## 6. Recovery Procedures

### App Crash
Docker is configured with `restart: unless-stopped`. It should auto-recover.
If loop crash:
```bash
docker-compose restart app
```

### Redis/DB Corruption
If data is corrupted:
1. Stop services: `docker-compose down`
2. Remove volumes: `docker volume rm eth-futures-signal-engine_redis_data`
3. Restart: `docker-compose up -d`
