# RUN_COMPOSE.md

Huong dan chay lai stack Lab 05 tren may moi.

## 1. Chuan bi

```bash
git clone <repo-url>
cd fit4110-lab5-nhom_6
npm install
cp .env.example .env
```

Neu dung PowerShell:

```powershell
Copy-Item .env.example .env
```

## 2. Build va chay Compose

```bash
docker compose up -d --build --wait
docker compose ps
```

Stack se khoi dong 4 service:

- `fit4110-api-lab05` tren port `8000` cho IoT
- `fit4110-notification-lab05` tren port `8002` cho Notification
- `fit4110-ai-lab05` tren port `9000`
- `fit4110-db-lab05` tren port `5432`

## 3. Kiem tra readiness

```bash
curl http://localhost:8000/health
curl http://localhost:8002/health
curl http://localhost:9000/health
docker compose exec -T db pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

## 4. Chay Newman test

```bash
mkdir -p reports
npm run test:compose
```

Report duoc tao tai:

```text
reports/newman-lab05-compose.xml
reports/newman-lab05-compose.html
```

## 5. Theo doi log va dung stack

```bash
docker compose logs -f
docker compose down -v
```
