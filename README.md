# mqlogs

Lightweight MQTT event logger with a live web UI and full-text search.  
One container. No external database. Works on ARM (iHost, Raspberry Pi) and amd64.

## What it does

- Subscribes to a MQTT broker and stores every message in a local SQLite database
- Serves a dark web UI with a live event stream (SSE) and keyword search over history
- Applies configurable retention (age + max row count) to protect limited storage

## Quick start

```yaml
# docker-compose.yml
services:
  mqlogs:
    image: ghcr.io/meep612/mqlogs:latest
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - mqlogs-data:/data
    environment:
      MQTT_HOST: 192.168.1.1   # your broker
      MQTT_TOPIC: "#"

volumes:
  mqlogs-data:
```

```bash
docker compose up -d
# open http://<host>:8080
```

## Environment variables

| Variable             | Default         | Description                                      |
|----------------------|-----------------|--------------------------------------------------|
| `MQTT_HOST`          | `localhost`     | MQTT broker hostname or IP                       |
| `MQTT_PORT`          | `1883`          | MQTT broker port                                 |
| `MQTT_USER`          | *(empty)*       | MQTT username (optional)                         |
| `MQTT_PASS`          | *(empty)*       | MQTT password (optional)                         |
| `MQTT_TOPIC`         | `#`             | Topic to subscribe to                            |
| `MQTT_CLIENT_ID`     | `mqlogs`        | MQTT client identifier                           |
| `DB_PATH`            | `/data/mqlogs.db` | SQLite database path                           |
| `RETENTION_DAYS`     | `14`            | Delete messages older than N days                |
| `MAX_ROWS`           | `1000000`       | Keep only the N most recent rows                 |
| `RETENTION_INTERVAL` | `300`           | Retention cleanup interval in seconds            |
| `WEB_PORT`           | `8080`          | Web server port                                  |
| `MAX_PAYLOAD`        | `8192`          | Truncate payloads larger than N bytes            |

## API

| Endpoint | Description |
|---|---|
| `GET /` | Web UI |
| `GET /api/search?q=&topic=&since=&until=&limit=&before_id=` | Search history (keyset pagination) |
| `GET /api/stream?q=&topic=` | Live SSE stream, filterable |
| `GET /api/stats` | Total count and oldest message timestamp |
| `GET /healthz` | Health check |

## Multi-arch build

The image is built for `linux/amd64`, `linux/arm64`, and `linux/arm/v7` via GitHub Actions on every push to `main`.

## License

MIT
