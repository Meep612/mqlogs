import os
import sqlite3
import threading
import time
import json
import queue
import logging
from flask import Flask, request, Response, render_template, jsonify
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mqlogs")

VERSION = "1.2.0"

# --- Config ---
MQTT_HOST      = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT      = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER      = os.environ.get("MQTT_USER", "")
MQTT_PASS      = os.environ.get("MQTT_PASS", "")
MQTT_TOPIC     = os.environ.get("MQTT_TOPIC", "#")
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "mqlogs")
DB_PATH        = os.environ.get("DB_PATH", "/data/mqlogs.db")
RETENTION_DAYS     = int(os.environ.get("RETENTION_DAYS", 14))
MAX_ROWS           = int(os.environ.get("MAX_ROWS", 1_000_000))
RETENTION_INTERVAL = int(os.environ.get("RETENTION_INTERVAL", 300))
WEB_PORT       = int(os.environ.get("WEB_PORT", 8080))
MAX_PAYLOAD    = int(os.environ.get("MAX_PAYLOAD", 8192))
UI_DEFAULT_FILTER = os.environ.get("UI_DEFAULT_FILTER", "")

app = Flask(__name__)

# SSE subscribers
_sse_queues: list[queue.Queue] = []
_sse_lock = threading.Lock()

# Write queue: MQTT callback never touches SQLite directly
_write_queue: queue.Queue = queue.Queue(maxsize=50_000)


# --- Database ---

def db_connect_read():
    """Read-only connection for Flask threads."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True,
                           check_same_thread=False, timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def db_connect_write():
    """Write connection — used only by the write worker thread."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint=500")
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            ts      REAL    NOT NULL,
            topic   TEXT    NOT NULL,
            payload TEXT    NOT NULL,
            qos     INTEGER NOT NULL DEFAULT 0,
            retain  INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts    ON messages(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic ON messages(topic)")
    conn.commit()
    conn.close()


# --- Write worker (single thread, owns the write connection) ---

def db_write_worker():
    conn = db_connect_write()
    while True:
        # Drain up to 100 items in one batch for efficiency
        batch = []
        try:
            batch.append(_write_queue.get(timeout=1))
        except queue.Empty:
            continue

        while len(batch) < 100:
            try:
                batch.append(_write_queue.get_nowait())
            except queue.Empty:
                break

        try:
            conn.executemany(
                "INSERT INTO messages(ts, topic, payload, qos, retain) VALUES(?,?,?,?,?)",
                batch
            )
            conn.commit()
        except sqlite3.OperationalError as e:
            log.error("DB write error: %s — reconnecting", e)
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(1)
            conn = db_connect_write()
            # Re-queue the batch so messages aren't lost
            for item in batch:
                try:
                    _write_queue.put_nowait(item)
                except queue.Full:
                    pass


# --- Retention ---

def retention_loop():
    while True:
        time.sleep(RETENTION_INTERVAL)
        try:
            conn = db_connect_write()
            cutoff = time.time() - RETENTION_DAYS * 86400

            # Delete by age
            conn.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
            conn.commit()

            # Delete oldest beyond MAX_ROWS — efficient: find cutoff id, one DELETE
            row = conn.execute(
                "SELECT id FROM messages ORDER BY id DESC LIMIT 1 OFFSET ?",
                (MAX_ROWS,)
            ).fetchone()
            if row:
                conn.execute("DELETE FROM messages WHERE id <= ?", (row["id"],))
                conn.commit()

            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            conn.close()
            log.info("Retention pass done")
        except Exception as e:
            log.error("Retention error: %s", e)


# --- MQTT ---

def decode_payload(raw: bytes) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.hex()
    if len(text) > MAX_PAYLOAD:
        text = text[:MAX_PAYLOAD] + " …[truncated]"
    return text

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("MQTT connected, subscribing to %s", MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC)
    else:
        log.warning("MQTT connect failed: %s", reason_code)

def on_message(client, userdata, msg):
    ts      = time.time()
    topic   = msg.topic
    payload = decode_payload(msg.payload)
    qos     = msg.qos
    retain  = int(msg.retain)

    # Queue the DB write — never blocks the MQTT callback
    try:
        _write_queue.put_nowait((ts, topic, payload, qos, retain))
    except queue.Full:
        log.warning("Write queue full, message dropped: %s", topic)

    # Broadcast to SSE subscribers immediately
    event = {"id": None, "ts": ts, "topic": topic, "payload": payload,
             "qos": qos, "retain": retain}
    with _sse_lock:
        for q in list(_sse_queues):
            try:
                q.put_nowait(event)
            except queue.Full:
                pass

def on_disconnect(client, userdata, flags, reason_code, properties):
    log.warning("MQTT disconnected: %s", reason_code)

def mqtt_loop():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        clean_session=True,
    )
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception as e:
            log.error("MQTT error: %s — retry in 10s", e)
            time.sleep(10)


# --- API ---

@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/api/version")
def api_version():
    return jsonify({"version": VERSION})

@app.route("/api/stats")
def api_stats():
    try:
        conn = db_connect_read()
        row = conn.execute(
            "SELECT COUNT(*) as total, MIN(ts) as oldest FROM messages"
        ).fetchone()
        conn.close()
        return jsonify({"total": row["total"], "oldest": row["oldest"]})
    except Exception as e:
        return jsonify({"total": 0, "oldest": None, "error": str(e)})

@app.route("/api/search")
def api_search():
    q         = request.args.get("q", "").strip()
    topic     = request.args.get("topic", "").strip()
    since     = request.args.get("since", type=float)
    until     = request.args.get("until", type=float)
    limit     = min(int(request.args.get("limit", 200)), 500)
    before_id = request.args.get("before_id", type=int)

    where, params = [], []
    if q:
        where.append("(topic LIKE ? OR payload LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    if topic:
        where.append("topic LIKE ?")
        params.append(f"%{topic}%")
    if since:
        where.append("ts >= ?")
        params.append(since)
    if until:
        where.append("ts <= ?")
        params.append(until)
    if before_id:
        where.append("id < ?")
        params.append(before_id)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"SELECT * FROM messages {clause} ORDER BY id DESC LIMIT ?"
    params.append(limit)

    try:
        conn = db_connect_read()
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify([])

@app.route("/api/stream")
def api_stream():
    q_filter = request.args.get("q", "").strip().lower()

    my_queue: queue.Queue = queue.Queue(maxsize=500)
    with _sse_lock:
        _sse_queues.append(my_queue)

    def generate():
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                try:
                    event = my_queue.get(timeout=20)
                    if q_filter and q_filter not in event["topic"].lower() \
                                and q_filter not in event["payload"].lower():
                        continue
                    yield f"data: {json.dumps(event)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sse_lock:
                try:
                    _sse_queues.remove(my_queue)
                except ValueError:
                    pass

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/")
def index():
    return render_template("index.html", default_filter=UI_DEFAULT_FILTER, version=VERSION)


# --- Boot ---

if __name__ == "__main__":
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    db_init()

    threading.Thread(target=db_write_worker, daemon=True).start()
    threading.Thread(target=retention_loop,  daemon=True).start()
    threading.Thread(target=mqtt_loop,       daemon=True).start()

    log.info("mqlogs v%s starting on port %d", VERSION, WEB_PORT)

    from waitress import serve
    serve(app, host="0.0.0.0", port=WEB_PORT, threads=8)
