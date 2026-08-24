"""
Kafka -> Face Recognition -> Attendance DB (SERVER side).

Run this on your central processing server, which can be on a completely
different network from the cameras. It consumes frames published by
edge_client.py (running on the camera's network) via Kafka, runs your
existing InsightFace recognition pipeline, and writes attendance to
Postgres - same logic as the original single-camera script, just fed by
Kafka instead of a direct RTSP connection.

SETUP:
  1. pip install -r requirements-server.txt
  2. Install Tailscale on this machine: https://tailscale.com/download
     then run: sudo tailscale up  (and run `tailscale ip -4` to get this
     server's IP for the edge_client.py / docker-compose.yml config)
  3. Make sure Kafka is running (see docker-compose.yml)
  4. Edit DATABASE_URL / DATASET_PATH below (or set as env vars) to match
     your setup.
  5. Run: python server_consumer.py
"""

import os
import pickle
import logging
import threading
import time
from datetime import datetime, time as dtime

import numpy as np
import cv2
from insightface.app import FaceAnalysis
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer

from fastapi import FastAPI
import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("attendance_server")

# =========================
# CONFIG (env-driven, same style as the original script)
# =========================

DATABASE_URL = os.environ.get(
    "ATTENDANCE_DB_URL",
    "postgresql+psycopg2://postgres:ankan@localhost:5432/AttendanceSystemIEM",
)

DATASET_PATH = os.environ.get("DATASET_PATH", "/home/iedc-cse/dataset/dataset")
CACHE_FILE = os.environ.get("EMBEDDING_CACHE", "face_embeddings.pkl")

THRESHOLD = float(os.environ.get("FACE_THRESHOLD", "0.35"))
DET_SIZE = tuple(int(x) for x in os.environ.get("DET_SIZE", "1280,1280").split(","))
DET_THRESH = float(os.environ.get("DET_THRESH", "0.35"))

# Kafka - this server is the consumer. In a single-node setup this is
# just "localhost:9092" since the broker runs on the same machine.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "camera.frames")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "attendance-processor")

# Show a live debug window per camera (only works if this server has a
# display attached - leave False on a headless server).
SHOW_WINDOWS = os.environ.get("SHOW_WINDOWS", "false").lower() == "true"

HEALTH_API_HOST = os.environ.get("HEALTH_API_HOST", "0.0.0.0")
HEALTH_API_PORT = int(os.environ.get("HEALTH_API_PORT", "8000"))

PERIOD_SCHEDULE = {
    "p1":  ("09:30", "10:20"),
    "p2":  ("10:20", "11:10"),
    "p3":  ("11:10", "12:00"),
    "p4":  ("12:00", "12:50"),
    "p5":  ("13:40", "14:30"),
    "p6":  ("14:30", "15:20"),
    "p7":  ("15:20", "16:10"),
    "p8":  ("16:10", "17:00"),
    "p9":  ("17:00", "17:50"),
    "p10": ("17:50", "18:40"),
}

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=5)


# =========================
# MODEL LOADING (unchanged from original)
# =========================

def load_face_model() -> FaceAnalysis:
    for providers in (["CUDAExecutionProvider", "CPUExecutionProvider"], ["CPUExecutionProvider"]):
        try:
            log.info("Loading InsightFace with providers=%s", providers)
            app = FaceAnalysis(name="buffalo_l", providers=providers)
            ctx_id = 0 if providers[0] == "CUDAExecutionProvider" else -1
            app.prepare(ctx_id=ctx_id, det_size=DET_SIZE, det_thresh=DET_THRESH)
            log.info("Model loaded, det_size=%s det_thresh=%s", DET_SIZE, DET_THRESH)
            return app
        except Exception as e:
            log.warning("Failed to load with %s: %s", providers[0], e)
    raise RuntimeError("Could not load InsightFace with any provider")


face_app = load_face_model()


def normalize(v: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(v)
    return v / norm if norm > 0 else v


def current_period(now: datetime | None = None) -> str:
    now = now or datetime.now()
    t = now.time()
    for label, (start_str, end_str) in PERIOD_SCHEDULE.items():
        start = dtime.fromisoformat(start_str)
        end = dtime.fromisoformat(end_str)
        if start <= t < end:
            return label.upper()
    return "OUT_OF_PERIOD"


# =========================
# FACE DATABASE (unchanged from original)
# =========================

class FaceDatabase:
    def __init__(self):
        self.names: list[str] = []
        self.matrix: np.ndarray | None = None

    def build(self):
        raw = self._load_or_create_raw()
        all_embs, all_names = [], []
        for name, embs in raw.items():
            for e in embs:
                all_embs.append(e)
                all_names.append(name)

        if not all_embs:
            log.warning("No embeddings found - recognition will always return UNKNOWN")
            self.matrix = np.zeros((0, 512), dtype=np.float32)
            self.names = []
            return

        self.matrix = np.vstack(all_embs).astype(np.float32)
        self.names = all_names
        log.info("Loaded %d embeddings across %d identities", len(all_names), len(raw))

    def _load_or_create_raw(self) -> dict:
        if os.path.exists(CACHE_FILE):
            log.info("Loading embeddings cache: %s", CACHE_FILE)
            with open(CACHE_FILE, "rb") as f:
                return pickle.load(f)

        log.info("Building embeddings from %s ...", DATASET_PATH)
        database: dict[str, list[np.ndarray]] = {}

        for person in sorted(os.listdir(DATASET_PATH)):
            person_path = os.path.join(DATASET_PATH, person)
            if not os.path.isdir(person_path):
                continue
            embeddings = []
            for img_name in os.listdir(person_path):
                img_path = os.path.join(person_path, img_name)
                img = cv2.imread(img_path)
                if img is None:
                    continue
                for face in face_app.get(img):
                    embeddings.append(normalize(face.embedding))
            if embeddings:
                database[person] = embeddings
                log.info("  %s: %d embeddings", person, len(embeddings))

        with open(CACHE_FILE, "wb") as f:
            pickle.dump(database, f)
        return database

    def recognize(self, embedding: np.ndarray) -> tuple[str, float]:
        if self.matrix is None or self.matrix.shape[0] == 0:
            return "UNKNOWN", 0.0
        scores = self.matrix @ embedding
        best_idx = int(np.argmax(scores))
        best_score = float(scores[best_idx])
        if best_score < THRESHOLD:
            return "UNKNOWN", best_score
        return self.names[best_idx], best_score


face_db = FaceDatabase()
face_db.build()


# =========================
# STUDENT CACHE (unchanged from original)
# =========================

class StudentCache:
    def __init__(self):
        self._cache: dict[str, tuple] = {}
        self._load()

    def _load(self):
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name, enrollment_number, department, section FROM students")
            ).fetchall()
        for row in rows:
            sid, name, enroll, dept, section = row
            self._cache[str(enroll).strip()] = (sid, name, dept, section)
        log.info("Cached %d student records", len(self._cache))

    def get(self, label_name: str):
        enroll = label_name.rsplit("_", 1)[-1].strip()
        record = self._cache.get(enroll)
        if record is None:
            log.warning("Student not found in cache: label=%s parsed_enrollment=%s", label_name, enroll)
        return record


student_cache = StudentCache()


# =========================
# ATTENDANCE WRITER (unchanged from original, camera_id added to log line)
# =========================

already_marked: set[tuple] = set()
_lock = threading.Lock()


def save_attendance(name: str, score: float, camera_id: str):
    with _lock:
        today = datetime.now().date()
        period = current_period()
        mark_key = (name, today, period)

        if mark_key in already_marked:
            return

        student = student_cache.get(name)
        if not student:
            return

        student_id, db_name, dept, section = student
        enroll = name.rsplit("_", 1)[-1].strip()

        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO attendance
                        (name, enrollment_number, department,
                         section, attendance_date, period)
                        VALUES
                        (:name, :enroll, :dept, :section,
                         :date, :period)
                    """),
                    {
                        "name": db_name,
                        "enroll": enroll,
                        "dept": dept,
                        "section": section,
                        "date": today,
                        "period": period,
                    },
                )
        except Exception as e:
            log.error("Failed to save attendance for %s: %s", name, e)
            return

        already_marked.add(mark_key)
        log.info("ATTENDANCE SAVED: %s (%.3f) period=%s camera=%s", name, score, period, camera_id)


# =========================
# KAFKA CONSUMER LOOP
# =========================

# Track basic per-camera stats for the /status endpoint
camera_stats = {}
stats_lock = threading.Lock()


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",   # only process new frames, don't replay old backlog on startup
        enable_auto_commit=True,
        max_poll_records=50,
        fetch_max_wait_ms=500,
    )


def consume_loop():
    consumer = make_consumer()
    log.info("Consuming from topic '%s' on %s (group=%s)",
              KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)

    for msg in consumer:
        camera_id = msg.key.decode("utf-8") if msg.key else "unknown"
        frame = cv2.imdecode(np.frombuffer(msg.value, dtype=np.uint8), cv2.IMREAD_COLOR)
        if frame is None:
            continue

        with stats_lock:
            s = camera_stats.setdefault(camera_id, {"frames": 0, "last_seen": None})
            s["frames"] += 1
            s["last_seen"] = time.time()

        faces = face_app.get(frame)
        for face in faces:
            emb = normalize(face.embedding)
            name, score = face_db.recognize(emb)

            if SHOW_WINDOWS:
                x1, y1, x2, y2 = face.bbox.astype(int)
                color = (0, 255, 0) if name != "UNKNOWN" else (0, 0, 255)
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                cv2.putText(frame, f"{name} {score:.2f}", (x1, y1 - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)

            if name != "UNKNOWN" and score > THRESHOLD:
                save_attendance(name, score, camera_id)

        if SHOW_WINDOWS:
            cv2.imshow(f"Feed: {camera_id}", frame)
            cv2.waitKey(1)


# =========================
# HEALTH / STATUS API
# =========================

service_start_time = time.time()
health_app = FastAPI(title="Smart Attendance System - Server")


@health_app.get("/health")
def health():
    return {"status": "ok"}


@health_app.get("/status")
def status():
    now = time.time()
    db_ok, db_error = True, None
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        db_ok, db_error = False, str(e)

    with stats_lock:
        cams = {
            cam_id: {
                "frames_received": s["frames"],
                "seconds_since_last_frame": round(now - s["last_seen"], 2) if s["last_seen"] else None,
            }
            for cam_id, s in camera_stats.items()
        }

    return {
        "status": "running",
        "uptime_seconds": round(now - service_start_time, 1),
        "database": {"connected": db_ok, "error": db_error},
        "kafka": {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "topic": KAFKA_TOPIC},
        "cameras": cams,
        "face_db": {"identities": len(set(face_db.names)), "embeddings": len(face_db.names)},
        "current_period": current_period(),
    }


@health_app.get("/")
def live_attendance(date: str | None = None, period: str | None = None):
    target_date = date or datetime.now().date().isoformat()
    query = """
        SELECT name, enrollment_number, department, section, attendance_date, period
        FROM attendance
        WHERE attendance_date = :date
    """
    params = {"date": target_date}
    if period:
        query += " AND period = :period"
        params["period"] = period.upper()
    query += " ORDER BY department, section, name"

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(query), params).mappings().all()
    except Exception as e:
        log.error("Failed to fetch live attendance: %s", e)
        return {"status": "error", "message": str(e), "records": []}

    records = [
        {
            "name": row["name"],
            "enrollment_number": row["enrollment_number"],
            "department": row["department"],
            "section": row["section"],
            "date": row["attendance_date"].isoformat()
            if hasattr(row["attendance_date"], "isoformat") else str(row["attendance_date"]),
            "period": row["period"],
        }
        for row in rows
    ]
    return {"status": "ok", "date": target_date, "count": len(records), "records": records}


def start_health_api():
    def _run():
        uvicorn.run(health_app, host=HEALTH_API_HOST, port=HEALTH_API_PORT, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    log.info("Health API listening on http://%s:%s", HEALTH_API_HOST, HEALTH_API_PORT)


def main():
    start_health_api()
    try:
        consume_loop()
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        if SHOW_WINDOWS:
            cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
