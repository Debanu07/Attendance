"""
Multi-camera RTSP -> Kafka forwarder (EDGE / CLIENT side).

Run this on a machine that sits on the SAME network as your CCTV cameras
(a mini PC, an old lab desktop, a Raspberry Pi 4/5 - anything that can run
Python and reach the cameras over RTSP).

What it does, per camera:
  1. Opens the camera's RTSP stream.
  2. Throttles to a low FPS (you don't need 30fps for attendance).
  3. Compresses each frame to JPEG.
  4. Publishes it to a Kafka topic, tagged with the camera's ID.

It does NOT run any face recognition - that all happens on the server,
which may be on a completely different network. This script's only job
is: get frames off this LAN reliably and hand them to Kafka.

SETUP:
  1. pip install -r requirements-client.txt
  2. Install Tailscale on this machine: https://tailscale.com/download
     then run: sudo tailscale up
  3. Edit KAFKA_BOOTSTRAP_SERVERS below to your SERVER's Tailscale IP.
  4. Edit the CAMERAS list below with your actual camera IPs/credentials.
  5. Run: python edge_client.py
"""

import os
import time
import threading
import logging

import cv2
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("edge_client")

# =========================
# CONFIG - EDIT THIS SECTION
# =========================

# Your central server's Tailscale IP + Kafka port.
# On the server, run `tailscale ip -4` to find this.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "100.100.42.127:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "camera.frames")

# One entry per camera: (camera_id, rtsp_url)
# camera_id can be anything unique - it gets stamped onto every frame
# so the server knows which camera it came from.
CAMERAS = [
    ("d1", "rtsp://edgeclient:Iema10%40123@192.168.2.115:554/cam/realmonitor?channel=1&subtype=0"),
    ("d2", "rtsp://edgeclient:Iema10%40123@192.168.2.110:554/cam/realmonitor?channel=1&subtype=0"),
    ("d3", "rtsp://edgeclient:Iema10%40123@192.168.2.118:554/cam/realmonitor?channel=1&subtype=0"),
    ("d4", "rtsp://edgeclient:Iema10%40123@192.168.2.100:554/cam/realmonitor?channel=1&subtype=0"),
    ("d5", "rtsp://edgeclient:Iema10%40123@192.168.2.114:554/cam/realmonitor?channel=1&subtype=0"),
    ("d6", "rtsp://edgeclient:Iema10%40123@192.168.2.108:554/cam/realmonitor?channel=1&subtype=0"),
    ("d7", "rtsp://edgeclient:Iema10%40123@192.168.2.104:554/cam/realmonitor?channel=1&subtype=0"),
    ("d8", "rtsp://edgeclient:Iema10%40123@192.168.2.111:554/cam/realmonitor?channel=1&subtype=0"),
    ("d9", "rtsp://edgeclient:Iema10%40123@192.168.2.106:554/cam/realmonitor?channel=1&subtype=0"),
    ("d10", "rtsp://edgeclient:Iema10%40123@192.168.2.109:554/cam/realmonitor?channel=1&subtype=0"),
    ("d11", "rtsp://edgeclient:Iema10%40123@192.168.2.119:554/cam/realmonitor?channel=1&subtype=0"),
    ("d12", "rtsp://edgeclient:Iema10%40123@192.168.2.116:554/cam/realmonitor?channel=1&subtype=0"),
    ("d13", "rtsp://edgeclient:Iema10%40123@192.168.2.117:554/cam/realmonitor?channel=1&subtype=0"),
    ("d14", "rtsp://edgeclient:Iema10%40123@192.168.2.103:554/cam/realmonitor?channel=1&subtype=0"),
    ("d15", "rtsp://edgeclient:Iema10%40123@192.168.2.105:554/cam/realmonitor?channel=1&subtype=0"),
    ("d16", "rtsp://edgeclient:Iema10%40123@192.168.2.102:554/cam/realmonitor?channel=1&subtype=0"),
    ("d17", "rtsp://edgeclient:Iema10%40123@192.168.2.107:554/cam/realmonitor?channel=1&subtype=0"),
    ("d18", "rtsp://edgeclient:Iema10%40123@192.168.2.101:554/cam/realmonitor?channel=1&subtype=0"),
    ("d19", "rtsp://edgeclient:Iema10%40123@192.168.2.112:554/cam/realmonitor?channel=1&subtype=0"),
    ("d20", "rtsp://edgeclient:Iema10%40123@192.168.2.113:554/cam/realmonitor?channel=1&subtype=0"),
]

# How many frames per second to SEND (not the camera's native fps).
# 3-5 is plenty for attendance - people don't walk that fast.
SEND_FPS = float(os.environ.get("SEND_FPS", "1"))

JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "60"))   # 1-100, higher = bigger files
RECONNECT_DELAY = float(os.environ.get("RECONNECT_DELAY", "3.0"))


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=10000,
        max_block_ms=10000,
        linger_ms=20,                       # small batching window
        max_request_size=2 * 1024 * 1024,   # 2MB - plenty for one compressed JPEG
    )


class CameraWorker(threading.Thread):
    """One background thread per camera: read RTSP -> throttle -> publish."""

    def __init__(self, camera_id: str, rtsp_url: str, producer: KafkaProducer):
        super().__init__(daemon=True, name=f"cam-{camera_id}")
        self.camera_id = camera_id
        self.rtsp_url = rtsp_url
        self.producer = producer
        self._cap = None
        self._running = True
        self.frames_sent = 0

    def _connect(self):
        log.info("[%s] connecting...", self.camera_id)
        self._cap = cv2.VideoCapture(self.rtsp_url)
        self._cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not self._cap.isOpened():
            log.warning("[%s] connection failed, will retry in %.0fs", self.camera_id, RECONNECT_DELAY)

    def run(self):
        self._connect()
        frame_interval = 1.0 / SEND_FPS
        last_sent = 0.0

        while self._running:
            if self._cap is None or not self._cap.isOpened():
                time.sleep(RECONNECT_DELAY)
                self._connect()
                continue

            ret, frame = self._cap.read()
            if not ret:
                log.warning("[%s] frame read failed, reconnecting...", self.camera_id)
                self._cap.release()
                self._cap = None
                time.sleep(RECONNECT_DELAY)
                continue

            now = time.time()
            if now - last_sent < frame_interval:
                continue  # not time to send yet, just keep draining the RTSP buffer
            last_sent = now

            ok, jpg = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
            if not ok:
                continue

            try:
                self.producer.send(
                    KAFKA_TOPIC,
                    key=self.camera_id.encode("utf-8"),
                    value=jpg.tobytes(),
                    headers=[("ts", str(now).encode("utf-8"))],
                )
                self.frames_sent += 1
            except KafkaError as e:
                log.error("[%s] failed to publish frame: %s", self.camera_id, e)

    def stop(self):
        self._running = False
        if self._cap:
            self._cap.release()


def main():
    if not CAMERAS or any("YOUR_PASSWORD" in url for _, url in CAMERAS):
        log.error("Edit the CAMERAS list at the top of this file with your real camera URLs first.")
        return

    producer = make_producer()
    workers = [CameraWorker(cam_id, url, producer) for cam_id, url in CAMERAS]

    for w in workers:
        w.start()

    log.info(
        "Started %d camera worker(s) -> topic '%s' on %s",
        len(workers), KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
    )

    try:
        while True:
            time.sleep(10)
            for w in workers:
                status = "alive" if w.is_alive() else "DEAD"
                log.info("[%s] %s, frames sent so far: %d", w.camera_id, status, w.frames_sent)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        for w in workers:
            w.stop()
        producer.flush(timeout=5)
        producer.close()


if __name__ == "__main__":
    main()
