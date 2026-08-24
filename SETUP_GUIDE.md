# Multi-Camera Attendance System — Setup Guide

You end up with two machines talking to each other across two different
networks, plus a message queue (Kafka) sitting in between so they don't
need to see each other's LAN directly.

```
[Camera Network]                    [Server Network]
  CCTV cam 1 ---\
  CCTV cam 2 ----> edge_client.py --> Kafka --> server_consumer.py --> Postgres
  CCTV cam 3 ---/     (edge PC)      (broker)     (your GPU/CPU server)
```

Tailscale is what lets the edge PC and the server "see" each other even
though they're on completely different, firewalled networks. Kafka is
what makes the actual camera frames flow.

Total setup = 6 steps. Do steps 1–3 on the **server**, steps 4–5 on the
**edge PC** (the one at the camera site), step 6 confirms it's working.

---

## Step 1 — Install Tailscale on BOTH machines

On the **server** and on the **edge PC**, run:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

This opens a browser link the first time — log in (or create a free
Tailscale account). Both machines are now part of your own private
network, no matter what physical network they're actually plugged into.

**On the server**, find its Tailscale IP:
```bash
tailscale ip -4
```
It'll look like `100.101.102.103`. **Write this down** — you'll need it
twice: once in `docker-compose.yml`, once in `edge_client.py`.

---

## Step 2 — Start Kafka on the server

1. Install Docker if you don't have it: https://docs.docker.com/engine/install/
2. Open `docker-compose.yml` and replace **both** occurrences of
   `100.x.x.x` with the server's real Tailscale IP from Step 1.
3. Start it:
   ```bash
   cd attendance-system
   docker compose up -d
   ```
4. Check it's running:
   ```bash
   docker ps
   ```
   You should see `attendance-kafka` listed as up.

That's it — you now have a Kafka broker reachable at
`<server-tailscale-ip>:9092` from anywhere on your tailnet.

---

## Step 3 — Set up the server's Python environment

```bash
cd attendance-system/server
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-server.txt
```

Edit the top of `server_consumer.py` (or set these as environment
variables) to match your setup:

- `ATTENDANCE_DB_URL` — your Postgres connection string
- `DATASET_PATH` / `EMBEDDING_CACHE` — same as your original script
- `KAFKA_BOOTSTRAP_SERVERS` — leave as `localhost:9092` since Kafka runs
  on this same machine

Then run it:
```bash
python server_consumer.py
```
It'll load the InsightFace model, build/load your face embedding cache,
connect to Postgres, and start waiting for frames from Kafka. Leave it
running.

---

## Step 4 — Set up the edge PC's Python environment

On the machine physically at the camera site:

```bash
cd attendance-system/edge_client
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-client.txt
```

This side is lightweight — no InsightFace, no GPU needed. It's just
reading RTSP and forwarding compressed JPEGs.

---

## Step 5 — Configure and run the edge client

Open `edge_client.py` and edit:

1. **`KAFKA_BOOTSTRAP_SERVERS`** → the server's Tailscale IP + `:9092`
   (same IP from Step 1), e.g. `"100.101.102.103:9092"`
2. **`CAMERAS`** → add one line per camera:
   ```python
   CAMERAS = [
       ("cam1", "rtsp://admin:yourpassword@192.168.2.109:554/cam/realmonitor?channel=1&subtype=0"),
       ("cam2", "rtsp://admin:yourpassword@192.168.2.110:554/cam/realmonitor?channel=1&subtype=0"),
       ("cam3", "rtsp://admin:yourpassword@192.168.2.111:554/cam/realmonitor?channel=1&subtype=0"),
   ]
   ```
   (These are local LAN IPs — camera to edge-PC traffic never leaves this
   network, which is correct and expected.)

Then run it:
```bash
python edge_client.py
```

You should see log lines like:
```
[cam1] connecting...
Started 3 camera worker(s) -> topic 'camera.frames' on 100.101.102.103:9092
[cam1] alive, frames sent so far: 15
[cam2] alive, frames sent so far: 15
[cam3] alive, frames sent so far: 14
```

---

## Step 6 — Confirm it's actually working end to end

On the **server**, `server_consumer.py`'s log should start showing
recognized faces and `ATTENDANCE SAVED` lines as people walk past any
camera.

You can also check the health endpoint from any browser on your tailnet:
```
http://<server-tailscale-ip>:8000/status
```
This shows Kafka connection info, per-camera frame counts and how long
ago each camera was last heard from — handy for spotting a dead camera
or a stalled edge process at a glance.

```
http://<server-tailscale-ip>:8000/
```
Shows today's live attendance as JSON.

---

## Running these permanently (not just in a terminal)

Once you've confirmed both sides work, you'll want them to survive
reboots and terminal closures. Simplest option — `systemd` services on
both machines (ask if you want these written out), or just run each
under `tmux`/`screen` for a quick-and-dirty start.

## Common gotchas

- **"Connection refused" from edge_client.py** → check the server's
  Tailscale IP is correct in both `docker-compose.yml` and
  `edge_client.py`, and that `docker ps` shows Kafka running.
- **Edge PC can't reach the camera at all** → that's a *local* network
  issue, unrelated to Tailscale/Kafka — test with
  `ffplay rtsp://...` or VLC first, before troubleshooting the rest.
- **High latency / frames arriving stale** → lower `SEND_FPS` in
  `edge_client.py`, or check your edge site's upload bandwidth (each
  camera at 5fps/quality-80 is roughly 200–500 KB/s, so N cameras ≈
  N × 0.3–0.5 Mbps upload needed).
- **Multiple cameras, one seems to stop reporting** → check `/status` on
  the server for that camera's `seconds_since_last_frame`; if it's
  climbing, the RTSP connection likely dropped on the edge side — check
  `edge_client.py`'s logs for that camera's reconnect attempts.
