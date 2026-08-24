"""Live 20-camera viewer for frames received from Kafka.

Run this on the server PC while the edge client is publishing frames.
It does not perform face recognition or write to the attendance database.
Press Q in the viewer window to exit.
"""

import os
import time

import cv2
import numpy as np
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "camera.frames")
GRID_COLUMNS = 5
TILE_WIDTH = 320
TILE_HEIGHT = 180


def make_tile(camera_id: str, frame: np.ndarray | None) -> np.ndarray:
    if frame is None:
        tile = np.zeros((TILE_HEIGHT, TILE_WIDTH, 3), dtype=np.uint8)
        label = f"{camera_id}: waiting for frame"
    else:
        tile = cv2.resize(frame, (TILE_WIDTH, TILE_HEIGHT), interpolation=cv2.INTER_AREA)
        label = camera_id
    cv2.rectangle(tile, (0, 0), (TILE_WIDTH, 28), (0, 0, 0), -1)
    cv2.putText(tile, label, (8, 20), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
    return tile


def make_grid(frames: dict[str, np.ndarray]) -> np.ndarray:
    camera_ids = [f"d{i}" for i in range(1, 21)]
    tiles = [make_tile(camera_id, frames.get(camera_id)) for camera_id in camera_ids]
    rows = [tiles[i : i + GRID_COLUMNS] for i in range(0, len(tiles), GRID_COLUMNS)]
    return cv2.vconcat([cv2.hconcat(row) for row in rows])


def main() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="camera-live-viewer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda value: value.decode("utf-8") if value else "unknown",
        consumer_timeout_ms=200,
    )
    frames: dict[str, np.ndarray] = {}
    print(f"Viewing '{KAFKA_TOPIC}' from {KAFKA_BOOTSTRAP_SERVERS}. Press Q to exit.")

    try:
        while True:
            for message in consumer:
                image = cv2.imdecode(np.frombuffer(message.value, dtype=np.uint8), cv2.IMREAD_COLOR)
                if image is not None:
                    frames[message.key] = image

            cv2.imshow("Attendance cameras (Kafka)", make_grid(frames))
            if cv2.waitKey(1) & 0xFF in (ord("q"), ord("Q")):
                break
            time.sleep(0.02)
    finally:
        consumer.close()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
