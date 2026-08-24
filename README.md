Attendance System



A real-time edge video streaming system that captures frames from multiple RTSP/IP cameras and forwards them to a remote Kafka broker for downstream processing.



Architecture



┌─────────────────────────────────────────────┐

│              Edge Client Machine            │

│                                             │

│  Camera D1 ─┐                               │

│  Camera D2 ─┤                               │

│  Camera D3 ─┤                               │

│     ...     ├──> OpenCV ──> JPEG Encoding   │

│  Camera D19 ┤              │                │

│  Camera D20 ┘              ▼                │

│                       Kafka Producer        │

└───────────────────────────┬─────────────────┘

&#x20;                           │

&#x20;                    Tailscale / Network

&#x20;                           │

&#x20;                           ▼

&#x20;                ┌─────────────────────┐

&#x20;                │    Kafka Broker     │

&#x20;                │                     │

&#x20;                │   camera.frames     │

&#x20;                └──────────┬──────────┘

&#x20;                           │

&#x20;                           ▼

&#x20;                Downstream Consumers



Features



Supports multiple RTSP/IP cameras.



One background worker thread per camera.



OpenCV-based RTSP frame capture.



JPEG compression before transmission.



Kafka-based frame streaming.



Camera IDs used as Kafka message keys.



Configurable frame rate and JPEG quality.



Automatic camera reconnection.



Kafka retries and connection timeouts.



Per-camera logging and frame counters.



Designed for low-bandwidth edge-to-server transmission.



Supports remote Kafka brokers over networks such as Tailscale.



Project Structure



Attendance/

│

├── edge\_client.py

├── server\_consumer.py

├── server\_viewer.py

├── docker-compose.yml

│

├── requirements-client.txt

├── requirements-server.txt

├── SETUP\_GUIDE.md

├── .gitignore

│

└── .venv/                 # Local only, not committed



File



Description



edge\_client.py



Captures RTSP camera frames and publishes them to Kafka



server\_consumer.py



Consumes camera frames from Kafka



server\_viewer.py



Provides server-side frame viewing/processing



docker-compose.yml



Defines containerized services



requirements-client.txt



Edge-client Python dependencies



requirements-server.txt



Server-side Python dependencies



SETUP\_GUIDE.md



Additional setup and deployment instructions



Edge Client



The main edge component is edge\_client.py.



Each configured camera gets a dedicated background worker:



Camera

&#x20;  │

&#x20;  ▼

OpenCV VideoCapture

&#x20;  │

&#x20;  ▼

Frame

&#x20;  │

&#x20;  ▼

FPS Throttling

&#x20;  │

&#x20;  ▼

JPEG Encoding

&#x20;  │

&#x20;  ▼

Kafka Producer

&#x20;  │

&#x20;  ▼

camera.frames



Each worker:



Connects to its RTSP stream.



Reads frames using OpenCV.



Limits the transmission rate.



Encodes selected frames as JPEG.



Publishes JPEG bytes to Kafka.



Reconnects automatically after failures.



Tracks the number of frames sent.



Kafka Configuration



The edge client uses the KAFKA\_BOOTSTRAP\_SERVERS environment variable.



Default broker:



100.76.209.67:9092



Default topic:



camera.frames



Example:



$env:KAFKA\_BOOTSTRAP\_SERVERS="YOUR\_KAFKA\_SERVER:9092"

$env:KAFKA\_TOPIC="camera.frames"

python edge\_client.py



Runtime Configuration



Variable



Default



Description



KAFKA\_BOOTSTRAP\_SERVERS



100.76.209.67:9092



Kafka broker address



KAFKA\_TOPIC



camera.frames



Kafka topic



SEND\_FPS



1



Frames sent per second per camera



JPEG\_QUALITY



60



JPEG compression quality



RECONNECT\_DELAY



3.0



Camera reconnection delay in seconds



Example:



$env:SEND\_FPS="2"

$env:JPEG\_QUALITY="70"

$env:RECONNECT\_DELAY="5"

python edge\_client.py



With 20 cameras at 1 FPS:



20 cameras × 1 frame/second = approximately 20 frames/second



Actual throughput depends on camera resolution, network conditions, encoding time, and Kafka performance.



Camera Configuration



Cameras are configured as (camera\_id, rtsp\_url) pairs:



CAMERAS = \[

&#x20;   ("d1", "rtsp://..."),

&#x20;   ("d2", "rtsp://..."),

]



Each camera must have a unique ID.



Security



Do not commit real camera credentials, passwords, API keys, or other secrets to GitHub.



Use environment variables or a secure configuration mechanism for sensitive RTSP credentials.



If credentials have already been exposed in Git history, rotate them and remove the secret from repository history when necessary.



Kafka Message Format



Each published message contains:



Key



The camera ID:



d1



Value



The JPEG-encoded frame as raw bytes.



Header



A timestamp header:



ts



The timestamp contains the Unix timestamp associated with the frame transmission.



Conceptually:



Kafka Message

├── Key: camera\_id

├── Value: JPEG frame bytes

└── Header:

&#x20;   └── ts: Unix timestamp



Installation



Create a Python virtual environment:



python -m venv .venv



Activate it:



.venv\\Scripts\\Activate.ps1



Install edge-client dependencies:



pip install -r requirements-client.txt



Install server dependencies when setting up the server:



pip install -r requirements-server.txt



Running the Edge Client



From the project directory:



python edge\_client.py



The client will start one worker per configured camera and attempt to publish frames to the configured Kafka topic.



Example startup log:



Started 20 camera worker(s) -> topic 'camera.frames' on <kafka-server>



Monitoring



The client periodically reports camera worker status:



\[d1] alive, frames sent so far: 120

\[d2] alive, frames sent so far: 119

\[d3] alive, frames sent so far: 121



A worker can be:



alive

DEAD



Connection and publishing failures are logged.



Reconnection



If a camera cannot be opened, the worker waits for RECONNECT\_DELAY and retries.



If a frame read fails, the current connection is released and the worker attempts to reconnect.



This allows temporary camera or network failures to recover without restarting the entire application.



JPEG Compression



Frames are encoded using OpenCV JPEG compression.



Default quality:



60



Higher quality provides better image quality but increases bandwidth and Kafka message size.



Lower quality reduces bandwidth at the cost of image quality.



Kafka Producer Configuration



The producer uses:



acks=1



retries=5



linger\_ms=20



request\_timeout\_ms=10000



max\_block\_ms=10000



max\_request\_size=2 MB



The camera ID is used as the Kafka message key.



Troubleshooting



Camera connection fails



Check:



Camera IP address.



RTSP URL.



Camera credentials.



Camera network connectivity.



RTSP port.



Firewall rules.



Whether the edge machine can reach the camera.



Kafka connection fails



Check:



Kafka broker address.



Kafka port.



Tailscale/network connectivity.



Kafka listener configuration.



Firewall rules.



Kafka availability.



Topic configuration.



No frames are received



Check:



Camera connection.



OpenCV VideoCapture status.



Kafka broker connectivity.



Kafka topic.



Consumer configuration.



JPEG encoding.



Network usage is too high



Reduce SEND\_FPS and/or JPEG\_QUALITY.



Example:



$env:SEND\_FPS="1"

$env:JPEG\_QUALITY="50"

python edge\_client.py



Performance Considerations



For 20 cameras at 1 FPS, the edge client attempts approximately:



20 frames/second



Actual bandwidth depends heavily on:



Camera resolution.



JPEG quality.



Frame rate.



Scene complexity.



Network conditions.



Kafka configuration.



CPU performance.



Increasing FPS or JPEG quality increases network and Kafka throughput requirements.



Future Improvements



Potential improvements include:



Move all camera credentials to environment variables.



Add centralized camera configuration.



Add health checks and metrics.



Add Prometheus/Grafana monitoring.



Add adaptive FPS based on network conditions.



Add hardware-accelerated video encoding.



Add Kafka TLS/authentication.



Add structured logging.



Add automatic service startup.



Add downstream AI-based attendance detection/recognition.



License



Add the project's license here if applicable.



Author



Debanu Guha Thakurta



Attendance / Edge Video Streaming Project

