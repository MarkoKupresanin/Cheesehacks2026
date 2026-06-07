# AEGIS: Byzantine Fault Tolerant Vision Swarm

AEGIS is a resilient computer vision pipeline built for critical infrastructure and edge AI 
serving. It guarantees that video frames remain uncorrupted and verifiable before any AI 
inference takes place — even under active network attacks, hardware faults, or malicious nodes.

Built at CheeseHacks 2026 (Google-sponsored).

---

## The Problem

Traditional encryption protects confidentiality but doesn't survive adversarial conditions 
at the edge — bit flips, data poisoning, and node hallucination are real failure modes in 
distributed AI pipelines. As systems move toward edge computing, cloud-native microservices, 
and critical infrastructure automation, resilience becomes as important as accuracy.

---

## How It Works

AEGIS implements a multi-stage cryptographic and fault-tolerant pipeline before any 
inference runs:

1. **Ingest** — Raw video captured via ARM camera or webcam using WebRTC
2. **Encrypt & Encode** — Each frame gets a SHA-256 hash + Reed-Solomon parity bits, 
   then AES-256-GCM encryption
3. **Stream** — Encrypted packets sent to a RedPanda (Kafka-compatible) message broker 
   for distribution across the swarm
4. **Validate & Reconstruct** — Swarm nodes decrypt and verify frames; corrupted frames 
   are rebuilt from Reed-Solomon parity before inference

---

## Key Features

**Cryptographic Pipeline**  
Real-time AES-256-GCM encryption with SHA-256 hashing per frame.

**Forward Error Correction (FEC)**  
Reed-Solomon integration auto-corrects bit flips and visual noise without retransmission.

**Byzantine Fault Tolerance**  
Multi-node swarm consensus (70% quorum) establishes ground truth for object detection, 
automatically dropping malicious or hallucinating nodes.

**Multi-Model Inference Swarm**  
Distributes frames concurrently across YOLOv8, MobileNet, and RT-DETR for consensus-based 
detection results.

**Live Attack Simulation Dashboard**  
Built-in stress-testing utility to inject:
- Visual noise (%)
- Bit flips (Reed-Solomon FEC simulation)
- Poison intensity (px)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend / Dashboard | HTML, CSS, JS, WebRTC |
| Message Broker | RedPanda (Kafka API compatible) |
| Cryptography | AES-256-GCM, SHA-256 |
| Error Correction | Reed-Solomon (RS) |
| AI Models | YOLOv8, MobileNet, RT-DETR |

---

## Local Setup

Requires Docker and Python 3.10+.

**1. Clone the repository**
```bash
git clone https://github.com/MarkoKupresanin/Cheesehacks2026.git
cd Cheesehacks2026
```

**2. Start the RedPanda broker**
```bash
docker-compose up -d redpanda
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Run the broker and swarm workers**
```bash
# Central broker
python broker.py --port 19892

# Vision nodes (separate terminals)
python worker.py --model yolov8
python worker.py --model mobilenet
python worker.py --model rtdetr
```

**5. Launch the dashboard**
```bash
python -m http.server 8000
```
Navigate to `http://localhost:8000` to access the Swarm UI and initialize camera ingest.

---

## Hackathon Context

Built at CheeseHacks 2026 (Google-sponsored hackathon). Designed to tackle modern challenges 
in distributed AI infrastructure and zero-trust edge environments.
