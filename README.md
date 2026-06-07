# 🛡️ AEGIS: Byzantine Fault Tolerant Vision Swarm

AEGIS is a highly resilient, Byzantine Fault Tolerant (BFT) computer vision pipeline designed for critical infrastructure, edge computing, and cloud-native AI model serving. It ensures that video streams ingested for AI processing remain secure, uncorrupted, and verifiable, even in the presence of malicious nodes, transmission errors, or active network attacks.

## ⚠️ The Problem

While traditional encryption guarantees confidentiality and integrity under normal conditions, real-world systems are still vulnerable to low-level corruption, hardware faults, and targeted data-poisoning attacks.

As systems aggressively move toward **Edge computing**, **Cloud-native microservices**, **Critical infrastructure automation**, and **AI model serving pipelines**, resilience against these faults becomes essential. Security is not just about preventing unauthorized access; it is about surviving adversaries, maintaining system uptime, and ensuring strict risk management.

## 🚀 How AEGIS Works (The Process)

AEGIS implements a multi-stage cryptographic and fault-tolerant pipeline to guarantee frame integrity before any AI inference takes place.

1. **Ingest Stream:** Raw video is captured via an ARM camera (or standard webcam) using WebRTC and sent for preprocessing.
2. **Encrypt & Encode Frames:** For every frame, a cryptographic hash (SHA-256) and parity bits (Reed-Solomon Forward Error Correction) are calculated. The frame is then encrypted using AES-256-GCM.
3. **RedPanda Message Broker:** The encrypted packets are streamed upstream to RedPanda (a Kafka-compatible event streaming platform), which handles the distribution of both unverified and verified frames to the server swarm.
4. **Validation & Reconstruction:** Swarm nodes attempt to decrypt the frames and validate the hashes. If a frame has suffered bit flips or corruption, AEGIS uses the Reed-Solomon parity bits to dynamically rebuild the frame before passing it to the AI backbone.

## ✨ Key Features (Dashboard & Swarm)

* **Cryptographic Pipeline:** Real-time AES-256-GCM encryption paired with SHA-256 hashing.
* **Forward Error Correction (FEC):** Reed-Solomon integration allows the system to auto-correct bit-flips and visual noise without requiring frame retransmission.
* **Byzantine Fault Tolerance (BFT):** Uses a multi-node swarm consensus mechanism (70% quorum) to establish "Ground Truth" for object detection, dropping malicious or hallucinating nodes.
* **Multi-Model Inference Swarm:** Capable of distributing frames across different computer vision models concurrently (YOLOv8, MobileNet, RT-DETR) to achieve consensus.
* **Live Attack Simulation Environment:** A built-in dashboard utility to stress-test the system by intentionally injecting:
  * *Visual Noise (%)*
  * *Bit Flips (RS FEC simulation)*
  * *Poison Intensity (px)*

## 🛠️ Tech Stack

* **Frontend / Dashboard:** HTML/CSS/JS, WebRTC (for live camera ingest)
* **Message Broker:** RedPanda (Kafka API compatible) for high-throughput, low-latency streaming
* **Cryptography:** AES-256-GCM, SHA-256
* **Error Correction:** Reed-Solomon (RS)
* **AI / ML Models:** YOLOv8, MobileNet, RT-DETR (Transformer-based object detection)

---

## ⚙️ Local Setup & Installation

*(Note: Ensure you have Docker and Python 3.10+ installed before beginning).*

**1. Clone the repository:**
```bash
git clone [https://github.com/MarkoKupresanin/Cheesehacks2026.git](https://github.com/MarkoKupresanin/Cheesehacks2026.git)
cd Cheesehacks2026
