import os, json, logging
import numpy as np
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from sklearn.cluster import DBSCAN

logging.basicConfig(level=logging.INFO, format="[AGGREGATOR] %(message)s")
BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")

def main():
    logging.info("Booting Instant BFT Consensus Engine...")
    consumer = KafkaConsumer('swarm_telemetry', bootstrap_servers=[BROKER], value_deserializer=lambda m: json.loads(m.decode("utf-8")), auto_offset_reset="latest")
    producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    frame_buffer = defaultdict(list)

    for message in consumer:
        report = message.value
        frame_id = report.get("frame_id", 0)
        
        # Crypto failure handling
        if "bbox" not in report:
            producer.send('ground_truth', value={"crypto_status": report.get("crypto_status", "ERROR")})
            producer.flush()
            continue

        frame_buffer[frame_id].append(report)

        # Fire instantly when all 10 simulated nodes report in!
        if len(frame_buffer[frame_id]) == 10:
            reports = frame_buffer[frame_id]
            del frame_buffer[frame_id] # Instantly clear memory to prevent flooding
            
            bboxes = np.array([r["bbox"] for r in reports], dtype=float)
            node_ids = [r["node_id"] for r in reports]
            
            clustering = DBSCAN(eps=90.0, min_samples=3).fit(bboxes[:, :2])
            labels = clustering.labels_
            
            unique_labels = set(labels) - {-1}
            if unique_labels:
                best_label = max(unique_labels, key=lambda l: np.sum(labels == l))
                honest_mask = labels == best_label
                honest_bboxes = bboxes[honest_mask]
                
                gt = honest_bboxes.mean(axis=0).tolist()
                variance = np.std(honest_bboxes, axis=0).tolist() if len(honest_bboxes) > 1 else [0,0,0,0]
                
                outlier_nodes = [node_ids[i] for i, m in enumerate(labels != best_label) if m]
                err_rate = (len(outlier_nodes) / 10) * 100
                c_status = reports[-1].get("crypto_status", "SECURE")

                payload = {
                    "ground_truth": [round(v, 2) for v in gt],
                    "variance": [round(v, 2) for v in variance],
                    "error_rate": round(err_rate, 1),
                    "crypto_status": c_status,
                    "all_boxes": [{"node_id": r["node_id"], "model": r["model"], "bbox": r["bbox"], "is_outlier": r["node_id"] in outlier_nodes} for r in reports]
                }
                producer.send('ground_truth', value=payload)
                producer.flush()
                logging.info(f"✅ Consensus Frame {frame_id} | Outliers: {len(outlier_nodes)} | Var: {round(variance[0], 2)}")

        # Keep buffer clean from old orphaned frames
        stale_keys = [k for k in frame_buffer.keys() if k < frame_id - 10]
        for k in stale_keys: del frame_buffer[k]

if __name__ == "__main__": main()
