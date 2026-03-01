import os, json, logging
import numpy as np
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from sklearn.cluster import DBSCAN

logging.basicConfig(level=logging.INFO, format="[AGGREGATOR] %(message)s")
BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")

def main():
    consumer = KafkaConsumer('swarm_telemetry', bootstrap_servers=[BROKER], value_deserializer=lambda m: json.loads(m.decode("utf-8")), auto_offset_reset="latest")
    producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    frame_buffer = defaultdict(list)

    for message in consumer:
        report = message.value
        frame_id = report.get("frame_id", 0)
        
        if "bbox" not in report:
            producer.send('ground_truth', value={"crypto_status": report.get("crypto_status", "ERROR")})
            producer.flush()
            continue

        frame_buffer[frame_id].append(report)

        if len(frame_buffer[frame_id]) == 10:
            reports = frame_buffer[frame_id]
            del frame_buffer[frame_id]
            
            # EXTRACT ONLY VALID VOTES
            valid_reports = [r for r in reports if r.get("bbox") and len(r["bbox"]) == 4]
            c_status = reports[-1].get("crypto_status", "SECURE")

            # IF NO OBJECT, CLEAR THE UI INSTANTLY
            if len(valid_reports) < 4:
                producer.send('ground_truth', value={"ground_truth": [], "variance": [0,0,0,0], "error_rate": 0, "crypto_status": c_status, "all_boxes": []})
                producer.flush()
                continue
            
            bboxes = np.array([r["bbox"] for r in valid_reports], dtype=float)
            node_ids = [r["node_id"] for r in valid_reports]
            
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

                payload = {
                    "ground_truth": [round(v, 2) for v in gt],
                    "variance": [round(v, 2) for v in variance],
                    "error_rate": round(err_rate, 1),
                    "crypto_status": c_status,
                    "all_boxes": [{"node_id": r["node_id"], "model": r["model"], "bbox": r["bbox"], "is_outlier": r["node_id"] in outlier_nodes} for r in valid_reports]
                }
                producer.send('ground_truth', value=payload)
                producer.flush()

        stale_keys = [k for k in frame_buffer.keys() if k < frame_id - 10]
        for k in stale_keys: del frame_buffer[k]

if __name__ == "__main__": main()
