import os, json, cv2, torch, hashlib, random, logging, gc
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from reedsolo import RSCodec, ReedSolomonError
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

BROKER = os.getenv("REDPANDA_BROKER", "redpanda:9092")
logging.basicConfig(level=logging.INFO, format="[WORKER] %(message)s")

rsc = RSCodec(10) 
AES_KEY = bytes.fromhex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
aesgcm = AESGCM(AES_KEY)

def decode_fec(payload):
    meta_enc = payload[:54]
    enc_img = payload[54:]
    try: rep_meta = rsc.decode(meta_enc)[0]
    except ReedSolomonError: return None, "RS_FAIL"
    nonce, og_hash = rep_meta[:12], rep_meta[12:]
    try: dec_img = aesgcm.decrypt(nonce, enc_img, None)
    except: return None, "AES_FAIL"
    if hashlib.sha256(dec_img).digest() == og_hash: return dec_img, "SECURE"
    return None, "HASH_FAIL"

def main():
    logging.info("Initializing God Mode Worker...")
    from ultralytics import YOLO
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = YOLO("yolov8n.pt").to(device)
    
    consumer = KafkaConsumer('encrypted_streams', bootstrap_servers=[BROKER], value_deserializer=lambda m: m, auto_offset_reset="latest")
    producer = KafkaProducer(bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    frames_processed = 0

    for msg in consumer:
        attack_val = int(dict(msg.headers).get('attack', b'0').decode()) if msg.headers else 0
        frame_bytes, status = decode_fec(msg.value)
        
        if not frame_bytes:
            producer.send('swarm_telemetry', value={"frame_id": msg.offset, "crypto_status": status})
            producer.flush()
            continue

        img = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
        if img is None: continue
        
        res = model(img, verbose=False, classes=[0, 39, 41, 67], conf=0.40) 
        
        if res[0].boxes and len(res[0].boxes) > 0:
            best = torch.argmax(res[0].boxes.conf).item()
            x, y, x2, y2 = res[0].boxes.xyxy[best].cpu().numpy()
            w, h = x2-x, y2-y
            
            for i in range(10):
                node_model = "yolo" if i < 4 else "mobilenet" if i < 8 else "rtdetr"
                bx, by = float(x + random.uniform(-4,4)), float(y + random.uniform(-4,4))
                
                if attack_val > 0 and i >= 7:
                    if i == 7: bx += attack_val; by += attack_val
                    if i == 8: bx -= attack_val; by += attack_val
                    if i == 9: bx += random.randint(-attack_val, attack_val)

                payload = {"frame_id": msg.offset, "node_id": i, "model": node_model, "bbox": [bx, by, float(w), float(h)], "crypto_status": status}
                producer.send('swarm_telemetry', value=payload)
        producer.flush()

        # AGGRESSIVE GARBAGE COLLECTION PREVENTS SYSTEM HANGS
        frames_processed += 1
        if frames_processed % 50 == 0:
            gc.collect()
            if device == "cuda": torch.cuda.empty_cache()

if __name__ == "__main__": main()
