from reedsolo import RSCodec, ReedSolomonError
import hashlib
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from redpanda_kafka_playground import send_to_redpanda

load_dotenv()

AES_KEY = bytes.fromhex(os.getenv("AES_KEY"))

rsc = RSCodec(128)
aesgcm = AESGCM(AES_KEY)

def decode_frame(encrypted_frame):
    """Decodes a single video frame from Kafka/RedPanda, repairs damage, and
    validates frame hash
    :param encrypted_frame: AES-256 encrypted frame data
    :returns: None, data is send to Kafka/RedPanda topic
    """
    try:
        repaired_blob = rsc.decode(encrypted_frame)[0]
    except ReedSolomonError as e:
        print('error with parity check')
        print(e)
        return

    try:
        nonce = repaired_blob[:12]
        encrypted_frame = repaired_blob[12:]
        decrypted_frame = aesgcm.decrypt(nonce, encrypted_frame, None)
    except Exception as e2:
        print('error with decryption')
        print(e2)
        return

    decrypted_frame_bytes = decrypted_frame[:-32]
    og_frame_hash = decrypted_frame[-32:]


    if (hashlib.sha256(decrypted_frame_bytes).digest() == og_frame_hash):
        # MITM did not change data
        send_to_redpanda(decrypted_frame_bytes, "verified_frames")
    else:
        print("hashes did not match, cannot confirm validity of frame")
