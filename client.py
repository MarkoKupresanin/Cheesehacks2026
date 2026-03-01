from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

load_dotenv()


rsc = RSCodec(128)
AES_KEY = bytes.fromhex(os.getenv("AES_KEY"))
aesgcm = AESGCM(AES_KEY)

def encrypt_frame(frame):
    frame_bytes = frame.tobytes()

    # Hash with SHA-256
    frame_SHA = hashlib.sha256(frame_bytes).digest()

    nonce = os.urandom(12)
    encrypted_frame = aesgcm.encrypt(nonce, frame_bytes + frame_SHA, None)

    combined_payload = rsc.encode(nonce + encrypted_frame)

    return combined_payload
