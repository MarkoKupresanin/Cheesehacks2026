from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os
load_dotenv()


from cryptography.hazmat.primitives.ciphers.aead import AESGCM
rsc = RSCodec(10)
#aesgcm = AESGCM(os.getenv("AES_KEY"))

def encrypt_frame(frame):
    frame_bytes = frame.tobytes()

    # Hash with SHA-256
    frame_SHA = hashlib.sha256(frame_bytes).digest()

	# Generate a 256-bit key (32 bytes), AES-256
	key = AESGCM.generate_key(bit_length=256)
	aesgcm = AESGCM(key)
    nonce = os.urandom(12)

    encrypted_frame = aesgcm.encrypt(nonce, frame_bytes + frame_SHA, None)

    combined_payload = rsc.encode(nonce + encrypted_frame)

    return combined_payload
