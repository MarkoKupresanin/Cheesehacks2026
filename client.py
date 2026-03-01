from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
rsc = RSCodec(10)


def encrypt_frame(frame):
    frame_bytes = frame.tobytes()
    parity_bits = rsc.encode(frame_bytes)

    # Hash with SHA-256
    frame_SHA = hashlib.sha256()
    frame_SHA.update(frame_bytes)

	# Generate a 256-bit key (32 bytes), AES-256
	key = AESGCM.generate_key(bit_length=256)
	aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    frame_AES = aesgcm.encrypt(nonce, frame_bytes)

    return (nonce+frame_AES, frame_SHA.digest(), parity_bits)
