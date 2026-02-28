from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
rsc = RSCodec(10)

def encrypt_frame(frame):
    frame_SHA = hashlib.sha256()
    frame_bytes = frame.tobytes()

    frame_SHA.update(frame_bytes)

    parity_bits = rsc.encode(frame_bytes)

	# Generate a 256-bit key (32 bytes)
	key = AESGCM.generate_key(bit_length=256)
	aesgcm = AESGCM(key)

    frame_AES = aesgcm.encrypt(os.urandom(12), frame_bytes)

    return (frame_AES, frame_SHA, parity_bits)
