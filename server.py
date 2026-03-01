from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os
from redpanda-kafka-playground import send_to_redpanda
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

rsc=RSCodec(10)

def decode_frame(incoming_frame, parity, original_sha256_hash):
    nonce = incoming_frame[:12]
    incoming_frame = incoming_frame[12:]

    decrypted_frame = aesgcm.decrypt(nonce, incoming_frame)
    incoming_frame_hash = hashlib.sha256()
    incoming_frame_hash.update(decrypted)

    if (incoming_frame_hash.digest() == original_sha256_hash):
        # MITM did not change data
        send_to_redpanda(decrypted_frame, "verified_frames")
    else:
        # repair based on parity bits from solomon
        decode_result = rsc.decode(decrypted_frame)
        send_to_redpanda(decode_result[0], "verified_frames")
