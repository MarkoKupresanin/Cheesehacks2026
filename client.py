from reedsolo import RSCodec, ReedSolomonError
import hashlib
import numpy
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import random

load_dotenv()

rsc = RSCodec(128)
AES_KEY = bytes.fromhex(os.getenv("AES_KEY"))
aesgcm = AESGCM(AES_KEY)

def encrypt_frame(frame):
    """Encrypts a single video frame via AES-256 including parity bits
    :param frame: byte data of a single frame
    :returns: Encrypted payload for Kafka/RedPanda
    """
    #frame_bytes = frame.tobytes()
    frame_bytes = frame
    # Hash with SHA-256
    frame_SHA = hashlib.sha256(frame_bytes).digest()
    print(f"Original Hash: {frame_SHA}")

    nonce = os.urandom(12)

    encrypted_frame = aesgcm.encrypt(nonce, frame_bytes + frame_SHA, None)

    combined_payload = rsc.encode(nonce + encrypted_frame)

    return bytes(combined_payload)

def random_bit_flip_attack(data, flips = 10):
    """
    Randomly flips `flips` number of bits in the payload.
	:param data: frame data in bytes to manipulate
	:param flip: Number of bit flips to perform
	:returns: Frame data that was bit-flipped
    """
    corrupted = bytearray(data)

    for _ in range(flips):
        byte_index = random.randint(0, len(corrupted) - 1)
        bit_index = random.randint(0, 7)

        # Flip one bit using XOR
        corrupted[byte_index] ^= (1 << bit_index)

    return bytes(corrupted)
