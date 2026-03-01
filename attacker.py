import random

def random_bit_flip_attack(data: bytes, flips: int = 10) -> bytes:
    """
    Randomly flips `flips` number of bits in the payload.
    """
    corrupted = bytearray(data)

    for _ in range(flips):
        byte_index = random.randint(0, len(corrupted) - 1)
        bit_index = random.randint(0, 7)

        # Flip one bit using XOR
        corrupted[byte_index] ^= (1 << bit_index)

    return bytes(corrupted)
