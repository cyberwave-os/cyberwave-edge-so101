"""Sign-magnitude encoding/decoding utilities for motor communication.

Different registers in STS_SMS_SERIES use different bits for sign encoding:
- Homing_Offset: bit 11 (0x800)
- Goal_Velocity, Present_Velocity: bit 15 (0x8000)
- Position values: typically no sign encoding (absolute position)
"""


def encode_sign_magnitude(value: int, sign_bit: int = 15) -> int:
    """
    Encode a signed integer using sign-magnitude representation.

    Args:
        value: Signed integer to encode
        sign_bit: Which bit to use as sign bit (default: 15 for velocity)

    Returns:
        Encoded value in sign-magnitude format
    """
    sign_mask = 1 << sign_bit
    magnitude_mask = sign_mask - 1  # All bits below sign bit

    if value < 0:
        # Set sign bit and use absolute value for magnitude
        return abs(value) | sign_mask
    # Positive value: just return the value (sign bit is 0)
    return value & magnitude_mask


def decode_sign_magnitude(encoded: int, sign_bit: int = 15) -> int:
    """
    Decode a sign-magnitude encoded integer.

    Args:
        encoded: Encoded value in sign-magnitude format
        sign_bit: Which bit is used as sign bit (default: 15 for velocity)

    Returns:
        Decoded signed integer
    """
    sign_mask = 1 << sign_bit
    magnitude_mask = sign_mask - 1  # All bits below sign bit

    if encoded & sign_mask:
        # Sign bit is set, value is negative
        return -(encoded & magnitude_mask)
    # Sign bit is clear, value is positive
    return encoded & magnitude_mask
