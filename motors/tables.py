"""Control tables for Feetech STS3215 motors.

Based on STS_SMS_SERIES_CONTROL_TABLE
Reference: http://doc.feetech.cn/#/prodinfodownload?srcType=FT-SMS-STS-emanual-229f4476422d4059abfb1cb0
"""

# Control table addresses for STS3215 (STS_SMS_SERIES)
# Format: (address, size_bytes)
# These match the STS_SMS_SERIES_CONTROL_TABLE

# EEPROM (non-volatile memory)
ADDR_FIRMWARE_MAJOR_VERSION = (0, 1)  # read-only
ADDR_FIRMWARE_MINOR_VERSION = (1, 1)  # read-only
ADDR_MODEL_NUMBER = (3, 2)  # read-only
ADDR_ID = (5, 1)
ADDR_BAUD_RATE = (6, 1)
ADDR_RETURN_DELAY_TIME = (7, 1)
ADDR_RESPONSE_STATUS_LEVEL = (8, 1)
ADDR_MIN_POSITION_LIMIT = (9, 2)
ADDR_MAX_POSITION_LIMIT = (11, 2)
ADDR_MAX_TEMPERATURE_LIMIT = (13, 1)
ADDR_MAX_VOLTAGE_LIMIT = (14, 1)
ADDR_MIN_VOLTAGE_LIMIT = (15, 1)
ADDR_MAX_TORQUE_LIMIT = (16, 2)
ADDR_PHASE = (18, 1)
ADDR_UNLOADING_CONDITION = (19, 1)
ADDR_LED_ALARM_CONDITION = (20, 1)
ADDR_P_COEFFICIENT = (21, 1)
ADDR_D_COEFFICIENT = (22, 1)
ADDR_I_COEFFICIENT = (23, 1)
ADDR_MINIMUM_STARTUP_FORCE = (24, 2)
ADDR_CW_DEAD_ZONE = (26, 1)
ADDR_CCW_DEAD_ZONE = (27, 1)
ADDR_PROTECTION_CURRENT = (28, 2)
ADDR_ANGULAR_RESOLUTION = (30, 1)
ADDR_HOMING_OFFSET = (31, 2)  # Uses bit 11 for sign-magnitude
ADDR_OPERATING_MODE = (33, 1)
ADDR_PROTECTIVE_TORQUE = (34, 1)
ADDR_PROTECTION_TIME = (35, 1)
ADDR_OVERLOAD_TORQUE = (36, 1)
ADDR_VELOCITY_CLOSED_LOOP_P = (37, 1)
ADDR_OVER_CURRENT_PROTECTION_TIME = (38, 1)
ADDR_VELOCITY_CLOSED_LOOP_I = (39, 1)

# SRAM (volatile memory)
ADDR_TORQUE_ENABLE = (40, 1)
ADDR_ACCELERATION = (41, 1)
ADDR_GOAL_POSITION = (42, 2)
ADDR_GOAL_TIME = (44, 2)
ADDR_GOAL_VELOCITY = (46, 2)  # Uses bit 15 for sign-magnitude
ADDR_TORQUE_LIMIT = (48, 2)
ADDR_LOCK = (55, 1)
ADDR_PRESENT_POSITION = (56, 2)  # read-only
ADDR_PRESENT_VELOCITY = (58, 2)  # read-only, uses bit 15 for sign-magnitude
ADDR_PRESENT_LOAD = (60, 2)  # read-only
ADDR_PRESENT_VOLTAGE = (62, 1)  # read-only
ADDR_PRESENT_TEMPERATURE = (63, 1)  # read-only
ADDR_STATUS = (65, 1)  # read-only
ADDR_MOVING = (66, 1)  # read-only
ADDR_PRESENT_CURRENT = (69, 2)  # read-only
ADDR_GOAL_POSITION_2 = (71, 2)  # read-only
ADDR_MOVING_VELOCITY = (80, 1)
ADDR_MOVING_VELOCITY_THRESHOLD = (80, 1)
ADDR_DTS = (81, 1)  # (ms)
ADDR_VELOCITY_UNIT_FACTOR = (82, 1)
ADDR_HTS = (83, 1)  # (ns) valid for firmware >= 2.54, other versions keep 0
ADDR_MAXIMUM_VELOCITY_LIMIT = (84, 1)
ADDR_MAXIMUM_ACCELERATION = (85, 1)
ADDR_ACCELERATION_MULTIPLIER = (86, 1)  # Acceleration multiplier in effect when acceleration is 0

# Operating modes
MODE_POSITION = 0
MODE_VELOCITY = 1
MODE_PWM = 2
MODE_STEP = 3

# Common values
TORQUE_ENABLE = 1
TORQUE_DISABLE = 0

# Sign-magnitude encoding bits for STS_SMS_SERIES
# Different registers use different bits for sign encoding
ENCODING_BIT_HOMING_OFFSET = 11  # Bit 11 for Homing_Offset
ENCODING_BIT_VELOCITY = 15  # Bit 15 for Goal_Velocity and Present_Velocity

