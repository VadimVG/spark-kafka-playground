# Load levels: messages per second per topic
RATES = {
    "low": 0.2,     # 1 msg every 5 seconds
    "medium": 1.0,  # 1 msg per second
    "high": 10.0,   # 10 msgs per second
}

# Total unique users in the system
NUM_USERS = 1000

# Products: id -> price
PRODUCTS = {
    1: 99.99,
    2: 49.50,
    3: 199.00,
    4: 5.99,
    5: 299.99,
    6: 149.00,
    7: 79.99,
    8: 19.99,
    9: 399.00,
    10: 24.50,
    11: 89.00,
    12: 159.99,
    13: 9.99,
    14: 249.00,
    15: 59.00,
    16: 129.99,
    17: 34.99,
    18: 499.00,
    19: 69.99,
    20: 189.00,
    21: 44.99,
    22: 299.00,
    23: 14.99,
    24: 119.00,
    25: 54.99,
    26: 349.00,
    27: 74.99,
    28: 29.99,
    29: 449.00,
    30: 64.99,
    31: 139.00,
    32: 84.99,
    33: 199.99,
    34: 39.99,
    35: 259.00,
    36: 94.99,
    37: 169.00,
    38: 49.99,
    39: 399.99,
    40: 109.00,
    41: 22.99,
    42: 279.00,
    43: 59.99,
    44: 149.99,
    45: 329.00,
    46: 79.00,
    47: 189.99,
    48: 99.00,
    49: 419.00,
    50: 239.00,
}

# Pages for user activity logs
PAGES = ["home", "catalog", "product", "cart", "checkout", "profile"]

# Order status transitions with probabilities
# Key: current status, Value: dict of possible next status -> probability
STATUS_TRANSITIONS = {
    "created": {"paid": 0.7, "cancelled": 0.3},
    "paid": {"shipped": 0.9, "refunded": 0.1},
    "shipped": {"delivered": 0.95, "lost": 0.05},
    "delivered": {},  # final state, no transitions
}