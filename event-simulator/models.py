from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional
import uuid


class OrderStatus(StrEnum):
    CREATED = "created"
    PAID = "paid"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"
    LOST = "lost"


class LogAction(StrEnum):
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT = "checkout"
    PAYMENT = "payment"


def _now_iso() -> str:
    """
    Current UTC timestamp in ISO 8601 format.
    Human-readable in logs, native parsing in Spark (to_timestamp) and Postgres.
    """
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    """Short unique ID."""
    return uuid.uuid4().hex[:12]


@dataclass
class SaleEvent:
    user_id: int
    product_id: int
    amount: float
    sale_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)


@dataclass
class OrderEvent:
    order_number: str
    user_id: int
    product_id: int
    quantity: int
    price: float
    status: OrderStatus
    total: float
    order_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)


@dataclass
class LogEvent:
    user_id: int
    action: LogAction
    page: str
    log_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now_iso)