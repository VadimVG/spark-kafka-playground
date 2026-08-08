import random
from dataclasses import asdict
from typing import List

from config import NUM_USERS, PRODUCTS, PAGES, STATUS_TRANSITIONS
from models import SaleEvent, OrderEvent, LogEvent, LogAction, OrderStatus


class EventGenerator:
    """Generates fake events for sales, orders, and logs with shared user IDs."""

    def __init__(self):
        # Pool of active orders: order_id -> {user_id, status, total}
        self._active_orders: dict[str, dict] = {}
        self._order_counter = 0

    # public API

    def generate_sale(self) -> List[SaleEvent]:
        user_id = random.randint(1, NUM_USERS)
        product_id = random.choice(list(PRODUCTS.keys()))
        return [SaleEvent(
            user_id=user_id,
            product_id=product_id,
            amount=PRODUCTS[product_id],
        )]

    def generate_log(self) -> List[LogEvent]:
        user_id = random.randint(1, NUM_USERS)
        action = random.choice(list(LogAction))
        page = random.choice(PAGES)

        return [LogEvent(
            user_id=user_id,
            action=action,
            page=page,
        )]

    def generate_orders(self) -> List[OrderEvent]:
        """Create a new order (30%) or update an existing one (70%)."""
        if self._active_orders and random.random() < 0.7:
            return self._update_order()
        return self._create_order()

    def _create_order(self) -> list[OrderEvent]:
        """
        Create a new order with random items.
        Returns one OrderEvent per item, all sharing the same order_number.
        """
        order_number = self._next_order_number()
        user_id = random.randint(1, NUM_USERS)

        # Pick 1-4 random products
        num_items = random.randint(1, 4)
        items = []
        for _ in range(num_items):
            product_id = random.choice(list(PRODUCTS.keys()))
            quantity = random.randint(1, 3)
            price = PRODUCTS[product_id]
            items.append({
                "product_id": product_id,
                "quantity": quantity,
                "price": price,
                "status": OrderStatus.CREATED,
            })

        # Total sum of all items
        total = round(sum(item["price"] * item["quantity"] for item in items), 2)

        # Store in active orders for later status updates
        self._active_orders[order_number] = {
            "user_id": user_id,
            "total": total,
            "items": items,
        }

        # Return one event per item
        return [
            OrderEvent(
                order_number=order_number,
                user_id=user_id,
                product_id=item["product_id"],
                quantity=item["quantity"],
                price=item["price"],
                status=item["status"],
                total=total,
            )
            for item in items
        ]
    
    # private helpers

    def _next_order_number(self) -> str:
        """
        Return the next sequential order number.
        Format: ORD-0000001, ORD-0000002, ...
        Counter increments on each call, shared across all orders in a session.
        """
        self._order_counter += 1
        return f"ORD-{self._order_counter:07d}"

    def _update_order(self) -> list[OrderEvent]:
        """
        Pick a random active order, move one item's status forward.
        Returns a single OrderEvent for the updated item.
        """
        # Pick a random active order
        order_number = random.choice(list(self._active_orders.keys()))
        order = self._active_orders[order_number]

        # Pick a random item from the order
        item = random.choice(order["items"])

        # Only update if item is not in a final state
        transitions = STATUS_TRANSITIONS.get(item["status"], {})
        if not transitions:
            return []

        # Pick next status based on probabilities
        next_statuses = list(transitions.keys())
        probabilities = list(transitions.values())
        next_status = random.choices(next_statuses, weights=probabilities, k=1)[0]

        # Update item status
        item["status"] = next_status

        # If all items are in final state, remove the order
        all_final = all(
            i["status"] in ("delivered", "cancelled", "refunded", "lost")
            for i in order["items"]
        )
        if all_final:
            del self._active_orders[order_number]

        # Return one event for this item update
        return [
            OrderEvent(
                order_number=order_number,
                user_id=order["user_id"],
                product_id=item["product_id"],
                quantity=item["quantity"],
                price=item["price"],
                status=next_status,
                total=order["total"],
            )
        ]