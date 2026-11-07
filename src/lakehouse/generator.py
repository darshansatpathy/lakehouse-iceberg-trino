"""Deterministic change-data-capture batches for an orders system, and the state they imply.

Each batch is what a CDC tool would deliver: create, update and delete events with a
monotonically increasing log sequence number (LSN), in no particular order within the batch, with
some events delivered twice. `Reference` applies the same events in plain Python. It is the
independent answer the lakehouse is checked against.
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field, replace
from decimal import Decimal

STATUS_FLOW = {"pending": ["paid", "cancelled"], "paid": ["shipped", "cancelled"], "shipped": ["delivered"], "delivered": ["returned"]}
REVENUE_STATUSES = ("paid", "shipped", "delivered")
CHANNELS = ["web", "app", "store", "marketplace"]
COUNTRIES = ["US", "DE", "IN", "BR", "JP", "GB", "FR", "AU"]
SEGMENTS = ["consumer", "small_business", "enterprise"]
COUPONS = ["WELCOME10", "SPRING15", "VIP20", "FREESHIP"]
START = dt.datetime(2025, 1, 1)


@dataclass(frozen=True)
class Customer:
    customer_id: int
    country: str
    segment: str
    signup_date: dt.date


@dataclass(frozen=True)
class Event:
    lsn: int
    op: str  # "c", "u" or "d"
    commit_ts: dt.datetime
    order_id: int
    customer_id: int
    order_ts: dt.datetime
    status: str
    amount: Decimal
    channel: str
    coupon_code: str | None = None


@dataclass
class Batch:
    number: int
    events: list[Event]
    customers: list[Customer]
    with_coupons: bool

    @property
    def distinct_events(self) -> int:
        return len({e.lsn for e in self.events})


@dataclass
class Generator:
    """Produces batches. Coupons appear from `coupons_from` on: the source system added a column."""

    seed: int = 7
    orders_per_batch: int = 2000
    updates_per_batch: int = 1500
    deletes_per_batch: int = 20
    duplicate_rate: float = 0.02
    customers: int = 400
    coupons_from: int = 3
    rng: random.Random = field(init=False)
    lsn: int = field(init=False, default=0)
    next_order: int = field(init=False, default=1)
    open_orders: dict[int, Event] = field(init=False, default_factory=dict)
    roster: list[Customer] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)

    def _customers(self, batch: int) -> list[Customer]:
        target = self.customers if batch == 1 else len(self.roster) + self.customers // 10
        while len(self.roster) < target:
            cid = len(self.roster) + 1
            self.roster.append(
                Customer(
                    cid, self.rng.choice(COUNTRIES), self.rng.choice(SEGMENTS), dt.date(2024, 1, 1) + dt.timedelta(days=self.rng.randint(0, 500))
                )
            )
        return list(self.roster)

    def batch(self, number: int) -> Batch:
        rng = self.rng
        customers = self._customers(number)
        with_coupons = number >= self.coupons_from
        day0 = START + dt.timedelta(days=(number - 1) * 30)
        events: list[Event] = []

        def emit(**kw: object) -> Event:
            self.lsn += 1
            ts = day0 + dt.timedelta(seconds=rng.randint(0, 30 * 86400 - 1))
            event = Event(lsn=self.lsn, commit_ts=ts, **kw)  # type: ignore[arg-type]
            events.append(event)
            return event

        for _ in range(self.orders_per_batch):
            oid = self.next_order
            self.next_order += 1
            customer = rng.choice(customers)
            order_ts = day0 + dt.timedelta(seconds=rng.randint(0, 30 * 86400 - 1))
            amount = Decimal(rng.randint(500, 250000)) / 100
            coupon = rng.choice([*COUPONS, None, None, None]) if with_coupons else None
            self.open_orders[oid] = emit(
                op="c",
                order_id=oid,
                customer_id=customer.customer_id,
                order_ts=order_ts,
                status="pending",
                amount=amount,
                channel=rng.choice(CHANNELS),
                coupon_code=coupon,
            )

        candidates = sorted(oid for oid, e in self.open_orders.items() if e.status in STATUS_FLOW)
        for oid in rng.sample(candidates, min(self.updates_per_batch, len(candidates))):
            last = self.open_orders[oid]
            status = rng.choice(STATUS_FLOW[last.status])
            amount = last.amount
            if rng.random() < 0.1:  # a price correction
                amount = (amount * Decimal(rng.choice(["0.9", "0.95", "1.05"]))).quantize(Decimal("0.01"))
            self.open_orders[oid] = emit(
                op="u",
                order_id=oid,
                customer_id=last.customer_id,
                order_ts=last.order_ts,
                status=status,
                amount=amount,
                channel=last.channel,
                coupon_code=last.coupon_code,
            )

        for oid in rng.sample(sorted(self.open_orders), min(self.deletes_per_batch, len(self.open_orders))):
            last = self.open_orders.pop(oid)
            emit(
                op="d",
                order_id=oid,
                customer_id=last.customer_id,
                order_ts=last.order_ts,
                status=last.status,
                amount=last.amount,
                channel=last.channel,
                coupon_code=last.coupon_code,
            )

        # Commit times follow LSN order, as they would in a database log.
        stamps = sorted(e.commit_ts for e in events)
        events = [replace(e, commit_ts=ts) for e, ts in zip(sorted(events, key=lambda e: e.lsn), stamps, strict=True)]
        delivered = events + rng.sample(events, int(len(events) * self.duplicate_rate))
        rng.shuffle(delivered)
        return Batch(number, delivered, customers, with_coupons)


@dataclass(frozen=True)
class OrderState:
    order_id: int
    customer_id: int
    order_ts: dt.datetime
    status: str
    amount: Decimal
    channel: str
    coupon_code: str | None
    is_deleted: bool
    last_lsn: int


class Reference:
    """The expected state, computed without the lakehouse."""

    def __init__(self) -> None:
        self.orders: dict[int, OrderState] = {}
        self.customers: dict[int, Customer] = {}

    def apply(self, batch: Batch) -> None:
        for c in batch.customers:
            self.customers[c.customer_id] = c
        for e in sorted({e.lsn: e for e in batch.events}.values(), key=lambda e: e.lsn):
            current = self.orders.get(e.order_id)
            if current is not None and current.last_lsn >= e.lsn:
                continue
            self.orders[e.order_id] = OrderState(
                e.order_id, e.customer_id, e.order_ts, e.status, e.amount, e.channel, e.coupon_code, e.op == "d", e.lsn
            )

    def live(self) -> list[OrderState]:
        return [o for o in self.orders.values() if not o.is_deleted]

    def daily_sales(self) -> dict[tuple[dt.date, str, str], tuple[int, Decimal, int]]:
        """(order date, channel, country) -> (orders, revenue, cancelled orders)."""
        out: dict[tuple[dt.date, str, str], tuple[int, Decimal, int]] = {}
        for o in self.live():
            key = (o.order_ts.date(), o.channel, self.customers[o.customer_id].country)
            n, rev, cancelled = out.get(key, (0, Decimal("0.00"), 0))
            out[key] = (n + 1, rev + (o.amount if o.status in REVENUE_STATUSES else 0), cancelled + (o.status == "cancelled"))
        return out

    def customer_value(self) -> dict[int, tuple[int, Decimal]]:
        """customer -> (orders, revenue), for customers with at least one live order."""
        out: dict[int, tuple[int, Decimal]] = {}
        for o in self.live():
            n, rev = out.get(o.customer_id, (0, Decimal("0.00")))
            out[o.customer_id] = (n + 1, rev + (o.amount if o.status in REVENUE_STATUSES else Decimal("0.00")))
        return out
