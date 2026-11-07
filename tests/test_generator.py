from __future__ import annotations

import datetime as dt
from decimal import Decimal

from lakehouse.generator import Batch, Event, Generator, Reference


def event(lsn: int, op: str, order: int, status: str = "pending", amount: str = "10.00") -> Event:
    ts = dt.datetime(2025, 1, 1) + dt.timedelta(minutes=lsn)
    return Event(lsn, op, ts, order, 1, ts, status, Decimal(amount), "web")


def test_batches_are_deterministic_and_shaped_like_cdc() -> None:
    a, b = (
        Generator(seed=3, orders_per_batch=50, updates_per_batch=30, deletes_per_batch=2),
        Generator(seed=3, orders_per_batch=50, updates_per_batch=30, deletes_per_batch=2),
    )
    first, again = a.batch(1), b.batch(1)
    assert first.events == again.events
    lsns = [e.lsn for e in first.events]
    assert lsns != sorted(lsns), "events arrive out of order"
    assert len(lsns) > len(set(lsns)), "some events are delivered twice"
    by_lsn = sorted({e.lsn: e for e in first.events}.values(), key=lambda e: e.lsn)
    assert [e.commit_ts for e in by_lsn] == sorted(e.commit_ts for e in by_lsn), "commit times follow the log"
    assert {e.op for e in first.events} == {"c", "u", "d"}
    assert not any(e.coupon_code for e in first.events) and not first.with_coupons
    a.batch(2)
    third = a.batch(3)
    assert third.with_coupons and any(e.coupon_code for e in third.events)
    assert len(third.customers) > len(first.customers)


def test_reference_applies_events_by_lsn() -> None:
    ref = Reference()
    events = [
        event(3, "u", 1, "paid", "12.00"),
        event(1, "c", 1),
        event(2, "u", 1, "cancelled"),
        event(3, "u", 1, "paid", "12.00"),
        event(4, "c", 2),
        event(5, "d", 2),
    ]
    ref.apply(Batch(1, events, [], False))
    one, two = ref.orders[1], ref.orders[2]
    assert (one.status, one.amount, one.last_lsn, one.is_deleted) == ("paid", Decimal("12.00"), 3, False)
    assert two.is_deleted and two.last_lsn == 5
    ref.apply(Batch(2, [event(2, "u", 1, "cancelled")], [], False))
    assert ref.orders[1].status == "paid", "an event older than the applied state is ignored"
    assert [o.order_id for o in ref.live()] == [1]


def test_reference_aggregates() -> None:
    gen = Generator(seed=5, orders_per_batch=300, updates_per_batch=200, deletes_per_batch=5)
    ref = Reference()
    for n in (1, 2):
        ref.apply(gen.batch(n))
    daily = ref.daily_sales()
    assert sum(v[0] for v in daily.values()) == len(ref.live())
    revenue = sum(o.amount for o in ref.live() if o.status in ("paid", "shipped", "delivered"))
    assert sum(v[1] for v in daily.values()) == revenue
    assert sum(v[0] for v in ref.customer_value().values()) == len(ref.live())
    assert sum(v[2] for v in daily.values()) == sum(1 for o in ref.live() if o.status == "cancelled")
