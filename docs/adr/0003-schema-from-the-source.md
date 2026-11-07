# ADR-0003: Columns flow from the source; dbt appends new ones

**Status:** accepted

## Context

Source systems add columns. If every model lists its columns, each addition is a code change in
several places, and a missed one silently drops data.

## Decision

Staging and silver take their column lists from the relation they read (`columns_of`), and the
incremental silver model uses `on_schema_change: append_new_columns`. Gold lists its columns
explicitly, because those are a contract with consumers.

## Consequences

- A new source column reaches silver on the next run, with nulls for earlier rows; the
  verification checks this for `coupon_code`.
- A dropped or renamed source column is not handled automatically, deliberately: that needs a
  decision.
