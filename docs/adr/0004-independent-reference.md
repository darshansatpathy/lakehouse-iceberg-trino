# ADR-0004: Verify against state computed without the lakehouse

**Status:** accepted

## Context

Row counts and dbt tests show that tables are internally consistent, not that they are right.

## Decision

Generate the CDC events deterministically and compute the expected state by applying them in
plain Python, sorted by LSN. Compare silver and gold with that state, row for row, after every
batch, after a replay, at each historical snapshot, and after maintenance.

## Consequences

- A wrong merge, a lost update or a double count fails a named check.
- The reference must implement the same business rules as the models (what counts as revenue);
  it is short enough to review side by side.
