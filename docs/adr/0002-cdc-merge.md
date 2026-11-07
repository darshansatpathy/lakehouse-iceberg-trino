# ADR-0002: Merge CDC events above an LSN watermark, with tombstones

**Status:** accepted

## Context

CDC tools deliver at least once, files within a batch arrive in no particular order, and
batches are sometimes replayed. The current-state table must come out the same regardless.

## Decision

- Deduplicate events by LSN in staging.
- In the incremental silver model, read only events above the highest LSN already applied, take
  the latest event per order, and MERGE on `order_id`.
- Keep deletes as tombstones with the LSN that deleted them.
- Test in dbt that every order carries the highest LSN seen for it.

## Consequences

- Replays and re-runs are no-ops, which the verification checks.
- An event below the watermark is not merged; the dbt test fails instead. That is the right
  behaviour for a single ordered log; several sources with independent sequences would need a
  watermark per source.
