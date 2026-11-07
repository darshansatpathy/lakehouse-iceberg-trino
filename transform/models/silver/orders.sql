{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    properties={"partitioning": "ARRAY['month(order_ts)']"},
  )
}}

-- Current state of every order, built by merging CDC events. Events at or below the highest LSN
-- already applied are ignored, so replays and re-runs change nothing. Within the new events the
-- last one per order wins. Deletes are kept as tombstones (is_deleted) for audit.
with new_events as (
    select *
    from {{ ref('stg_order_events') }}
    {% if is_incremental() %}
    where lsn > (select coalesce(max(last_lsn), -1) from {{ this }})
    {% endif %}
),

latest as (
    select *, row_number() over (partition by order_id order by lsn desc) as recency
    from new_events
)

select
    {{ columns_of(ref('stg_order_events'), exclude=['lsn', 'op', 'commit_ts', 'batch_id']) }},
    op = 'd' as is_deleted,
    lsn as last_lsn,
    commit_ts as last_commit_ts,
    batch_id as last_batch_id
from latest
where recency = 1
