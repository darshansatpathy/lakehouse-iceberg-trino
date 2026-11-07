-- At-least-once delivery means some events arrive twice. An event is identified by its LSN.
select {{ columns_of(source('raw', 'order_events')) }}
from (
    select *, row_number() over (partition by lsn order by batch_id) as delivery
    from {{ source('raw', 'order_events') }}
)
where delivery = 1
