-- Every order's silver row carries the highest LSN seen for that order.
select e.order_id
from (select order_id, max(lsn) as lsn from {{ ref('stg_order_events') }} group by 1) as e
left join {{ ref('orders') }} as o on o.order_id = e.order_id
where o.order_id is null or o.last_lsn <> e.lsn
