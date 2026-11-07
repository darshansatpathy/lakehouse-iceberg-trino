select
    customer_id,
    count(*) as orders,
    sum(case when status in ('paid', 'shipped', 'delivered') then amount else 0 end) as revenue,
    min(cast(order_ts as date)) as first_order_date,
    max(cast(order_ts as date)) as last_order_date
from {{ ref('orders') }}
where not is_deleted
group by 1
