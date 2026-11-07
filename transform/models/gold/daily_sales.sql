{{ config(properties={"partitioning": "ARRAY['month(order_date)']"}) }}

select
    cast(o.order_ts as date) as order_date,
    o.channel,
    c.country,
    count(*) as orders,
    sum(case when o.status in ('paid', 'shipped', 'delivered') then o.amount else 0 end) as revenue,
    count_if(o.status = 'cancelled') as cancelled_orders
from {{ ref('orders') }} as o
join {{ ref('dim_customers') }} as c on c.customer_id = o.customer_id
where not o.is_deleted
group by 1, 2, 3
