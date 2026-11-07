{{ config(materialized='table') }}

select customer_id, country, segment, signup_date
from {{ ref('stg_customers') }}
