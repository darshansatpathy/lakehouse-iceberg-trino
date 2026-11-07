select customer_id, country, segment, signup_date
from {{ source('raw', 'customers') }}
