select
    order_id,
    order_status,
    try_cast(nullif(trim(order_delivered_customer_date), '') as timestamp) as delivered_to_customer_at,
    try_cast(nullif(trim(order_estimated_delivery_date), '') as timestamp) as estimated_delivery_at
from {{ source('bronze', 'olist_orders_dataset') }}
