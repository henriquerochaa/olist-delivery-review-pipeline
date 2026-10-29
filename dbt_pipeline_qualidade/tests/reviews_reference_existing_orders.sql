select reviews.order_id
from {{ ref('stg_order_reviews') }} as reviews
left join {{ ref('stg_orders') }} as orders using (order_id)
where orders.order_id is null
