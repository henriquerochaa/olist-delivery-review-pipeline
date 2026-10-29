select order_id, review_id
from {{ ref('stg_order_reviews') }}
group by order_id, review_id
having count(*) > 1
