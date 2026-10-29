select
    order_id,
    review_id,
    try_cast(nullif(trim(review_score), '') as integer) as review_score
from {{ source('bronze', 'olist_order_reviews_dataset') }}
