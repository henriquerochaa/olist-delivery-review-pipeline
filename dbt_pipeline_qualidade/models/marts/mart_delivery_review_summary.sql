-- avg(average_review_score) é a média das médias por pedido: cada pedido pesa
-- igual, mesmo com várias avaliações. NULLs (pedidos sem nota) ficam de fora.
select
    delivery_bucket,
    count(*) as order_count,
    count(*) filter (where review_count > 0) as reviewed_order_count,
    sum(review_count) as review_record_count,
    avg(average_review_score) as average_score_per_order,
    avg(delivery_delay_days) as average_delay_days
from {{ ref('fct_delivery_reviews') }}
group by delivery_bucket
