-- Grão: uma linha por pedido. As avaliações são agregadas ANTES do join, porque
-- um pedido pode ter várias; juntar antes multiplicaria as linhas do pedido.
with reviews_by_order as (
    select
        order_id,
        avg(review_score) as average_review_score,
        count(*) as review_count
    from {{ ref('stg_order_reviews') }}
    group by order_id
), orders_with_reviews as (
    select
        orders.*,
        reviews.average_review_score,
        coalesce(reviews.review_count, 0) as review_count
    from {{ ref('stg_orders') }} as orders
    -- left join: pedido sem avaliação continua na tabela
    left join reviews_by_order as reviews using (order_id)
)
select
    order_id,
    order_status,
    delivered_to_customer_at,
    estimated_delivery_at,
    -- O case avalia de cima para baixo: os casos sem data real vêm primeiro.
    -- Comparamos datas de calendário (não horas), como a data estimada da Olist.
    case
        when delivered_to_customer_at is null and order_status = 'delivered'
            then 'delivered_missing_timestamp'
        when delivered_to_customer_at is null then 'not_delivered'
        when cast(delivered_to_customer_at as date) > cast(estimated_delivery_at as date)
            then 'late'
        when cast(delivered_to_customer_at as date) < cast(estimated_delivery_at as date)
            then 'early'
        else 'on_time'
    end as delivery_bucket,
    case
        when delivered_to_customer_at is not null
        then date_diff(
            'day',
            cast(estimated_delivery_at as date),
            cast(delivered_to_customer_at as date)
        )
    end as delivery_delay_days,
    average_review_score,
    review_count
from orders_with_reviews
