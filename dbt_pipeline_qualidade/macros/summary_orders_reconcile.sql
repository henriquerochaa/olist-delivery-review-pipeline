{% test summary_orders_reconcile(model) %}
select 1 as mismatch
where (select coalesce(sum(order_count), 0) from {{ model }}) != (
    select count(*) from {{ ref('fct_delivery_reviews') }}
)
{% endtest %}
