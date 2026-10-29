{% test orders_keys_match_bronze(model) %}
(
    select order_id from {{ source('bronze', 'olist_orders_dataset') }}
    except
    select order_id from {{ model }}
)
union all
(
    select order_id from {{ model }}
    except
    select order_id from {{ source('bronze', 'olist_orders_dataset') }}
)
{% endtest %}
