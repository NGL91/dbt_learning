with payments as (
    SELECT * FROM {{ref('stg_payments')}}
    
),
orders as (
    SELECT * FROM {{ref('stg_orders')}}
    

),
final as (
    SELECT orders.order_id,
           orders.customer_id,
           amount
    FROM payments inner join orders using (order_id)
)
SELECT * FROM final