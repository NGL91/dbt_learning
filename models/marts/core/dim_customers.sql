
with customers as (

    select * FROM {{ ref('stg_customers')}}


),

orders as (

    select *

    from {{ref('stg_orders')}}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders


    from orders

    group by 1

),
amount_per_customer as (
    SELECT customer_id, sum(amount) as total_amount from {{ref('fct_orders')}}
    GROUP BY customer_id
),

pre_final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

),
final as (
    SELECT pre_final.*, 
           amount_per_customer.total_amount as lifetime_value
    FROM pre_final inner join amount_per_customer on pre_final.customer_id = amount_per_customer.customer_id
)


select * from final