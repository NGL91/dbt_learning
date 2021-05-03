with payments as (
    SELECT orderid as order_id, 
           amount FROM raw.stripe.payment WHERE STATUS='success'
)
SELECT * FROM payments