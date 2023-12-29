with orders as (
    select
        id as order_id,
        person_id as customer_id,
        order_date,
        country,
        payment_date,
	    payment_info
    from invoices
)

select *
from orders
