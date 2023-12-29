with customers as(
    select
        id as customer_id,
        name as first_name,
        email
    from people
)

select *
from customers
