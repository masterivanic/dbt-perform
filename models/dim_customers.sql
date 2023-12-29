/*
in view just appear, but 
as table this is create in db
*/

{{
  config(
    materialized = 'table',
    )
}}

with customers as(
    select
        id as customer_id,
        name as first_name,
        email
    from people
),

orders as (
    select
        id as order_id,
        person_id as customer_id,
        order_date,
        country
    from invoices
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as numbers_of_orders

    from orders
    group by 1
),

final as (
    select
        customers.customer_id,
        customers.first_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.numbers_of_orders, 0) as numbers_of_orders
    from customers
    left join customer_orders using(customer_id)
)

select *
from final

