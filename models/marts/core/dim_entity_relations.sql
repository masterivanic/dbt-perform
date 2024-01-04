with 
line_source as (
    select * from {{ source('dbt_test', 'lineitems') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

lineitems as( 
    select
        invoice_id,
        quantity * price as amount
    from line_source
),

Entity_relations as (
    select
        customers.customer_id,
        orders.order_id,
        lineitems.invoice_id
    from customers
    left join orders
        on customers.customer_id = orders.customer_id
    left join lineitems
        on lineitems.invoice_id = orders.order_id
)

select *
from Entity_relations