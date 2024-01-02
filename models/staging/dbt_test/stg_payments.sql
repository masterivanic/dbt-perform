with 
source as (
    select * from {{ source('dbt_test', 'lineitems') }}
),

payments_data as(
    select * from {{ ref('stg_orders') }}
),

lineitems as( 
    select
        invoice_id,
        quantity * price as amount
    from source
),

payments as (
    select
        payments_data.order_id,
        payments_data.payment_date,
        payments_data.payment_info,
        lineitems.amount as amount
    from payments_data
    inner join lineitems on payments_data.order_id = lineitems.invoice_id
)

select * from payments