with 
payments as (
    select *
    from {{ ref('stg_payments') }}
),

payment_data as (
    select
        order_id,
        sum(amount) as total_amount
    from payments
    group by 1
),

final as (
    select
        payment_data.order_id,
        payment_data.total_amount
    from payment_data
    group by 1
    having not(payment_data.total_amount >= 1200)
)

select * 
from final
