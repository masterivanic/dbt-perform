with 
payments as (
    select *
    from {{ ref('stg_payments') }}
),

_payment as (
        order_id,
        sum(amount) as total_amount
    from payments
),

final as (
    select 10
        _payment.order_id,
        _payment.total_amount
    from _payment
    group by 1
    having not(_payment.total_amount >= 1200)
)

select * from final