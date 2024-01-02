with 
source as (
    select * from {{ source('dbt_test', 'invoices') }}
),

staged as (
    select
        id as order_id,
        person_id as customer_id,
        order_date,
        country,
        payment_date,
	    payment_info
    from source
)

select *
from staged
