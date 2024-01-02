with 
source as (
    select * from {{ source('dbt_test', 'people') }}
),

staged as (
    select
        id as customer_id,
        name as first_name,
        email
    from source
)

select *
from staged

