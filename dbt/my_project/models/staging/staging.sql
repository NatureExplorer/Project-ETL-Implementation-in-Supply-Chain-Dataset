{{
    config(
        materialized='table',
        unique_key='id'

    )
}}


select * from {{source('cuaca_schema', 'cuaca_data')}}