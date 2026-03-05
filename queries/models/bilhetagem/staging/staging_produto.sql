{{
    config(
        materialized="table",
        alias="produto",
    )
}}
select 1 / 0
