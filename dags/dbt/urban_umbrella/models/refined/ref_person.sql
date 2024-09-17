with src_data as (
    SELECT *
    FROM {{ ref('stg_person') }}
)

SELECT * FROM src_data