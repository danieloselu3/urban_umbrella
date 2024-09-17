with src_data as (
    SELECT *
    FROM {{ source('receipts_source', 'person') }}
)

SELECT * FROM src_data