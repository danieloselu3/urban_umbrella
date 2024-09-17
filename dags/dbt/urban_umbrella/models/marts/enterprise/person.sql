with src_data as (
    SELECT *
    FROM {{ ref('ref_person') }}
)

SELECT * FROM src_data