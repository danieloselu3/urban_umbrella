with source_data as (
    SELECT *
    FROM {{ ref('ref_billing') }}
)
SELECT * FROM source_data
