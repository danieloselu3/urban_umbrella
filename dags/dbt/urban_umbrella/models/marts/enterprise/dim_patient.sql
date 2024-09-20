with source_data as (
    SELECT *
    FROM {{ ref('ref_patient') }}
)
SELECT * FROM source_data