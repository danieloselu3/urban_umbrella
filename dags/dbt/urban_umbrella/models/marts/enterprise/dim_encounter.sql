with source_data as (
    SELECT *
    FROM {{ ref('ref_encounter') }}
)
SELECT * FROM source_data