with source_data as (
    SELECT *
    FROM {{ ref('ref_encounter_treatment') }}
)
SELECT * FROM source_data