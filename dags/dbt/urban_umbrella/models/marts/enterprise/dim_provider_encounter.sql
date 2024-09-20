with source_data as (
    SELECT *
    FROM {{ ref('ref_provider_encounter') }}
)
SELECT * FROM source_data