with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT
        procedure_cd,
        procedure_desc
    FROM source_data
),
default_value as (
    SELECT
        'Missing' as receipt_id,
        'Missing' as encounter_id,
        'Missing' as patient_id,
),
with_default_value as (
    SELECT * FROM norm_dt
    UNION ALL
    SELECT * FROM default_value
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['receipt_id']) }} as sgk_medical_receipt_id,
        {{ dbt_utils.generate_surrogate_key([
            'receipt_id',
            'encounter_id',
            'patient_id',
        ]) }} as sgk_medical_receipt_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM with_default_value
)
SELECT * FROM hashed_data