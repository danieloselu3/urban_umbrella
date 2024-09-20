with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT
        patient_id,
        encounter_id,
        encounter_type_cd,
        encounter_dt,
        encounter_tm,
        (encounter_dt || ' ' || encounter_tm)::TIMESTAMP AS encounter_dts,
        record_source_nm
    FROM source_data
    ORDER BY encounter_id
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['patient_id','encounter_id']) }} as sgk_encounter_id,
        {{ dbt_utils.generate_surrogate_key([
            'patient_id',
            'encounter_id',
            'encounter_type_cd',
            'encounter_dt',
            'encounter_tm',
            'encounter_dts',
            'record_source_nm'
        ]) }} as sgk_encounter_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data