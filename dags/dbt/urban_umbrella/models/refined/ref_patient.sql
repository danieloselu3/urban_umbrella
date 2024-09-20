with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT DISTINCT
        patient_id,
        patient_nm,
        birth_dt,
        gender_cd,
        phone_num,
        email_id,
        insurance_provider_nm,
        insurance_num,
        record_source_nm
    FROM source_data
    ORDER BY patient_id
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as sgk_patient_id,
        {{ dbt_utils.generate_surrogate_key([
            'patient_id',
            'patient_nm',
            'birth_dt',
            'gender_cd',
            'phone_num',
            'email_id',
            'insurance_provider_nm',
            'insurance_num',
            'record_source_nm'
        ]) }} as sgk_patient_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data