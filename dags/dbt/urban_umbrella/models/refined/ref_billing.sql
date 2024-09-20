with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT
        patient_id,
        encounter_id,
        receipt_id,
        insurance_coverage_amt,
        deductible_amt,
        total_charge_amt,
        record_source_nm
    FROM source_data
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['patient_id','encounter_id','receipt_id',]) }} as sgk_billing_id,
        {{ dbt_utils.generate_surrogate_key([
            'patient_id',
            'encounter_id',
            'receipt_id',
            'insurance_coverage_amt',
            'deductible_amt',
            'total_charge_amt',
            'record_source_nm'
        ]) }} as sgk_billing_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data