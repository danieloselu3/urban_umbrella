with source_data as (
    SELECT *
    FROM {{ ref('dim_encounter') }}
),
billing_data as (
    SELECT *
    FROM {{ ref('dim_billing') }}
),
date_data as (
    SELECT *
    FROM {{ ref('dim_date') }}
),
encounter_treatment_data as (
    SELECT *
    FROM {{ ref('dim_encounter_treatment') }}
),
patient_data as (
    SELECT *
    FROM {{ ref('dim_patient') }}
),
provider_encounter as (
    SELECT *
    FROM {{ ref('dim_provider_encounter') }}
), norm_dt as (
SELECT 
    ROW_NUMBER() OVER () AS fact_patient_visit_id,
    sgk_billing_id,
    sgk_encounter_treatment_id,
    sgk_encounter_id,
    sgk_patient_id,
    sgk_provider_encounter_id,
    date_key,
    bill_dt.insurance_coverage_amt,
    bill_dt.deductible_amt,
    bill_dt.total_charge_amt
FROM source_data s_dt
LEFT JOIN patient_data pat_dt
    ON s_dt.patient_id = pat_dt.patient_id
LEFT JOIN billing_data bill_dt
    ON s_dt.encounter_id = bill_dt.encounter_id
LEFT JOIN date_data d_dt
    ON s_dt.encounter_dt = d_dt.date_day
LEFT JOIN encounter_treatment_data e_t_dt
    ON s_dt.encounter_id = e_t_dt.encounter_id
LEFT JOIN provider_encounter prov_dt
    ON s_dt.encounter_id = prov_dt.encounter_id
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['fact_patient_visit_id']) }} as sgk_fact_patient_visit_id,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data