with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
),
encounter_data as (
    SELECT *
    FROM {{ ref('ref_encounter') }}
), 
diagnosis_data as (
    SELECT *
    FROM {{ ref('ref_diagnosis') }}
),
procedure_data as (
    SELECT *
    FROM {{ ref('ref_procedure') }}
),
norm_dt as (
    SELECT
        e_dt.patient_id,
        e_dt.encounter_id,
        d_dt.icd_10_cd,
        p_dt.procedure_cd,
        s_dt.treatment_desc,
        s_dt.record_source_nm
    FROM source_data s_dt
    LEFT JOIN encounter_data e_dt
        ON s_dt.encounter_id = e_dt.encounter_id
    LEFT JOIN diagnosis_data d_dt
        ON s_dt.icd_10_cd = d_dt.icd_10_cd
    LEFT JOIN procedure_data p_dt
        ON s_dt.procedure_cd = p_dt.procedure_cd
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['patient_id', 'encounter_id', 'icd_10_cd', 'procedure_cd']) }} as sgk_encounter_treatment_id,
        {{ dbt_utils.generate_surrogate_key([
            'patient_id',
            'encounter_id',
            'icd_10_cd',
            'procedure_cd',
            'treatment_desc',
            'record_source_nm'
        ]) }} as sgk_encounter_treatment_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data