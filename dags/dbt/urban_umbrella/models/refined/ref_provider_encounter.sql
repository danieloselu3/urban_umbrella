with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
encounter_data as (
    SELECT *
    FROM {{ ref('ref_encounter') }}
), 
provider_data as (
    SELECT *
    FROM {{ ref('ref_provider') }}
),
norm_dt as (
    SELECT
        p_dt.provider_npi_num,
        e_dt.encounter_id,
        e_dt.patient_id,
        s_dt.record_source_nm
    FROM source_data s_dt
    LEFT JOIN encounter_data e_dt
        ON s_dt.encounter_id = e_dt.encounter_id
    LEFT JOIN provider_data p_dt
        ON s_dt.provider_npi_num = p_dt.provider_npi_num
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['provider_npi_num', 'encounter_id', 'patient_id']) }} as sgk_provider_encounter_id,
        {{ dbt_utils.generate_surrogate_key([
            'provider_npi_num',
            'encounter_id',
            'patient_id',
            'record_source_nm'
        ]) }} as sgk_provider_encounter_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data