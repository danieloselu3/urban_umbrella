with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT
        provider_npi_num,
        provider_nm,
        provider_specialty_nm,
        record_source_nm
    FROM source_data
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['provider_npi_num']) }} as sgk_provider_id,
        {{ dbt_utils.generate_surrogate_key([
            'provider_npi_num',
            'provider_nm',
            'provider_specialty_nm',
            'record_source_nm'
        ]) }} as sgk_provider_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data