with source_data as (
    SELECT *
    FROM {{ ref('stg_medical_receipt_data') }}
), 
norm_dt as (
    SELECT DISTINCT
        procedure_cd,
        procedure_desc,
        record_source_nm
    FROM source_data
    ORDER BY procedure_cd
),
hashed_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['procedure_cd']) }} as sgk_procedure_id,
        {{ dbt_utils.generate_surrogate_key([
            'procedure_cd',
            'procedure_desc',
            'record_source_nm'
        ]) }} as sgk_procedure_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM norm_dt
)
SELECT * FROM hashed_data