with src_data as (
    SELECT
        cast("Receipt Identifier" as VARCHAR) as receipt_id    --character varying,
        ,cast("Encounter ID" as VARCHAR) as encounter_id    --character varying,
        ,cast("Patient ID" as VARCHAR) as patient_id    --character varying,
        ,cast("Patient Name" as VARCHAR) as patient_nm    --character varying,
        ,TO_DATE("Date of Birth", 'DD/MM/YYYY') as birth_dt    --date,
        ,cast("Gender" as VARCHAR(10)) as gender_cd    --character varying,
        ,cast("Phone Number" as VARCHAR(30)) as phone_num    --character varying,
        ,cast("Email" as VARCHAR) as email_id    --character varying,
        ,cast("Insurance Provider" as VARCHAR) as insurance_provider_nm    --character varying,
        ,cast("Insurance ID" as VARCHAR) as insurance_num    --character varying,
        ,cast("Provider Name" as VARCHAR) as provider_nm    --character varying,
        ,cast("Provider Specialty" as VARCHAR) as provider_specialty_nm    --character varying,
        ,cast("Provider NPI" as VARCHAR(20)) as provider_npi_num    --character varying,
        ,TO_DATE("Encounter Date", 'DD/MM/YYYY') as encounter_dt    --date,
        ,"Encounter Time"::time as encounter_tm    --TIME,
        ,cast("Encounter Type" as VARCHAR(20)) as encounter_type_cd    --character varying,
        ,cast("Diagnosis" as VARCHAR) as diagnosis_desc    --character varying,
        ,cast("ICD-10 Code" as VARCHAR(10)) as icd_10_cd    --character varying,
        ,cast("Treatment" as VARCHAR) as treatment_desc    --character varying,
        ,cast("Procedure Code" as VARCHAR(10)) as procedure_cd    --character varying,
        ,cast("Procedure Description" as VARCHAR) as procedure_desc    --character varying,
        ,cast("Total Charges" as decimal(10,2)) as total_charge_amt    --decimal,
        ,cast("Insurance Coverage" as decimal(10,2)) as insurance_coverage_amt    --decimal,
        ,cast("Patient Responsibility" as decimal(10,2)) as deductible_amt    --decimal,
        ,'seed.medical_receipt_data' as record_source_nm
    FROM {{ source('seeds', 'medical_receipt_data') }}
),
default_value as (
    SELECT
        'Missing' as receipt_id,
        'Missing' as encounter_id,
        'Missing' as patient_id,
        'Missing' as patient_nm,
        '2001-01-01'::date as birth_dt,
        'Missing' as gender_cd,
        'Missing' as phone_num,
        'Missing' as email_id,
        'Missing' as insurance_provider_nm,
        'Missing' as insurance_num,
        'Missing' as provider_nm,
        'Missing' as provider_specialty_nm,
        'Missing' as provider_npi_num,
        '2001-01-01'::date as encounter_dt,
        '00:00:00'::time as encounter_tm,
        'Missing' as encounter_type_cd,
        'Missing' as diagnosis_desc,
        'Missing' as icd_10_cd,
        'Missing' as treatment_desc,
        'Missing' as procedure_cd,
        'Missing' as procedure_desc,
        '0.0'::decimal as total_charge_amt,
        '0.0'::decimal as insurance_coverage_amt,
        '0.0'::decimal as deductible_amt,
        'System.DefaultKey' as record_source_nm
),
with_default_value as (
    SELECT * FROM src_data
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
            'patient_nm',
            'birth_dt',
            'gender_cd',
            'phone_num',
            'email_id',
            'insurance_provider_nm',
            'insurance_num',
            'provider_nm',
            'provider_specialty_nm',
            'provider_npi_num',
            'encounter_dt',
            'encounter_tm',
            'encounter_type_cd',
            'diagnosis_desc',
            'icd_10_cd',
            'treatment_desc',
            'procedure_cd',
            'procedure_desc',
            'total_charge_amt',
            'insurance_coverage_amt',
            'deductible_amt',
            'record_source_nm'
        ]) }} as sgk_medical_receipt_diff,
        *,
        '{{ run_started_at }}'::timestamptz as load_dts
    FROM with_default_value
)
SELECT * FROM hashed_data