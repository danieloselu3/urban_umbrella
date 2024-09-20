{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )
    }}
),
date_dimension AS (
    SELECT
        date_day AS date_key,
        date_day,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        DATE_TRUNC('week', date_day) AS week_start_date,
        DATE_TRUNC('month', date_day) AS month_start_date,
        DATE_TRUNC('quarter', date_day) AS quarter_start_date,
        DATE_TRUNC('year', date_day) AS year_start_date,
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_spine
)

SELECT * FROM date_dimension