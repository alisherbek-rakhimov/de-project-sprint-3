WITH source_dates AS (SELECT date_time::date AS fact_date
                      FROM staging.user_order_log
                      UNION
                      SELECT date_time::date
                      FROM staging.user_activity_log
                      UNION
                      SELECT date_id
                      FROM staging.customer_research),
     all_dates AS (SELECT DISTINCT fact_date
                   FROM source_dates)

INSERT
INTO de.mart.d_calendar (date_id, date_actual, epoch,
                         day_suffix, day_name, day_of_week,
                         day_of_month, day_of_quarter, day_of_year,
                         week_of_month, week_of_year, week_of_year_iso,
                         month_actual, month_name, month_name_abbreviated,
                         quarter_actual, quarter_name,
                         year_actual,
                         first_day_of_week, last_day_of_week,
                         first_day_of_month, last_day_of_month,
                         first_day_of_quarter, last_day_of_quarter,
                         first_day_of_year, last_day_of_year,
                         mmyyyy, mmddyyyy,
                         weekend_indr)
SELECT
    /* ─── Keys & basics ─────────────────────── */
    TO_CHAR(fact_date, 'YYYYMMDD')::int                                    AS date_id,
    fact_date                                                              AS date_actual,
    EXTRACT(EPOCH FROM fact_date)::bigint                                  AS epoch,

    /* ─── Day-level attributes ──────────────── */
    /* Suffix: 1st, 2nd, 3rd, 4th …  */
    CASE
        WHEN EXTRACT(DAY FROM fact_date)::int IN (11, 12, 13) THEN 'th'
        ELSE
            CASE (EXTRACT(DAY FROM fact_date)::int % 10)
                WHEN 1 THEN 'st'
                WHEN 2 THEN 'nd'
                WHEN 3 THEN 'rd'
                ELSE 'th'
                END
        END                                                                AS day_suffix,
    TRIM(TO_CHAR(fact_date, 'FMDay'))                                      AS day_name,
    EXTRACT(ISODOW FROM fact_date)::int                                    AS day_of_week,      -- Mon=1 … Sun=7
    EXTRACT(DAY FROM fact_date)::int                                       AS day_of_month,
    (fact_date::date - DATE_TRUNC('quarter', fact_date)::date)::int + 1    AS day_of_quarter,
    EXTRACT(DOY FROM fact_date)::int                                       AS day_of_year,

    /* ─── Week-level attributes ─────────────── */
    TO_CHAR(fact_date, 'W')::int                                           AS week_of_month,    -- 1-5
    EXTRACT(WEEK FROM fact_date)::int                                      AS week_of_year,     -- 1-53
    TO_CHAR(fact_date, 'IYYY-"W"IW')                                       AS week_of_year_iso, -- e.g. 2025-W21

    /* ─── Month, quarter, year ──────────────── */
    EXTRACT(MONTH FROM fact_date)::int                                     AS month_actual,
    TRIM(TO_CHAR(fact_date, 'FMMonth'))                                    AS month_name,
    TO_CHAR(fact_date, 'Mon')                                              AS month_name_abbreviated,
    EXTRACT(QUARTER FROM fact_date)::int                                   AS quarter_actual,
    CASE EXTRACT(QUARTER FROM fact_date)::int
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
        ELSE 'Fourth'
        END                                                                AS quarter_name,
    EXTRACT(YEAR FROM fact_date)::int                                      AS year_actual,

    /* ─── Period boundaries ─────────────────── */
    DATE_TRUNC('week', fact_date)::date                                    AS first_day_of_week,
    (DATE_TRUNC('week', fact_date) + INTERVAL '6 days')::date              AS last_day_of_week,
    DATE_TRUNC('month', fact_date)::date                                   AS first_day_of_month,
    (DATE_TRUNC('month', fact_date) + INTERVAL '1 month - 1 day')::date    AS last_day_of_month,
    DATE_TRUNC('quarter', fact_date)::date                                 AS first_day_of_quarter,
    (DATE_TRUNC('quarter', fact_date) + INTERVAL '3 months - 1 day')::date AS last_day_of_quarter,
    DATE_TRUNC('year', fact_date)::date                                    AS first_day_of_year,
    (DATE_TRUNC('year', fact_date) + INTERVAL '1 year - 1 day')::date      AS last_day_of_year,

    /* ─── Handy text keys ───────────────────── */
    TO_CHAR(fact_date, 'MMYYYY')                                           AS mmyyyy,
    TO_CHAR(fact_date, 'MMDDYYYY')                                         AS mmddyyyy,

    /* ─── Flags ─────────────────────────────── */
    (EXTRACT(ISODOW FROM fact_date) IN (6, 7))                             AS weekend_indr
FROM all_dates
where TO_CHAR(fact_date, 'YYYYMMDD')::int not in (select date_id from mart.d_calendar);
