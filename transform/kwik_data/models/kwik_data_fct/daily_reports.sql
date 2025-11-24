with stage as (
    select
        md5(
            a.report_id::text ||
            a.report_name::text ||
            a.row_index::text
        ) as id,
        a.report_id,
        a.store_id,
        a.original_filename,
        a.report_name,
        a.report_date,
        a.operator_name,
        a.operator_id,
        a.software_version,
        a.report_printed_at,
        a.report_period_start,
        a.report_period_end,
        a.processed_utc as processed_parsed_utc,
        _dlt_processed_utc as processed_load_utc,
        row_number() over(partition by a.report_id order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'daily_reports_raw') }} a
)

select
    id,
    report_id,
    store_id,
    original_filename,
    report_name,
    report_date,
    operator_name,
    operator_id,
    software_version,
    report_printed_at,
    report_period_start,
    report_period_end,
    processed_parsed_utc,
    processed_load_utc
from
    stage
where
    load_order = 1