with stage as (
    select
        md5(
            a.report_id::text ||
            a.report_name::text ||
            a.row_index::text
        ) as id,
        a.report_id,
        a.store_id,
        a.report_name,
        a.grade_number,
        a.grade_name,
        a.volume,
        a.sales_amount,
        a.percent_of_total,
        a.processed_utc as processed_parsed_utc,
        a._dlt_processed_utc as processed_load_utc,
        dense_rank() over(order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'fuel_sales_by_grade_raw') }} a

    join
        {{ ref('daily_reports') }} b
    on
        a.report_id = b.report_id
        /* filters for matching parsed timestamp in daily_reports */
        and a.processed_utc = b.processed_parsed_utc
)

select
    id::varchar as id,
    report_id::varchar as report_id,
    store_id::int as store_id,
    report_name::varchar as report_name,
    grade_number::int as grade_number,
    grade_name::varchar as grade_name,
    volume::numeric as volume,
    sales_amount::numeric as sales_amount,
    percent_of_total::numeric / 100 as percent_of_total,
    processed_parsed_utc::timestamp as processed_parsed_utc,
    processed_load_utc::timestamp as processed_load_utc
from
    stage
where
    /*
        In case of duplicate loads of the same parsed data
        - Filters for most recent load processed timestamp
    */
    load_order = 1