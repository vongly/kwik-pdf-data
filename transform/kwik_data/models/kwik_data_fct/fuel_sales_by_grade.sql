{% set relation = adapter.get_relation(
    database=target.database,
    schema=source('kwik_data_raw', 'fuel_sales_by_grade_raw').schema,
    identifier=source('kwik_data_raw', 'fuel_sales_by_grade_raw').name
) %}

{% if relation is none %}
    {{ return(None) }}
{% endif %}

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
    id,
    report_id,
    store_id,
    report_name,
    grade_number,
    grade_name,
    volume,
    sales_amount,
    percent_of_total,
    processed_parsed_utc,
    processed_load_utc
from
    stage
where
    /*
        In case of duplicate loads of the same parsed data
        - Filters for most recent load processed timestamp
    */
    load_order = 1