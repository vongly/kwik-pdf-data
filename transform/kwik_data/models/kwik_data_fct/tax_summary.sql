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
        a.tax_type::varchar as tax_type,
        a.tax_collected::numeric as tax_collected,
        a.sales_amount::numeric as sales_amount,
        a.sales_forgiven_amount::numeric as sales_forgiven_amount,
        a.actual_sales::numeric as actual_sales,
        a.exempt_amount::numeric as exempt_amount,
        a.processed_utc as processed_parsed_utc,
        a._dlt_processed_utc as processed_load_utc,
        dense_rank() over(order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'tax_summary_raw') }} a

    join
        {{ ref('daily_reports') }} b
    on
        a.report_id = b.report_id
        /* filters for matching parsed timestamp in daily_reports */
        and a.processed_utc = b.processed_parsed_utc

    where
        /*
            If a PDF is missing a report, a dummy report is
            generated to not break the generated models
            -> if a report is missing, has_report = 0
        */
        a.has_report = 1
)

select
    id,
    report_id,
    store_id,
    report_name,
    tax_type,
    tax_collected,
    sales_amount,
    sales_forgiven_amount,
    actual_sales,
    exempt_amount,
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
