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
        a.department_name::varchar as department_name,
        a.gross_sales::numeric as gross_sales,
        a.item_count::int as item_count,
        a.refund_count::int as refund_count,
        a.net_count::int as net_count,
        a.refund_amount::numeric as refund_amount,
        a.discount_amount::numeric as discount_amount,
        a.net_sales::numeric as net_sales,
        a.percent_sales::numeric / 100 as percent_sales,
        a.processed_utc as processed_parsed_utc,
        a._dlt_processed_utc as processed_load_utc,
        dense_rank() over(order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'department_sales_raw') }} a

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
    department_name,
    gross_sales,
    item_count,
    refund_count,
    net_count,
    refund_amount,
    discount_amount,
    net_sales,
    percent_sales,
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
