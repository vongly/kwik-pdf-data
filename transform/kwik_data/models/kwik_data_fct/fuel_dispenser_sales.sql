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
        a.dispenser_id,
        a.grade_name,
        a.grade_sales,
        a.sales_in_progress,
        a.ppu_discount,
        a.post_pay_discount,
        a.cash_credit_conversion,
        a.book_sales,
        a.metered_money,
        a.volume_sold,
        a.volume_in_progress,
        a.book_volume,
        a.metered_volume,
        a.processed_utc as processed_parsed_utc,
        a._dlt_processed_utc as processed_load_utc,
        dense_rank() over(order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'fuel_dispenser_sales_raw') }} a

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
    dispenser_id,
    grade_name,
    grade_sales,
    sales_in_progress,
    ppu_discount,
    post_pay_discount,
    cash_credit_conversion,
    book_sales,
    metered_money,
    volume_sold,
    volume_in_progress,
    book_volume,
    metered_volume,
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