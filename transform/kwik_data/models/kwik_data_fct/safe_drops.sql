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
        case
            when a.register_number ~ '^[0-9]+$'
            then a.register_number::int
            when lower(a.register_number) = 'blank'
            then 0
            end as register_number,
        a.cashier_name,
        a.pouch_envelope_number,
        a.drop_amount,
        a.tender_type,
        a.total_drop,
        a.drop_time,
        a.processed_utc as processed_parsed_utc,
        a._dlt_processed_utc as processed_load_utc,
        dense_rank() over(order by a._dlt_processed_utc desc) as load_order
    from
        {{ source('kwik_data_raw', 'safe_drops_raw') }} a

    join
        {{ ref('daily_reports') }} b
    on
        a.report_id = b.report_id
        /* filters for matching parsed timestamp in daily_reports */
        and a.processed_utc = b.processed_parsed_utc
    
    where
        /* checks for  */
        a.register_number ~ '^[0-9]+$' or lower(a.register_number) = 'blank'
)

select
    id,
    report_id,
    store_id,
    report_name,
    register_number,
    cashier_name,
    pouch_envelope_number,
    drop_amount,
    tender_type,
    total_drop,
    drop_time,
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