from _report_parsing_scripts import (
    card_transaction_counts,
    daily_cashier_stats,
    daily_reports,
    daily_sales_summary,
    department_sales,
    fuel_dispensers_sales,
    fuel_sales_by_grade,
    paid_in_out,
    safe_drops,
    tax_summary,
    tender_summary,
)

parse_functions = [
    card_transaction_counts,
    daily_cashier_stats,
    daily_reports,
    daily_sales_summary,
    department_sales,
    fuel_dispensers_sales,
    fuel_sales_by_grade,
    paid_in_out,
    safe_drops,
    tax_summary,
    tender_summary,
]

PARSE_FUNCTIONS = [{'name': pf.__name__.split('.')[-1], 'function': pf.parse_pdf} for pf in parse_functions]
