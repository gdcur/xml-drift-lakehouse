-- int_invoices_deduped.sql
-- Deduplication layer — one row per invoice_id, latest version wins
-- Unions both variants first, then deduplicates on invoice_id
-- ordered by action_datetime DESC (latest approval/status wins)
-- Deduplication uses QUALIFY (DuckDB native) — no subquery needed
-- Materialised as view — no storage cost, always reflects latest staging

{{
    config(
        materialized='view',
        description='Deduplicated invoices — one row per invoice_id, latest status, both variants unioned'
    )
}}

with detailed as (

    select
        line_sk,
        invoice_sk,
        invoice_sk_header,
        ingested_at,
        source_file,
        variant,
        invoice_id,
        document_number,
        document_date,
        document_type,
        submission_method,
        currency_code,
        invoice_total,
        vendor_total,
        line_count,
        invoice_notes,
        invoice_discount_total,
        action_type,
        action_status,
        action_datetime,
        vendor_entity_code,
        vendor_entity_code_type,
        vendor_entity_name,
        vendor_location_code,
        vendor_location_name,
        vendor_city,
        vendor_state,
        vendor_country_code,
        client_entity_code,
        client_entity_code_type,
        client_entity_name,
        client_location_code,
        client_location_name,
        client_city,
        client_state,
        client_country_code,
        line_number,
        charge_class,
        period_start_date,
        line_period_date,
        line_notes,
        purchase_category,
        service_code,
        product_description,
        product_category,
        quantity,
        units,
        unit_price,
        line_subtotal,
        line_pretax_total,
        line_total,
        line_discount_total,
        line_discount_rate,
        category_code,
        category_scheme,
        alloc_rate,
        alloc_total,
        alloc_cost_center,
        alloc_project_code,
        alloc_work_order,
        alloc_order_ref,
        alloc_site_ref_name,
        alloc_account_major,
        alloc_account_minor,
        alloc_account_type,
        tax_type,
        tax_total,
        tax_exempt_code,
        early_pay_due_date,
        early_pay_days_due,
        early_pay_eligible,
        cross_ref_doc_number,
        cross_ref_doc_type,
        cross_ref_date,
        -- SummaryInvoice-only fields — null for DetailedInvoice
        null::varchar                   as header_period_date

    from {{ ref('stg_detailed_invoice') }}

),

summary as (

    select
        line_sk,
        invoice_sk,
        invoice_sk_header,
        ingested_at,
        source_file,
        variant,
        invoice_id,
        document_number,
        document_date,
        document_type,
        submission_method,
        currency_code,
        invoice_total,
        vendor_total,
        line_count,
        invoice_notes,
        invoice_discount_total,
        action_type,
        action_status,
        action_datetime,
        vendor_entity_code,
        vendor_entity_code_type,
        vendor_entity_name,
        vendor_location_code,
        vendor_location_name,
        vendor_city,
        vendor_state,
        vendor_country_code,
        client_entity_code,
        client_entity_code_type,
        client_entity_name,
        client_location_code,
        client_location_name,
        client_city,
        client_state,
        client_country_code,
        line_number,
        charge_class,
        null::varchar                   as period_start_date,
        line_period_date,
        line_notes,
        purchase_category,
        service_code,
        product_description,
        product_category,
        quantity,
        units,
        unit_price,
        line_subtotal,
        line_pretax_total,
        line_total,
        line_discount_total,
        line_discount_rate,
        category_code,
        category_scheme,
        alloc_rate,
        alloc_total,
        alloc_cost_center,
        alloc_project_code,
        alloc_work_order,
        alloc_order_ref,
        alloc_site_ref_name,
        alloc_account_major,
        alloc_account_minor,
        alloc_account_type,
        tax_type,
        tax_total,
        tax_exempt_code,
        early_pay_due_date,
        early_pay_days_due,
        early_pay_eligible,
        cross_ref_doc_number,
        cross_ref_doc_type,
        cross_ref_date,
        header_period_date

    from {{ ref('stg_summary_invoice') }}

),

-- Union both variants
unioned as (

    select * from detailed
    union all
    select * from summary

)

-- Deduplicate using QUALIFY — DuckDB native, no subquery needed
-- latest action_datetime wins per invoice_id + line_number

select *
from unioned
qualify row_number() over (
    partition by invoice_id, line_number
    order by
        action_datetime desc nulls last,
        ingested_at desc
) = 1