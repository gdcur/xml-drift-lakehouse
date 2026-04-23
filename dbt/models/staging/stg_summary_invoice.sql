-- stg_summary_invoice.sql
-- Staging model for SummaryInvoice variant
-- Source: invoices.parquet + line_items.parquet, filtered to variant = 'SummaryInvoice'
-- Key structural differences vs DetailedInvoice:
--   - No service item (product_description, service_code are null)
--   - No unit economics (line_subtotal, line_pretax_total are null)
--   - Allocation lives at header level → propagated to all lines by parser.py
--   - PeriodDate at header level (not line level)

{{
    config(
        materialized='view',
        description='Staged SummaryInvoice records — lean lines, header-level allocation'
    )
}}

with invoices as (

    select *
    from read_parquet('{{ env_var("DBT_PARQUET_PATH", "../output") }}/invoices/invoices.parquet')
    where variant = 'SummaryInvoice'

),

line_items as (

    select *
    from read_parquet('{{ env_var("DBT_PARQUET_PATH", "../output") }}/line_items/line_items.parquet')
    where variant = 'SummaryInvoice'

),

staged as (

    select
        -- ── Surrogate keys ────────────────────────────────────────────────
        li.sk                                   as line_sk,
        li.invoice_sk,
        inv.sk                                  as invoice_sk_header,

        -- ── Audit fields ──────────────────────────────────────────────────
        inv.ingested_at,
        inv.source_file,
        inv.variant,

        -- ── Invoice header ─────────────────────────────────────────────────
        inv.invoice_id,
        inv.invoice_db_id,
        inv.document_date,
        inv.document_type,
        inv.submission_method,
        inv.currency_code,
        inv.total                               as invoice_total,
        inv.vendor_total,
        inv.line_count,
        inv.notes                               as invoice_notes,
        inv.period_date                         as header_period_date,
        inv.discount_total                      as invoice_discount_total,

        -- ── Action / approval ──────────────────────────────────────────────
        inv.action_type,
        inv.action_status,
        inv.action_datetime,

        -- ── Vendor ────────────────────────────────────────────────────────
        inv.vendor_entity_code,
        inv.vendor_entity_code_type,
        inv.vendor_entity_name,
        inv.vendor_location_code,
        inv.vendor_location_name,
        inv.vendor_city,
        inv.vendor_state,
        inv.vendor_country_code,

        -- ── Client ────────────────────────────────────────────────────────
        inv.client_entity_code,
        inv.client_entity_code_type,
        inv.client_entity_name,
        inv.client_location_code,
        inv.client_location_name,
        inv.client_city,
        inv.client_state,
        inv.client_country_code,

        -- ── Line item ──────────────────────────────────────────────────────
        li.line_number,
        li.charge_class,
        li.period_date                          as line_period_date,
        li.notes                                as line_notes,
        li.purchase_category,
        -- li.requested_by,  -- TODO: add to parser.py

        -- ── Service item — NULL for SummaryInvoice ─────────────────────────
        null::varchar                           as service_code,
        null::varchar                           as product_description,
        null::varchar                           as product_category,

        -- ── Unit economics — NULL for SummaryInvoice ───────────────────────
        li.quantity,
        li.unit_price,
        null::double                            as units,
        null::double                            as line_subtotal,
        null::double                            as line_pretax_total,
        li.line_total,
        li.discount_total                       as line_discount_total,
        li.discount_rate                        as line_discount_rate,

        -- ── Category ──────────────────────────────────────────────────────
        li.category_code,
        li.category_scheme,

        -- ── Allocation (propagated from header by parser.py) ───────────────
        li.alloc_rate,
        li.alloc_total,
        li.alloc_cost_center,
        li.alloc_project_code,
        li.alloc_work_order,
        li.alloc_order_ref,
        li.alloc_site_ref_name,
        li.alloc_account_major,
        li.alloc_account_minor,
        li.alloc_account_type,

        -- ── Tax — NULL for SummaryInvoice ─────────────────────────────────
        null::varchar                           as tax_type,
        null::double                            as tax_total,
        null::varchar                           as tax_exempt_code,

        -- ── Early payment — NULL for SummaryInvoice ───────────────────────
        null::varchar                           as early_pay_due_date,
        null::integer                           as early_pay_days_due,
        null::varchar                           as early_pay_eligible,

        -- ── Cross reference — NULL for SummaryInvoice ─────────────────────
        null::varchar                           as cross_ref_doc_number,
        null::varchar                           as cross_ref_doc_type,
        null::varchar                           as cross_ref_date

    from line_items li
    inner join invoices inv
        on li.invoice_sk = inv.sk

)

select * from staged
