-- mart_invoices.sql
-- Final analytical table — one row per invoice_id + line_number, latest status
-- Materialised as table in DuckDB — fast queries, no recomputation
-- This is the source of truth for all analytics / Superset dashboards
--
-- Key design decisions:
--   - COALESCE across variants for fields that exist in both but differ in name/position
--   - Derived metrics computed here (effective_unit_price, has_tax, has_early_pay etc.)
--   - period_date resolved: line-level for DetailedInvoice, header-level for SummaryInvoice
--   - All monetary fields cast to DOUBLE for consistency

{{
    config(
        materialized='table',
        description='Final analytical invoice mart — both variants reconciled, deduplicated, enriched'
    )
}}

with base as (

    select * from {{ ref('int_invoices_deduped') }}

),

reconciled as (

    select
        -- ── Surrogate keys ────────────────────────────────────────────────
        line_sk,
        invoice_sk,
        ingested_at,
        source_file,
        variant,

        -- ── Invoice identity ───────────────────────────────────────────────
        invoice_id,
        invoice_db_id,
        document_date,
        document_type,
        submission_method,
        currency_code,

        -- ── Invoice totals ─────────────────────────────────────────────────
        invoice_total,
        vendor_total,
        line_count,
        invoice_discount_total,

        -- ── Approval / workflow ────────────────────────────────────────────
        action_type,
        action_status,
        action_datetime,

        -- ── Vendor ────────────────────────────────────────────────────────
        vendor_entity_code,
        vendor_entity_code_type,
        vendor_entity_name,
        vendor_location_code,
        vendor_location_name,
        vendor_city,
        vendor_state,
        vendor_country_code,

        -- ── Client ────────────────────────────────────────────────────────
        client_entity_code,
        client_entity_code_type,
        client_entity_name,
        client_location_code,
        client_location_name,
        client_city,
        client_state,
        client_country_code,

        -- ── Line identity ──────────────────────────────────────────────────
        line_number,
        charge_class,
        purchase_category,

        -- ── Period date — resolved across variants ─────────────────────────
        -- DetailedInvoice: line-level period_date
        -- SummaryInvoice:  header-level period_date
        coalesce(line_period_date, header_period_date)  as period_date,
        period_start_date,

        -- ── Service item ───────────────────────────────────────────────────
        service_code,
        product_description,
        product_category,

        -- ── Unit economics ─────────────────────────────────────────────────
        quantity,
        units,
        unit_price,
        line_subtotal,
        line_pretax_total,
        line_total,
        line_discount_total,
        line_discount_rate,

        -- ── Category ──────────────────────────────────────────────────────
        category_code,
        category_scheme,

        -- ── Allocation ────────────────────────────────────────────────────
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

        -- ── Tax ───────────────────────────────────────────────────────────
        tax_type,
        tax_total,
        tax_exempt_code,

        -- ── Early payment ─────────────────────────────────────────────────
        early_pay_due_date,
        early_pay_days_due,
        early_pay_eligible,

        -- ── Cross reference ───────────────────────────────────────────────
        cross_ref_doc_number,
        cross_ref_doc_type,
        cross_ref_date,

        -- ── Derived metrics ────────────────────────────────────────────────
        -- Effective unit price: use unit_price if available, else derive from total/quantity
        case
            when unit_price is not null and unit_price > 0
                then unit_price
            when line_total is not null and quantity is not null and quantity > 0
                then line_total / quantity
            else null
        end                                             as effective_unit_price,

        -- Effective line total: use line_total if available, else invoice_total (SummaryInvoice single-line)
        coalesce(line_total, invoice_total)             as effective_line_total,

        -- Flags
        (tax_total is not null and tax_total > 0)       as has_tax,
        (early_pay_due_date is not null)                as has_early_payment,
        (alloc_cost_center is not null)                 as has_cost_center,
        (alloc_project_code is not null)                as has_project_code,
        (cross_ref_doc_number is not null)              as has_cross_reference,
        (action_status = 'Approved')                    as is_approved,

        -- Document year / month for partitioned analysis
        cast(left(document_date, 4) as integer)         as document_year,
        cast(substr(document_date, 6, 2) as integer)    as document_month

    from base

)

select * from reconciled
