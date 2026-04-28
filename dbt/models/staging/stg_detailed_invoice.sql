-- stg_detailed_invoice.sql
-- Staging model for DetailedInvoice variant
-- Source: invoices.parquet + line_items.parquet, filtered to variant = 'DetailedInvoice'
-- Materialised as view — lightweight, always reflects latest Parquet

{{
    config(
        materialized='view',
        description='Staged DetailedInvoice records — full unit economics, service items, allocations'
    )
}}

with invoices as (

    {% if target.type == 'snowflake' %}
        select * from {{ source('raw', 'invoices') }}
    {% else %}
        select * from read_parquet('{{ env_var("DBT_DUCKDB_PATH", "../output") }}/invoices/invoices.parquet')
    {% endif %}
    where variant = 'DetailedInvoice'

),

line_items as (

    {% if target.type == 'snowflake' %}
        select * from {{ source('raw', 'line_items') }}
    {% else %}
        select * from read_parquet('{{ env_var("DBT_DUCKDB_PATH", "../output") }}/line_items/line_items.parquet')
    {% endif %}
    where variant = 'DetailedInvoice'

),

-- Join header + lines into one wide staging row
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
        inv.document_number,
        inv.document_date,
        inv.document_type,
        inv.submission_method,
        inv.currency_code,
        inv.total                               as invoice_total,
        inv.vendor_total,
        inv.line_count,
        inv.notes                               as invoice_notes,
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
        li.period_start_date,
        li.period_date                          as line_period_date,
        li.notes                                as line_notes,
        li.purchase_category,

        -- ── Service item (DetailedInvoice only) ────────────────────────────
        li.service_code,
        li.product_description,
        li.product_category,

        -- ── Unit economics (DetailedInvoice only) ──────────────────────────
        li.quantity,
        cast(li.units as varchar)              as units,
        li.unit_price,
        li.line_subtotal,
        li.line_pretax_total,
        li.line_total,
        li.discount_total                       as line_discount_total,
        li.discount_rate                        as line_discount_rate,

        -- ── Category ──────────────────────────────────────────────────────
        li.category_code,
        li.category_scheme,

        -- ── Allocation ────────────────────────────────────────────────────
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

        -- ── Tax (sparse) ──────────────────────────────────────────────────
        li.tax_type,
        cast(li.tax_total as varchar)          as tax_total,
        li.tax_exempt_code,

        -- ── Early payment (sparse) ────────────────────────────────────────
        li.early_pay_due_date,
        cast(li.early_pay_days_due as varchar) as early_pay_days_due,
        li.early_pay_eligible,

        -- ── Cross reference (sparse) ──────────────────────────────────────
        li.cross_ref_doc_number,
        li.cross_ref_doc_type,
        li.cross_ref_date

    from line_items li
    inner join invoices inv
        on li.invoice_sk = inv.sk

)

select * from staged