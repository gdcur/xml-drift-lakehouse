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

    {% if target.type == 'snowflake' %}
        select * from {{ source('raw', 'invoices') }}
    {% elif env_var("XDL_SOURCE", "parquet") == "iceberg" %}
        -- Iceberg keeps real DATE/TIMESTAMP types; the models expect the text form that the
        -- Parquet path and Snowflake use, so the same SQL runs on every source.
        select * replace (
            cast(document_date as varchar)                as document_date,
            cast(period_date as varchar)                  as period_date,
            strftime(action_datetime, '%Y-%m-%dT%H:%M:%S') as action_datetime,
            strftime(ingested_at at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.%f') || '+00:00' as ingested_at
        )
        from iceberg_scan('{{ env_var("XDL_ICEBERG_WAREHOUSE", "../output/iceberg") }}/lakehouse/invoices',
                                   version_name_format='%s%s.metadata.json', allow_moved_paths=true)
    {% else %}
        select * from read_parquet('{{ env_var("DBT_DUCKDB_PATH", "../output") }}/invoices/invoices.parquet')
    {% endif %}
    where variant = 'SummaryInvoice'

),

line_items as (

    {% if target.type == 'snowflake' %}
        select * from {{ source('raw', 'line_items') }}
    {% elif env_var("XDL_SOURCE", "parquet") == "iceberg" %}
        -- Iceberg keeps real DATE/TIMESTAMP types; the models expect the text form that the
        -- Parquet path and Snowflake use, so the same SQL runs on every source.
        select * replace (
            cast(period_start_date as varchar)  as period_start_date,
            cast(period_date as varchar)        as period_date,
            cast(cross_ref_date as varchar)     as cross_ref_date,
            cast(early_pay_due_date as varchar) as early_pay_due_date,
            strftime(ingested_at at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.%f') || '+00:00' as ingested_at
        )
        from iceberg_scan('{{ env_var("XDL_ICEBERG_WAREHOUSE", "../output/iceberg") }}/lakehouse/line_items',
                                   version_name_format='%s%s.metadata.json', allow_moved_paths=true)
    {% else %}
        select * from read_parquet('{{ env_var("DBT_DUCKDB_PATH", "../output") }}/line_items/line_items.parquet')
    {% endif %}
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
        inv.document_number,
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
        null::varchar                            as units,
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
        null::varchar                            as tax_total,
        null::varchar                           as tax_exempt_code,

        -- ── Early payment — NULL for SummaryInvoice ───────────────────────
        null::varchar                           as early_pay_due_date,
        null::varchar                           as early_pay_days_due,
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
