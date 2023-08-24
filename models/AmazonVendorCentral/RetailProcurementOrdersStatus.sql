{% if var("RetailProcurementOrdersStatus") %} 
    {{ config( 
        enabled = True,
        post_hook = "drop table {{this|replace('RetailProcurementOrdersStatus', 'RetailProcurementOrdersStatus_temp')}}"
    ) }}
{% else %}
    {{ config(enabled=False) }}
{% endif %}

  
{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}


{% if is_incremental() %}
{%- set max_loaded_query -%}
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%} 
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %} 
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%retailprocurementordersstatus')}}    
{% endset %} 

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %} 
{% set results_list = [] %} {% set tables_lowercase_list = [] %}
{% endif %}


{% for i in results_list %}
    {% if var("get_brandname_from_tablename_flag") %}
        {% set brand = i.split(".")[2].split("_")[var("brandname_position_in_tablename")] %}
    {% else %} 
        {% set brand = var("default_brandname") %}
    {% endif %}

    {% if var("get_storename_from_tablename_flag") %}
        {% set store = i.split(".")[2].split("_")[var("storename_position_in_tablename")] %}
    {% else %}
        {% set store = var("default_storename") %}
    {% endif %}

    {% if var("timezone_conversion_flag") and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
        {% set hr = var("raw_table_timezone_offset_hours")[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    {% if i==results_list[0] %}
        {% set action1 = 'create or replace table' %}
        {% set tbl = this ~ ' as ' %}
    {% else %}
        {% set action1 = 'insert into ' %}
        {% set tbl = this %}
    {% endif %}

    {%- set query -%}
    {{action1}}
    {{tbl|replace('RetailProcurementOrdersStatus', 'RetailProcurementOrdersStatus_temp')}}

    select a.* {{ exclude() }} (_daton_user_id, _daton_batch_runtime, _daton_batch_id, _last_updated, _run_id),
    {% if var("currency_conversion_flag") %}
        case when c.value is null then 1 else c.value end as exchange_currency_rate,
        case when c.from_currency_code is null then a.netcost_currencycode else c.from_currency_code end as exchange_currency_code,
    {% else %}
        cast(1 as decimal) as exchange_currency_rate,
        cast(a.netcost_currencycode as string) as exchange_currency_code,
    {% endif %}
    a._daton_user_id,
    a._daton_batch_runtime,
    a._daton_batch_id,
    a._last_updated,
    a._run_id
    from (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        requeststartdate,
        requestenddate,
        marketplacename,
        marketplaceid,
        vendorid,
        purchaseordernumber,
        purchaseorderstatus,
        cast({{dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(purchaseOrderDate as timestamp)")}} as {{ dbt.type_timestamp() }}) as purchaseorderdate,
        cast({{dbt.dateadd(datepart="hour", interval=hr, from_date_or_timestamp="cast(lastUpdatedDate as timestamp)")}} as {{ dbt.type_timestamp() }}) as lastupdateddate,
        {{extract_nested_value("sellingParty","partyId","string")}} as sellingParty_partyId,
        {{extract_nested_value("sellingParty","address","string")}} as sellingParty_address,
        {{extract_nested_value("sellingParty","taxInfo","string")}} as sellingParty_taxInfo,
        shiptoparty,
        {{extract_nested_value("itemstatus","itemsequencenumber","integer")}} as itemstatus_itemsequencenumber,
        {{extract_nested_value("itemstatus","buyerproductidentifier","string")}} as itemstatus_buyerproductidentifier,
        {{extract_nested_value("itemstatus","vendorproductidentifier","string")}} as itemstatus_vendorproductidentifier,
        {{extract_nested_value("netcost","currencycode","string")}} as netcost_currencycode,
        {{extract_nested_value("netcost","amount","numeric")}} as netcost_amount,
        {{extract_nested_value("listprice","currencycode","string")}} as listprice_currencycode,
        {{extract_nested_value("listprice","amount","numeric")}} as listprice_amount,
        {{extract_nested_value("orderedquantity","amount","integer")}} as orderedquantity_amount,
        {{extract_nested_value("orderedquantity","unitofmeasure","string")}} as orderedquantity_unitofmeasure,
        {{extract_nested_value("orderedquantity","unitsize","integer")}} as orderedquantity_unitsize,
        {{extract_nested_value("acknowledgementstatus","confirmationstatus","string")}} as acknowledgementstatus_confirmationstatus,
        {{extract_nested_value("acceptedquantity","amount","integer")}} as acceptedquantity_amount,
        {{extract_nested_value("acceptedquantity","unitofmeasure","string")}} as acceptedquantity_unitofmeasure,
        {{extract_nested_value("acceptedquantity","unitsize","integer")}} as acceptedquantity_unitsize,
        {{extract_nested_value("rejectedquantity","amount","integer")}} as rejectedquantity_amount,
        {{extract_nested_value("rejectedquantity","unitofmeasure","string")}} as rejectedquantity_unitofmeasure,
        {{extract_nested_value("rejectedquantity","unitsize","integer")}} as rejectedquantity_unitsize,
        {{extract_nested_value("receivingstatus","receivestatus","string")}} as receivingstatus_receivestatus,
        {{extract_nested_value("receivedquantity","amount","integer")}} as receivedquantity_amount,
        {{extract_nested_value("receivedquantity","unitofmeasure","string")}} as receivedquantity_unitofmeasure,
        {{extract_nested_value("receivedquantity","unitsize","integer")}} as receivedquantity_unitsize,
        {{extract_nested_value("receivingstatus","lastreceivedate","timestamp")}} as receivingstatus_lastreceivedate,
        {{ daton_user_id() }} as _daton_user_id,
        {{ daton_batch_runtime() }} as _daton_batch_runtime,
        {{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from {{i}}
            {{ unnesting("itemstatus") }}
            {{ unnesting("sellingParty") }}
            {{ multi_unnesting("itemstatus", "netcost") }}
            {{ multi_unnesting("itemstatus", "listprice") }}
            {{ multi_unnesting_custom("itemstatus", "orderedquantity") }} as nested_orderedquantity
            {{ multi_unnesting("nested_orderedquantity", "orderedquantity") }}
            {{ multi_unnesting("itemstatus", "acknowledgementstatus") }}
            {{ multi_unnesting("acknowledgementstatus", "acceptedquantity") }}
            {{ multi_unnesting("acknowledgementstatus", "rejectedquantity") }}
            {{ multi_unnesting("itemstatus", "receivingstatus") }}
            {{ multi_unnesting("receivingstatus", "receivedQuantity") }}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{ daton_batch_runtime() }} >= {{ max_loaded }}
            {% endif %}
            ) a
            {% if var("currency_conversion_flag") %}
                left join {{ ref("ExchangeRates") }} c on date(a.purchaseorderdate) = c.date and a.netcost_currencycode = c.to_currency_code
            {% endif %}
            qualify dense_rank() over (partition by a.purchaseordernumber, date(a.purchaseorderdate), a.itemstatus_buyerproductidentifier order by _daton_batch_runtime desc) = 1

    {% endset %}

    {% do run_query(query) %}

{% endfor %}
select * from {{this|replace('RetailProcurementOrdersStatus', 'RetailProcurementOrdersStatus_temp')}}    
