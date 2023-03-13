{% if var("RetailProcurementOrdersStatus") %} {{ config(enabled=True) }}
{% else %} {{ config(enabled=False) }}
{% endif %}
  
{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}


{% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
    {% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%} {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %} {% set max_loaded = 0 %}
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
{% else %} {% set results_list = [] %} {% set tables_lowercase_list = [] %}
{% endif %}


{% for i in results_list %}
{% if var("get_brandname_from_tablename_flag") %}
{% set brand = i.split(".")[2].split("_")[var("brandname_position_in_tablename")] %}
{% else %} {% set brand = var("default_brandname") %}
{% endif %}

{% if var("get_storename_from_tablename_flag") %}
{% set store = i.split(".")[2].split("_")[var("storename_position_in_tablename")] %}
{% else %} {% set store = var("default_storename") %}
{% endif %}

{% if var("timezone_conversion_flag") and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
{% set hr = var("raw_table_timezone_offset_hours")[i] %}
{% else %} {% set hr = 0 %}
{% endif %}

select * {{ exclude() }} (row_num)
from
    (
        select
            *,
            dense_rank() over (
                partition by
                    purchaseordernumber,
                    purchaseorderstatus,
                    date(purchaseorderdate),
                    buyerproductidentifier
                order by _daton_batch_runtime desc
            ) row_num
        from
            (
                select
                    a.* {{ exclude() }} (_daton_user_id, _daton_batch_runtime, _daton_batch_id, _last_updated, _run_id),
                    {% if var("currency_conversion_flag") %}
                    case
                        when c.value is null then 1 else c.value
                    end as exchange_currency_rate,
                    case
                        when c.from_currency_code is null
                        then a.currencycode
                        else c.from_currency_code
                    end as exchange_currency_code,
                    {% else %}
                    cast(1 as decimal) as exchange_currency_rate,
                    cast(a.currencycode as string) as exchange_currency_code,
                    {% endif %}
                    a._daton_user_id, a._daton_batch_runtime, a._daton_batch_id, a._last_updated, a._run_id
                from
                    (
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
                            cast(
                                {{
                                    dbt.dateadd(
                                        datepart="hour",
                                        interval=hr,
                                        from_date_or_timestamp="cast(purchaseOrderDate as timestamp)",
                                    )
                                }}
                                as {{ dbt.type_timestamp() }}
                            ) as purchaseorderdate,
                            cast(
                                {{
                                    dbt.dateadd(
                                        datepart="hour",
                                        interval=hr,
                                        from_date_or_timestamp="cast(lastUpdatedDate as timestamp)",
                                    )
                                }}
                                as {{ dbt.type_timestamp() }}
                            ) as lastupdateddate,
                            sellingparty,
                            shiptoparty,
                            {% if target.type == "snowflake" %}
                            itemstatus.value:itemsequencenumber::int
                            as itemsequencenumber,
                            itemstatus.value:buyerproductidentifier::varchar
                            as buyerproductidentifier,
                            itemstatus.value:vendorproductidentifier::varchar
                            as vendorproductidentifier,
                            netcost.value:currencycode::varchar as currencycode,
                            netcost.value:amount::float as netcost_amount,
                            listprice.value:amount::float as listprice_amount,
                            nestedorderedquantity.value:amount::float
                            as ordered_quantity_amount,
                            nestedorderedquantity.value:unitofmeasure::varchar
                            as unitofmeasure,
                            nestedorderedquantity.value:unitsize::int as unitsize,
                            orderedquantity.value:orderedquantitydetails::varchar
                            as orderedquantitydetails,
                            acknowledgementstatus.value:confirmationstatus::varchar
                            as confirmationstatus,
                            acceptedquantity.value:amount::varchar
                            as accepted_quantity_amount,
                            rejectedquantity.value:amount::varchar
                            as rejected_quantity_amount,
                            acknowledgementstatus.value:acknowledgementstatusdetails
                            ::varchar as acknowledgementstatusdetails,
                            receivingstatus.value:receivestatus::varchar
                            as receivestatus,
                            receivedquantity.value:amount::float
                            as received_quantity_amount,
                            receivingstatus.value:lastreceivedate::date
                            as lastreceivedate,
                            {% else %}
                            itemstatus.itemsequencenumber,
                            itemstatus.buyerproductidentifier,
                            itemstatus.vendorproductidentifier,
                            netcost.currencycode,
                            netcost.amount as netcost_amount,
                            listprice.amount as listprice_amount,
                            nestedorderedquantity.amount as ordered_quantity_amount,
                            nestedorderedquantity.unitofmeasure,
                            nestedorderedquantity.unitsize,
                            orderedquantity.orderedquantitydetails,
                            acknowledgementstatus.confirmationstatus,
                            acceptedquantity.amount as accepted_quantity_amount,
                            rejectedquantity.amount as rejected_quantity_amount,
                            acknowledgementstatus.acknowledgementstatusdetails,
                            receivingstatus.receivestatus,
                            receivedquantity.amount as received_quantity_amount,
                            receivingstatus.lastreceivedate,
                            {% endif %}
                            {{ daton_user_id() }} as _daton_user_id,
                            {{ daton_batch_runtime() }} as _daton_batch_runtime,
                            {{ daton_batch_id() }} as _daton_batch_id,
                            current_timestamp() as _last_updated,
                            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
                        from
                            {{ i }}
                            {{ unnesting("itemstatus") }}
                            {{ multi_unnesting("itemstatus", "netcost") }}
                            {{ multi_unnesting("itemstatus", "listprice") }}
                            {{ multi_unnesting("itemstatus", "orderedquantity") }}
                            {{ multi_unnesting("itemstatus", "acknowledgementstatus") }}
                            {{
                                multi_unnesting(
                                    "acknowledgementstatus",
                                    "acknowledgementstatusdetails"
                                )
                            }}
                            {{
                                multi_unnesting(
                                    "acknowledgementstatus", "acceptedquantity"
                                )
                            }}
                            {{
                                multi_unnesting(
                                    "acknowledgementstatus", "rejectedquantity"
                                )
                            }}
                            {{ multi_unnesting("itemstatus", "receivingstatus") }}
                            {{ multi_unnesting("receivingstatus", "receivedQuantity") }}
                            {% if target.type == "snowflake" %}
                            ,
                            lateral flatten(
                                input
                                => parse_json(orderedquantity.value:orderedquantity)
                            ) nestedorderedquantity
                        {% else %}
                        left join
                            unnest(
                                orderedquantity.orderedquantity
                            ) nestedorderedquantity
                        {% endif %}
                        {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        where {{ daton_batch_runtime() }} >= {{ max_loaded }}
                        {% endif %}
                    ) a
                {% if var("currency_conversion_flag") %}
                left join
                    {{ ref("ExchangeRates") }} c
                    on date(a.purchaseorderdate) = c.date
                    and a.currencycode = c.to_currency_code
                {% endif %}
            )
    )
where row_num = 1
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
