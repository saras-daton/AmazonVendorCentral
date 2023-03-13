{% if var("VendorInventoryReportBySourcing") %} {{ config(enabled=True) }}
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
    {{set_table_name('%vendorinventoryreportbysourcing')}}    
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
            row_number() over (
                partition by startdate, asin order by _daton_batch_runtime desc
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
                            reportrequesttime,
                            vendorid,
                            marketplacename,
                            marketplaceid,
                            startdate,
                            enddate,
                            asin,
                            openpurchaseorderunits,
                            averagevendorleadtimedays,
                            sellthroughrate,
                            unfilledcustomerorderedunits,
                            {% if target.type == "snowflake" %}
                            netreceivedinventorycost.value:amount::float
                            as netreceivedinventorycost,
                            netreceivedinventorycost.value:currencycode::varchar
                            as currencycode,
                            netreceivedinventoryunits,
                            sellableonhandinventorycost.value:amount::float
                            as sellableonhandinventorycost,
                            sellableonhandinventoryunits,
                            unsellableonhandinventorycost.value:amount::float
                            as unsellableonhandinventorycost,
                            unsellableonhandinventoryunits,
                            aged90plusdayssellableinventorycost.value:amount::float
                            as aged90plusdayssellableinventorycost,
                            aged90plusdayssellableinventoryunits,
                            unhealthyinventorycost.value:amount::float
                            as unhealthyinventorycost,
                            {% else %}
                            netreceivedinventorycost.amount as netreceivedinventorycost,
                            netreceivedinventorycost.currencycode,
                            netreceivedinventoryunits,
                            sellableonhandinventorycost.amount
                            as sellableonhandinventorycost,
                            sellableonhandinventoryunits,
                            unsellableonhandinventorycost.amount
                            as unsellableonhandinventorycost,
                            unsellableonhandinventoryunits,
                            aged90plusdayssellableinventorycost.amount
                            as aged90plusdayssellableinventorycost,
                            aged90plusdayssellableinventoryunits,
                            unhealthyinventorycost.amount as unhealthyinventorycost,
                            {% endif %}
                            unhealthyinventoryunits,
                            procurableproductoutofstockrate,
                            {{ daton_user_id() }} as _daton_user_id,
                            {{ daton_batch_runtime() }} as _daton_batch_runtime,
                            {{ daton_batch_id() }} as _daton_batch_id,
                            current_timestamp() as _last_updated,
                            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
                        from
                            {{ i }}
                            {{ unnesting("netreceivedinventorycost") }}
                            {{ unnesting("sellableonhandinventorycost") }}
                            {{ unnesting("unsellableonhandinventorycost") }}
                            {{ unnesting("aged90PlusDaysSellableInventoryCost") }}
                            {{ unnesting("unhealthyInventoryCost") }}
                        {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        where {{ daton_batch_runtime() }} >= {{ max_loaded }}
                        {% endif %}
                    ) a
                {% if var("currency_conversion_flag") %}
                left join
                    {{ ref("ExchangeRates") }} c
                    on date(a.startdate) = c.date
                    and a.currencycode = c.to_currency_code
                {% endif %}
            )
    )
where row_num = 1
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
