{% if var("VendorInventoryReportByManufacturing") %} {{ config(enabled=True) }}
{% else %} {{ config(enabled=False) }}
{% endif %}

{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}
{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("VendorInventoryReportByManufacturing_tbl_ptrn","VendorInventoryReportByManufacturing_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}

{% for i in result %}


            select
            "{{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }}" as brand,
            "{{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }}" as store,
            {{timezone_conversion('reportrequesttime')}} as reportrequesttime,
            vendorid,
            marketplacename,
            marketplaceid,
            {{timezone_conversion('startdate')}} as startdate,
            {{timezone_conversion('enddate')}} as enddate,
            asin,
            openpurchaseorderunits,
            averagevendorleadtimedays,
            sellthroughrate,
            unfilledcustomerorderedunits,
            {{extract_nested_value("netReceivedInventoryCost","amount","numeric")}} as netReceivedInventoryCost_amount,
            {{extract_nested_value("netReceivedInventoryCost","currencyCode","string")}}  as netReceivedInventoryCost_currencyCode,
            netreceivedinventoryunits,
            {{extract_nested_value("sellableOnHandInventoryCost","amount","numeric")}} as sellableOnHandInventoryCost_amount,
            {{extract_nested_value("sellableOnHandInventoryCost","currencyCode","string")}}  as sellableOnHandInventoryCost_currencyCode,
            sellableonhandinventoryunits,
            {{extract_nested_value("unsellableOnHandInventoryCost","amount","numeric")}} as unsellableOnHandInventoryCost_amount,
            {{extract_nested_value("unsellableOnHandInventoryCost","currencyCode","string")}}  as unsellableOnHandInventoryCost_currencyCode,
            unsellableonhandinventoryunits,
            {{extract_nested_value("aged90plusdayssellableinventorycost","amount","numeric")}} as aged90plusdayssellableinventorycost_amount,
            {{extract_nested_value("aged90plusdayssellableinventorycost","currencyCode","string")}}  as aged90plusdayssellableinventorycost_currencyCode,
            aged90plusdayssellableinventoryunits,
            {{extract_nested_value("unhealthyInventoryCost","amount","numeric")}} as unhealthyInventoryCost_amount,
            {{extract_nested_value("unhealthyInventoryCost","currencyCode","string")}}  as unhealthyInventoryCost_currencyCode,
            unhealthyInventoryUnits,
            procurableproductoutofstockrate,
            {{ currency_conversion('b.value','b.from_currency_code','netReceivedInventoryCost.currencyCode') }},
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from
            {{ i }} a
            {{ unnesting("netreceivedinventorycost") }}
            {{ unnesting("sellableonhandinventorycost") }}
            {{ unnesting("unsellableonhandinventorycost") }}
            {{ unnesting("aged90PlusDaysSellableInventoryCost") }}
            {{ unnesting("unhealthyInventoryCost") }}

        {% if var('currency_conversion_flag') %}
                            left join {{ref('ExchangeRates')}} b on date(startdate) = b.date and netReceivedInventoryCost.currencyCode = b.to_currency_code
                        {% endif %}
                        {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('VendorInventoryReportByManufacturing_lookback') }},0) from {{ this }})
                        {% endif %}
                    qualify dense_rank() over (partition by startdate, asin order by a.{{daton_batch_runtime()}} desc) = 1
                {% if not loop.last %} union all {% endif %}
{% endfor %}
