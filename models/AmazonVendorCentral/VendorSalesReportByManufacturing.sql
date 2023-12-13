{% if var("VendorSalesReportByManufacturing") %} {{ config(enabled=True) }}
{% else %} {{ config(enabled=False) }}
{% endif %}

{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}
{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("VendorSalesReportByManufacturing_tbl_ptrn","VendorSalesReportByManufacturing_exclude_tbl_ptrn") %}
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
            customerReturns,
            {{extract_nested_value("orderedRevenue","amount","numeric")}} as orderedRevenue_amount,
            {{extract_nested_value("orderedRevenue","currencyCode","string")}}  as orderedRevenue_currencyCode,
            orderedUnits,
            {{extract_nested_value("shippedCogs","amount","numeric")}} as shippedCogs_amount,
            {{extract_nested_value("shippedCogs","currencyCode","string")}}  as shippedCogs_currencyCode,
            {{extract_nested_value("shippedRevenue","amount","numeric")}} as shippedRevenue_amount,
            {{extract_nested_value("shippedRevenue","currencyCode","string")}}  as shippedRevenue_currencyCode,
            shippedUnits,
            {{ currency_conversion('b.value','b.from_currency_code','orderedRevenue.currencyCode') }},
            a.{{ daton_user_id() }} as _daton_user_id,
            a.{{ daton_batch_runtime() }} as _daton_batch_runtime,
            a.{{ daton_batch_id() }} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from
            {{ i }} a
            {{ unnesting("orderedRevenue") }}
            {{ unnesting("shippedCogs") }}
            {{ unnesting("shippedRevenue") }}
            
        {% if var('currency_conversion_flag') %}
                            left join {{ref('ExchangeRates')}} b on date(startdate) = b.date and orderedRevenue.currencyCode = b.to_currency_code
                        {% endif %}
                        {% if is_incremental() %}
                        {# /* -- this filter will only be applied on an incremental run */ #}
                        WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('VendorSalesReportByManufacturing_lookback') }},0) from {{ this }})
                        {% endif %}
                    qualify dense_rank() over (partition by marketplaceId, startdate, asin order by a.{{daton_batch_runtime()}} desc) = 1
                {% if not loop.last %} union all {% endif %}
{% endfor %}
