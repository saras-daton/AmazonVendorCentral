{% if var("VendorRealTimeTrafficReport") %} {{ config(enabled=True) }}
{% else %} {{ config(enabled=False) }}
{% endif %}

{% if var("currency_conversion_flag") %} 
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("VendorRealTimeTrafficReport_tbl_ptrn","VendorRealTimeTrafficReport_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}

{% for i in result %}


        select
        "{{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }}" as brand,
        "{{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }}" as store,  
        {{timezone_conversion('ReportstartDate')}} as ReportstartDate,
        {{timezone_conversion('ReportendDate')}} as ReportendDate,
        {{timezone_conversion('reportrequesttime')}} as reportrequesttime,
        vendorid,
        marketplacename,
        marketplaceid,
        {{timezone_conversion('startTime')}} as startTime,
        {{timezone_conversion('endTime')}} as endTime,
        asin,
        glanceViews,
        {{ daton_user_id() }} as _daton_user_id,
        {{ daton_batch_runtime() }} as _daton_batch_runtime,
        {{ daton_batch_id() }} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from
            {{ i }} a
        {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('VendorRealTimeTrafficReport_lookback') }},0) from {{ this }})
        {% endif %}
        qualify dense_rank() over (partition by startTime, endTime, asin order by a.{{daton_batch_runtime()}} desc) = 1
        {% if not loop.last %} union all {% endif %}
{% endfor %}