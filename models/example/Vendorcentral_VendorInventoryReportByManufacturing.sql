    -- depends_on: {{ ref('ExchangeRates') }}
    {% if var('table_partition_flag') %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by = {'field': 'startDate', 'data_type': 'date'},
        cluster_by = ['startDate', 'asin'],
        unique_key = ['marketplaceId','startDate', 'asin'])}}
    {% else %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['startDate', 'asin'])}}
    {% endif %}


    {% if is_incremental() %}
    {%- set max_loaded_query -%}
    SELECT coalesce(MAX({{daton_batch_runtime()}}) - 2592000000,0) FROM {{ this }}
    {% endset %}

    {%- set max_loaded_results = run_query(max_loaded_query) -%}

    {%- if execute -%}
    {% set max_loaded = max_loaded_results.rows[0].values()[0] %}
    {% else %}
    {% set max_loaded = 0 %}
    {%- endif -%}
    {% endif %}

    {% set table_name_query %}
    {{set_table_name('%vendorinventoryreportbymanufacturing')}}    
    {% endset %} 


    {% set results = run_query(table_name_query) %}
    {% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {% if var('timezone_conversion_flag') %}
        {% set hr = var('timezone_conversion_hours') %}
    {% endif %}

    {% for i in results_list %}
        {% if var('brand_consolidation_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
        {% else %}
            {% set brand = var('brand_name') %}
        {% endif %}

        {% if var('store_consolidation_flag') %}
            {% set store =i.split('.')[2].split('_')[var('store_name_position')] %}
        {% else %}
            {% set store = var('store') %}
        {% endif %}

        select * {{exclude()}} (row_num) from
        (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY startDate,asin order by {{daton_batch_runtime()}} desc) row_num 
            From (
            select
            {% if var('currency_conversion_flag') %}
                case when c.value is null then 1 else c.value end as exchange_currency_rate,
                case when c.from_currency_code is null then a.currencyCode else c.from_currency_code end as exchange_currency_code,
            {% else %}
                cast(1 as decimal) as exchange_currency_rate,
                cast(null as string) as exchange_currency_code,
            {% endif %}
             a.* from (
            select
            '{{brand}}' as brand,
            '{{store}}' as store,
            reportRequestTime,
            vendorId,
            marketplaceName, 
            marketplaceId,
            startDate,
            endDate,
            asin,
            openPurchaseOrderUnits,
            averageVendorLeadTimeDays,
            sellThroughRate,
            unfilledCustomerOrderedUnits,
            {% if var('snowflake_database_flag') %}
            NETRECEIVEDINVENTORYCOST.VALUE:amount :: FLOAT as netReceivedInventoryCost,
            NETRECEIVEDINVENTORYCOST.VALUE:currencyCode :: VARCHAR AS currencyCode,
            netReceivedInventoryUnits,
            SELLABLEONHANDINVENTORYCOST.VALUE:amount :: FLOAT as sellableOnHandInventoryCost,
            sellableOnHandInventoryUnits,
            UNSELLABLEONHANDINVENTORYCOST.VALUE:amount :: FLOAT as unsellableOnHandInventoryCost,
            unsellableOnHandInventoryUnits,
            AGED90PLUSDAYSSELLABLEINVENTORYCOST.VALUE:amount :: FLOAT as aged90PlusDaysSellableInventoryCost,
            aged90PlusDaysSellableInventoryUnits,
            UNHEALTHYINVENTORYCOST.VALUE:amount :: FLOAT as unhealthyInventoryCost,
            {% else %}
            netReceivedInventoryCost.amount as netReceivedInventoryCost,
            netReceivedInventoryCost.currencyCode,
            netReceivedInventoryUnits,
            sellableOnHandInventoryCost.amount as sellableOnHandInventoryCost,
            sellableOnHandInventoryUnits,
            unsellableOnHandInventoryCost.amount as unsellableOnHandInventoryCost,
            unsellableOnHandInventoryUnits,
            aged90PlusDaysSellableInventoryCost.amount as aged90PlusDaysSellableInventoryCost,
            aged90PlusDaysSellableInventoryUnits,
            unhealthyInventoryCost.amount as unhealthyInventoryCost,
            {% endif %}
            unhealthyInventoryUnits,
            procurableProductOutOfStockRate,
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(startDate as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id
            {% else %}
                cast(startDate as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id
            {% endif %}
            from {{i}}
            {{unnesting("netreceivedinventorycost")}}
            {{unnesting("sellableonhandinventorycost")}}
            {{unnesting("unsellableonhandinventorycost")}}
            {{unnesting("aged90PlusDaysSellableInventoryCost")}}
            {{unnesting("unhealthyInventoryCost")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}    
            ) a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.startDate) = c.date and a.currencyCode = c.to_currency_code                      
            {% endif %}
            )
            )
            where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}









