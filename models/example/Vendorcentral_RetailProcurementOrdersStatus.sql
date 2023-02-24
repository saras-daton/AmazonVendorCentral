 -- depends_on: {{ ref('ExchangeRates') }}  
    {% if var('table_partition_flag') %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        partition_by = {'field': 'purchaseOrderDate', 'data_type': 'date'},
        cluster_by = ['purchaseOrderNumber'],
        unique_key = ['purchaseOrderDate','purchaseOrderNumber','purchaseOrderStatus','buyerProductIdentifier'])}}

    {% else %}
    {{config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['purchaseOrderDate','purchaseOrderNumber','purchaseOrderStatus','buyerProductIdentifier'])}}
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
    {{set_table_name('%retailprocurementordersstatus')}}    
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

        select * {{exclude()}} (row_num)
        from(
            select *, DENSE_RANK() OVER (PARTITION BY purchaseOrderNumber, purchaseOrderStatus,date(purchaseOrderDate),buyerProductIdentifier order by {{daton_batch_runtime()}} desc) row_num
            from(
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
            RequestStartDate,
            RequestEndDate,
            marketplaceName,
            marketplaceId,
            vendorId,
            purchaseOrderNumber,
            purchaseOrderStatus,
            {% if var('timezone_conversion_flag') %}
                cast(DATETIME_ADD(purchaseOrderDate, INTERVAL {{hr}} HOUR ) as date) purchaseOrderDate,
                cast(DATETIME_ADD(lastUpdatedDate, INTERVAL {{hr}} HOUR ) as date) purchaseOrderChangedDate,
            {% else %}
                date(purchaseOrderDate) as purchaseOrderDate,
                date(lastUpdatedDate) as lastUpdatedDate,
            {% endif %}
            sellingParty,
            shipToParty,
            {% if var('snowflake_database_flag') %}
            ITEMSTATUS.VALUE:itemSequenceNumber :: INT AS itemSequenceNumber,
            ITEMSTATUS.VALUE:buyerProductIdentifier :: VARCHAR AS buyerProductIdentifier,
            ITEMSTATUS.VALUE:vendorProductIdentifier :: VARCHAR AS vendorProductIdentifier,
            NETCOST.VALUE:currencyCode :: VARCHAR as currencyCode,
            NETCOST.VALUE:amount :: FLOAT as netcost_amount,
            LISTPRICE.VALUE:amount :: FLOAT as listprice_amount,
            NESTEDORDEREDQUANTITY.VALUE:amount :: FLOAT as ordered_quantity_amount,
            NESTEDORDEREDQUANTITY.VALUE:unitOfMeasure :: VARCHAR AS unitOfMeasure ,
            NESTEDORDEREDQUANTITY.VALUE:unitsize :: int as unitsize,
            orderedQuantity.VALUE:orderedQuantityDetails :: VARCHAR as orderedQuantityDetails ,
            ACKNOWLEDGEMENTSTATUS.VALUE:confirmationStatus :: VARCHAR AS confirmationStatus,
            ACCEPTEDQUANTITY.VALUE:amount :: VARCHAR as accepted_quantity_amount,
            REJECTEDQUANTITY.VALUE:amount :: VARCHAR as rejected_quantity_amount,
            acknowledgementStatus.VALUE:acknowledgementStatusDetails :: VARCHAR as acknowledgementStatusDetails,
            RECEIVINGSTATUS.VALUE:receiveStatus :: VARCHAR as receiveStatus,
            receivedQuantity.VALUE:amount :: FLOAT as received_quantity_amount,
            receivingStatus.VALUE:lastReceiveDate :: DATE as lastReceiveDate,
            {% else %}
            itemStatus.itemSequenceNumber,
            itemStatus.buyerProductIdentifier,
            itemStatus.vendorProductIdentifier,
            netCost.currencyCode,
            netCost.amount as netcost_amount,
            listPrice.amount as listprice_amount,
            nestedorderedQuantity.amount as ordered_quantity_amount,
            nestedorderedQuantity.unitOfMeasure,
            nestedorderedQuantity.unitsize,
            orderedQuantity.orderedQuantityDetails,
            acknowledgementStatus.confirmationStatus,
            acceptedQuantity.amount as accepted_quantity_amount,
            rejectedQuantity.amount as rejected_quantity_amount,
            acknowledgementStatus.acknowledgementStatusDetails,
            receivingStatus.receiveStatus,
            receivedQuantity.amount as received_quantity_amount,
            receivingStatus.lastReceiveDate,
            {% endif%}
	        {{daton_user_id()}},
            {{daton_batch_runtime()}},
            {{daton_batch_id()}},
	        {% if var('timezone_conversion_flag') %}
                DATETIME_ADD(cast(lastUpdatedDate as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
                null as effective_end_date,
                DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
                null as run_id
            {% else %}
                cast(lastUpdatedDate as timestamp) as effective_start_date,
                null as effective_end_date,
                current_timestamp() as last_updated,
                null as run_id
            {% endif %}
            from {{i}}  
            {{unnesting("itemstatus")}}
            {{multi_unnesting("itemstatus","netcost")}}
            {{multi_unnesting("itemstatus","listprice")}}
            {{multi_unnesting("itemstatus","orderedquantity")}}
            {{multi_unnesting("itemstatus","acknowledgementstatus")}}
            {{multi_unnesting("acknowledgementstatus","acknowledgementstatusdetails")}}
            {{multi_unnesting("acknowledgementstatus","acceptedquantity")}}
            {{multi_unnesting("acknowledgementstatus","rejectedquantity")}}
            {{multi_unnesting("itemstatus","receivingstatus")}}
            {{multi_unnesting("receivingstatus","receivedQuantity")}}
            {% if var('snowflake_database_flag') %}
                , LATERAL FLATTEN( input => PARSE_JSON(orderedquantity.VALUE:orderedquantity)) nestedorderedQuantity
            {% else%}
                left join unnest(orderedQuantity.orderedQuantity) nestedorderedQuantity
            {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}    
            )a 
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.purchaseOrderDate) = c.date and a.currencyCode = c.to_currency_code                      
            {% endif %}
            ))
        where row_num = 1
        {% if not loop.last %} union all {% endif %}
    {% endfor %}