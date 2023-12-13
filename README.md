# Amazon Vendor Central Data Unification

This dbt package is for Data Unification of Amazon Vendor Central ingested data by [Daton](https://sarasanalytics.com/daton/). [Daton](https://sarasanalytics.com/daton/) is the Unified Data Platform for Global Commerce with 100+ pre-built connectors and data sets designed for accelerating the eCommerce data and analytics journey by [Saras Analytics](https://sarasanalytics.com).

### Supported Datawarehouses:
- BigQuery
- Snowflake

#### Typical challenges with raw data are:
- Array/Nested Array columns makes queries for Data Analytics complex
- Data duplication due to look back period while fetching report data from Amazon
- Separate tables at marketplaces/Store, brand, account level for same kind of report/data feeds

Data Unification simplifies Data Analytics by doing:
- Consolidation - Different marketplaces/Store/account & different brands would have similar raw Daton Ingested tables, which are consolidated into one table with column distinguishers brand & store
- Deduplication - Based on primary keys, the data is De-duplicated and the latest records are only loaded into the consolidated stage tables
- Incremental Load - Models are designed to include incremental load which when scheduled would update the tables regularly
- Standardization -
	- Currency Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local currency of the corresponding marketplace/store/account. Values that are in local currency are Standardized by converting to desired currency using Daton Exchange Rates data.
	  Prerequisite - Exchange Rates connector in Daton needs to be present - Refer [this](https://github.com/saras-daton/currency_exchange_rates)
	- Time Zone Conversion (Optional) - Raw Tables data created at Marketplace/Store/Account level may have data in local timezone of the corresponding marketplace/store/account. DateTime values that are in local timezone are Standardized by converting to specified timezone using input offset hours.

#### Prerequisite 
Daton Integrations for  
- Amazon Vendor Central
- Exchange Rates(Optional, if currency conversion is not required)

*Note:* 
*Please select 'Do Not Unnest' option while setting up Daton Integrataion*

# Configuration 

## Required Variables

This package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested. Source data is located using the following variables which must be set in your `dbt_project.yml` file.
```yaml
vars:
    raw_database: "your_database"
    raw_schema: "your_schema"
```

## Setting Target Schema

Models will be create unified tables under the schema (<target_schema>_stg_amazon). In case, you would like the models to be written to the target schema or a different custom schema, please add the following in the dbt_project.yml file.

```yaml
models:
  amazon_vendorcentral:
    AmazonVendorCentral:
      +schema: custom_schema_extension

```

## Optional Variables

Package offers different configurations which must be set in your `dbt_project.yml` file. These variables can be marked as True/False based on your requirements. Details about the variables are given below.

### Currency Conversion 

To enable currency conversion, which produces two columns - exchange_currency_rate & exchange_currency_code, please mark the currency_conversion_flag as True. By default, it is False.
Prerequisite - Daton Exchange Rates Integration

Example:
```yaml
vars:
    currency_conversion_flag: True
```

### Timezone Conversion 

To enable timezone conversion, which converts the timezone columns from UTC timezone to local timezone, please mark the timezone_conversion_flag as True in the dbt_project.yml file, by default, it is False. Additionally, you need to provide offset hours between UTC and the timezone you want the data to convert into for each raw table for which you want timezone converison to be taken into account.

Example:
```yaml
vars:
timezone_conversion_flag: False
raw_table_timezone_offset_hours: {
    "Amazon.VendorCentral.Brand_UK_AmazonVendorCentral_RetailProcurementOrdersStatus":-7
    }
```
Here, -7 represents the offset hours between UTC and PDT considering we are sitting in PDT timezone and want the data in this timezone

### Table Exclusions

If you need to exclude any of the models, declare the model names as variables and mark them as False. Refer the table below for model details. By default, all tables are created.

Example:
```yaml
vars:
VendorInventoryReportByManufacturing: False
```

## Models

This package contains models from the Amazon Vendor Central API which includes reports on {{sales, margin, inventory, product}}. The primary outputs of this package are described below.

| **Category**                 | **Model**  | **Description** |
| ------------------------- | ---------------| ----------------------- |
|Margin | [NetPureProductMarginReport](models/AmazonVendorCentral/NetPureProductMarginReport.sql)  | Amazon Vendor Central Net PPM (Pure Profit Margin) Report provides valuable ASIN-level vendor profit data. Net PPM stands for Net Pure Profit Margin and represents Amazon’s retail profit margin. Net PPM is a critical metric to track on an ongoing basis as it has major implications to product sales velocity. |
|Purchase Order | [RetailProcurementOrdersStatus](models/AmazonVendorCentral/RetailProcurementOrdersStatus.sql)  | A list of orders created or changed during a time period |
|Inventory | [VendorInventoryReportByManufacturing](models/AmazonVendorCentral/VendorInventoryReportByManufacturing.sql)  | The amount of inventory that is sitting in Amazon's warehouses (or at least in their possession). If you're a manufacturer, you can see how much total inventory is flowing into Amazon. This report will show you metrics like what's on open purchase order, what's sellable vs. unsellable, and how old inventory is |
|Inventory | [VendorInventoryReportBySourcing](models/AmazonVendorCentral/VendorInventoryReportBySourcing.sql)| the amount of inventory that is sitting in Amazon's warehouses (or at least in their possession). If Amazon is sourcing from multiple other places, it lets you see how much total inventory is flowing into Amazon. This report will show you metrics like what's on open purchase order, what's sellable vs. unsellable, and how old inventory is |
|Sales | [VendorSalesReportByManufacturing](models/AmazonVendorCentral/VendorSalesReportByManufacturing.sql)| The Sales View shows vital stats like shipped revenue, shipped COGS (cost of goods sold), and ordered revenue by Manufacturing |
|Sales | [VendorSalesReportBySourcing](models/AmazonVendorCentral/VendorSalesReportBySourcing.sql)| The Sales View shows vital stats like shipped revenue, shipped COGS (cost of goods sold), and ordered revenue By Sourcing |
|Traffic | [VendorSalesReportBySourcing](models/AmazonVendorCentral/VendorSalesReportBySourcing.sql)| Traffic details of different products |



### For details about default configurations for Table Primary Key columns, Partition columns, Clustering columns, please refer the properties.yaml used for this package as below. 
	You can overwrite these default configurations by using your project specific properties yaml.
```yaml
version: 2
models:
  - name: NetPureProductMarginReport
    description: NetPureProductMarginReport
    config: 
      materialized: 'incremental'
      incremental_strategy: 'merge'
      partition_by: {'field': 'startDate', 'data_type': 'date'}
      cluster_by: ['startDate' , 'asin']
      unique_key: ['startDate' , 'asin']

  - name: RetailProcurementOrdersStatus
    description: RetailProcurementOrdersStatus
    config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: {'field': 'purchaseOrderDate', 'data_type': 'timestamp', 'granularity': 'day'}
        cluster_by: ['purchaseOrderNumber']
        unique_key: ['purchaseOrderDate','purchaseOrderNumber','purchaseOrderStatus','buyerProductIdentifier']

  - name: VendorInventoryReportByManufacturing
    description: VendorInventoryReportByManufacturing
    config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: {'field': 'startDate', 'data_type': 'date'}
        cluster_by: ['startDate', 'asin']
        unique_key: ['marketplaceId','startDate', 'asin']

  - name: VendorInventoryReportBySourcing
    description: VendorInventoryReportBySourcing
    config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: {'field': 'startDate', 'data_type': 'date'}
        cluster_by: ['startDate', 'asin']
        unique_key: ['marketplaceId','startDate', 'asin']

  - name: VendorSalesReportByManufacturing
    description: VendorSalesReportByManufacturing
    config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: {'field': 'startDate', 'data_type': 'date'}
        cluster_by: ['startDate' , 'asin']
        unique_key: ['marketplaceId','startDate' , 'asin']

  - name: VendorSalesReportBySourcing
    description: VendorSalesReportBySourcing
    config:
        materialized: 'incremental'
        incremental_strategy: 'merge'
        partition_by: {'field': 'startDate', 'data_type': 'date'}
        cluster_by: ['marketplaceId','startDate' , 'asin']
        unique_key: ['marketplaceId','startDate' , 'asin']

  - name: VendorTrafficReport
    description: VendorTrafficReport
    config: 
        materialized: 'incremental'
        incremental_strategy: 'merge' 
        partition_by: {'field': 'startDate', 'data_type': 'date'}
        cluster_by: ['startDate' , 'asin'] 
        unique_key: ['startDate' , 'asin']
```



## Resources:
- Have questions, feedback, or need [help](https://calendly.com/srinivas-janipalli/30min)? Schedule a call with our data experts or email us at info@sarasanalytics.com.
- Learn more about Daton [here](https://sarasanalytics.com/daton/).
- Refer [this](https://youtu.be/6zDTbM6OUcs) to know more about how to create a dbt account & connect to {{Bigquery/Snowflake}}
