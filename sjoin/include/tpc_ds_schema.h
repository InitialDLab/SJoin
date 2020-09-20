#ifndef TPC_DS_SCHEMA_H
#define TPC_DS_SCHEMA_H

#include <schema.h>
#include <string>

Schema *get_tpc_ds_store_sales_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_date_dim_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_item_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_store_returns_schema(bool enable_primary_key = false);
// FIXME enable_primary_key not honored for catalog_sales yet
Schema *get_tpc_ds_catalog_sales_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_customer_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_household_demographics_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_web_sales_schema(bool enable_primary_key = false);
Schema *get_tpc_ds_inventory_schema(bool enable_primary_key = false);

// combined schema
Schema *get_tpc_ds_store_sales_c_hd_schema();
Schema *get_tpc_ds_store_sales_c_hd_i_schema();
Schema *get_tpc_ds_customer_hd_schema();

#endif
