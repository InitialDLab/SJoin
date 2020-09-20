#include <config.h>
#include <tpc_ds_schema.h>
#include <memmgr.h>
#include <dtypes.h>

static Schema *sch_tpc_ds_date_dim = nullptr;
static Schema *sch_tpc_ds_item = nullptr;
static Schema *sch_tpc_ds_customer = nullptr;
static Schema *sch_tpc_ds_household_demographics = nullptr;

static Schema *sch_tpc_ds_store_sales = nullptr;
static Schema *sch_tpc_ds_store_returns = nullptr;
static Schema *sch_tpc_ds_catalog_sales = nullptr;
static Schema *sch_tpc_ds_web_sales = nullptr;
static Schema *sch_tpc_ds_inventory = nullptr;

//combined schema
static Schema *sch_tpc_ds_store_sales_c_hd = nullptr;
static Schema *sch_tpc_ds_store_sales_c_hd_i = nullptr;
static Schema *sch_tpc_ds_customer_hd = nullptr;

/*
The number of selected columns of each table :
(exclude "time"-column)
(* represents that its last column is a combined column)

store_sales         : 12*
store_returns         : 11*
catalog_sales        : 18
web_sales        : 20*
inventory        : 5*

date_dim        : 11
item            : 15
customer        : 4
household_demographics    : 5
*/


Schema *get_tpc_ds_store_sales_schema(bool enable_primary_key) {
    // TODO remove column 12 item_sk_and_ticket_number when
    // we support composite keys.
    if (!sch_tpc_ds_store_sales) {
        const char *names[] = {
            "time",
            "sold_date_sk",
            "sold_time_sk",
            "item_sk",
            "customer_sk",
            "cdemo_sk",
            "hdemo_sk",
            "addr_sk",
            "store_sk",
            "promo_sk",
            "ticket_number",
            "quantity",
            "item_sk_and_ticket_number"
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
        };
        sch_tpc_ds_store_sales = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            13,
            ti,
            names,
            INVALID_COLID,
            enable_primary_key ? 12 : INVALID_COLID
        );
    }
    
    return sch_tpc_ds_store_sales;
}

Schema *get_tpc_ds_store_returns_schema(bool enable_primary_key) {
    // TODO remove column 11 item_sk_and_ticket_number
    // when we support composite keys
    if (!sch_tpc_ds_store_returns) {
        const char *names[] = {
                    "time",
                    "returned_date_sk",
                    "return_time_sk",
                    "item_sk",
                    "customer_sk",
                    "cdemo_sk",
                    "hdemo_sk",
                    "addr_sk",
                    "store_sk",
                    "reason_sk",
                    "ticket_number",
            "item_sk_and_ticket_number"
            };
            typeinfo *ti[] = {
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
            };
            sch_tpc_ds_store_returns = g_tracked_mmgr->newobj<Schema>(
                g_tracked_mmgr,
                12,
                ti,
                names,
                INVALID_COLID,
                enable_primary_key ? 11 : 0
            );
    }

    return sch_tpc_ds_store_returns;
}

Schema *get_tpc_ds_catalog_sales_schema(bool enable_primary_key) {
    if (!sch_tpc_ds_catalog_sales) {
        const char *names[] = {
                    "time",
                    "sold_date_sk",
                    "sold_time_sk",
                    "ship_date_sk",
                    "bill_customer_sk",
                    "bill_cdemo_sk",
                    "bill_hdemo_sk",
                    "bill_addr_sk",
                    "ship_customer_sk",
                    "ship_cdemo_sk",
                    "ship_hdemo_sk",
                    "ship_addr_sk",
                    "call_center_sk",
                    "catalog_page_sk",
                    "ship_mode_sk",
                    "warehouse_sk",
                    "item_sk",
                    "promo_sk",
                    "order_number"
            };
            typeinfo *ti[] = {
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG)
            };
            sch_tpc_ds_catalog_sales = g_tracked_mmgr->newobj<Schema>(
                    g_tracked_mmgr,
                    19,
                    ti,
                    names,
                    INVALID_COLID, /* ts_colid */
                    // TODO specify the primary key when we support composite keys
                    INVALID_COLID /* primary_key_colid */
            );
    }

    return sch_tpc_ds_catalog_sales;
}

Schema *get_tpc_ds_web_sales_schema(bool enable_primary_key) {
    // TODO remove column 20 item_sk_and_sold_date_sk
    // when we support composite keys
    if (!sch_tpc_ds_web_sales) {
        const char *names[] = {
            "time",
            "sold_date_sk",
            "sold_time_sk",
            "ship_date_sk",
            "item_sk",
            "bill_customer_sk",
            "bill_cdemo_sk",
            "bill_hdemo_sk",
            "bill_addr_sk",
            "ship_customer_sk",
            "ship_cdemo_sk",
            "ship_hdemo_sk",
            "ship_addr_sk",
            "web_page_sk",
            "web_site_sk",
            "web_ship_mode_sk",
            "warehouse_sk",
            "promo_sk",
            "order_number",
            "quantity",
            "item_sk_and_sold_date_sk"    
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
        };
        sch_tpc_ds_web_sales = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            21,
            ti,
            names,
            INVALID_COLID,
            enable_primary_key ? 20: INVALID_COLID
        );
    }

    return sch_tpc_ds_web_sales;
}

Schema *get_tpc_ds_inventory_schema(bool enable_primary_key) {
    // TODO remove column 5 item_sk_and_date_sk when we support
    // composite keys
    if (!sch_tpc_ds_inventory) {
        const char *names[] = {
            "time",
            "date_sk",
            "item_sk",
            "warehouse_sk",
            "quantity_on_hand",
            "item_sk_and_date_sk"
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
        };
        sch_tpc_ds_inventory = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            6,
            ti,
            names,
            INVALID_COLID,
            enable_primary_key ? 5 : INVALID_COLID
        );
    }

    return sch_tpc_ds_inventory;
}

Schema *get_tpc_ds_date_dim_schema(bool enable_primary_key) {
    if (!sch_tpc_ds_date_dim) {
        const char *names[] = {
                  "time",
                "date_sk",
                "date_id",
                 "date",
                "month_seq",
                "week_seq",
                "quarter_seq",
                "year",
                "dow",
                "moy",
                "dom",
                "qoy"
            };
            typeinfo *ti[] = {
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(16 + 2),
                    get_fixedlen_ti(DTYPE_DATE),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
            };
            sch_tpc_ds_date_dim = g_tracked_mmgr->newobj<Schema>(
                    g_tracked_mmgr,
                    12,
                    ti,
                    names,
                    INVALID_COLID, /* ts_colid */
                    enable_primary_key ? 1 : INVALID_COLID
            );
    }
    
    return sch_tpc_ds_date_dim;
}

Schema *get_tpc_ds_item_schema(bool enable_primary_key) {
    if (!sch_tpc_ds_item) {
        const char *names[] = {
                    "time",
                    "item_sk",
                    "item_id",
                    "rec_start_date",
                    "rec_end_date",
                    "item_desc",
                    "current_price",
                    "wholesale_cost",
                    "brand_id",
                    "brand",
                    "class_id",
                    "class",
                    "category_id",
                    "category",
                    "manufact_id",
                    "manufact"
            };
            typeinfo *ti[] = {
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(16 + 2),
                    get_fixedlen_ti(DTYPE_DATE),
                    get_fixedlen_ti(DTYPE_DATE),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(210),
                    get_fixedlen_ti(DTYPE_DOUBLE),
                    get_fixedlen_ti(DTYPE_DOUBLE),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(50 + 2),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(50 + 2),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(50 + 2),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(50 + 2),
            };
            sch_tpc_ds_item = g_tracked_mmgr->newobj<Schema>(
                    g_tracked_mmgr,
                    16,
                    ti,
                    names,
                    INVALID_COLID,
                    enable_primary_key ? 1 : INVALID_COLID
            );
    }

    return sch_tpc_ds_item;
}

Schema *get_tpc_ds_customer_schema(bool enable_primary_key) {
    if (!sch_tpc_ds_customer) {
        const char *names[] = {
            "time",
            "customer_sk",
            "customer_id",
            "current_cdemo_sk",
            "current_hdemo_sk"
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            g_tracked_mmgr->newobj<typeinfo_varchar>(16 + 2),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
        };
        sch_tpc_ds_customer = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            5,
            ti,
            names,
            INVALID_COLID,
            enable_primary_key ? 1 : INVALID_COLID
        );
    }

    return sch_tpc_ds_customer;
}

Schema *get_tpc_ds_household_demographics_schema(bool enable_primary_key) {
    if (!sch_tpc_ds_household_demographics) {
        const char *names[] = {
            "time",
            "demo_sk",
            "income_band_sk",
            "buy_potential",
            "dep_count",
            "vehicle_count"
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            g_tracked_mmgr->newobj<typeinfo_varchar>(15 + 2),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
        };
        sch_tpc_ds_household_demographics = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            6,
            ti,
            names,
            INVALID_COLID,
            enable_primary_key ? 1 : INVALID_COLID
        );
    }

    return sch_tpc_ds_household_demographics;
}

// combined schema
// TODO remove me when we support primary keys
Schema *get_tpc_ds_store_sales_c_hd_schema() {
        if (!sch_tpc_ds_store_sales_c_hd) {
                const char *names[] = {
                        "time",
                        "sold_date_sk",
                        "sold_time_sk",
                        "item_sk",
                        "customer_sk",
                        "cdemo_sk",
                        "hdemo_sk",
                        "addr_sk",
                        "store_sk",
                        "promo_sk",
                        "ticket_number",
                        "quantity",
                        "item_sk_and_ticket_number",
            "c_current_hdemo_sk",
            "hd_income_band_sk"
                };
                typeinfo *ti[] = {
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
                };
                sch_tpc_ds_store_sales_c_hd = g_tracked_mmgr->newobj<Schema>(
                        g_tracked_mmgr,
                        15,
                        ti,
                        names
                );
        }

        return sch_tpc_ds_store_sales_c_hd;
}

// TODO remove me when we have support for primary keys
Schema *get_tpc_ds_store_sales_c_hd_i_schema() {
        if (!sch_tpc_ds_store_sales_c_hd_i) {
                const char *names[] = {
                        "time",
                        "sold_date_sk",
                        "sold_time_sk",
                        "item_sk",
                        "customer_sk",
                        "cdemo_sk",
                        "hdemo_sk",
                        "addr_sk",
                        "store_sk",
                        "promo_sk",
                        "ticket_number",
                        "quantity",
                        "item_sk_and_ticket_number",
                        "c_current_hdemo_sk",
                        "hd_income_band_sk",
            "i_category_id"
                };
                typeinfo *ti[] = {
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
                        get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_LONG)
                };
                sch_tpc_ds_store_sales_c_hd_i = g_tracked_mmgr->newobj<Schema>(
                        g_tracked_mmgr,
                        16,
                        ti,
                        names
                );
        }

        return sch_tpc_ds_store_sales_c_hd_i;
}

// TODO remove me when we support primary keys
Schema *get_tpc_ds_customer_hd_schema() {
        if (!sch_tpc_ds_customer_hd) {
            const char *names[] = {
                    "time",
                    "customer_sk",
                    "customer_id",
                    "current_cdemo_sk",
                    "current_hdemo_sk",
                    "hd_income_band_sk"
            };
            typeinfo *ti[] = {
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    g_tracked_mmgr->newobj<typeinfo_varchar>(16 + 2),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG),
                    get_fixedlen_ti(DTYPE_LONG)
            };
            sch_tpc_ds_customer_hd = g_tracked_mmgr->newobj<Schema>(
                    g_tracked_mmgr,
                    6,
                    ti,
                    names
            );
        }

        return sch_tpc_ds_customer_hd;
}
