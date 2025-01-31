#include <config.h>
#include <tpc_ds_queries.h>
#include <memmgr.h>
#include <dtypes.h>
#include <schema.h>
#include <tpc_ds_schema.h>

#include <iostream>
#include <string>

static int setup_tpcds_qx(Query *query, bool use_sjoin_opt);
static int setup_tpcds_qy(Query *query, bool use_sjoin_opt);
static int setup_tpcds_qz(Query *query, bool use_sjoin_opt);

int setup_tpcds_query(
    Query *query,
    const std::string &query_name,
    bool use_sjoin_opt,
    const std::vector<std::string> &additional_args) {

    if (!query->is_unwindowed()) {
        std::cerr << "[ERROR] tpcds queries must be unwindowed"
            << std::endl;
        return 1;
    }
    
    int ret_code = 0;
    if (query_name == "tpcds_qx") {
        ret_code = setup_tpcds_qx(query, use_sjoin_opt);
    } else if (query_name == "tpcds_qy") {
        ret_code = setup_tpcds_qy(query, use_sjoin_opt);
    } else if (query_name == "tpcds_qz") {
        ret_code = setup_tpcds_qz(query, use_sjoin_opt);
    } else {
        std::cerr << "[ERROR] unknown query name: "
            << query_name << std::endl;
        return 1;
    }

    return ret_code;
}

static int setup_tpcds_qx(Query *query, bool use_sjoin_opt) {
    // If use_sjoin_opt == true, add primary key declaration
    // in the schemas.
    Schema *date_dim_sch = get_tpc_ds_date_dim_schema(use_sjoin_opt);
    Schema *store_sales_sch = get_tpc_ds_store_sales_schema(use_sjoin_opt);
    Schema *store_returns_sch =
        get_tpc_ds_store_returns_schema(use_sjoin_opt);
    Schema *catalog_sales_sch =
        get_tpc_ds_catalog_sales_schema(use_sjoin_opt);
    
    SID store_returns_sid = query->add_stream(store_returns_sch);
    SID catalog_sales_sid = query->add_stream(catalog_sales_sch);
    SID date_dim_1_sid = query->add_stream(date_dim_sch);
    SID date_dim_2_sid = query->add_stream(date_dim_sch);
    SID store_sales_sid = query->add_stream(store_sales_sch);

    // ss_item_sk_and_ticket_number = sr_item_sk_and_ticket_number
    COLID ss_item_sk_and_ticket_number_cid =
        store_sales_sch->get_colid_from_name(
            "item_sk_and_ticket_number");
    COLID sr_item_sk_and_ticket_number_cid =
        store_returns_sch->get_colid_from_name(
            "item_sk_and_ticket_number");
    query->add_equi_join(
        store_sales_sid,
        ss_item_sk_and_ticket_number_cid,
        store_returns_sid,
        sr_item_sk_and_ticket_number_cid,
        use_sjoin_opt ? Predicate::RIGHT_IS_FKEY : Predicate::NO_FKEY);

    // sr_customer_sk = cs_bill_customer_sk
    COLID sr_customer_sk_cid =
        store_returns_sch->get_colid_from_name("customer_sk");
    COLID cs_bill_customer_sk_cid =
        catalog_sales_sch->get_colid_from_name("bill_customer_sk");
    query->add_equi_join(
        store_returns_sid,
        sr_customer_sk_cid,
        catalog_sales_sid,
        cs_bill_customer_sk_cid,
        Predicate::NO_FKEY);

    // d1.d_date_sk = sr_returned_date_sk
    COLID d1_date_sk_cid =
        date_dim_sch->get_colid_from_name("date_sk");
    COLID sr_returned_date_sk_cid =
        store_returns_sch->get_colid_from_name("returned_date_sk");
    query->add_equi_join(
        date_dim_1_sid,
        d1_date_sk_cid,
        store_returns_sid,
        sr_returned_date_sk_cid,
        use_sjoin_opt ? Predicate::RIGHT_IS_FKEY : Predicate::NO_FKEY);

    // d2.d_date_sk = cs_sold_date_sk
    COLID d2_date_sk_cid =
        date_dim_sch->get_colid_from_name("date_sk");
    COLID cs_sold_date_sk_cid =
        catalog_sales_sch->get_colid_from_name("sold_date_sk");
    query->add_equi_join(
        date_dim_2_sid,
        d2_date_sk_cid,
        catalog_sales_sid,
        cs_sold_date_sk_cid,
        use_sjoin_opt ? Predicate::RIGHT_IS_FKEY : Predicate::NO_FKEY);

    // Filter out cs and sr records with NULL date_sk
    // FIXME We currently don't have NULL bits in the record
    // format. NULL values are stored as 0's for integers.
    // This filter is a temporary hack because the keys are positive in
    // TPC-DS.
    DATUM null_value = DATUM_from_INT8(0);
    query->add_neq_filter(
        store_returns_sid,
        sr_returned_date_sk_cid,
        store_returns_sch->get_ti(sr_returned_date_sk_cid),
        &null_value);
    query->add_neq_filter(
        catalog_sales_sid,
        cs_sold_date_sk_cid,
        catalog_sales_sch->get_ti(cs_sold_date_sk_cid),
        &null_value);

    // Not expecting any key constraint violation because
    // we did not filter out anything from the consolidated
    // tables (those that we look up from a foreign key).
    query->allow_key_constraint_warnings(10);

    return 0;
}

static int setup_tpcds_qy(Query *query, bool use_sjoin_opt) {
    // See setup_tpcds_qx() for why using use_sjoin_opt as arg.
    Schema *customer_sch = get_tpc_ds_customer_schema(use_sjoin_opt);
    Schema *household_demographics_sch =
        get_tpc_ds_household_demographics_schema(use_sjoin_opt);
    Schema *store_sales_sch = get_tpc_ds_store_sales_schema(use_sjoin_opt);

    SID store_sales_sid = query->add_stream(store_sales_sch);
    SID customer_2_sid = query->add_stream(customer_sch);
    SID household_demographics_1_sid =
        query->add_stream(household_demographics_sch);
    SID household_demographics_2_sid =
        query->add_stream(household_demographics_sch);
    SID customer_1_sid = query->add_stream(customer_sch);

    // ss_customer_sk = c1.c_customer_sk
    COLID ss_customer_sk_cid =
        store_sales_sch->get_colid_from_name("customer_sk");
    COLID c1_customer_sk_cid =
        customer_sch->get_colid_from_name("customer_sk");
    query->add_equi_join(
        store_sales_sid,
        ss_customer_sk_cid,
        customer_1_sid,
        c1_customer_sk_cid,
        use_sjoin_opt ? Predicate::LEFT_IS_FKEY : Predicate::NO_FKEY);

    // c1_current_hdemo_sk = hd1_demo_sk
    COLID c1_current_hdemo_sk_cid =
        customer_sch->get_colid_from_name("current_hdemo_sk");
    COLID hd1_demo_sk_cid =
        household_demographics_sch->get_colid_from_name("demo_sk");
    query->add_equi_join(
        customer_1_sid,
        c1_current_hdemo_sk_cid,
        household_demographics_1_sid,
        hd1_demo_sk_cid,
        use_sjoin_opt ? Predicate::LEFT_IS_FKEY : Predicate::NO_FKEY);

    // hd1_income_band_sk = hd2_income_band_sk
    COLID hd1_income_band_sk_cid =
        household_demographics_sch->get_colid_from_name("income_band_sk");
    COLID hd2_income_band_sk_cid =
        household_demographics_sch->get_colid_from_name("income_band_sk");
    query->add_equi_join(
        household_demographics_1_sid,
        hd1_income_band_sk_cid,
        household_demographics_2_sid,
        hd2_income_band_sk_cid); // no fkey

    // hd2_demo_sk = c2_current_hdemo_sk
    COLID hd2_demo_sk_cid =
        household_demographics_sch->get_colid_from_name("demo_sk");
    COLID c2_current_hdemo_sk_cid =
        customer_sch->get_colid_from_name("current_hdemo_sk");
    query->add_equi_join(
        household_demographics_2_sid,
        hd2_demo_sk_cid,
        customer_2_sid,
        c2_current_hdemo_sk_cid,
        use_sjoin_opt ? Predicate::RIGHT_IS_FKEY : Predicate::NO_FKEY);

    // Add null filters
    // See setup_tpcds_qx() for the comments why the null filters
    // are necessary.
    DATUM null_value = DATUM_from_INT8(0);
    // ss_customer_sk IS NOT NULL
    query->add_neq_filter(
        store_sales_sid,
        ss_customer_sk_cid,
        store_sales_sch->get_ti(ss_customer_sk_cid),
        &null_value);
    // c1_current_hdemo_sk IS NOT NULL
    query->add_neq_filter(
        customer_1_sid,
        c1_current_hdemo_sk_cid,
        customer_sch->get_ti(c1_current_hdemo_sk_cid),
        &null_value);
    // c2_current_hdemo_sk IS NOT NULL
    query->add_neq_filter(
        customer_2_sid,
        c2_current_hdemo_sk_cid,
        customer_sch->get_ti(c2_current_hdemo_sk_cid),
        &null_value);
    
    // We need to suppress key constraint warnings because
    // some of the customer records are filtered out
    // due to NULL values in the c_current_hdemo_sk column,
    // where the join processor will complain about
    // missing primary keys.
    //
    // This is an expected behavior because the records
    // that has NULL join attribute values never appear in
    // the result of an equi-join and they are simply dropped
    // by the SJoin implementation.
    query->suppress_key_constraint_warnings(); 

    return 0;
}

static int setup_tpcds_qz(Query *query, bool use_sjoin_opt) {
    // See setup_tpcds_qx() for comments why using use_sjoin_opt as arg.
    Schema *store_sales_sch = get_tpc_ds_store_sales_schema(use_sjoin_opt);
    Schema *customer_sch = get_tpc_ds_customer_schema(use_sjoin_opt);
    Schema *household_demographics_sch =
        get_tpc_ds_household_demographics_schema(use_sjoin_opt);
    Schema *item_sch = get_tpc_ds_item_schema(use_sjoin_opt);

    SID store_sales_sid = query->add_stream(store_sales_sch);
    SID customer_2_sid = query->add_stream(customer_sch);
    SID item_2_sid = query->add_stream(item_sch);
    SID household_demographics_1_sid =
        query->add_stream(household_demographics_sch);
    SID household_demographics_2_sid =
        query->add_stream(household_demographics_sch);
    SID customer_1_sid = query->add_stream(customer_sch);
    SID item_1_sid = query->add_stream(item_sch);

    // ss_customer_sk = c1.c_customer_sk
    COLID ss_customer_sk_cid =
        store_sales_sch->get_colid_from_name("customer_sk");
    COLID c1_customer_sk_cid =
        customer_sch->get_colid_from_name("customer_sk");
    query->add_equi_join(
        store_sales_sid,
        ss_customer_sk_cid,
        customer_1_sid,
        c1_customer_sk_cid,
        use_sjoin_opt ? Predicate::LEFT_IS_FKEY : Predicate::NO_FKEY);

    // c1.c_current_hdemo_sk = hd1.hd_demo_sk
    COLID c1_current_hdemo_sk_cid =
        customer_sch->get_colid_from_name("current_hdemo_sk");
    COLID hd1_demo_sk_cid =
        household_demographics_sch->get_colid_from_name("demo_sk");
    query->add_equi_join(
        customer_1_sid,
        c1_current_hdemo_sk_cid,
        household_demographics_1_sid,
        hd1_demo_sk_cid,
        use_sjoin_opt ? Predicate::LEFT_IS_FKEY : Predicate::NO_FKEY);

    // hd1.hd_income_band_sk = hd2.hd_income_band
    COLID hd1_income_band_sk_cid =
        household_demographics_sch->get_colid_from_name(
            "income_band_sk");
    COLID hd2_income_band_sk_cid =
        household_demographics_sch->get_colid_from_name(
            "income_band_sk");
    query->add_equi_join(
        household_demographics_1_sid,
        hd1_income_band_sk_cid,
        household_demographics_2_sid,
        hd2_income_band_sk_cid); // no fkey

    // hd2.hd_demo_sk = c2.c_current_hdemo_sk
    COLID hd2_demo_sk_cid =
        household_demographics_sch->get_colid_from_name("demo_sk");
    COLID c2_current_hdemo_sk_cid =
        customer_sch->get_colid_from_name("current_hdemo_sk");
    query->add_equi_join(
        household_demographics_2_sid,
        hd2_demo_sk_cid,
        customer_2_sid,
        c2_current_hdemo_sk_cid,
        use_sjoin_opt ? Predicate::RIGHT_IS_FKEY : Predicate::NO_FKEY);

    // ss_item_sk = i1.i_item_sk
    COLID ss_item_sk_cid =
        store_sales_sch->get_colid_from_name("item_sk");
    COLID i1_item_sk_cid =
        item_sch->get_colid_from_name("item_sk");
    query->add_equi_join(
        store_sales_sid,
        ss_item_sk_cid,
        item_1_sid,
        i1_item_sk_cid,
        use_sjoin_opt ? Predicate::LEFT_IS_FKEY : Predicate::NO_FKEY);

    // i1.i_category_id = i2.i_category_id
    COLID i1_category_id_cid =
        item_sch->get_colid_from_name("category_id");
    COLID i2_category_id_cid =
        item_sch->get_colid_from_name("category_id");
    query->add_equi_join(
        item_1_sid,
        i1_category_id_cid,
        item_2_sid,
        i2_category_id_cid); // no fkey

    // Add null filters on foreign key columns.
    // FIXME see above in setup_tpcds_qx()
    DATUM null_value = DATUM_from_INT8(0);
    query->add_neq_filter(
        store_sales_sid,
        ss_customer_sk_cid,
        store_sales_sch->get_ti(ss_customer_sk_cid),
        &null_value);
    query->add_neq_filter(
        customer_1_sid,
        c1_current_hdemo_sk_cid,
        customer_sch->get_ti(c1_current_hdemo_sk_cid),
        &null_value);
    query->add_neq_filter(
        customer_2_sid,
        c2_current_hdemo_sk_cid,
        customer_sch->get_ti(c2_current_hdemo_sk_cid),
        &null_value);
    
    // Expecting the key constraint warnings because
    // customer_1 is a consolidated table which is also
    // filtered, meaning we could be missing rows from customer_1.
    // Also see above in setup_tpcds_qy() for reasoning.
    query->suppress_key_constraint_warnings();

    return 0;
}

