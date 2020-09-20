#include <config.h>
#include <s_queries.h>
#include <memmgr.h>
#include <dtypes.h>
#include <schema.h>
#include <tpc_ds_schema.h>

#include <iostream>
#include <string>

void print_s_query_join_help(Query *query, char *argv[], int qname_idx) {
        std::cout << "usage :" << argv[0];
        if (qname_idx == 3) {
                std::cout << " -m <meter_outfile> ";
        }
        std::cout << argv[qname_idx] << " <infile> <outfile> <ntables> <window_type:T/S> <window_size>";
        query->print_additional_help();
        std::cout << std::endl;
}

int setup_s_query_join(
        Query *query,
        int argc,
        char *argv[],
        int qname_idx) {

        if (argc <= qname_idx + 5) {
                print_s_query_join_help(query, argv, qname_idx);
                return 1;
        }

        int ntables = atoi(argv[qname_idx + 3]);
        char window_type = argv[qname_idx + 4][0];
        TIMESTAMP window_size = (TIMESTAMP) strtoull(argv[qname_idx + 5], nullptr, 0);

        Schema *date_dim_sch = get_tpc_ds_date_dim_schema();
        Schema *store_sales_sch = get_tpc_ds_store_sales_schema();
        Schema *store_returns_sch = get_tpc_ds_store_returns_schema();
        Schema *catalog_sales_sch = get_tpc_ds_catalog_sales_schema();

        SID date_dim_1_sid = query->add_stream(date_dim_sch);
        SID date_dim_2_sid = query->add_stream(date_dim_sch);
        SID store_sales_sid = query->add_stream(store_sales_sch);
        SID store_returns_sid = query->add_stream(store_returns_sch);
        SID catalog_sales_sid = query->add_stream(catalog_sales_sch);

        // ss_item_sk_and_ticket_number = sr_item_sk_and_ticket_number
        COLID ss_item_sk_and_ticket_number_cid = store_sales_sch->get_colid_from_name("item_sk_and_ticket_number");
        COLID sr_item_sk_and_ticket_number_cid = store_returns_sch->get_colid_from_name("item_sk_and_ticket_number");
        query->add_equi_join(store_sales_sid, ss_item_sk_and_ticket_number_cid, store_returns_sid, sr_item_sk_and_ticket_number_cid);

        // we modify query S
	// sr_customer_sk = cs_bill_customer_sk
        COLID sr_customer_sk_cid = store_returns_sch->get_colid_from_name("customer_sk");
        COLID cs_bill_customer_sk_cid = catalog_sales_sch->get_colid_from_name("bill_customer_sk");
        query->add_equi_join(store_returns_sid, sr_customer_sk_cid, catalog_sales_sid, cs_bill_customer_sk_cid);

        // d1.d_date_sk = sr_returned_date_sk
        COLID d1_date_sk_cid = date_dim_sch->get_colid_from_name("date_sk");
        COLID sr_returned_date_sk_cid = store_returns_sch->get_colid_from_name("returned_date_sk");
        query->add_equi_join(date_dim_1_sid, d1_date_sk_cid, store_returns_sid, sr_returned_date_sk_cid);

        // d2.d_date_sk = cs_sold_date_sk
        COLID d2_date_sk_cid = date_dim_sch->get_colid_from_name("date_sk");
        COLID cs_sold_date_sk_cid = catalog_sales_sch->get_colid_from_name("sold_date_sk");
        query->add_equi_join(date_dim_2_sid, d2_date_sk_cid, catalog_sales_sid, cs_sold_date_sk_cid);

        if (window_type == 'T') {
                query->set_tumbling_window(window_size);
        } else if (window_type == 'S') {
                query->set_sliding_window(window_size);
        } else {
                print_s_query_join_help(query, argv, qname_idx);
                std::cout << "unknown window_type: " << window_type << std::endl;
                return 1;
        }

        return 0;
}
