#include <config.h>
#include <f_queries.h>
#include <memmgr.h>
#include <dtypes.h>
#include <schema.h>
#include <tpc_ds_schema.h>

#include <iostream>
#include <string>

void print_f_query_join_help(Query *query, char *argv[], int qname_idx) {
	std::cout << "usage :" << argv[0];
	if (qname_idx >= 3) {
		std::cout << " -m <meter_outfile> ";
	}
	if (qname_idx >= 5) {
		std::cout << " -b <batch_size> ";
	}
	std::cout << argv[qname_idx] << " <infile> <outfile> <ntables> <window_type:T/S> <window_size>";
	query->print_additional_help();
	std::cout << std::endl;
}

int setup_f_query_join(
	Query *query,
	int argc,
	char *argv[],
	int qname_idx) {

	if (argc <= qname_idx + 5) {
		print_f_query_join_help(query, argv, qname_idx);
		return 1;
	}
	
	int ntables = atoi(argv[qname_idx + 3]);
	char window_type = argv[qname_idx + 4][0];
	TIMESTAMP window_size = (TIMESTAMP) strtoull(argv[qname_idx + 5], nullptr, 0);

	Schema *item_sch = get_tpc_ds_item_schema();
	Schema *date_dim_sch = get_tpc_ds_date_dim_schema();
	Schema *store_sales_sch = get_tpc_ds_store_sales_schema();

	SID item_sid = query->add_stream(item_sch);
	SID date_dim_sid = query->add_stream(date_dim_sch);
	SID store_sales_sid = query->add_stream(store_sales_sch);
	
	// d_date_sk = ss_sold_date_sk
	COLID d_date_sk_cid = date_dim_sch->get_colid_from_name("date_sk");
	COLID ss_sold_date_sk_cid = store_sales_sch->get_colid_from_name("sold_date_sk");
	query->add_equi_join(date_dim_sid, d_date_sk_cid, store_sales_sid, ss_sold_date_sk_cid);

	// ss_item_sk = i_item_sk
	COLID ss_item_sk_cid = store_sales_sch->get_colid_from_name("item_sk");
	COLID i_item_sk_cid = item_sch->get_colid_from_name("item_sk");
	query->add_equi_join(store_sales_sid, ss_item_sk_cid, item_sid, i_item_sk_cid);

	// i_manufact_id = 128
	COLID i_manufact_id_cid = item_sch->get_colid_from_name("manufact_id");
	DATUM expected_value = DATUM_from_INT8(128);
	query->add_equi_filter(item_sid, i_manufact_id_cid, item_sch->get_ti(i_manufact_id_cid), &expected_value);

	if (window_type == 'T') {
		query->set_tumbling_window(window_size);
	} else if (window_type == 'S') {
		query->set_sliding_window(window_size);
	} else {
		print_f_query_join_help(query, argv, qname_idx);
		std::cout << "unknown window_type: " << window_type << std::endl;
		return 1;
	}

	return 0;
}
