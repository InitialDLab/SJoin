#include <config.h>
#include <t_queries.h>
#include <memmgr.h>
#include <dtypes.h>
#include <schema.h>
#include <tpc_ds_schema.h>

#include <iostream>
#include <string>

void print_t_query_join_help(Query *query, char *argv[], int qname_idx) {
	std::cout << "usage :" << argv[0];
	if (qname_idx == 3) {
		std::cout << " -m <meter_outfile> ";
	}
	std::cout << argv[qname_idx] << " <infile> <outfile> <ntables> <window_type:T/S> <window_size>";
	query->print_additional_help();
	std::cout << std::endl;
}

int setup_t_query_join(
	Query *query,
	int argc,
	char *argv[],
	int qname_idx) {
	
	if (argc <= qname_idx + 5) {
		print_t_query_join_help(query, argv, qname_idx);
		return 1;
	}

	int ntables = atoi(argv[qname_idx + 3]);
	char window_type = argv[qname_idx + 4][0];
	TIMESTAMP window_size = (TIMESTAMP) strtoull(argv[qname_idx + 5], nullptr, 0);

	Schema *customer_sch = get_tpc_ds_customer_schema();
	Schema *household_demographics_sch = get_tpc_ds_household_demographics_schema();
	Schema *store_sales_sch = get_tpc_ds_store_sales_schema();

	SID customer_1_sid = query->add_stream(customer_sch);
	SID customer_2_sid = query->add_stream(customer_sch);
	SID household_demographics_1_sid = query->add_stream(household_demographics_sch);
	SID household_demographics_2_sid = query->add_stream(household_demographics_sch);
	SID store_sales_sid = query->add_stream(store_sales_sch);

	// ss_customer_sk = c1.c_customer_sk
	COLID ss_customer_sk = store_sales_sch->get_colid_from_name("customer_sk");
	COLID c1_customer_sk = customer_sch->get_colid_from_name("customer_sk");
	query->add_equi_join(store_sales_sid, ss_customer_sk, customer_1_sid, c1_customer_sk);

	// c1_current_hdemo_sk = hd1_demo_sk
	COLID c1_current_hdemo_sk = customer_sch->get_colid_from_name("current_hdemo_sk");
	COLID hd1_demo_sk = household_demographics_sch->get_colid_from_name("demo_sk");
	query->add_equi_join(customer_1_sid, c1_current_hdemo_sk, household_demographics_1_sid, hd1_demo_sk);

	// hd1_income_band_sk = hd2_income_band_sk
	COLID hd1_income_band_sk = household_demographics_sch->get_colid_from_name("income_band_sk");
	COLID hd2_income_band_sk = household_demographics_sch->get_colid_from_name("income_band_sk");
	query->add_equi_join(household_demographics_1_sid, hd1_income_band_sk, household_demographics_2_sid, hd2_income_band_sk);

	// hd2_demo_sk = c2_current_hdemo_sk
	COLID hd2_demo_sk = household_demographics_sch->get_colid_from_name("demo_sk");
	COLID c2_current_hdemo_sk = customer_sch->get_colid_from_name("current_hdemo_sk");
	query->add_equi_join(household_demographics_2_sid, hd2_demo_sk, customer_2_sid, c2_current_hdemo_sk);

	if (window_type == 'T') {
		query->set_tumbling_window(window_size);
	} else if (window_type == 'S') {
		query->set_sliding_window(window_size);
	} else {
		print_t_query_join_help(query, argv, qname_idx);
		std::cout << "unknown window_type: " << window_type << std::endl;
	}

	return 0;
}
