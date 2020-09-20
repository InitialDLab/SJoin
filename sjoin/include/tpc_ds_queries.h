#ifndef TPC_DS_QUERIES_H
#define TPC_DS_QUERIES_H

#include <config.h>
#include <query.h>
#include <iostream>
#include <string>
#include <cstring>

inline void print_tpcds_query_help(const std::string &query_name) {
    std::cerr << "No additional args required for tpcds queries."
        << std::endl;
}

int setup_tpcds_query(
    Query *query,
    const std::string &query_name,
    bool use_sjoin_opt,
    const std::vector<std::string> &additional_args);

////////////////////////////////////////////////////////
// Sample dump function for the old query interface ////
// Only kept here for reference. Don't use.         ////
////////////////////////////////////////////////////////
//    virtual void handle_report_event(
//            std::ostream &result_out,
//            TIMESTAMP min_ts,
//            TIMESTAMP max_ts,
//            UINT8 num_processed_since_last_report,
//            UINT8 num_tuples_since_last_report) {
//
//        auto iter_pair = this->get_samples();
//
//        std::cout << "n_proc = " << num_processed_since_last_report
//            << ", window [" << min_ts << ", " << max_ts << "]"
//            << ", n_seen = " << this->num_seen()
//            << ", ssize = " << iter_pair.second - iter_pair.first
//            << ", ntup = " << num_tuples_since_last_report
//            << std::endl;
//        result_out << "n_proc = " << num_processed_since_last_report
//            << ", window [" << min_ts << ", " << max_ts << "]"
//            << ", n_seen = " << this->num_seen()
//            << ", ssize = " << iter_pair.second - iter_pair.first
//            << ", ntup = " << num_tuples_since_last_report
//            << std::endl;
//
//        Record rec;
//        /* only for m_is_query_si */
//        ROWID rowids[4];
//        UINT8 i = 0;
//
//        if (!this->m_is_no_dumping)
//            for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
//                const JoinResult &jr = *iter;
//                result_out << "sample " << i << ", ts = " << jr.m_ts << std::endl;
//                if (this->m_is_query_si) {
//                    /* sr -> d1, ss */
//                    this->m_stables[0]->get_record(jr.m_rowids[0], &rec);
//                    rowids[0] = this->m_umap_d1_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("returned_date_sk"))];
//                    rowids[2] = this->m_umap_ss_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("item_sk_and_ticket_number"))];
//
//                    /* cs -> d2 */
//                    this->m_stables[1]->get_record(jr.m_rowids[1], &rec);
//                    rowids[1] = this->m_umap_d2_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("sold_date_sk"))];
//                }
//                if (this->m_is_query_se) {
//                    /* ss -> hd1, c1, i1 */
//                    this->m_stables[0]->get_record(jr.m_rowids[0], &rec);
//                    rowids[2] = this->m_umap_c_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("customer_sk"))];
//                    Record c_rec;
//                    this->m_stables[5]->get_record(rowids[2], &c_rec);
//                    rowids[0] = this->m_umap_hd1_attr2rec[c_rec.get_long(c_rec.get_schema()->get_colid_from_name("current_hdemo_sk"))];
//                    rowids[3] = this->m_umap_i_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("item_sk"))];                
//
//                    /* c2 -> hd2 */
//                    this->m_stables[1]->get_record(jr.m_rowids[1], &rec);
//                    rowids[1] = this->m_umap_hd2_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("current_hdemo_sk"))];
//                }
//                if (this->m_is_query_e) {
//                    /* ss -> hd1, c1 */
//                    this->m_stables[0]->get_record(jr.m_rowids[0], &rec);
//                    rowids[2] = this->m_umap_c_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("customer_sk"))];
//                    Record c_rec;
//                    this->m_stables[4]->get_record(rowids[2], &c_rec);
//                    rowids[0] = this->m_umap_hd1_attr2rec[c_rec.get_long(c_rec.get_schema()->get_colid_from_name("current_hdemo_sk"))];
//                    /* c2 -> hd2 */
//                    this->m_stables[1]->get_record(jr.m_rowids[1], &rec);
//                    rowids[1] = this->m_umap_hd2_attr2rec[rec.get_long(rec.get_schema()->get_colid_from_name("current_hdemo_sk"))];
//                }
//                for (SID sid = 0; sid < this->num_streams(); ++sid) {
//                    if ((this->m_is_query_si || this->m_is_query_e) && sid >= 2) {
//                        this->m_stables[sid]->get_record(rowids[sid - 2], &rec);
//                        result_out << "\t(" << sid << ";" << rowids[sid - 2] << "),"
//                            << rec.to_string(',') << std::endl;
//                        continue;
//                    }
//                    if (this->m_is_query_se && sid >= 3) {
//                        this->m_stables[sid]->get_record(rowids[sid - 3], &rec);
//                        result_out << "\t(" << sid << ";" << rowids[sid - 2] << "),"
//                            << rec.to_string(',') << std::endl;
//                        continue;
//                    }
//
//                    this->m_stables[sid]->get_record(jr.m_rowids[sid], &rec);
//                    result_out << "\t(" << sid << ";"  << jr.m_rowids[sid] << "),"
//                        << rec.to_string(',') << std::endl;
//                }
//                ++i;
//                if (i >= 10000) break;
//            }
//    }

#endif
