#ifndef LROAD_QUERIES_H
#define LROAD_QUERIES_H

#include <config.h>
#include <query.h>
#include <vector>

void print_linear_road_help();

int setup_linear_road_query(
    Query *query,
    const std::vector<std::string> &additional_args);

////////////////////////////////////////////////////////
// Sample dump function for the old query interface ////
// Only kept here for reference. Don't use.         ////
////////////////////////////////////////////////////////
//    virtual void handle_report_event(
//        std::ostream &result_out,
//        TIMESTAMP min_ts,
//        TIMESTAMP max_ts,
//        UINT8 num_processed_since_last_report,
//        UINT8 num_tuples_since_last_report) {
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
//        UINT8 i = 0;
//	if (!this->m_is_no_dumping)
//        for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
//            const JoinResult &jr = *iter;
//            result_out << "sample " << i << ", ts = " << jr.m_ts << std::endl;
//            for (SID sid = 0; sid < this->num_streams(); ++sid) {
//                this->m_stables[sid]->get_record(jr.m_rowids[sid], &rec);
//                result_out << "\t(" << sid << ";"  << jr.m_rowids[sid] << "),"
//                    << rec.to_string(',') << std::endl;
//            }
//            ++i;
//        }
//    }

#endif

