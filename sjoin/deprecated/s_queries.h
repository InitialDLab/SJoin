#ifndef S_QUERIES_H
#define S_QUERIES_H

#include <config.h>
#include <query.h>
#include <record.h>
#include <std_containers.h>
#include <iostream>
#include <string>
#include <cstring>

void print_s_query_join_help(Query *query, char *argv[], int qname_idx);

int setup_s_query_join(Query *query, int argc, char *argv[], int qname_idx);

template<class QueryBase>
class SQueryJoinSampling: public QueryBase {
public:
	SQueryJoinSampling()
		: QueryBase(g_root_mmgr) {}

	virtual void print_addtional_help() { 
		std::cout << " <SamplingType:B/W/O> <SRate/SSize> ";
	}

	virtual int additional_setup(int argc, char *argv[], int qname_idx) {
		if (argc <= qname_idx + 7) {
			print_s_query_join_help(this, argv, qname_idx);
			return 1;
		}

		char sampling_type = argv[qname_idx + 6][0];
		double sampling_rate;
		UINT8 sample_size;
		
		switch (sampling_type) {
                        case 'B':
                                sampling_rate = strtod(argv[qname_idx + 7], nullptr);
                                this->set_bernoulli_sampling(sampling_rate);
                                break;
                        case 'W':
                                sample_size = strtoull(argv[qname_idx + 7], nullptr, 0);
                                this->set_sampling_with_replacement(sample_size);
                                break;
                        case 'O':
                                sample_size = strtoull(argv[qname_idx + 7], nullptr, 0);
                                this->set_sampling_without_replacement(sample_size);
                                break;
                        default:
                                std::cout << "unknown sampling type: " << sampling_type << std::endl;
                                return 1;
                }

                return 0;
	}

protected:
	virtual void handle_report_event(
                std::ostream &result_out,
                TIMESTAMP min_ts,
                TIMESTAMP max_ts,
                UINT8 num_processed_since_last_report,
                UINT8 num_tuples_since_last_report) {

                auto iter_pair = this->get_samples();

                std::cout << "n_proc = " << num_processed_since_last_report
                        << ", window [" << min_ts << ", " << max_ts << "]"
                        << ", n_seen = " << this->num_seen()
                        << ", ssize = " << iter_pair.second - iter_pair.first
                        << ", ntup = " << num_tuples_since_last_report
                        << std::endl;
                result_out << "n_proc = " << num_processed_since_last_report
                        << ", window [" << min_ts << ", " << max_ts << "]"
                        << ", n_seen = " << this->num_seen()
                        << ", ssize = " << iter_pair.second - iter_pair.first
                        << ", ntup = " << num_tuples_since_last_report
                        << std::endl;
                Record rec;
                UINT8 i = 0;
                for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
                        const JoinResult &jr = *iter;
                        result_out << "sample " << i << ", ts = " << jr.m_ts << std::endl;
                        for (SID sid = 0; sid < this->num_streams(); ++sid) {
                                this->m_stables[sid]->get_record(jr.m_rowids[sid], &rec);
                                result_out << "\t(" << sid << ";"  << jr.m_rowids[sid] << "),"
                                        << rec.to_string(',') << std::endl;
                        }
                         ++i;
                }
        }
};

#endif
