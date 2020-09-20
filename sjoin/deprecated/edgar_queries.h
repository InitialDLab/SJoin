#ifndef EDGAR_QUERIES_H
#define EDGAR_QUERIES_H

#include <config.h>
#include <schema.h>
#include <record.h>
#include <join.h>
#include <cstdlib>
#include <iostream>

Schema *get_edgar_schema();

template<class QueryBase>
class EDGARJoinSampling : public QueryBase {
public:
    EDGARJoinSampling()
        : QueryBase(g_root_mmgr) {}

    virtual void print_additional_help() {}

    inline void print_help(char *argv[], int qname_idx) {
        std::cout << "usage: " << argv[0] << " [-m <meter_outfile>] "
            << argv[qname_idx]
            << " <infile> <outfile> <window_type:T/S> <window_size> <SamplingType:B/W/O> <SRate/SSize>"
            << std::endl;
    }

    virtual int additional_setup(int argc, char *argv[], int qname_idx) {
        if (argc <= qname_idx + 6) {
            print_help(argv, qname_idx);
            return 1;
        }

        char window_type = argv[qname_idx + 3][0];
        TIMESTAMP window_size = (TIMESTAMP) strtoull(argv[qname_idx + 4], nullptr, 0);
        char sampling_type = argv[qname_idx + 5][0];
        double sampling_rate;
        UINT8 sample_size;
        
        Schema *sch = get_edgar_schema();
        COLID subnet_colid = sch->get_colid_from_name("ip/24");
        int nstreams = 3;

        for (int i = 0; i < nstreams; ++i) {
            SID sid = this->add_stream(sch);
            if (i != 0) {
                this->add_equi_join(
                    sid - 1,
                    subnet_colid,
                    sid,
                    subnet_colid);
            }
        }

        if (window_type == 'T') {
            this->set_tumbling_window(window_size);
        } else if (window_type == 'S') {
            this->set_sliding_window(window_size);
        } else {
            print_help(argv, qname_idx);
            std::cout << "unknown window_type: " << window_type << std::endl;
            return 1;
        }

        switch(sampling_type) {
        case 'B':
            sampling_rate = strtod(argv[qname_idx + 6], nullptr);
            this->set_bernoulli_sampling(sampling_rate);
            break;
        case 'W':
            sample_size = strtoull(argv[qname_idx + 6], nullptr, 0);
            this->set_sampling_with_replacement(sample_size);
            break;
        case 'O':
            sample_size = strtoull(argv[qname_idx + 6], nullptr, 0);
            this->set_sampling_without_replacement(sample_size);
            break;
        default:
            print_help(argv, qname_idx);
            std::cout << "unknown sampling_type: " << sampling_type << std::endl;
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

        Record rec;
        UINT8 i = 0;
        for (auto iter = iter_pair.first; iter != iter_pair.second; ++iter) {
            const JoinResult &jr = *iter;
            result_out << "sample " << i << ", ts = " << jr.m_ts << std::endl;
            for (SID sid = 0; sid < this->num_streams(); ++sid) {
                this->m_stables[sid]->get_record(jr.m_rowids[sid], &rec);
                result_out << "\t(" << sid << ";" << jr.m_rowids[sid] << "),"
                    << rec.to_string(',') << std::endl;
            }
            ++i;
        }
    }
};


#endif
