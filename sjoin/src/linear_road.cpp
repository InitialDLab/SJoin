#include <config.h>
#include <linear_road.h>
#include <linear_road_schema.h>
#include <memmgr.h>
#include <stable.h>
#include <sjoin.h>
#include <sinljoin.h>

#include <fstream>
#include <iostream>
#include <string>

UINT8 g_sum = 0;

void dump_join_results(
    JoinExecState *exec_state,
    std_vector<STable*> &stables) {

    SID num_streams = (SID) stables.size();
    Record rec;

    exec_state->init_join_result_iterator();
    
    while (exec_state->has_next_join_result()) {
        const JoinResult &jr = exec_state->next_join_result();
        std::cout << jr.m_ts << std::endl;
        for (SID sid = 0; sid < num_streams; ++sid) {
            stables[sid]->get_record(jr.m_rowids[sid], &rec);
            //g_sum += rec.get_int(2);
            std::cout << '\t' << jr.m_rowids[sid] << ','
                << rec.to_string(',') << std::endl;
        }
    }
}

UINT8 print_window_stats(
    JoinExecState *exec_state,
    UINT8 prev_num_processed,
    UINT8 n_tuples,
    TIMESTAMP min_ts,
    TIMESTAMP max_ts) {
    
    UINT8 num_processed = exec_state->num_processed();

    //std::cout << g_sum << std::endl;
    //g_sum = 0;

    std::cout << "n_processed = " << num_processed - prev_num_processed
              << ", n_tuples = " << n_tuples
              << ", min_ts = " << min_ts
              << ", max_ts = " << max_ts
              << std::endl;

    return num_processed;
}

UINT8 dump_join_samples(
    SJoinExecState *exec_state,
    UINT8 prev_num_processed,
    UINT8 n_tuples,
    TIMESTAMP min_ts,
    TIMESTAMP max_ts,
    std_vector<STable*> &stables) {
    
    UINT8 num_processed = exec_state->num_processed();

    dump_join_results(exec_state, stables);

    //std::cout << g_sum << std::endl;
    auto p = exec_state->get_samples();

    std::cout << "n_processed = " << num_processed - prev_num_processed
              << ", n_samples = " << p.second - p.first
              << ", n_tuples = " << n_tuples
              << ", min_ts = " << min_ts
              << ", max_ts = " << max_ts
              << std::endl;
    
    return num_processed;
}

void run_linear_road(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width) {
    memmgr *top_mmgr = g_root_mmgr;

    Schema *sch_lroad = get_linear_road_schema(); 
    COLID dir_colid = sch_lroad->get_colid_from_name("dir");
    COLID lane_colid = sch_lroad->get_colid_from_name("lane");
    COLID pos_colid = sch_lroad->get_colid_from_name("pos");
    COLID ts_colid = sch_lroad->get_ts_colid();
    constexpr SID nstreams = 3;

    mempool_memmgr *rec_mempool = create_memmgr<mempool_memmgr>(
        top_mmgr,
        sch_lroad->get_max_len(),
        mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, // block_size
            sch_lroad->get_max_len()));
    
    // build stables
    std_vector<STable*> stables(top_mmgr, nstreams, nullptr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        stables[sid] = top_mmgr->newobj<STable>(
                            top_mmgr,
                            rec_mempool,
                            sch_lroad,
                            sid);
    }

    // build sjoin
    SJoin *sjoin = top_mmgr->newobj<SJoin>(top_mmgr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        sjoin->add_stream(sch_lroad);
        if (sid > 0) {
            DATUM value = DATUM_from_INT4(band_width);
            sjoin->add_band_join(
                sid - 1,
                pos_colid,
                sid,
                pos_colid,
                &value);
        }
    }

    sjoin->set_window_non_overlapping();    
    sjoin->set_bernoulli_sampling(0.001);
    SJoinExecState *exec_state = (SJoinExecState*) sjoin->create_exec_state(
            top_mmgr,
            stables.data(),
            19950810);
    
    RT_ASSERT(exec_state->initialized());

    // start streaming
    Record rec;
    rec.set_schema(sch_lroad);
    rec.set_buf(nullptr);
    TIMESTAMP max_ts;
    TIMESTAMP min_ts;
    //TIMESTAMP report_boundary;

    max_ts = window_size - 1;
    min_ts = 0;
    //report_boundary = max_ts;
    
    UINT8 n_tuples = 0;
    UINT8 num_processed = 0;
    //UINT8 num_seen = 0;
    std::ifstream fin(lroad_filename);
    std::string line;
    while (std::getline(fin, line), !line.empty()) {
        if (line[0] != '0') continue;
    
        if (!rec.get_buf()) {
            BYTE *buf = rec_mempool->allocate_array<BYTE>(sch_lroad->get_max_len());
            rec.set_buf(buf, sch_lroad->get_max_len());
        }
        rec.parse_line(line.c_str(), ',');
        
        TIMESTAMP ts = rec.get_long(ts_colid);
        if (ts > max_ts) {
            num_processed = dump_join_samples(
                exec_state,
                num_processed,
                n_tuples,
                min_ts,
                max_ts,
                stables);

            // update window
            min_ts = ts / window_size * window_size;
            max_ts = min_ts + window_size - 1;
            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[sid]->expire_all();
            }
            exec_state->expire_all();
        }
        /*
        if (ts > max_ts) {
            max_ts = ts;
            min_ts = max_ts - window_size;

            if (min_ts >= report_boundary) {
                // TODO dump samples
                report_boundary += report_interval;
            }

            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[i]->expire(min_ts);
            }
        } */

        INT4 dir = rec.get_int(dir_colid);
        INT4 lane = rec.get_int(lane_colid);
        if (dir == 0 && lane >= 1 && lane <= 3) {
            stables[lane - 1]->notify(&rec);
        }
        ++n_tuples;
    }
    
    num_processed = dump_join_samples(
        exec_state,
        num_processed,
        n_tuples,
        min_ts,
        max_ts,
        stables);
}

void run_linear_road_full(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width) {
    memmgr *top_mmgr = g_root_mmgr;

    Schema *sch_lroad = get_linear_road_schema(); 
    COLID dir_colid = sch_lroad->get_colid_from_name("dir");
    COLID lane_colid = sch_lroad->get_colid_from_name("lane");
    COLID pos_colid = sch_lroad->get_colid_from_name("pos");
    COLID ts_colid = sch_lroad->get_ts_colid();
    constexpr SID nstreams = 3;

    mempool_memmgr *rec_mempool = create_memmgr<mempool_memmgr>(
        top_mmgr,
        sch_lroad->get_max_len(),
        mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, // block_size
            sch_lroad->get_max_len()));
    
    // build stables
    std_vector<STable*> stables(top_mmgr, nstreams, nullptr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        stables[sid] = top_mmgr->newobj<STable>(
                            top_mmgr,
                            rec_mempool,
                            sch_lroad,
                            sid);
    }

    // build sinljoin
    SINLJoin *join = top_mmgr->newobj<SINLJoin>(top_mmgr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        join->add_stream(sch_lroad);
        if (sid > 0) {
            DATUM value = DATUM_from_INT4(band_width);
            join->add_band_join(
                sid - 1,
                pos_colid,
                sid,
                pos_colid,
                &value);
        }
    }

    join->set_window_non_overlapping();    
    JoinExecState *exec_state = join->create_exec_state(
            top_mmgr,
            stables.data(),
            19950810);
    
    RT_ASSERT(exec_state->initialized());

    // start streaming
    Record rec;
    rec.set_schema(sch_lroad);
    rec.set_buf(nullptr);
    TIMESTAMP max_ts;
    TIMESTAMP min_ts;
    //TIMESTAMP report_boundary;

    max_ts = window_size - 1;
    min_ts = 0;
    //report_boundary = max_ts;
    
    UINT8 n_tuples = 0;
    UINT8 num_processed = 0;
    std::ifstream fin(lroad_filename);
    std::string line;
    while (std::getline(fin, line), !line.empty()) {
        if (line[0] != '0') continue;
    
        if (!rec.get_buf()) {
            BYTE *buf = rec_mempool->allocate_array<BYTE>(sch_lroad->get_max_len());
            rec.set_buf(buf, sch_lroad->get_max_len());
        }
        rec.parse_line(line.c_str(), ',');
        
        TIMESTAMP ts = rec.get_long(ts_colid);
        if (ts > max_ts) {
            //report_boundary = max_ts;
            //num_seen += exec_state->num_seen();

            num_processed = print_window_stats(
                exec_state,
                num_processed,
                n_tuples,
                min_ts,
                max_ts);

            min_ts = ts / window_size * window_size;
            max_ts = min_ts + window_size - 1;
            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[sid]->expire_all();
            }
            exec_state->expire_all();
        }
        /*
        if (ts > max_ts) {
            max_ts = ts;
            min_ts = max_ts - window_size;

            if (min_ts >= report_boundary) {
                // TODO dump samples
                report_boundary += report_interval;
            }

            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[i]->expire(min_ts);
            }
        } */

        INT4 dir = rec.get_int(dir_colid);
        INT4 lane = rec.get_int(lane_colid);
        if (dir == 0 && lane >= 1 && lane <= 3) {
            stables[lane - 1]->notify(&rec);
            dump_join_results(
                exec_state,
                stables);
        }
        ++n_tuples;
    }

    num_processed = print_window_stats(
        exec_state,
        num_processed,
        n_tuples,
        min_ts,
        max_ts);
}

void run_linear_road_sliding_window(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width) {
    memmgr *top_mmgr = g_root_mmgr;

    Schema *sch_lroad = get_linear_road_schema(); 
    COLID dir_colid = sch_lroad->get_colid_from_name("dir");
    COLID lane_colid = sch_lroad->get_colid_from_name("lane");
    COLID pos_colid = sch_lroad->get_colid_from_name("pos");
    COLID ts_colid = sch_lroad->get_ts_colid();
    constexpr SID nstreams = 3;

    mempool_memmgr *rec_mempool = create_memmgr<mempool_memmgr>(
        top_mmgr,
        sch_lroad->get_max_len(),
        mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, // block_size
            sch_lroad->get_max_len()));
    
    // build stables
    std_vector<STable*> stables(top_mmgr, nstreams, nullptr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        stables[sid] = top_mmgr->newobj<STable>(
                            top_mmgr,
                            rec_mempool,
                            sch_lroad,
                            sid);
    }

    // build sjoin
    SJoin *sjoin = top_mmgr->newobj<SJoin>(top_mmgr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        sjoin->add_stream(sch_lroad);
        if (sid > 0) {
            DATUM value = DATUM_from_INT4(band_width);
            sjoin->add_band_join(
                sid - 1,
                pos_colid,
                sid,
                pos_colid,
                &value);
        }
    }

    sjoin->clear_window_non_overlapping();    
    sjoin->set_bernoulli_sampling(0.001);
    SJoinExecState *exec_state = (SJoinExecState*) sjoin->create_exec_state(
            top_mmgr,
            stables.data(),
            19950810);
    
    RT_ASSERT(exec_state->initialized());

    // start streaming
    Record rec;
    rec.set_schema(sch_lroad);
    rec.set_buf(nullptr);
    TIMESTAMP max_ts;
    TIMESTAMP min_ts;
    TIMESTAMP report_boundary;

    max_ts = 0;
    min_ts = max_ts - window_size;
    report_boundary = report_interval;
    
    UINT8 n_tuples = 0;
    UINT8 num_processed = 0;
    std::ifstream fin(lroad_filename);
    std::string line;
    while (std::getline(fin, line), !line.empty()) {
        if (line[0] != '0') continue;
    
        if (!rec.get_buf()) {
            BYTE *buf = rec_mempool->allocate_array<BYTE>(sch_lroad->get_max_len());
            rec.set_buf(buf, sch_lroad->get_max_len());
        }
        rec.parse_line(line.c_str(), ',');
        
        TIMESTAMP ts = rec.get_long(ts_colid);
        if (ts > max_ts) {
            if (ts >= report_boundary) {
                num_processed = dump_join_samples(
                    exec_state,
                    num_processed,
                    n_tuples,
                    min_ts,
                    max_ts,
                    stables);
            }

            // update window
            max_ts = ts;
            min_ts = ts - window_size + 1;
            report_boundary = (max_ts / report_interval + 1) * report_interval;
            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[sid]->expire(min_ts);
            }
            exec_state->set_min_ts(min_ts);
        }

        INT4 dir = rec.get_int(dir_colid);
        INT4 lane = rec.get_int(lane_colid);
        if (dir == 0 && lane >= 1 && lane <= 3) {
            stables[lane - 1]->notify(&rec);
        }
        ++n_tuples;
    }
    
    num_processed = dump_join_samples(
        exec_state,
        num_processed,
        n_tuples,
        min_ts,
        max_ts,
        stables);
}

void run_linear_road_full_sliding_window(
    std::string lroad_filename,
    TIMESTAMP window_size,
    TIMESTAMP report_interval,
    INT4 band_width) {
    memmgr *top_mmgr = g_root_mmgr;

    Schema *sch_lroad = get_linear_road_schema(); 
    COLID dir_colid = sch_lroad->get_colid_from_name("dir");
    COLID lane_colid = sch_lroad->get_colid_from_name("lane");
    COLID pos_colid = sch_lroad->get_colid_from_name("pos");
    COLID ts_colid = sch_lroad->get_ts_colid();
    constexpr SID nstreams = 3;

    mempool_memmgr *rec_mempool = create_memmgr<mempool_memmgr>(
        top_mmgr,
        sch_lroad->get_max_len(),
        mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, // block_size
            sch_lroad->get_max_len()));
    
    // build stables
    std_vector<STable*> stables(top_mmgr, nstreams, nullptr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        stables[sid] = top_mmgr->newobj<STable>(
                            top_mmgr,
                            rec_mempool,
                            sch_lroad,
                            sid);
    }

    // build sinljoin
    SINLJoin *join = top_mmgr->newobj<SINLJoin>(top_mmgr);
    for (SID sid = 0; sid < nstreams; ++sid) {
        join->add_stream(sch_lroad);
        if (sid > 0) {
            DATUM value = DATUM_from_INT4(band_width);
            join->add_band_join(
                sid - 1,
                pos_colid,
                sid,
                pos_colid,
                &value);
        }
    }

    join->clear_window_non_overlapping();    
    JoinExecState *exec_state = join->create_exec_state(
            top_mmgr,
            stables.data(),
            19950810);
    
    RT_ASSERT(exec_state->initialized());

    // start streaming
    Record rec;
    rec.set_schema(sch_lroad);
    rec.set_buf(nullptr);
    TIMESTAMP max_ts;
    TIMESTAMP min_ts;
    TIMESTAMP report_boundary;

    max_ts = 0;
    min_ts = max_ts - window_size;
    report_boundary = report_interval;
    
    UINT8 n_tuples = 0;
    UINT8 num_processed = 0;
    std::ifstream fin(lroad_filename);
    std::string line;
    while (std::getline(fin, line), !line.empty()) {
        if (line[0] != '0') continue;
    
        if (!rec.get_buf()) {
            BYTE *buf = rec_mempool->allocate_array<BYTE>(sch_lroad->get_max_len());
            rec.set_buf(buf, sch_lroad->get_max_len());
        }
        rec.parse_line(line.c_str(), ',');
        
        TIMESTAMP ts = rec.get_long(ts_colid);
        if (ts > max_ts) {

            if (ts >= report_boundary) {
                num_processed = print_window_stats(
                    exec_state,
                    num_processed,
                    n_tuples,
                    min_ts,
                    max_ts);
            }

            max_ts = ts;
            min_ts = ts - window_size + 1;
            report_boundary = (max_ts / report_interval + 1) * report_interval;
            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[sid]->expire_downstream_only(min_ts);
            }
            //exec_state->set_min_ts(min_ts);
            
            // drain expired join results
            exec_state->init_join_result_iterator();
            while (exec_state->has_next_join_result()) {
                (void) exec_state->next_join_result();
            }

            for (SID sid = 0; sid < nstreams; ++sid) {
                stables[sid]->expire_records_only(min_ts);
            }
        }

        INT4 dir = rec.get_int(dir_colid);
        INT4 lane = rec.get_int(lane_colid);
        if (dir == 0 && lane >= 1 && lane <= 3) {
            stables[lane - 1]->notify(&rec);
            dump_join_results(
                exec_state,
                stables);
        }
        ++n_tuples;
    }

    num_processed = print_window_stats(
        exec_state,
        num_processed,
        n_tuples,
        min_ts,
        max_ts);
}

