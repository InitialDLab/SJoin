/*
 * sijoin.h
 *
 * Symmetrical Index Nested Loop Join
 *
 */
#ifndef SINLJOIN_H
#define SINLJOIN_H



#include <config.h>
#include <basictypes.h>
#include <dtypes.h>
#include <join.h>
#include <memmgr.h>
#include <stable.h>
#include <avl.h>
#include <vertex.h>

class SINLJoin;
class SINLJoinExecState;

class SINLJoin: public Join {
public:

    SINLJoin(memmgr *mmgr);

    ~SINLJoin();

    JoinExecState *create_exec_state(
        memmgr *mmgr,
        STable **stables,
        UINT8  seed);

    void set_require_iterating_expired_join_results() {
        m_require_iterating_expired_join_results = true;
    }

    void clear_require_iterating_expired_join_results() {
        m_require_iterating_expired_join_results = false;
    }

    void set_require_reiterating_active_join_results() {
        m_require_reiterating_active_join_results = true;
    }

    void clear_require_reiterating_active_join_results() {
        m_require_reiterating_active_join_results = false;
    }

private:
    bool                        m_require_iterating_expired_join_results;

    bool                        m_require_reiterating_active_join_results;

    friend class SINLJoinExecState;
};

struct SINLJoinVertexIterator {
    DATUM               m_high;
    SINLJoinVertex      *m_vertex;
};

class SINLJoinExecState: public JoinExecState {
private:
    typedef ::AVL<SINLJoinVertex> AVL;
public:
    SINLJoinExecState(
        memmgr *mmgr,
        SINLJoin *sinljoin,
        STable **stables);

    ~SINLJoinExecState();

    bool initialized() const { return m_layout; }

    void notify(SID sid, ROWID rowid);

    void expire(SID sid, ROWID rowid);

    void set_min_ts(TIMESTAMP min_ts);

    void expire_all();

    void expire_multiple_and_retire_samples(SID sid, const ROWID
            start_rowid, UINT8 num_rows) {
        for (UINT8 i = 0; i != num_rows; ++i) {
            expire(sid, start_rowid + i);
        }
    }

    void init_join_result_iterator();

    bool has_next_join_result();

    const JoinResult &next_join_result();

    UINT8 num_seen() const {
        //if (m_window_non_overlapping ||
        //    m_require_iterating_expired_join_results ||
        //    m_require_reiterating_active_join_results) {

        return m_num_seen;
        //}

        // unknown in this case
        //return 0;
    }

    void cancel_reiterating_active_join_results() {
        m_reiterating_active_join_results = false;
        m_buffered_records.clear();
    }
    
private:
    
    bool create_vertex_layout(std_vector<Predicate> &predicates);

    SID compute_iteration_seq(
        SID s_root,
        SID *seq,
        SID i,
        SID s0,
        SID s1,
        UINT4 neighbor_idx_of_s1);

    void clean_vertex_layout();

    bool create_indexes();

    void clean_indexes();

    void clean_all();

    void init_join();

    void reinit();

    void compute_join_result_timestamp();
    
    SINLJoinVertex *remove_vertex(SID sid, ROWID rowid);

    bool init_vertex_iters();

    bool get_next_join_result(SID s_root, SID i0);

    SINLJoinVertex *project_current_record();

private:
    memmgr                      *m_mmgr;

    const SID                   m_n_streams;

    const bool                  m_window_non_overlapping;

    const bool                  m_require_iterating_expired_join_results;

    const bool                  m_require_reiterating_active_join_results;

    const bool                  m_is_unwindowed;

    bool                        m_iterating_expired_join_results;

    bool                        m_reiterating_active_join_results;

    STable * const * const      m_stables;

    generic_mempool_memmgr      *m_generic_mempool;

    SINLJoinVertexLayout        *m_layout;
    
    mempool_memmgr              **m_vertex_mempool;

    IndexId                     m_n_indexes;

    AVL                         **m_indexes;

    std_unordered_map<ROWID, SINLJoinVertex*>
                                *m_vertex_hash_tables;

    UINT8                       m_num_seen;
    
    std_deque<RecordId>         m_buffered_records;

    JoinResult                  m_join_result;

    RecordId                    m_current_record;

    SINLJoinVertexIterator      *m_vertex_iter;
};


#endif // SINLJOIN_H

