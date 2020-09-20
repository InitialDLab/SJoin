#ifndef JOIN_H
#define JOIN_H

#include <config.h>
#include <basictypes.h>
#include <predicate.h>
#include <schema.h>
#include <random>

#include <iostream>

class STable;
class Join;
class JoinExecState;
class JoinResult;

class Join {
public:
    Join(memmgr *mmgr);

    virtual ~Join();

    SID add_stream(Schema *schema);
    
    // lsid.lcolid == lrid.rcolid
    //
    // If fkey_spec != Predicate::NO_FKEY, that the other side of the key must
    // be specified as the primary key in the other side's schema.
    void add_equi_join(
        SID lsid,
        COLID lcolid,
        SID rsid,
        COLID rcolid,
        Predicate::ForeignKeySpec fkey_spec = Predicate::NO_FKEY);

    // |lsid.lcolid - rsid.rcolid| <= datum
    void add_band_join(SID lsid, COLID lcolid, SID rsid, COLID rcolid, Datum datum);

    SID num_streams() const { return (SID) m_schemas.size(); }

    JoinExecState *create_exec_state(
        memmgr          *mmgr,
        STable          **stables) {
        return create_exec_state(mmgr, stables, std::random_device()());
    }

    virtual JoinExecState *create_exec_state(
        memmgr          *mmgr,
        STable          **stables,
        UINT8           seed) = 0;
   
    void set_window_non_overlapping() {
        m_window_non_overlapping = true;
    }

    void clear_window_non_overlapping() {
        m_window_non_overlapping = false;
    }

    bool is_window_non_overlapping() {
        return m_window_non_overlapping;
    }

    void set_is_unwindowed() {
        m_is_unwindowed = true;
    }

    void clear_is_unwindowed() {
        m_is_unwindowed = false;
    }

    bool is_unwindowed() {
        return m_is_unwindowed;
    }

protected:
    memmgr                      *m_mmgr;

    std_vector<Schema*>         m_schemas;

    std_vector<Predicate>       m_predicates;

    bool                        m_window_non_overlapping;
    
    // TPC-DS is unwindowed, where set_min_ts will never be called
    // m_is_unwindowed && m_window_non_overlapping: no deletion
    // m_is_unwindowed && !m_window_non_overlapping: w/ deletion
    bool                        m_is_unwindowed;
};

struct JoinResult {
    TIMESTAMP   m_ts;
    ROWID       *m_rowids;
};

inline bool operator<(const JoinResult &j1, const JoinResult &j2) {
    return j1.m_ts < j2.m_ts;
}

inline bool operator>(const JoinResult &j1, const JoinResult &j2) {
    return j1.m_ts > j2.m_ts;
}

struct ROWIDArrayHash {
    ROWIDArrayHash(SID num_streams)
        : m_num_streams(num_streams) {}

    size_t operator()(const ROWID *rowids) const {
        size_t h = 0;
        for (SID sid = 0; sid < m_num_streams; ++sid) {
            h = h * 131 + rowids[sid];
        }
        return h;
    }

private:
    SID m_num_streams;
};

struct ROWIDArrayEqual {
    ROWIDArrayEqual(SID num_streams)
        : m_num_streams(num_streams) {}

    bool operator()(const ROWID *rowids1, const ROWID *rowids2) const {
        return std::equal(rowids1, rowids1 + m_num_streams, rowids2);
    }

private:
    SID m_num_streams;
};

class JoinExecState {
public:
    virtual ~JoinExecState() {}
    
    virtual bool initialized() const = 0;

    virtual void notify(SID sid, ROWID rowid) = 0;

    virtual void expire(SID sid, ROWID rowid) = 0;

    virtual void notify_multiple(SID sid, const ROWID *rowid, UINT8 num_rows) {
        for (UINT8 i = 0; i != num_rows; ++i) {
            notify(sid, rowid[i]);
        }
    }

    virtual void expire_multiple(SID sid, const ROWID start_rowid, UINT8 num_rows) {
        for (UINT8 i = 0; i != num_rows; ++i) {
            expire(sid, start_rowid + num_rows);
        }
    }

    virtual void set_min_ts(TIMESTAMP min_ts) = 0;

    virtual void expire_all() = 0;

    virtual void expire_multiple_and_retire_samples(SID sid, const ROWID
            start_rowid, UINT8 num_rows) = 0;

    UINT8 num_processed() const {
        return m_num_processed;
    }

    UINT8 read_num_processed() const {
        return *(const volatile UINT8*) &m_num_processed;
    }
    
    virtual void init_join_result_iterator() = 0;
    
    virtual bool has_next_join_result() = 0;

    virtual const JoinResult &next_join_result() = 0;

    virtual void set_max_key_constraint_warnings(
        UINT4 max_key_constraint_warnings) {
        // It is ignored by default.
        // Currently only implemented by SJoinExecState.
    }

protected:
    UINT8                   m_num_processed;
};

#endif

