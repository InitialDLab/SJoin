#ifndef STABLE_H
#define STABLE_H

#include <schema.h>
#include <record.h>
#include <memmgr.h>
#include <join.h>
#include <deque>

class STable {
public:
    STable(
        memmgr *mmgr,
        mempool_memmgr *rec_mempool,
        const Schema *schema,
        SID sid);

    ~STable();
    
    ROWID num_records() const { return (ROWID) m_recbufs.size(); }

    BYTE *get_record_buffer(ROWID rowid) {
        return m_recbufs[rowid - m_base_rowid];
    }

    void get_record(ROWID rowid, Record *rec) {
        rec->set_schema(m_schema);
        rec->set_buf(get_record_buffer(rowid));
    }

    void set_downstream_exec_state(JoinExecState *exec_state) {
        m_downstream_exec_state = exec_state;
    }
    
    /*
     * The row buffer will be owned by the stream and thus cleared once it's
     * notified.
     */
    ROWID notify(Record *rec);

    ROWID notify_records_only(Record *rec);

    void expire(TIMESTAMP min_ts);

    void expire_downstream_only(TIMESTAMP min_ts);

    void expire_records_only(TIMESTAMP min_ts);

    void expire_all();

    void expire_front_records_only(UINT8 num_tups);
    
    /*
     * Rec is a circular queue that stores batch_size of records
     * to be inserted into the stream. The row buffers will be owned
     * by the stream and thus cleared once it's notified with them.
     */
    void notify_minibatch(
            Record *rec,
            UINT8 start_idx,
            UINT8 batch_size);

    void expire_minibatch(TIMESTAMP min_ts);

    const Schema *get_schema() const { return m_schema; }

    ROWID num_seen() const {
        return m_base_rowid + (ROWID)  m_recbufs.size();
    }

    ROWID base_rowid() const {
        return m_base_rowid;
    }

private:
    ROWID notify_impl(BYTE *rec);

    ROWID notify_records_only_impl(BYTE *rec);

    memmgr                      *m_mmgr;
    
    mempool_memmgr              *m_rec_mempool;

    const Schema                *m_schema;

    SID                         m_sid;

    ROWID                       m_base_rowid;
    
    std_deque<BYTE*>            m_recbufs;

    Record                      m_rec;

    JoinExecState               *m_downstream_exec_state;
};


#endif

