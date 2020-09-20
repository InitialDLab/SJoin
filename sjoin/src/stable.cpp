#include <stable.h>

STable::STable(
    memmgr *mmgr,
    mempool_memmgr *rec_mempool,
    const Schema *schema,
    SID sid)
    : m_mmgr(mmgr),
    m_rec_mempool(rec_mempool),
    m_schema(schema),
    m_sid(sid),
    m_base_rowid(0),
    m_recbufs(m_mmgr),
    m_rec(),
    m_downstream_exec_state(nullptr) {
    
    m_rec.set_schema(m_schema);
}

STable::~STable() {
}

ROWID STable::notify(Record *rec) {
    ROWID rowid = notify_impl(rec->get_buf());
    rec->set_buf(nullptr, 0);
    return rowid;
}

ROWID STable::notify_impl(BYTE *rec) {
    ROWID rowid = (ROWID)(m_base_rowid + m_recbufs.size());
    m_recbufs.push_back(rec);
    // notify downstream
    m_downstream_exec_state->notify(m_sid, rowid);
    return rowid;
}

ROWID STable::notify_records_only(Record *rec) {
    ROWID rowid = notify_records_only_impl(rec->get_buf());
    rec->set_buf(nullptr, 0);
    return rowid;
}

ROWID STable::notify_records_only_impl(BYTE *rec) {
    ROWID rowid = (ROWID)(m_base_rowid + m_recbufs.size());
    m_recbufs.push_back(rec);
    return rowid;
}

void STable::expire(TIMESTAMP min_ts) {
    while (m_recbufs.size()) {
        m_rec.set_buf(m_recbufs.front());
        TIMESTAMP ts = m_rec.get_long(m_schema->get_ts_colid());
        if (ts >= min_ts) {
            return ;
        }

        // notify downstream
        m_downstream_exec_state->expire(m_sid, m_base_rowid);
        
        m_rec_mempool->deallocate(m_recbufs.front());
        m_recbufs.pop_front();
        ++m_base_rowid;
    }
}

void STable::expire_downstream_only(TIMESTAMP min_ts) {
    for (ROWID i = 0; i != m_recbufs.size(); ++i) {
        m_rec.set_buf(m_recbufs[i]);
        TIMESTAMP ts = m_rec.get_long(m_schema->get_ts_colid());
        if (ts >= min_ts) {
            return ;
        }

        // notify downstream
        m_downstream_exec_state->expire(m_sid, m_base_rowid + i);
    }
}

void STable::expire_records_only(TIMESTAMP min_ts) {
    while (m_recbufs.size()) {
        m_rec.set_buf(m_recbufs.front());
        TIMESTAMP ts = m_rec.get_long(m_schema->get_ts_colid());
        if (ts >= min_ts) {
            return ;
        }
        
        m_rec_mempool->deallocate(m_recbufs.front());
        m_recbufs.pop_front();
        ++m_base_rowid;
    }
}

void STable::expire_all() {
    m_base_rowid += (ROWID) m_recbufs.size();
    m_recbufs.clear();
    m_rec_mempool->deallocate_all();
}

/* For TPC-DS tuple deletion support */
void STable::expire_front_records_only(UINT8 num_tups) {
    for (UINT8 i = 0; i < num_tups; ++i) {
        m_rec_mempool->deallocate(m_recbufs.front());
        m_recbufs.pop_front();
        ++m_base_rowid;
    }
}

void STable::notify_minibatch(
        Record *rec,
        UINT8 start_idx,
        UINT8 batch_size) {
    
    std_vector<ROWID> rowids(m_mmgr, batch_size);
    for (UINT8 i = 0; i < batch_size; ++i) {
        UINT8 row_idx = (start_idx + i) % batch_size;
        ROWID rowid = (ROWID) (m_base_rowid + m_recbufs.size());

        m_recbufs.push_back(rec[row_idx].get_buf());
        rowids[i] = rowid;
    }
    
    m_downstream_exec_state->notify_multiple(
            m_sid, rowids.data(), batch_size);
}

void STable::expire_minibatch(TIMESTAMP min_ts) {
    UINT8 num_tups_expired = 0;

    for (ROWID i = 0; i < m_recbufs.size(); ++i) {
        m_rec.set_buf(m_recbufs.front());
        TIMESTAMP ts = m_rec.get_long(m_schema->get_ts_colid());
        if (ts >= min_ts) break;
        ++num_tups_expired;
    }
    
    if (num_tups_expired) {
        m_downstream_exec_state->expire_multiple(
            m_sid, m_base_rowid, num_tups_expired);

        for (ROWID i = 0; i < num_tups_expired; ++i) {
            m_rec_mempool->deallocate(m_recbufs.front());
            m_recbufs.pop_front();
        }
        m_base_rowid += num_tups_expired;
    }
}

