#include <config.h>
#include <sinljoin.h>

SINLJoin::SINLJoin(memmgr *mmgr)
    : Join(mmgr),
    m_require_iterating_expired_join_results(false),
    m_require_reiterating_active_join_results(false) {}

SINLJoin::~SINLJoin() {}

JoinExecState *SINLJoin::create_exec_state(
    memmgr          *mmgr,
    STable          **stables,
    UINT8           seed) {
    
    SINLJoinExecState *exec_state = nullptr;

    exec_state = mmgr->newobj<SINLJoinExecState>(
                    mmgr,
                    this,
                    stables);

    if (!exec_state->initialized()) {
        exec_state->~SINLJoinExecState();
        mmgr->deallocate(exec_state);
        return nullptr;
    }

    for (SID sid = 0; sid < num_streams(); ++sid) {
        stables[sid]->set_downstream_exec_state(exec_state);
    }

    return exec_state;
}

SINLJoinExecState::SINLJoinExecState(
    memmgr              *mmgr,
    SINLJoin            *sinljoin,
    STable              **stables)
    : m_mmgr(mmgr),
      m_n_streams(sinljoin->num_streams()),
      m_window_non_overlapping(sinljoin->m_window_non_overlapping),
      m_require_iterating_expired_join_results(
        sinljoin->m_require_iterating_expired_join_results),
      m_require_reiterating_active_join_results(
        sinljoin->m_require_reiterating_active_join_results),
      m_is_unwindowed(sinljoin->m_is_unwindowed),
      m_iterating_expired_join_results(false),
      m_reiterating_active_join_results(false),
      m_stables(stables),
      m_generic_mempool(nullptr),
      m_layout(nullptr),
      m_vertex_mempool(nullptr),
      m_n_indexes(0),
      m_indexes(nullptr),
      m_vertex_hash_tables(nullptr),
      m_num_seen(0),
      m_buffered_records(mmgr),
      m_join_result{.m_rowids = nullptr},
      m_current_record{.m_sid = INVALID_SID},
      m_vertex_iter(nullptr) {
    
    if (!create_vertex_layout(sinljoin->m_predicates)) goto fail;
    
    if (!create_indexes()) goto fail;

    init_join();
    
    return ;
fail:
    clean_all();
}

SINLJoinExecState::~SINLJoinExecState() {
    clean_all();
}

bool SINLJoinExecState::create_vertex_layout(std_vector<Predicate> &predicates) {
    m_layout = m_mmgr->allocate_array<SINLJoinVertexLayout>(m_n_streams);
    memset(m_layout, 0, sizeof(SINLJoinVertexLayout) * m_n_streams);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        // INVALID_SID represents unvisited vertices
        m_layout[sid].m_sid = INVALID_SID;
        m_layout[sid].m_pred_join = m_mmgr->allocate_array<Predicate*>(m_n_streams);
        memset(m_layout[sid].m_pred_join, 0, sizeof(Predicate*) * m_n_streams);
    }

    // initialize edges
    std_vector<std_vector<Predicate*>> edges(
            m_mmgr, m_n_streams, std_vector<Predicate*>(m_mmgr));
    for (Predicate& pred: predicates) {
        edges[pred.get_lsid()].push_back(&pred);
        edges[pred.get_rsid()].push_back(&pred);
    }

    // BFS
    std_deque<SID> q(m_mmgr);
    m_layout[0].m_sid = 0;
    q.push_back(0);
    SID n_visited = 0;

    while (!q.empty()) {
        ++n_visited;
        
        SID cur_sid = q.front();
        q.pop_front();

        for (Predicate *pred : edges[cur_sid]) {
            SID other_sid = pred->get_the_other_sid(cur_sid);
            if (m_layout[other_sid].m_sid == INVALID_SID) {
                ++m_layout[cur_sid].m_n_neighbors;
                m_layout[cur_sid].m_pred_join[other_sid] = pred;

                ++m_layout[other_sid].m_n_neighbors;
                m_layout[other_sid].m_pred_join[cur_sid] = pred;

                q.push_back(other_sid);
                m_layout[other_sid].m_sid = other_sid;
            }
        }
    }
    
    if (n_visited < m_n_streams) {
        return false;
    }

    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SINLJoinVertexLayout *s = &m_layout[sid];
        RT_ASSERT(s->m_n_neighbors > 0);

        s->m_neighbor_sid = m_mmgr->allocate_array<SID>(s->m_n_neighbors);
        SID i = 0;
        for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
            if (s->m_pred_join[sid2]) {
                s->m_pred_join[i++] = s->m_pred_join[sid2];
            }
        }
        std::sort(s->m_pred_join, s->m_pred_join + s->m_n_neighbors,
            [sid](Predicate *p1, Predicate *p2) -> bool {
                return p1->get_my_colid(sid) < p2->get_my_colid(sid);
            });
        for (i = 0; i < s->m_n_neighbors; ++i) {
            s->m_neighbor_sid[i] = s->m_pred_join[i]->get_the_other_sid(sid);
        }
    
        // XXX assume there's no filter at all
        // TODO handle filters
        s->m_n_filters = edges[sid].size() - s->m_n_neighbors;
        RT_ASSERT(s->m_n_filters == 0);
        s->m_filters = nullptr;
    }

    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SINLJoinVertexLayout *s = &m_layout[sid];
        PayloadOffset payload_offset = 0;

        // Indexes
        s->m_n_keys = 1;
        for (UINT4 i = 1; i < s->m_n_neighbors; ++i) {
            if (s->m_pred_join[i]->get_my_colid(sid) != 
                s->m_pred_join[i-1]->get_my_colid(sid)) {
                ++s->m_n_keys;
            }
        }
    
        s->m_index_id = m_mmgr->allocate_array<IndexId>(s->m_n_keys);
        s->m_key_colid = m_mmgr->allocate_array<COLID>(s->m_n_keys);
        s->m_key_ti = m_mmgr->allocate_array<typeinfo*>(s->m_n_keys);
        s->m_offset_key = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_hdiff = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_parent = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_left = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_right = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        IndexId j = 0;
        for (UINT4 i = 0; i < s->m_n_neighbors; ++i) {
            if (i == 0 ||
                (s->m_pred_join[i]->get_my_colid(sid) != 
                s->m_pred_join[i-1]->get_my_colid(sid))) {

                s->m_index_id[j] = m_n_indexes++;
                s->m_key_colid[j] = s->m_pred_join[i]->get_my_colid(sid);
                s->m_key_ti[j] = m_stables[sid]->get_schema()->get_ti(s->m_key_colid[j]);
                s->m_offset_key[j] = allocate_vertex_payload<SINLJoinVertex>(
                    payload_offset,
                    s->m_key_ti[i]->get_size(),
                    s->m_key_ti[i]->get_alignment());
                s->m_offset_hdiff[j] = allocate_vertex_payload<SINLJoinVertex>(
                    payload_offset,
                    sizeof(INT4),
                    sizeof(INT4));
                s->m_offset_left[j] = allocate_vertex_payload<SINLJoinVertex>(
                    payload_offset,
                    sizeof(SINLJoinVertexLayout*),
                    sizeof(SINLJoinVertexLayout*));
                s->m_offset_right[j] = allocate_vertex_payload<SINLJoinVertex>(
                    payload_offset,
                    sizeof(SINLJoinVertexLayout*),
                    sizeof(SINLJoinVertexLayout*));
                s->m_offset_parent[j] = allocate_vertex_payload<SINLJoinVertex>(
                    payload_offset,
                    sizeof(SINLJoinVertexLayout*),
                    sizeof(SINLJoinVertexLayout*));
                ++j;
            }
        }
    
        allocate_vertex_payload<SINLJoinVertex>(payload_offset, 0, 8);
        s->m_vertex_size = sizeof(SINLJoinVertex) + payload_offset;
        
        // to be filled in the next loop
        s->m_offset_key_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<PayloadOffset>(s->m_n_neighbors);
        s->m_index_id_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<IndexId>(s->m_n_neighbors);
        s->m_key_ti_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<typeinfo*>(s->m_n_neighbors);

        s->m_iteration_seq = m_mmgr->allocate_array<SID>(m_n_streams);
        s->m_neighbor_idx_of_parent_in_me = m_mmgr->allocate_array<UINT4>(m_n_streams);
        s->m_neighbor_idx_of_me_in_parent = m_mmgr->allocate_array<UINT4>(m_n_streams);
    }

    // fill in fast indexes
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SINLJoinVertexLayout *s = &m_layout[sid];

        // for each key
        UINT4 neighbor_idx = 0;
        for (UINT4 index_idx = 0; index_idx < s->m_n_keys; ++index_idx) {
            // for each neighbor
            while (neighbor_idx < s->m_n_neighbors &&
                    s->m_pred_join[neighbor_idx]->get_my_colid(sid) ==
                    s->m_key_colid[index_idx]) {

                /*SID sid2 = s->m_pred_join[neighbor_idx]
                            ->get_the_other_sid(sid);
                SINLJoinVertexLayout *s2 = &m_layout[sid2];

                for (UINT4 j = 0; j < s2->m_n_neighbors; ++j) {
                    if (s2->m_neighbor_sid[j] == sid) {
                        s2->m_neighbor_idx_of_me_in_neighbor[j] = neighbor_idx;
                        break;
                    }
                } */

                s->m_offset_key_indexed_by_neighbor_idx[neighbor_idx] =
                    s->m_offset_key[index_idx];
                s->m_index_id_indexed_by_neighbor_idx[neighbor_idx] =
                    s->m_index_id[index_idx];
                s->m_key_ti_indexed_by_neighbor_idx[neighbor_idx] =
                    s->m_key_ti[index_idx];

                ++neighbor_idx;
            }
        }

        DARG0(SID n =) compute_iteration_seq(
            sid,
            s->m_iteration_seq,
            0,
            INVALID_SID,
            sid,
            0);

        RT_ASSERT(n == m_n_streams);
    }

    return true;
}

SID SINLJoinExecState::compute_iteration_seq(
    SID s_root,
    SID *seq,
    SID i,
    SID s0,
    SID s1,
    UINT4 neighbor_idx_of_s1) {

    SINLJoinVertexLayout *layout = &m_layout[s1];
    
    seq[i++] = s1;
    for (UINT4 neighbor_idx = 0;
            neighbor_idx < layout->m_n_neighbors;
            ++neighbor_idx) {
        if (layout->m_neighbor_sid[neighbor_idx] == s0) {
            layout->m_neighbor_idx_of_parent_in_me[s_root] = neighbor_idx;
            break;
        }
    }
    layout->m_neighbor_idx_of_me_in_parent[s_root] = neighbor_idx_of_s1;
    
    for (UINT4 neighbor_idx = 0;
            neighbor_idx < layout->m_n_neighbors;
            ++neighbor_idx) {
        
        if (layout->m_neighbor_sid[neighbor_idx] == s0) continue;

        i = compute_iteration_seq(
            s_root,
            seq,
            i,
            s1,
            layout->m_neighbor_sid[neighbor_idx],
            neighbor_idx);
    }

    return i; 
}

void SINLJoinExecState::clean_vertex_layout() {
    if (m_layout) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            SINLJoinVertexLayout *s = &m_layout[sid];
            if (!s) continue;
            m_mmgr->deallocate(s->m_filters);
            m_mmgr->deallocate(s->m_neighbor_sid);
            m_mmgr->deallocate(s->m_pred_join);
            m_mmgr->deallocate(s->m_index_id);
            m_mmgr->deallocate(s->m_key_colid);
            m_mmgr->deallocate(s->m_key_ti);
            m_mmgr->deallocate(s->m_offset_key);
            m_mmgr->deallocate(s->m_offset_hdiff);
            m_mmgr->deallocate(s->m_offset_left);
            m_mmgr->deallocate(s->m_offset_right);
            m_mmgr->deallocate(s->m_offset_parent);
            m_mmgr->deallocate(s->m_offset_key_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_index_id_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_key_ti_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_iteration_seq);
            m_mmgr->deallocate(s->m_neighbor_idx_of_parent_in_me);
            m_mmgr->deallocate(s->m_neighbor_idx_of_me_in_parent);
        }

        m_mmgr->deallocate(m_layout);
        m_layout = nullptr;
    }
}


bool SINLJoinExecState::create_indexes() {
    m_indexes = m_mmgr->allocate_array<AVL*>(m_n_indexes);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SINLJoinVertexLayout *layout = &m_layout[sid];
        for (UINT4 i = 0; i < layout->m_n_keys; ++i) {
            IndexId index_id = layout->m_index_id[i];

            AVLNodeDesc desc;
            desc.m_offset_key = layout->m_offset_key[i];
            desc.m_offset_left = layout->m_offset_left[i];
            desc.m_offset_right = layout->m_offset_right[i];
            desc.m_offset_parent = layout->m_offset_parent[i];
            desc.m_offset_hdiff = layout->m_offset_hdiff[i];
                desc.m_key_ti = layout->m_key_ti[i];

            m_indexes[index_id] = m_mmgr->newobj<AVL>(&desc);
        }
    }

    m_vertex_mempool = m_mmgr->allocate_array<mempool_memmgr*>(m_n_streams);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        size_t vsize = m_layout[sid].m_vertex_size;
        size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, vsize);
        m_vertex_mempool[sid] = create_memmgr<mempool_memmgr>(
            m_mmgr,
            vsize,
            block_size);
    }
    
    if (!m_window_non_overlapping) {
        m_generic_mempool = create_memmgr<generic_mempool_memmgr>(m_mmgr, 1*1024*1024);
        m_vertex_hash_tables = m_mmgr->allocate_array<
            std_unordered_map<ROWID, SINLJoinVertex*>>(m_n_streams);
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            new (m_vertex_hash_tables + sid)
                std_unordered_map<ROWID, SINLJoinVertex*>(m_generic_mempool);
        }
    }

    return true;
}

void SINLJoinExecState::clean_indexes() {
    // vertex mempool
    if (m_vertex_mempool) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            destroy_memmgr(m_vertex_mempool[sid]);
        }
        m_mmgr->deallocate(m_vertex_mempool);
        m_vertex_mempool = nullptr;
    }

    // AVL tree indexes
    if (m_indexes) {
        for (IndexId i = 0; i < m_n_indexes; ++i) {
            m_mmgr->deallocate(m_indexes[i]);
        }
        m_mmgr->deallocate(m_indexes);
        m_indexes = nullptr;
    }

    if (m_vertex_hash_tables) {
        m_mmgr->deallocate(m_vertex_mempool);
        m_vertex_mempool = nullptr;
    }

    if (m_generic_mempool) {
        destroy_memmgr(m_generic_mempool);
        m_generic_mempool = nullptr;
    }
}

void SINLJoinExecState::clean_all() {
    clean_indexes();

    clean_vertex_layout();
}

void SINLJoinExecState::init_join() {
    m_join_result.m_rowids = m_mmgr->allocate_array<ROWID>(m_n_streams);
    m_vertex_iter = m_mmgr->allocate_array<SINLJoinVertexIterator>(m_n_streams);
    
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_vertex_iter[sid].m_vertex = nullptr;
    }
}

void SINLJoinExecState::reinit() {
    m_buffered_records.clear();
    m_current_record.m_sid = INVALID_SID;

    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_indexes[sid]->clear();
        m_vertex_mempool[sid]->deallocate_all();
        m_vertex_iter[sid].m_vertex = nullptr;
    }
}

void SINLJoinExecState::notify(SID sid, ROWID rowid) {
    RT_ASSERT(!m_iterating_expired_join_results
            && !m_reiterating_active_join_results,
            "finish fetching expired/active join results before call notify()");
    //m_iterating_expired_join_results = false;
    //m_reiterating_active_join_results = true;
    m_buffered_records.push_back(RecordId{sid, rowid});
}

void SINLJoinExecState::expire(SID sid, ROWID rowid) {
    if (m_require_iterating_expired_join_results) {
        RT_ASSERT(!m_reiterating_active_join_results &&
                (m_iterating_expired_join_results
                 || m_buffered_records.empty()),
                "finish fetching active join results before call expire()");
        m_iterating_expired_join_results = true;
        m_buffered_records.push_back(RecordId{sid, rowid});
    } else {
        SINLJoinVertex *vertex = remove_vertex(sid, rowid);
        m_vertex_mempool[sid]->deallocate(vertex);
    }
}

void SINLJoinExecState::set_min_ts(TIMESTAMP min_ts) {
    if (m_require_reiterating_active_join_results) {
        RT_ASSERT(!m_iterating_expired_join_results &&
                m_buffered_records.empty());

        SID sid = 0;
        for (const auto &p: m_vertex_hash_tables[sid]) {
            m_buffered_records.push_back(RecordId{sid, p.first});
        }

        m_reiterating_active_join_results = true;
        m_num_seen = 0;
    }
}

void SINLJoinExecState::expire_all() {
    reinit();
    m_num_seen = 0;
}

SINLJoinVertex *SINLJoinExecState::remove_vertex(SID sid, ROWID rowid) {
    SINLJoinVertexLayout *layout = &m_layout[sid];
    SINLJoinVertex *vertex;

    auto iter = m_vertex_hash_tables[sid].find(rowid);
    RT_ASSERT(iter != m_vertex_hash_tables[sid].end());
    m_vertex_hash_tables[sid].erase(iter);
    vertex = iter->second; 
    for (UINT4 i = 0; i < layout->m_n_keys; ++i) {
        IndexId index_id =layout->m_index_id[i];
        m_indexes[index_id]->erase(
            vertex,
            0,
            nullptr,
            nullptr);
    }

    return vertex;
}

bool SINLJoinExecState::init_vertex_iters() {
    SID s_root = m_current_record.m_sid;
    SINLJoinVertexLayout *layout = &m_layout[s_root];
    SINLJoinVertex *vertex;
    if (m_iterating_expired_join_results) {
        // delete vertex
        vertex = remove_vertex(s_root, m_current_record.m_rowid);
    } else if (m_reiterating_active_join_results) {
        auto iter = m_vertex_hash_tables[s_root].find(m_current_record.m_rowid);
        RT_ASSERT(iter != m_vertex_hash_tables[s_root].end());
        vertex = iter->second;
    } else {
        // insert vertex
        vertex = project_current_record();
        for (UINT4 i = 0; i < layout->m_n_keys; ++i) {
            IndexId index_id = layout->m_index_id[i];
            m_indexes[index_id]->insert(
                vertex,
                0,
                nullptr,
                nullptr);
        }
        if (!m_window_non_overlapping) {
            DARG0(bool success = ) m_vertex_hash_tables[s_root].emplace(
                m_current_record.m_rowid,
                vertex).second;
            RT_ASSERT(success);
        }
    }
    m_vertex_iter[s_root].m_vertex = vertex;
    return get_next_join_result(s_root, 1);
}

bool SINLJoinExecState::get_next_join_result(SID s_root, SID i) {
    SINLJoinVertexLayout *layout = &m_layout[s_root];
    
    while (i > 0 && i < m_n_streams) {
        SID s1 = layout->m_iteration_seq[i];
        SINLJoinVertexLayout *layout1 = &m_layout[s1];
        UINT4 neighbor_idx_of_s0 = layout1->m_neighbor_idx_of_parent_in_me[s_root];
        UINT4 neighbor_idx_of_s1 = layout1->m_neighbor_idx_of_me_in_parent[s_root];        
        IndexId index_id =
            layout1->m_index_id_indexed_by_neighbor_idx[neighbor_idx_of_s0]; 
        AVL *I = m_indexes[index_id];
        SINLJoinVertex *vertex = m_vertex_iter[s1].m_vertex;
        if (vertex) {
            vertex = I->next(vertex);
        } else {
            // initialize vertex 
            SID s0 = layout1->m_neighbor_sid[neighbor_idx_of_s0];
            SINLJoinVertexLayout *layout0 = &m_layout[s0]; 
            PayloadOffset offset_key0 =
                layout0->m_offset_key_indexed_by_neighbor_idx[neighbor_idx_of_s1];
            Datum key0 = datum_at(m_vertex_iter[s0].m_vertex, offset_key0);

            Predicate *pred = layout1->m_pred_join[neighbor_idx_of_s0];
            DATUM low;
            pred->compute_range_ii(
                s0,
                key0,
                &low,
                &m_vertex_iter[s1].m_high);

            vertex = I->lower_bound(&low);
        }

        if (vertex) {
	    typeinfo *ti =
                layout1->m_key_ti_indexed_by_neighbor_idx[neighbor_idx_of_s0];
            PayloadOffset offset_key1 =
                layout1->m_offset_key_indexed_by_neighbor_idx[neighbor_idx_of_s0];
            Datum key1 = datum_at(vertex, offset_key1);
            int cmp_res = ti->compare(key1, &m_vertex_iter[s1].m_high);
	    if (cmp_res <= 0) {
                m_vertex_iter[s1].m_vertex = vertex;
                ++i;
                continue;
            }
        }
        // don't have next
        m_vertex_iter[s1].m_vertex = nullptr;
        --i;
    }
    if (i == 0) {
        if (m_iterating_expired_join_results) {
            m_vertex_mempool[s_root]->deallocate(m_vertex_iter[s_root].m_vertex);
        }
        m_vertex_iter[s_root].m_vertex = nullptr;
        return false;
    }

    return true;
}

SINLJoinVertex *SINLJoinExecState::project_current_record() {
    SID sid = m_current_record.m_sid;
    ROWID rowid = m_current_record.m_rowid;
    SINLJoinVertex *vertex;
    Record rec;
    SINLJoinVertexLayout *layout = &m_layout[sid];

    vertex = (SINLJoinVertex *) m_vertex_mempool[sid]->allocate(layout->m_vertex_size);
    vertex->m_rowid = rowid;

    m_stables[sid]->get_record(rowid, &rec); 
    for (UINT4 i = 0; i < layout->m_n_keys; ++i) {
        rec.copy_column(
            datum_at(vertex, layout->m_offset_key[i]),
            layout->m_key_colid[i]);
    }

    return vertex;
}

void SINLJoinExecState::init_join_result_iterator() {
    if (m_current_record.m_sid != INVALID_SID) return ;
    while (!m_buffered_records.empty()) {
        m_current_record = m_buffered_records.front();
        m_buffered_records.pop_front();
        if (init_vertex_iters()) {
	    return;
        }
    }
    if (m_iterating_expired_join_results) {
        m_iterating_expired_join_results = false;
    }
    if (m_reiterating_active_join_results) {
        m_reiterating_active_join_results = false;
    }
    m_current_record.m_sid = INVALID_SID;
}

bool SINLJoinExecState::has_next_join_result() {
    return m_current_record.m_sid != INVALID_SID;
}

const JoinResult &SINLJoinExecState::next_join_result() {
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_join_result.m_rowids[sid] = m_vertex_iter[sid].m_vertex->m_rowid;
    }
    compute_join_result_timestamp();
    if (m_iterating_expired_join_results) {
        --m_num_seen;
    } else {
        if (!m_reiterating_active_join_results) {
            ++m_num_processed;
        }
        ++m_num_seen;
    }
    
    if (!get_next_join_result(m_current_record.m_sid, m_n_streams - 1)) {
        m_current_record.m_sid = INVALID_SID;
        init_join_result_iterator();
    }

    return m_join_result;
}

void SINLJoinExecState::compute_join_result_timestamp() {
    // unwindowed tables aren't required to have timestamp columns
    if (m_is_unwindowed) return;
    RT_ASSERT(m_join_result.m_rowids);

    TIMESTAMP min_ts = TS_INFTY;
    Record rec;
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_stables[sid]->get_record(m_join_result.m_rowids[sid], &rec);
        TIMESTAMP ts = rec.get_ts();
        if (ts < min_ts) min_ts = ts;
    }

    m_join_result.m_ts = min_ts;
}
