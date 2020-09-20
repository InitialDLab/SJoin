/*
 * sjoin.h
 *
 * Streaming Join Sampling
 *
 *
 */
#ifndef SJOIN_H
#define SJOIN_H

#include <config.h>
#include <memmgr.h>
#include <join.h>
#include <stable.h>
#include <predicate.h>
#include <vertex.h>
#include <avl.h>
#include <walker.h>

#include <type_traits>
#include <random>

class SJoin;
class SJoinExecState;

constexpr UINT4 max_walker_N = 1000000;

typedef AVL<SJoinJoinGraphVertex> SJoinJoinGraphVertexAVL;

enum SamplingType {
    UNSPECIFIED_SAMPLING_TYPE,
    BERNOULLI_SAMPLING,
    SAMPLING_WITH_REPLACEMENT,
    SAMPLING_WITHOUT_REPLACEMENT
};

class SJoin: public Join {
public:
    SJoin(memmgr *mmgr);

    ~SJoin();

    void set_bernoulli_sampling(DOUBLE sampling_rate) {
        m_sampling_type = BERNOULLI_SAMPLING;
        m_sampling_param.m_sampling_rate = sampling_rate;
    }

    void set_sampling_with_replacement(ROWID sample_size) {
        m_sampling_type = SAMPLING_WITH_REPLACEMENT;
        m_sampling_param.m_sample_size = sample_size;
    }

    void set_sampling_without_replacement(ROWID sample_size) {
        m_sampling_type = SAMPLING_WITHOUT_REPLACEMENT;
        m_sampling_param.m_sample_size = sample_size;
    }

    JoinExecState *create_exec_state(
        memmgr *mmgr,
        STable **stables,
        UINT8 seed);
    
private:
    SamplingType                m_sampling_type;

    union {
        DOUBLE                  m_sampling_rate;
        UINT8                   m_sample_size;
    }                           m_sampling_param;

    friend class SJoinExecState;
};

struct VertexHash {
    VertexHash(SJoinQueryGraphVertex *gq_vertex)
        : m_n_keys(gq_vertex->m_n_keys),
        m_offset_key(gq_vertex->m_offset_key),
        m_key_ti(gq_vertex->m_key_ti) {}

    size_t operator()(SJoinJoinGraphVertex *v) const {
        size_t h = 0;
        for (IndexId i = 0; i < m_n_keys; ++i) {
            h = h * 131 + m_key_ti[i]->hash(datum_at(v, m_offset_key[i]));
        }
        return h;
    }
    
    IndexId           m_n_keys;
    PayloadOffset     *m_offset_key;
    typeinfo          **m_key_ti;
};

struct VertexEqual {
    VertexEqual(SJoinQueryGraphVertex *gq_vertex)
        : m_n_keys(gq_vertex->m_n_keys),
        m_offset_key(gq_vertex->m_offset_key),
        m_key_ti(gq_vertex->m_key_ti) {}

    bool operator()(
        SJoinJoinGraphVertex *v1,
        SJoinJoinGraphVertex *v2) const {
        
        for (IndexId i = 0; i < m_n_keys; ++i) {
            if (m_key_ti[i]->compare(
                    datum_at(v1, m_offset_key[i]),
                    datum_at(v2, m_offset_key[i]))) {
                return false;
            }
        }

        return true;
    }

    IndexId           m_n_keys;
    PayloadOffset     *m_offset_key;
    typeinfo          **m_key_ti;
};

struct VertexHashTable {
    typedef std_unordered_set<
                SJoinJoinGraphVertex*,
                VertexHash,
                VertexEqual> HashTable;

    void initialize(SJoinQueryGraphVertex *gq_vertex, memmgr *mmgr);
    
    // returns true if found; or false if inserted
    bool find_or_insert(SJoinJoinGraphVertex *&vertex);

    SID                     m_sid;

    HashTable               m_index;
};

struct ConsolidatedTableTupleHash {
    ConsolidatedTableTupleHash(typeinfo *ti)
        : m_ti(ti) {}

    size_t operator()(const DATUM &d) const {
        return m_ti->hash(&d);
    }

    typeinfo                *m_ti;
};

struct ConsolidatedTableTupleEqual {
    ConsolidatedTableTupleEqual(typeinfo *ti)
        : m_ti(ti) {}

    bool operator()(
        const DATUM &d1,
        const DATUM &d2) const {
        
        return m_ti->compare(&d1, &d2) == 0;
    }
    
    typeinfo                *m_ti;
};

struct ConsolidatedTableTupleHashTable {
    typedef std_unordered_map<
                DATUM, ROWID,
                ConsolidatedTableTupleHash,
                ConsolidatedTableTupleEqual>
            HashTable;

    void initialize(
        STable *stable,
        memmgr *mmgr);

    bool insert(ROWID rowid);
    
    // Returns the ROWID that matches the key. If not found,
    // returns INVALID_ROWID instead.
    ROWID find(const DATUM &key);

    void remove(ROWID rowid);

private:
    STable                  *m_stable;

    COLID                   m_pkey_colid;
    
    Record                  m_rec;
    
    HashTable               m_index;
};

struct IndexInfo {
    SID                     m_base_sid;
                            
    SJoinJoinGraphVertexAVL *m_index;
};

/*
 * Struct containing all the info about the updated join graph vertices
 * required by batch sampler used in notify_multiple and/or expire_multiple.
 *
 */
struct UpdatedVertexInfo {
    SJoinJoinGraphVertex *m_vertex;

    UINT8 m_old_rowids_len;
};

struct ForeignKeyRef {
    SID                 m_sid;

    COLID               m_colid;
};


class SJoinExecState: public JoinExecState {
private:
    typedef SJoinJoinGraphVertexAVL AVL;
public:
    SJoinExecState(
        memmgr *mmgr,
        SJoin *sjoin,
        STable **stables,
        UINT8 seed);

    ~SJoinExecState();

    virtual void notify(SID sid, ROWID rowid);
    
    virtual void expire(SID sid, ROWID rowid);

    virtual void expire_all();

    virtual void notify_multiple(SID sid, ROWID *rowid, UINT8 num_rows);

    virtual void expire_multiple(SID sid, ROWID start_rowid, UINT8 num_rows);

    virtual void set_min_ts(TIMESTAMP min_ts);

    virtual void expire_multiple_and_retire_samples(SID sid, const ROWID
            start_rowid, UINT8 num_rows);
    
    bool initialized() const { return m_gq_vertices; }

    std::pair<
        std_vector<JoinResult>::const_iterator,
        std_vector<JoinResult>::const_iterator> get_samples() const {
        
        switch (m_sampling_type) {
        case BERNOULLI_SAMPLING:
            return std::make_pair(m_reservoir.begin(), m_reservoir.end());
        case SAMPLING_WITH_REPLACEMENT:
            if (m_num_seen == 0) {
                return std::make_pair(m_reservoir.begin(), m_reservoir.begin());
            }
            return std::make_pair(m_reservoir.begin(), m_reservoir.end());
        case SAMPLING_WITHOUT_REPLACEMENT:
            return std::make_pair(m_reservoir.begin() + 1, m_reservoir.end());
        default:
            RT_ASSERT(false);
        }
        
        return std::make_pair(m_reservoir.end(), m_reservoir.end());
    }

    UINT8 num_seen() const { return m_num_seen; }

    UINT8 num_processed() const { return m_num_expired + m_num_seen; }

    UINT8 read_num_processed() const {
        return *((volatile UINT8*) m_num_expired) + *((volatile UINT8*) m_num_seen);
    }

    void init_join_result_iterator();

    bool has_next_join_result();

    const JoinResult &next_join_result();

    void set_max_key_constraint_warnings(
        UINT4 max_key_constraint_warnings) override {
        
        m_max_key_constraint_warnings = max_key_constraint_warnings;
    }

private:
    bool consolidate_key_joins();

    SID get_my_sid_in_pred_join(
        SID sid,
        Predicate *pred) const;

    bool create_query_graph();

    bool create_node_layout();

    void clean_query_graph();

    bool create_indexes();

    void clean_indexes();

    void create_index(
        SJoinQueryGraphVertex *gq_vertex,
        UINT4 i);

    void clean_all();

    void reinit();
    
    // Insert a batch of records into a table.
    // If table ``sid'' is a consolidatd table, it returns
    // an empty vector.
    std_vector<UpdatedVertexInfo> insert_multiple_records(
        SID sid,
        ROWID *rowid,
        UINT8 num_rows); 
    
    // Insert a record into a table.
    // If table ``sid'' is a consolidated table, it returns
    // nullptr and *num_results is set to 0.
    SJoinJoinGraphVertex *insert_record(
        SID sid,
        ROWID rowid,
        WEIGHT *num_results);
    
    // remove a batch of records
    void remove_multiple_records(
        SID sid,
        ROWID start_rowid,
        UINT8 num_rows);
    
    // Insert a record into a consolidated table.
    // Table ``sid'' must be a consolidated table.
    void insert_record_into_consolidated_table(
        SID sid,
        ROWID rowid);

    ROWID find_record_from_consolidated_table(
        SID sid,
        ROWID parent_rowid);
    
    // Remove a record from a consolidated table.
    // Table ``sid'' must be a consolidated table.
    void remove_record_from_consolidated_table(
        SID sid,
        ROWID rowid);
    
    // remove one record
    void remove_record(
        SID sid,
        ROWID rowid);

    SJoinJoinGraphVertex *project_record(
        SID sid,
        ROWID rowid);

    WEIGHT compute_child_sum_weight(
        SJoinQueryGraphVertex   *gq_vertex,
        SJoinJoinGraphVertex    *vertex,
        UINT4                   neighbor_id);

    void compute_range_ii(
        Predicate               *pred,
        SID                     my_sid,
        Datum                   my_value,
        Datum                   other_value_low,
        Datum                   other_value_high);

    void update_neighbor(
        SID                     s0,
        SID                     s1,
        UINT4                   neighbor_idx_of_s0,
        std_vector<std::pair<Datum, WEIGHT>>
                                &u);

    void draw_random_sample(ROWID *rowids);

    void get_sample_by_join_number(
        SID                     s0,
        SID                     s1,
        SJoinJoinGraphVertex    *vertex,
        UINT4                   neighbor_idx_of_s0,
        WEIGHT                  join_number,
        ROWID                   *rowids);

    bool initialize_sampling();
    
    void reinit_sampling();

    void sample_next_batch(
        SID                     sid,
        SJoinJoinGraphVertex    *vertex,
        WEIGHT                  start_join_number,
        WEIGHT                  end_join_number);

    void clean_sampling();

    void compute_join_sample_timestamp(JoinResult *jr);

    inline void update_sample_reservoir() {
        switch (m_sampling_type) {
        case BERNOULLI_SAMPLING:
            bernoulli_update_sample_reservoir();
            break;

        case SAMPLING_WITH_REPLACEMENT:
            sampling_with_replacement_update_sample_reservoir();
            break;

        case SAMPLING_WITHOUT_REPLACEMENT:
            sampling_without_replacement_update_sample_reservoir();
            break;

        default:
            RT_ASSERT(false);
        }
    }

    inline WEIGHT get_next_skip_number() {
        switch (m_sampling_type) {
        case BERNOULLI_SAMPLING:
            return bernoulli_get_next_skip_number();

        case SAMPLING_WITH_REPLACEMENT:
            return sampling_with_replacement_get_next_skip_number();

        case SAMPLING_WITHOUT_REPLACEMENT:
            return sampling_without_replacement_get_next_skip_number();

        default:
            RT_ASSERT(false);
        }
        
        return 0;
    }

    WEIGHT vitter_algo_Z(
        UINT8 num_seen,
        UINT8 sample_size,
        UINT8 algo_Z_threshold);

    bool bernoulli_initialize();

    void bernoulli_reinit();
    
    WEIGHT bernoulli_get_next_skip_number();

    void bernoulli_update_sample_reservoir();
    
    void bernoulli_set_min_ts(TIMESTAMP min_ts);

    void bernoulli_retire_samples(SID sid, ROWID start_rowid, UINT8 num_rows);

    void bernoulli_clean();

    bool sampling_without_replacement_initialize();

    void sampling_without_replacement_reinit();

    WEIGHT sampling_without_replacement_get_next_skip_number() {
        return vitter_algo_Z(
            m_num_seen,
            m_sampling_param.m_sample_size,
            m_algo_Z_threshold);
    }

    void sampling_without_replacement_update_sample_reservoir();

    void sampling_without_replacement_set_min_ts(TIMESTAMP min_ts);

    void sampling_without_replacement_rebuild_reservoir();

    void sampling_without_replacement_retire_samples(
            SID sid, ROWID start_rowid, UINT8 num_rows);

    void sampling_without_replacement_clean();

    bool sampling_with_replacement_initialize();

    void sampling_with_replacement_reinit();

    inline WEIGHT sampling_with_replacement_get_next_skip_number() {
        return ((WEIGHT)(m_min_heap_nproc[1].m_next_num_processed))
            - num_processed();
    }

    void sampling_with_replacement_update_sample_reservoir();

    void sampling_with_replacement_set_min_ts(TIMESTAMP min_ts);

    void sampling_with_replacement_retire_samples(
            SID sid, ROWID start_rowid, UINT8 num_rows);

    void sampling_with_replacement_clean();

    struct SampleSkipNumber {
        UINT8                   m_next_num_processed;
        UINT8                   m_sample_idx;
    };

private:
    memmgr                      *m_mmgr;
    
    // The original number of streams.
    const SID                   m_n_streams;

    const bool                  m_window_non_overlapping;
    
    const bool                  m_is_unwindowed;

    const SamplingType          m_sampling_type;

    const std::decay_t<decltype(((SJoin*)nullptr)->m_sampling_param)>
                                m_sampling_param;
    
    STable* const * const       m_stables;
    

    // Objects above this line are not owned by this.
    // Objects below this line are owned by this.
    
    // TODO explicitly list all things that are alloccated
    // from the generic_mempool. All that I can think of
    // right now is the rowid arrays in each vertex and the
    // nodes in the STL hash tables, anything else?
    //
    // Everything is allocated from m_generic_mempool, except that
    // 1. Vertices are allocated from m_vertex_mempool(s);
    // 2. Query graph vertices are allocated from m_mmgr;
    // 3. The class members are allocated from m_mmgr;
    // 4. AVL indexes are allocated from m_mmgr;
    //      Note that AVL only has a pointer m_root.
    //      Nodes in the tree are allocated here and owned by this.
    // 5. Vertex mempools are created from m_mmgr;
    //
    // On expire_all(), it can be safely cleared without
    // explicit deallocate() calls.
    generic_mempool_memmgr      *m_generic_mempool;

    std_vector<Predicate*>      m_predicates;

    SJoinQueryGraphVertex       *m_gq_vertices;
    
    mempool_memmgr              **m_vertex_mempool;

    // Stand-by join graph vertices to be inserted
    // This avoids exccessive call to allocator just to allocate
    // vertices for hash table look-ups
    SJoinJoinGraphVertex        **m_vertices;

    VertexHashTable             *m_vertex_hash_table;

    ConsolidatedTableTupleHashTable
                                *m_consolidated_table_tuple_hash_table;

    IndexId                     m_n_indexes;

    IndexInfo                   *m_index_info;

    JoinResult                  m_join_sample;

    std_vector<JoinResult>      m_reservoir;
    
    UINT8                       m_num_expired;

    UINT8                       m_num_seen;

    WEIGHT                      m_skip_number;

    std::mt19937_64             m_rng;

    WalkerParam                 *m_walker_param;

    std_vector<JoinResult>::const_iterator
                                m_reservoir_iter;
    
    std_unordered_set<
        ROWID*,
        ROWIDArrayHash,
        ROWIDArrayEqual>        m_sample_set; 

    std_multiset<std::pair<
        ROWID, UINT8>>          *m_rowid2sample_idx;

    std::uniform_real_distribution<DOUBLE>
                                m_algo_Z_unif;

    std::uniform_int_distribution<UINT8>
                                m_unif_sample_size;

    UINT8                       m_algo_Z_threshold;

    std_vector<SampleSkipNumber>
                                m_min_heap_nproc;

    std_vector<JoinResult*>     m_min_heap_ts;

    std_vector<UINT8>           m_sample_idx2min_heap_ts_idx;

    std_vector<ForeignKeyRef>   m_foreign_key_refs;

    ROWID                       *m_rowids_for_projection;

    UINT4                       m_num_key_constraint_warnings;

    UINT4                       m_max_key_constraint_warnings;

    // the multiplier T in algorithm Z of Vitter's paper
    // Random Sampling with a Reservoir
    static constexpr const UINT8
                                m_algo_Z_T = 22;
    
    // The maximum number of key constraint warnings to be printed
    // on stderr.
    static constexpr const UINT4
                                m_default_max_key_constraint_warnings = 10;
};

#endif // JOIN_H

