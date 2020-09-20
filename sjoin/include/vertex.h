#ifndef VERTEX_H
#define VERTEX_H

#include <config.h>
#include <memmgr.h>
#include <basictypes.h>
#include <predicate.h>
#include <cstddef>

typedef UINT2 IndexId;

struct SJoinQueryGraphVertex {
    SID             m_sid;
    
    // Set to m_sid if this is not a consolidated table,
    // or the root table for a primary key join predicate
    // tree.
    SID             m_root_sid;
    
    // Set to 0 if this is a consolidated table.
    UINT4           m_n_neighbors;

    UINT4           m_n_filters; 
    
    // TODO Multi-table filters are not implemented yet.
    // of length m_n_filters
    Predicate       **m_filters;
    
    // Not set if m_root_sid != m_sid;
    // For convenience, index 0 is m_sid
    // SIDs of neighbors (or its root table's if it's a consolidated table);
    // index starts from 1 It is of length m_n_neighbors + 1.
    SID             *m_neighbor_sid;
    
    // Not set if m_root_sid != m_sid;
    // For convenience, index 0 is the same as index 1
    // Join predicates with the neighbors; index starts from 1
    // It is of length m_n_neighbors + 1.
    // It is sorted by (my SID, my COLID) pairs.
    Predicate       **m_pred_join;
    
    // Set to 0 if m_root_sid != m_sid;
    // The number of the consolidated child tables.
    UINT4           m_n_child_tables;

DARG0(
    // A copy of m_n_child_tables for debug only.
    UINT4           m_n_child_tables_copy;
)

    // Not set if m_root_sid != m_sid;
    // SID of the consolidated child tables that can be
    // looked up using foreign key references starting
    // from this table.
    SID             *m_child_table_sid;
    
    // Not set if m_root_sid != m_sid;
    // Whether a child table is needed for projection.
    // If not, we don't need to look up it in project_record().
    // Of size m_n_child_tables.
    bool            *m_child_table_needed_for_projection;
    
    //
    // Indexes
    //

    // number of keys
    UINT4           m_n_keys;

    // index ids of the indexes for the keys; of length m_n_keys
    IndexId         *m_index_id;

    // Prefix sum of number of neighbors of each index;
    // of length m_n_keys + 1 ([0] is always 0, [m_n_keys] is
    // m_n_neighbors + 1)
    UINT4           *m_index_n_neighbors_prefix_sum;
    
    // The actual SID of the keys; of length m_n_keys
    SID             *m_key_sid;
    
    // column id of the keys; of length m_n_keys
    COLID           *m_key_colid;

    typeinfo        **m_key_ti;

    // index into payload for keys; of length m_n_keys
    PayloadOffset   *m_offset_key;

    // starting index into payload for hdiff & tree ptrs
    // of length m_n_keys;
    PayloadOffset   *m_offset_hdiff;
    PayloadOffset   *m_offset_left;
    PayloadOffset   *m_offset_right;
    PayloadOffset   *m_offset_parent;

    // Weights & ptrs
    //

    // index into payload for weight
    // w/ m_neighbor_sid[i] as root;
    // of length m_n_neighbors + 1
    PayloadOffset    *m_offset_weight;
    
    // index into payload for subtree aggregate of weight
    // w/ m_neighbor_sid[i] as root;
    // of length m_n_neighbors + 1
    PayloadOffset    *m_offset_subtree_weight;
    
    // index into payload for
    // m_neighbor_sid[i]'s sum weight in range;
    // of length m_n_neighbors + 1;
    // [0] is undefined
    PayloadOffset    *m_offset_child_sum_weight;

    size_t           m_vertex_size;
    
    // provide fast lookups
    UINT4           *m_neighbor_idx_of_me_in_neighbor;

    PayloadOffset   *m_offset_key_indexed_by_neighbor_idx;

    SID             *m_key_sid_indexed_by_neighbor_idx;

    COLID           *m_key_colid_indexed_by_neighbor_idx;

    IndexId         *m_index_id_indexed_by_neighbor_idx;
};

struct SJoinJoinGraphVertex {
    std_deque<ROWID>                m_rowids;
    
    BYTE                            m_payload[0];
};

template<class VertexType>
inline PayloadOffset allocate_vertex_payload(
        PayloadOffset &base, size_t n, size_t align) {

    size_t ret;

// this is a hack and works on current G++
// replace std_deque with a standard-layout vector later!!
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
    size_t mod = (base + offsetof(VertexType, m_payload)) % align;
#pragma GCC diagnostic warning "-Winvalid-offsetof"

    if (mod > 0) {
        ret = base + (align - mod);
    } else {
        ret = base;
    }
    base = ret + n;
    return ret;
}

template<typename node_pointer>
inline WEIGHT &weight_at(node_pointer p, PayloadOffset offset) {
    return *reinterpret_cast<WEIGHT*>(p->m_payload + offset);
}

template<typename node_pointer>
inline INT4 &int4_at(node_pointer p, PayloadOffset offset) {
    return *reinterpret_cast<INT4*>(p->m_payload + offset);
}

template<typename node_pointer>
inline Datum datum_at(node_pointer p, PayloadOffset offset) {
    return reinterpret_cast<Datum>(p->m_payload + offset);
}

template<typename node_pointer>
inline node_pointer &tree_ptr_at(node_pointer p, PayloadOffset offset) {
    return *reinterpret_cast<node_pointer*>(p->m_payload + offset);
}

struct SINLJoinVertexLayout {
    SID             m_sid;
    
    UINT4           m_n_neighbors;

    UINT4           m_n_filters; 
    
    // TODO Multi-table filters are not implemented yet.
    // of length m_n_filters
    Predicate       **m_filters;
    
    // SIDs of neighbors; index starts from 0
    // It is of length m_n_neighbors
    SID             *m_neighbor_sid;
    
    // Join predicates with the neighbors; index starts from 0
    // It is of length m_n_neighbors.
    // It is sorted by my COLID.
    Predicate       **m_pred_join;
    
    //
    // Indexes
    //

    // number of keys
    UINT4           m_n_keys;

    // index ids of the indexes for the keys; of length m_n_keys
    IndexId         *m_index_id;
    
    // column id of the keys; of length m_n_keys
    COLID           *m_key_colid;

    typeinfo        **m_key_ti;

    // index into payload for keys; of length m_n_keys
    PayloadOffset   *m_offset_key;

    // starting index into payload for hdiff & tree ptrs
    // of length m_n_keys;
    PayloadOffset   *m_offset_hdiff;
    PayloadOffset   *m_offset_left;
    PayloadOffset   *m_offset_right;
    PayloadOffset   *m_offset_parent;

    size_t           m_vertex_size;
    
    // provide fast lookups
    PayloadOffset   *m_offset_key_indexed_by_neighbor_idx;

    IndexId         *m_index_id_indexed_by_neighbor_idx;

    typeinfo        **m_key_ti_indexed_by_neighbor_idx;

    SID             *m_iteration_seq;

    UINT4           *m_neighbor_idx_of_parent_in_me;

    UINT4           *m_neighbor_idx_of_me_in_parent;
};

struct SINLJoinVertex {
    ROWID           m_rowid;

    BYTE            m_payload[0];
};

#endif

