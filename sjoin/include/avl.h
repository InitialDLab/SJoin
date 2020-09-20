#ifndef AVL_H
#define AVL_H

// Outdated: the AVL implementation has had significant changes
// in a separate dsimpl repo.
// TODO switch to the newer implementation

#include <config.h>
#include <basictypes.h>
#include <address.h>
#include <debug.h>
#include <dtypes.h>

#include <utility>
#include <type_traits>

#if defined(IN_TEST_AVL) || !defined(NDEBUG)
#include <iostream>
#include <iomanip>

struct print_hex {
    explicit print_hex(uintptr_t v): v(v) {}
    uintptr_t v;
};

static inline std::ostream &operator<<(std::ostream &out, print_hex &&p) {
    auto flags = out.flags();
    out << "0x" << std::hex << std::setfill('0') << std::setw(16) << p.v;
    out.flags(flags);
    return out;
}
#endif

struct AVLNodeDesc {
    PayloadOffset      m_offset_key;
    PayloadOffset      m_offset_left;
    PayloadOffset      m_offset_right;
    PayloadOffset      m_offset_parent;
    PayloadOffset      m_offset_hdiff;
    typeinfo           *m_key_ti;
};

namespace avl_impl {

template<typename node_pointer>
inline node_pointer convert_left_handle(node_pointer node) {
    return xor_bit<63>(node);
}

template<typename node_pointer>
inline node_pointer convert_right_handle(node_pointer node) {
    return xor_bit<62>(node);
}

template<typename node_pointer>
inline bool is_left_handle(node_pointer node) {
    return is_not_bit47_extended<63>(node);
}

template<typename node_pointer>
inline bool is_right_handle(node_pointer node) {
    return is_not_bit47_extended<62>(node);
}

template<typename node_pointer>
inline node_pointer convert_root_handle() {
    return xor_bit<63>(xor_bit<62, node_pointer>(nullptr));
}

// avl_t<N>: AVL Tree with N as node type
//
// 1. N is required to have m_payload[] field
// 2. pointers, hdiff, key offset into payload is described by AVLNodeDesc
//    or its subclass
// 3. AVLNodeDesc, in addition, should specify an aggregate function
// 4. Nodes are not owned by the tree. Caller should take care of
//    bookkeeping for memory allocation/deallocation
//
template<typename N>
class avl_t {
public:
    typedef N node_t;
    typedef node_t *node_pointer;
    typedef node_t &node_reference;

public:
    avl_t(
        const AVLNodeDesc * const avl_node_desc):
        m_offset_key(avl_node_desc->m_offset_key),
        m_offset_left(avl_node_desc->m_offset_left),
        m_offset_right(avl_node_desc->m_offset_right),
        m_offset_parent(avl_node_desc->m_offset_parent),
        m_offset_hdiff(avl_node_desc->m_offset_hdiff),
        m_key_ti(avl_node_desc->m_key_ti),
        m_root(nullptr)
    DARG(
        m_debug(nullptr)
    ){}

    ~avl_t() {}
    
    // TODO implement me
    avl_t(const avl_t&) = delete;
    avl_t(avl_t&&) = delete;
    avl_t& operator=(const avl_t&) = delete;
    avl_t& operator=(avl_t&&) = delete;

    struct iterator_t {
        iterator_t(avl_t *avl, node_pointer node)
            : m_avl(avl), m_node(node) {}

        iterator_t(const iterator_t&) = default;

        iterator_t &operator=(const iterator_t&) = default;

        bool operator==(const iterator_t &other) {
            return m_avl == other.m_avl && m_node == other.m_node;
        }

        bool operator!=(const iterator_t &other) {
            return m_avl != other.m_avl || m_node != other.m_node;
        }

        iterator_t& operator++() {
            m_node = avl_t::_next(m_node);  
            return *this; 
        }

        iterator_t operator++(int) {
            iterator_t me = *this;
            ++*this; 
            return me;
        }

        node_reference operator*() {
            return *m_node;
        }

        node_pointer operator->() {
            return m_node;
        }

        bool is_null() {
            return is_left_handle(m_node) | is_right_handle(m_node);
        }

    private:
        avl_t               *m_avl;
        node_pointer        m_node;
    
        friend class avl_t<N>;
    };

    typedef iterator_t iterator;

    void insert(
        node_pointer node,
        UINT4 n_weights,
        PayloadOffset *offset_weight,
        PayloadOffset *offset_subtree_weight) {

        node_pointer handle = _find_handle_for_insert(_key(node));
        insert_at(
            handle,
            node,
            n_weights,
            offset_weight,
            offset_subtree_weight);
    }

    WEIGHT insert_and_get_sum_left(
        node_pointer node,
        UINT4 n_weights,
        PayloadOffset *offset_weight,
        PayloadOffset *offset_subtree_weight,
        PayloadOffset offset_subtree_weight_for_sum_left) {
        
        WEIGHT agg;
        node_pointer handle = _find_handle_for_insert_and_get_sum_left(
            _key(node),
            offset_subtree_weight_for_sum_left,
            agg);
        insert_at(
            handle,
            node,
            n_weights,
            offset_weight,
            offset_subtree_weight);
        return agg;
    }

    void erase(
        node_pointer node,
        UINT4 n_weights,
        PayloadOffset *offset_weight,
        PayloadOffset *offset_subtree_weight) {

        _remove(node,
            n_weights,
            offset_weight,
            offset_subtree_weight);
    }

    //iterator find_for_update(key_type key) {
    //    return iterator(this, _find_node_or_handle_for_update(key));
    //}

    //iterator find(key_type key) {
    //    auto iter = find_for_update(key);
    //    return iter.is_null() ? end() : iter;
    //}

    //iterator &insert_at(iterator& handle, node_pointer *node) {
    //    insert_at(handle.m_node, node);
    //    handle.m_node = node;
    //    return handle;
    //}

    //inline void erase(iterator iter) {
    //    _remove(iter.m_node);
    //}

    //inline void remove(node_pointer node) {
    //    _remove(node);
    //}

    //iterator begin() {
    //    return iterator(this, _left_most(m_root));
    //}

    //iterator end() {
    //    return iterator(this, nullptr);
    //}
    
    node_pointer next(node_pointer node) {
        return _next(node);
    }

    node_pointer lower_bound(Datum key) {
        return _lower_bound(key);
    }

    node_pointer upper_bound(Datum key) {
        return _upper_bound(key);
    }

    void clear() {
        m_root = nullptr;
    }

    WEIGHT sum_range_ii(
        Datum lower,
        Datum upper,
        PayloadOffset offset_subtree_weight) {
        WEIGHT cnt1 = sum_left<false>(lower, offset_subtree_weight);
        WEIGHT cnt2 = sum_left<true>(upper, offset_subtree_weight);
        return (cnt2 >= cnt1) ? (cnt2 - cnt1) : 0;
    }

    WEIGHT sum_range_ie(
        Datum lower,
        Datum upper,
        PayloadOffset offset_subtree_weight) {

        WEIGHT cnt1 = sum_left<false>(lower, offset_subtree_weight);
        WEIGHT cnt2 = sum_left<false>(upper, offset_subtree_weight);
        return (cnt2 >= cnt1) ? (cnt2 - cnt1) : 0;
    }

    node_pointer get_nth(
        WEIGHT &offset,
        PayloadOffset offset_weight,
        PayloadOffset offset_subtree_weight) {

        node_pointer cur = m_root;

        for (; cur;) {
            // NOTE: fast path is incorrect
            // in case of many 0 valued nodes,
            // in which case, the left branch
            // could have 0 weight and the real
            // nth node is the subtree root or 
            // on the right
            //if (offset == 0) return _left_most(cur);
            WEIGHT left_sum = _weight(_left(cur), offset_subtree_weight);
            if (offset >= left_sum) {
                offset -= left_sum;
                WEIGHT cur_weight = _weight_nochk(cur, offset_weight);
                if (offset < cur_weight) return cur;
                offset -= cur_weight;
                cur = _right(cur);
            } else {
                cur = _left(cur);
            }
        }

        return cur;
    }

    node_pointer get_nth_from_lower_bound(
        Datum low,
        WEIGHT &offset,
        PayloadOffset offset_weight,
        PayloadOffset offset_subtree_weight) {
        
        WEIGHT agg_left = sum_left<false>(low, offset_subtree_weight);
        offset += agg_left;
        return get_nth(
            offset,
            offset_weight,
            offset_subtree_weight);
    }

#if defined(IN_TEST_AVL) || !defined(NDEBUG)
    void print(std::ostream &out) {
        if (m_root) print(out, m_root);     
        else out << "empty tree" << std::endl;
    }

    void print(std::ostream &out, node_pointer node) {
        out << print_hex((uintptr_t) node) << ' '
            << print_hex((uintptr_t) _left(node)) << ' '
            << print_hex((uintptr_t) _right(node)) << ' '
            << _key(node)  << ' '
            << _hdiff(node) << ' ' << std::endl;
        if (_left(node)) print(out, _left(node));
        if (_right(node)) print(out, _right(node));
    }
#endif

private:
    node_pointer _find_node_or_handle_for_update(Datum key) {
        if (!m_root) return convert_root_handle<node_pointer>();
        
        node_pointer cur = m_root;
        for (;;) {
            int cmp_res = m_key_ti->compare(key, _key(cur));
            if (cmp_res < 0) {
                if (_left(cur)) {
                    cur = _left(cur);
                } else {
                    return convert_left_handle(cur);
                }
            } else if (cmp_res == 0) {
                return cur; 
            } else {
                if (_right(cur)) {
                    cur = _right(cur);
                } else {
                    return convert_right_handle(cur);
                }
            }
        }
    
        return nullptr;
    }

    node_pointer _find_handle_for_insert(Datum key) {
        if (!m_root) return convert_root_handle<node_pointer>();

        node_pointer cur = m_root;
        for (;;) {
            int cmp_res = m_key_ti->compare(key, _key(cur));
            if (cmp_res < 0) {
                if (_left(cur)) {
                    cur = _left(cur);             
                } else {
                    return convert_left_handle(cur);
                }
            } else {
                if (_right(cur)) {
                    cur = _right(cur);
                } else {
                    return convert_right_handle(cur);
                }
            }
        }

        RT_ASSERT(false);
        return nullptr;
    }
    
    node_pointer _find_handle_for_insert_and_get_sum_left(
        Datum key,
        PayloadOffset offset_subtree_weight,
        WEIGHT &agg) {
        
        agg = 0;
        if (!m_root) return convert_root_handle<node_pointer>();

        node_pointer cur = m_root;
        for (;;) {
            int cmp_res = m_key_ti->compare(key, _key(cur));
            if (cmp_res < 0) {
                if (_left(cur)) {
                    cur = _left(cur);             
                } else {
                    return convert_left_handle(cur);
                }
            } else {
                if (_right(cur)) {
                    agg += _weight_nochk(cur, offset_subtree_weight)
                        - _weight_nochk(_right(cur), offset_subtree_weight);
                    cur = _right(cur);
                } else {
                    agg += _weight_nochk(cur, offset_subtree_weight);
                    return convert_right_handle(cur);
                }
            }
        }
    
        RT_ASSERT(false);
        return nullptr;
    }

    void insert_at(
        node_pointer        handle,
        node_pointer        node,
        UINT4               n_weights,
        PayloadOffset       *offset_weight,
        PayloadOffset       *offset_subtree_weight) {

        node_pointer cur;
        if (is_left_handle(handle)) {
            if (!is_right_handle(handle)) {
                cur = convert_left_handle(handle);
                _left(cur) = node;
            } else {
                m_root = node;
                cur = nullptr;
            }
        } else {
            cur = convert_right_handle(handle);
            _right(cur) = node;
        }
        _parent(node) = cur;
        _left(node) = _right(node) = nullptr;
        // skip checks for nullptr in _fix_agg
        for (UINT4 idx = 0; idx < n_weights; ++idx) {
            PayloadOffset offset_w = offset_weight[idx];
            PayloadOffset offset_W = offset_subtree_weight[idx];

            _weight_nochk(node, offset_W) = _weight_nochk(node, offset_w);
        }
        _hdiff(node) = 0;
        
        rebalance_for_insertion_and_fix_agg(
            node,
            n_weights,
            offset_weight,
            offset_subtree_weight);
    }

    void rebalance_for_insertion_and_fix_agg(
        node_pointer        node,
        UINT4               n_weights,
        PayloadOffset       *offset_weight,
        PayloadOffset       *offset_subtree_weight) {

        node_pointer cur = _parent(node);
        for (; cur != nullptr; cur = _parent(node)) {
            if (node == _right(cur)) {
                if (_hdiff(cur) > 0) {
                    if (_hdiff(node) > 0) {
                        // left rotate cur
                        node = _L(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                    } else {
                        // right rotate node, and then left rotate cur
                        node = _RL(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                    }
                    _fix_parent(cur, node);
                    cur = _parent(node);
                    break;
                } else if (_hdiff(cur) == 0) {
                    _fix_agg(
                        cur,
                        n_weights,
                        offset_weight,
                        offset_subtree_weight);
                    _hdiff(cur) = 1;
                    node = cur;
                } else {
                    _hdiff(cur) = 0;
                    break;
                }
            } else {
                // node == cur->left
                if (_hdiff(cur) < 0) {
                    if (_hdiff(node) > 0) {
                        // left rotate node, and then right rotate cur
                        node = _LR(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                    } else {
                        // right rotate cur
                        node = _R(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                    }
                    _fix_parent(cur, node);
                    cur = _parent(node);
                    break;
                } else if (_hdiff(cur) == 0) {
                    _fix_agg(
                        cur,
                        n_weights,
                        offset_weight,
                        offset_subtree_weight);
                    _hdiff(cur) = -1;
                    node = cur;
                } else {
                    _hdiff(cur) = 0;
                    break;
                }
            }
        }
        
        fix_agg_to_root(cur, n_weights, offset_weight, offset_subtree_weight);
    }

    void _remove(
        node_pointer node,
        UINT4 n_weights,
        PayloadOffset *offset_weight,
        PayloadOffset *offset_subtree_weight) {

        node_pointer rebalance_point;
        bool del_in_left;

        if (_left(node)) {
            if (_right(node)) {
                // case 1
                // both left and right are non-null
                node_pointer leftmost = _left_most(_right(node));
                
                RT_ASSERT(!_left(leftmost));
                node_pointer leftmost_parent = _parent(leftmost);
                node_pointer leftmost_right = _right(leftmost);

                _parent(leftmost) = _parent(node);
                _fix_parent(node, leftmost);
                _left(leftmost) = _left(node);
                if (_left(node)) _parent(_left(node)) = leftmost;
                _hdiff(leftmost) = _hdiff(node);

                if (leftmost_parent != node) {
                    // case 1(a):
                    // leftmost is not the right child of node
                    //
                    //          node
                    //          /  \
                    //         /    \
                    //      left    right
                    //              /   /\
                    //            ...  /__\
                    //             /
                    //          leftmost_parent
                    //            /     /\
                    //           /     /__\
                    //     leftmost
                    //            \
                    //             \
                    //         leftmost_right
                    //            /\
                    //           /__\
                    //
                    

                    // leftmost will be rebalanced and fixed with correct agg
                    // along the path to root
                    _right(leftmost) = _right(node);
                    _parent(_right(node)) = leftmost;

                    RT_ASSERT(leftmost == _left(leftmost_parent));
                    _left(leftmost_parent) = leftmost_right;
                    if (leftmost_right) _parent(leftmost_right) = leftmost_parent;

                    rebalance_point = leftmost_parent;
                    del_in_left = true;
                } else {
                    // leftmost is the right child of node
                    // case 1(b):
                    //          node (leftmost_parent)
                    //          /  \
                    //         /    \
                    //      left    leftmost
                    //                 /\
                    //                /__\
                    //               
                    //
                    
                    rebalance_point = leftmost;
                    del_in_left = false;
                }
            } else {
                // case 2:
                // only right is null
                // the left sub-tree must be of height 1
                //
                //      node->parent
                //       |
                //       |
                //      node
                //      /
                //     /
                //    left
                
                node_pointer left = _left(node);

                RT_ASSERT(_hdiff(node) == -1);
                RT_ASSERT(_hdiff(left) == 0);
                RT_ASSERT(!_left(left));
                RT_ASSERT(!_right(left));

                // left->hdiff does not change
                // no need to fix agg on left
                _parent(left) = _parent(node);
                del_in_left = _fix_parent(node, left);
                
                rebalance_point = _parent(left);
            }
        } else {
            if (_right(node)) {
                // case 3:
                // only left is null
                // the right sub-tree must be of height 1
                //
                //      node->parent
                //       |
                //       |
                //      node
                //         \
                //          \
                //          right
                //

                node_pointer right = _right(node);

                RT_ASSERT(_hdiff(node) == 1);
                RT_ASSERT(_hdiff(right) == 0);
                RT_ASSERT(!_left(right));
                RT_ASSERT(!_right(right));

                // right->hdiff does not change
                // no need to fix agg on right
                _parent(right) = _parent(node);
                del_in_left = _fix_parent(node, right);
                rebalance_point = _parent(right);
            } else {
                // case 4:
                // single-node sub-tree tree
                //
                //          node->parent
                //          |
                //          |
                //          node
                //

                if (!_parent(node)) {
                    m_root = nullptr;
                    rebalance_point = nullptr;
                } else {
                    if (_left(_parent(node)) == node) {
                        _left(_parent(node)) = nullptr;
                        del_in_left = true;
                    } else {
                        _right(_parent(node)) = nullptr;
                        del_in_left = false;
                    }
                    rebalance_point = _parent(node);
                }
            }
        }

        if (rebalance_point) {
            rebalance_for_deletion_and_fix_agg(
                rebalance_point,
                del_in_left,
                n_weights,
                offset_weight,
                offset_subtree_weight);
        }
    }

    void rebalance_for_deletion_and_fix_agg(
        node_pointer cur,
        bool del_in_left,
        UINT4 n_weights,
        PayloadOffset *offset_weight,
        PayloadOffset *offset_subtree_weight) {

        while (cur) {
skip_while_condition:
            if (del_in_left) {
                if (_hdiff(cur) < 0) {
                    _fix_agg(
                        cur,
                        n_weights,
                        offset_weight,
                        offset_subtree_weight);
                    _hdiff(cur) = 0;
                    if (_parent(cur)) {
                        del_in_left = _left(_parent(cur)) == cur;
                        cur = _parent(cur);
                        goto skip_while_condition;
                    } else {
                        break;
                    }
                    cur = _parent(cur);
                } else if (_hdiff(cur) == 0) {
                    // agg fixed in fix_agg_to_root
                    _hdiff(cur) = 1;
                    break;
                } else { // cur->hdiff > 0
                    // now cur->diff is 2
                    if (_hdiff(_right(cur)) < 0) {
                        // height is decreased by 1
                        node_pointer node = _RL(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                        del_in_left = _fix_parent(cur, node);
                        cur = _parent(node);
                    } else {
                        node_pointer node = _L(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                        del_in_left = _fix_parent(cur, node);
                        cur = _parent(node);
                        if (_hdiff(node) != 0) {
                            // height does not change
                            break;
                        }
                    }
                }
            } else {
                if (_hdiff(cur) > 0) {
                    _fix_agg(
                        cur,
                        n_weights,
                        offset_weight,
                        offset_subtree_weight);
                    _hdiff(cur) = 0;
                    if (_parent(cur)) {
                        del_in_left = _left(_parent(cur)) == cur;
                        cur = _parent(cur);
                        goto skip_while_condition;
                    } else {
                        break;
                    }
                } else if (_hdiff(cur) == 0) {
                    // agg fixed in fix_agg_to_root
                    _hdiff(cur) = -1;
                    break;
                } else { // cur->hdiff < 0
                    // now cur->diff is -2 
                    if (_hdiff(_left(cur)) > 0) {
                        // height is decrased by 1
                        node_pointer node = _LR(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                        del_in_left = _fix_parent(cur, node);
                        cur = _parent(node);
                    } else {
                        node_pointer node = _R(
                                cur,
                                n_weights,
                                offset_weight,
                                offset_subtree_weight);
                        del_in_left = _fix_parent(cur, node);
                        cur = _parent(node);
                        if (_hdiff(node) != 0) {
                            // height does not change
                            break;
                        }
                    }
                }
            }
        }

        fix_agg_to_root(
            cur,
            n_weights,
            offset_weight,
            offset_subtree_weight);
    }

    inline void fix_agg_to_root(
        node_pointer    cur,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        for (; cur; cur = _parent(cur)) {
            _fix_agg(cur, n_weights, offset_weight, offset_subtree_weight);
        }
    }

    inline void _fix_agg(
        node_pointer    node,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        
        for (UINT4 idx = 0; idx < n_weights; ++idx) {
            PayloadOffset offset_w = offset_weight[idx];
            PayloadOffset offset_W = offset_subtree_weight[idx];

            _weight_nochk(node, offset_W) =
                _weight(_left(node), offset_W) + 
                _weight(_right(node), offset_W) +
                _weight_nochk(node, offset_w);
        }
    }

    inline bool _fix_parent(node_pointer old_X, node_pointer new_X) {
        if (!_parent(new_X)) {
            m_root = new_X;
            return true;
        } else {
            if (old_X == _left(_parent(new_X))) {
                _left(_parent(new_X)) = new_X; 
                return true;
            } else /*if (old_X == new_X->parent->right)*/ {
                _right(_parent(new_X)) = new_X;
                return false;
            }
        }
    }

    node_pointer _L(
        node_pointer    X,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        node_pointer Z = _right(X);
        RT_ASSERT(Z);

        _right(X) = _left(Z);
        if (_right(X)) {
            _parent(_right(X)) = X;
        }
        _left(Z) = X;
        _parent(Z) = _parent(X);
        _parent(X) = Z;
    
        // insertion
        RT_ASSERT(_hdiff(X) == 1);
        if (_hdiff(Z) == 0) {
            // deletion only
            _hdiff(X) = 1;
            _hdiff(Z) = -1;
        } else {
            // insertion or deletion
            RT_ASSERT(_hdiff(Z) == 1);
            _hdiff(X) = 0;
            _hdiff(Z) = 0;
        }
        
        _fix_agg(X, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(Z, n_weights, offset_weight, offset_subtree_weight);
        return Z;
    }

    node_pointer _RL(
        node_pointer    X,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        node_pointer Z = _right(X);
        RT_ASSERT(Z);
        node_pointer W = _left(Z);
        RT_ASSERT(W);
        
        _right(X) = _left(W);
        if (_right(X)) {
            _parent(_right(X)) = X;
        }
        _left(Z) = _right(W);
        if (_left(Z)) {
            _parent(_left(Z)) = Z;
        }
        _left(W) = X;
        _right(W) = Z;
        _parent(W) = _parent(X);
        _parent(X) = _parent(Z) = W;

        RT_ASSERT(_hdiff(X) == 1);
        RT_ASSERT(_hdiff(Z) == -1);
        if (_hdiff(W) < 0) {
            _hdiff(X) = 0;
            _hdiff(Z) = 1;
        } else if (_hdiff(W) == 0) {
            _hdiff(X) = 0;
            _hdiff(Z) = 0;
        } else {
            _hdiff(X) = -1;
            _hdiff(Z) = 0;
        }

        _hdiff(W) = 0;

        _fix_agg(X, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(Z, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(W, n_weights, offset_weight, offset_subtree_weight);
        return W;
    }

    node_pointer _R(
        node_pointer    X,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        node_pointer Z = _left(X);
        RT_ASSERT(Z);

        _left(X) = _right(Z);
        if (_left(X)) {
            _parent(_left(X)) = X;
        }
        _right(Z) = X;
        _parent(Z) = _parent(X);
        _parent(X) = Z;

        RT_ASSERT(_hdiff(X) == -1);
        _hdiff(X) = 0;
        if (_hdiff(Z) == 0) {
            // deletion only
            _hdiff(X) = -1;
            _hdiff(Z) = 1;
        } else {
           // insertin or deletion
            RT_ASSERT(_hdiff(Z) == -1);
            _hdiff(X) = 0;
            _hdiff(Z) = 0;
        }

        _fix_agg(X, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(Z, n_weights, offset_weight, offset_subtree_weight);
        return Z;
    }

    node_pointer _LR(
        node_pointer    X,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {

        node_pointer Z = _left(X);
        RT_ASSERT(Z);
        node_pointer W = _right(Z);
        RT_ASSERT(W);

        _right(Z) = _left(W);
        if (_right(Z)) {
            _parent(_right(Z)) = Z;
        }
        _left(X) = _right(W);
        if (_left(X)) {
            _parent(_left(X)) = X;
        }
        _left(W) = Z;
        _right(W) = X;
        _parent(W) = _parent(X);
        _parent(Z) = _parent(X) = W;

        RT_ASSERT(_hdiff(X) == -1);
        RT_ASSERT(_hdiff(Z) == 1);
        if (_hdiff(W) < 0) {
            _hdiff(Z) = 0;
            _hdiff(X) = 1;
        } else if (_hdiff(W) == 0) {
            _hdiff(Z) = 0;
            _hdiff(X) = 0;
        } else {
            _hdiff(Z) = -1;
            _hdiff(X) = 0;
        }
        _hdiff(W) = 0;

        _fix_agg(X, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(Z, n_weights, offset_weight, offset_subtree_weight);
        _fix_agg(W, n_weights, offset_weight, offset_subtree_weight);
        return W;
    }

    node_pointer _left_most(node_pointer node) {
        while (_left(node)) node = _left(node);
        return node;
    }

    node_pointer _next(node_pointer node) {
        if (node == nullptr) return nullptr;
        
        if (_right(node)) {
            return _left_most(_right(node));
        }

        while (_parent(node) && _right(_parent(node)) == node) {
            node = _parent(node);
        }
        return _parent(node);
    }

    node_pointer _lower_bound(Datum key) {
        node_pointer candidate = nullptr;
        node_pointer cur = m_root;
        while (cur) {
            int cmp_res = m_key_ti->compare(_key(cur), key);
            if (cmp_res < 0) {
                cur = _right(cur);
            } else /*if (cur->key >= key)*/ {
                candidate = cur;
                cur = _left(cur);
            }
        }

        return candidate;
    }

    node_pointer _upper_bound(Datum key) {
        node_pointer candidate = nullptr;
        node_pointer cur = m_root;
        while (cur) {
            int cmp_res = m_key_ti->compare(_key(cur), key);
            if (cmp_res <= 0) {
                cur = _right(cur);
            } else /* if (cur->key < key) */ {
                candidate = cur;
                cur = _left(cur);
            }
        }

        return candidate;
    }


public:
    template<bool inc> 
    WEIGHT sum_left(Datum key, PayloadOffset offset_subtree_weight) const {
	node_pointer cur = m_root;
        WEIGHT agg = 0;
        while (cur) {
	    int cmp_res = m_key_ti->compare(_key(cur), key);
            if (inc ? (cmp_res <= 0) : (cmp_res < 0)) {
                agg += _weight(cur, offset_subtree_weight) -
                    _weight(_right(cur), offset_subtree_weight);
                    
                cur = _right(cur);
            } else {
                cur = _left(cur);
            }
        }
        return agg;
    }
    
    WEIGHT get_total_sum(PayloadOffset offset_subtree_weight) const {
        return _weight(m_root, offset_subtree_weight);
    }

    inline void fix_agg(
        node_pointer    node,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight) {
        
        fix_agg_to_root(node, n_weights, offset_weight, offset_subtree_weight);
    }

    WEIGHT fix_agg_and_get_sum_left(
        node_pointer    node,
        UINT4           n_weights,
        PayloadOffset   *offset_weight,
        PayloadOffset   *offset_subtree_weight,
        PayloadOffset   offset_subtree_weight_for_sum_left) {
        
        _fix_agg(node, offset_subtree_weight_for_sum_left);
        WEIGHT agg = _weight(_left(node), offset_subtree_weight_for_sum_left);

        node_pointer cur = _parent(node);
        while (cur) {
            if (node == _right(cur)) {
                for (UINT4 idx = 0; idx < n_weights; ++idx) {
                    PayloadOffset offset_w = offset_weight[idx];
                    PayloadOffset offset_W = offset_subtree_weight[idx];

                    _weight_nochk(cur, offset_W) =
                        _weight_nochk(cur, offset_w) + 
                        _weight(_left(cur), offset_W) +
                        _weight_nochk(node, offset_W);
                }

                agg += _weight_nochk(cur, offset_subtree_weight_for_sum_left)
                    - _weight_nochk(node, offset_subtree_weight_for_sum_left);
            } else {
                for (UINT4 idx = 0; idx < n_weights; ++idx) {
                    PayloadOffset offset_w = offset_weight[idx];
                    PayloadOffset offset_W = offset_subtree_weight[idx];

                    _weight_nochk(cur, offset_W) =
                        _weight_nochk(cur, offset_w) + 
                        _weight_nochk(node, offset_W) +
                        _weight(_right(cur), offset_W);
                }
            }

            node = cur;
            cur = _parent(node);
        }
    
        return agg;
    }

    WEIGHT get_sum_left(
        node_pointer    node,
        UINT4           n_weights,
        PayloadOffset   offset_subtree_weight) {
        
        WEIGHT agg = _weight(_left(node), offset_subtree_weight);

        node_pointer cur = _parent(node);
        while (cur) {
            if (node == _right(cur)) {
                agg += _weight_nochk(cur, offset_subtree_weight)
                    - _weight_nochk(node, offset_subtree_weight);
            }

            node = cur;
            cur = _parent(node);
        }

        return agg;
    }

private:
    inline Datum _key(node_pointer node) const {
        return datum_at(node, m_offset_key);
    }

    inline node_pointer &_left(node_pointer node) const {
        return tree_ptr_at(node, m_offset_left);
    }

    inline node_pointer &_right(node_pointer node) const {
        return tree_ptr_at(node, m_offset_right);
    }

    inline node_pointer &_parent(node_pointer node) const {
        return tree_ptr_at(node, m_offset_parent);
    }

    inline INT4 &_hdiff(node_pointer node) const {
        return int4_at(node, m_offset_hdiff);
    }

    static inline WEIGHT _weight(node_pointer node, PayloadOffset offset_weight) {
        return (node) ?
            _weight_nochk(node, offset_weight)
            : 0;
    }

    static inline WEIGHT &_weight_nochk(node_pointer node, PayloadOffset offset_weight) {
        return weight_at(node, offset_weight);
    }

private:
    const PayloadOffset             m_offset_key;
    const PayloadOffset             m_offset_left;
    const PayloadOffset             m_offset_right;
    const PayloadOffset             m_offset_parent;
    const PayloadOffset             m_offset_hdiff;

    typeinfo                        *m_key_ti;

    node_pointer                    m_root;

#ifndef NDEBUG
    node_pointer                    m_debug;
#endif // NDEBUG
};

} // namespace avl_impl

template<typename N>
using AVL = avl_impl::avl_t<N>;

#endif
