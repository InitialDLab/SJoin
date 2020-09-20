#ifndef MEMMGR_H
#define MEMMGR_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <limits>
#include <functional>
#include <set>
#include <basictypes.h>
#include <debug.h>
#include <std_containers.h>

class memmgr {
public:
    virtual ~memmgr();

    void add_child(memmgr *mmgr);

    void remove_child(memmgr *mmgr);

    memmgr(const memmgr&) = delete;
    memmgr(memmgr&&) = delete;
    memmgr &operator=(const memmgr&) = delete;
    memmgr &operator=(memmgr&&) = delete;

    void* allocate(size_t n) {
        return allocate(n, nullptr);
    }

    virtual void* allocate(size_t n, const void *hint) = 0;

    virtual void deallocate(void *ptr) = 0;

    virtual void deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset);

    virtual void deallocate_ptrs_in_array(void **ptr, size_t n);

    template<class T>
    T *allocate_array(size_t n) {
        return reinterpret_cast<T*>(allocate(sizeof(T) * n));
    }

    template<class T, class... Args>
    T *newobj(Args... args) {
        void *buf = allocate(sizeof(T));
        T *obj = new (buf) T(std::forward<Args>(args)...);
        return obj;
    }

    memmgr *get_parent_mmgr() const {
        return m_parent;
    }

protected:
    memmgr(memmgr *parent_mmgr);

    void clean_child_mmgr();

    memmgr *                            m_parent;

    std::unordered_set<memmgr*>         m_child_mmgr;

    bool                                m_child_mmgr_cleaned;
};

template<class T>
class memmgr_std_allocator {
public:
    typedef T               value_type;
    typedef T               *pointer;
    typedef const T         *const_pointer;
    typedef T&              reference;
    typedef const T&        const_reference;
    typedef size_t          size_type;
    typedef ptrdiff_t       difference_type;
    typedef std::false_type propagate_on_container_copy_assignment;
    typedef std::true_type  propagate_on_container_move_assignment;
    typedef std::true_type  propagate_on_container_swap;
    typedef std::false_type is_always_equal;

    template<class U>
    struct rebind { typedef memmgr_std_allocator<U> other; };


    memmgr_std_allocator(memmgr *mmgr) noexcept
        : m_mmgr(mmgr) {}

    ~memmgr_std_allocator() noexcept {}

    template<class U>
    memmgr_std_allocator(const memmgr_std_allocator<U>& other) noexcept
    : m_mmgr(other.m_mmgr) {}

    pointer address(reference x) const noexcept { return &x; }
    const_pointer address(const_reference x) const noexcept { return &x; }

    pointer allocate(size_type n, const void *hint = 0) {
        pointer buf = (pointer) m_mmgr->allocate(n * sizeof(value_type), hint);
        return buf;
    }

    void deallocate(pointer p, size_t n) {
        m_mmgr->deallocate(static_cast<void*>(p));
    }
    
    template<class U, class... Args>
    void construct(pointer p, Args&&... args) {
        ::new(static_cast<void *>(p)) U(std::forward<Args>(args)...);
    }

    template<class U>
    void destroy(U *p) {
        p->~U();
    }

    size_type max_size() const noexcept {
        return std::numeric_limits<size_type>::max() / sizeof(value_type);
    }

private:
    memmgr      *m_mmgr;

    template<class T1, class T2>
    friend bool operator==(const memmgr_std_allocator<T1> &lhs, const memmgr_std_allocator<T2> &rhs);

    
    template<class T1, class T2>
    friend bool operator!=(const memmgr_std_allocator<T1> &lhs, const memmgr_std_allocator<T2> &rhs);

    template<class U>
    friend class memmgr_std_allocator;
};

template<class T1, class T2>
bool operator==(const memmgr_std_allocator<T1>& lhs, const memmgr_std_allocator<T2> &rhs) {
    return lhs.m_mmgr == rhs.m_mmgr;
}

template<class T1, class T2>
bool operator!=(const memmgr_std_allocator<T1>& lhs, const memmgr_std_allocator<T2> &rhs) {
    return lhs.m_mmgr != rhs.m_mmgr;
}

class basic_memmgr: public memmgr {
public:
    basic_memmgr(memmgr *parent_mmgr);

    virtual ~basic_memmgr();
    
    virtual void *allocate(size_t n, const void *hint) {
        return (void *) new BYTE[n];
    }

    virtual void deallocate(void *ptr) {
        delete[] (BYTE *) ptr;
    }
};

class basic_tracked_memmgr: public memmgr {
public:
    basic_tracked_memmgr(memmgr * parent_mmgr);

    virtual ~basic_tracked_memmgr();

    virtual void *allocate(size_t n, const void *hint = nullptr);

    virtual void deallocate(void *ptr);

    void register_destroy_func(void *p, std::function<void(void*)>);

private:
    // default is nop; potential mem leak if destroy_func is not registered
    // ?? can we use std containers in tracked memmgr?
    std_unordered_map<void*, std::function<void(void*)>> m_tracked_ptrs;

    /*std::unordered_map<
        void*,
        std::function<void(void*)>,
        std::hash<void*>,
        std::equal_to<void*>,
        memmgr_std_allocator<std::pair<void * const, std::function<void(void*)>>>>
                        m_tracked_ptrs; */
};

struct MemBlock {
    bool            m_in_full_chain : 1;
    bool            : 1;
    UINT4           m_order : 6;
    uint32_t        m_hint_for_alloc;
    MemBlock        *m_next,
                    *m_prev;
    void            *m_reserved;
    BYTE            m_alloc_bitmap[0];

    inline void disconnect_from_list(MemBlock **phead) {
        if (m_prev) {
            m_prev->m_next = m_next;
        }
        if (m_next) {
            m_next->m_prev = m_prev;
        }

        if (m_prev == nullptr) {
            RT_ASSERT(*phead == this);
            *phead = m_next;
        }
    }

    inline void prepend_to_list(MemBlock **phead) {
        m_next = *phead;
        m_prev = nullptr;
        if (*phead) (*phead)->m_prev = this;
        *phead = this;
    }
};

class mempool_memmgr: public memmgr {
public:
    mempool_memmgr(
        memmgr *parent_mmgr,
        size_t obj_size,
        size_t pool_size // in terms of num objs
    );
    
    virtual ~mempool_memmgr();

    virtual void *allocate(size_t n, const void *hint = nullptr);

    virtual void deallocate(void *ptr);

    virtual void deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset);

    virtual void deallocate_ptrs_in_array(void **ptr, size_t n);

    void deallocate_all();

private:
    MemBlock *find_containing_memblock(const void *ptr);

    inline BYTE* payload(MemBlock *blk) {
        return ((BYTE *) blk) + m_payload_offset;
    }

    inline bool is_ptr_in_memblock(const void *ptr, MemBlock *blk) {
        return blk && ((size_t)(((BYTE *) ptr) - ((BYTE *) blk)) < m_memblock_size);
    }

    inline BYTE &bitmap(MemBlock *blk, size_t idx) {
        return blk->m_alloc_bitmap[idx];
    }

    static inline void mark_allocated(MemBlock *blk, size_t idx) {
        blk->m_alloc_bitmap[idx / 8] |= (1 << (idx & 0x7));
    }

    static inline void mark_unallocated(MemBlock *blk, size_t idx) {
        blk->m_alloc_bitmap[idx / 8] &= ~(BYTE) (1 << (idx & 0x7));
    }

    constexpr static size_t bitmap2freeslotidx[256] = {
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 8
    };

    
    template<bool move_hint>
    size_t find_free_obj(MemBlock *blk) {
        size_t hint;
        size_t idx;

        hint = (size_t) blk->m_hint_for_alloc;
        while (hint < m_bitmap_num_bytes && blk->m_alloc_bitmap[hint] == 0xff) {
            hint += (size_t) (1ull << 32);
        }
        if (hint < m_bitmap_num_bytes) {

            if (move_hint &&
                (blk->m_alloc_bitmap[hint] |
                 (1 << bitmap2freeslotidx[blk->m_alloc_bitmap[hint]])) == 0xff) {
                blk->m_hint_for_alloc = hint + 1;
            }
            return bitmap2freeslotidx[blk->m_alloc_bitmap[hint]] + (hint * 8);
        }

        for (idx = 0; idx < m_bitmap_num_bytes; ++idx) {
            if (blk->m_alloc_bitmap[idx] != 0xff) break;
        }
        if (idx >= m_bitmap_num_bytes) {
            if (move_hint) blk->m_hint_for_alloc = m_bitmap_num_bytes;
            return (size_t) ~0ull;
        }

        if (move_hint) {
            if ((blk->m_alloc_bitmap[idx] |
                (1 << bitmap2freeslotidx[blk->m_alloc_bitmap[idx]])) == 0xff) {
                blk->m_hint_for_alloc = idx + 1;
            } else {
                blk->m_hint_for_alloc = idx;
            }
        }
        return bitmap2freeslotidx[blk->m_alloc_bitmap[idx]] + idx * 8;
    }

    MemBlock *deallocate_with_hint(void *ptr, MemBlock *hint);

    void deallocate_from_block(void *ptr, MemBlock *blk);

public:
    static constexpr size_t calc_block_size_from_block_bytes(
        size_t block_bytes,
        size_t obj_size) {
        
        //size_t block_size = (block_bytes - m_bitmap_offset) / (2 + 16 * obj_size);
        if (block_bytes - m_bitmap_offset < 16) return 1;
        size_t block_size = 
            (8 * (block_bytes - m_bitmap_offset) - 127) / (1 + 8 * obj_size);
        if (block_size == 0) return 1;
        return block_size;
    }

private:
    size_t                          m_aligned_obj_size;

    size_t                          m_pool_size; // in terms of obj

    static constexpr ptrdiff_t      m_bitmap_offset = offsetof(MemBlock, m_alloc_bitmap);

    ptrdiff_t                       m_payload_offset;

    size_t                          m_memblock_size;

    size_t                          m_bitmap_num_bytes;

    MemBlock                        *m_full_blocks;

    MemBlock                        *m_free_blocks;

    std_set<uintptr_t>              m_block_end_addr;
};

class generic_mempool_memmgr: public memmgr {
public:
    generic_mempool_memmgr(
        memmgr *parent_mmgr,
        size_t block_bytes
    );
    
    virtual ~generic_mempool_memmgr();

    virtual void *allocate(size_t n, const void *hint = nullptr);

    virtual void deallocate(void *ptr);

    virtual void deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset);

    virtual void deallocate_ptrs_in_array(void **ptr, size_t n);

    void deallocate_all();

private:
    MemBlock *find_containing_memblock(const void *ptr);

    inline BYTE* payload(MemBlock *blk, UINT4 order) {
        return ((BYTE *) blk) + m_payload_offset[order];
    }

    inline bool is_ptr_in_memblock(const void *ptr, MemBlock *blk) {
        return ((size_t)(((BYTE *) ptr) - ((BYTE *) blk))) <
            m_memblock_size[blk->m_order];
    }

    inline BYTE &bitmap(MemBlock *blk, size_t idx) {
        return blk->m_alloc_bitmap[idx];
    }

    static inline void mark_allocated(MemBlock *blk, size_t idx) {
        blk->m_alloc_bitmap[idx / 8] |= (1 << (idx & 0x7));
    }

    static inline void mark_unallocated(MemBlock *blk, size_t idx) {
        blk->m_alloc_bitmap[idx / 8] &= ~(BYTE) (1 << (idx & 0x7));
    }

    constexpr static size_t bitmap2freeslotidx[256] = {
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 
        0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 8
    };

    
    template<bool move_hint>
    size_t find_free_obj(MemBlock *blk, UINT4 order) {
        size_t hint;
        size_t idx;

        hint = (size_t) blk->m_hint_for_alloc;
        while (hint < m_bitmap_num_bytes[order] &&
                blk->m_alloc_bitmap[hint] == 0xff) {
            hint += (size_t) (1ull << 32);
        }
        if (hint < m_bitmap_num_bytes[order]) {

            if (move_hint &&
                (blk->m_alloc_bitmap[hint] |
                 (1 << bitmap2freeslotidx[blk->m_alloc_bitmap[hint]])) == 0xff) {
                blk->m_hint_for_alloc = hint + 1;
            }
            return bitmap2freeslotidx[blk->m_alloc_bitmap[hint]] + (hint * 8);
        }

        for (idx = 0; idx < m_bitmap_num_bytes[order]; ++idx) {
            if (blk->m_alloc_bitmap[idx] != 0xff) break;
        }
        if (idx >= m_bitmap_num_bytes[order]) {
            if (move_hint) blk->m_hint_for_alloc = m_bitmap_num_bytes[order];
            return (size_t) ~0ull;
        }

        if (move_hint) {
            if ((blk->m_alloc_bitmap[idx] |
                (1 << bitmap2freeslotidx[blk->m_alloc_bitmap[idx]])) == 0xff) {
                blk->m_hint_for_alloc = idx + 1;
            } else {
                blk->m_hint_for_alloc = idx;
            }
        }
        return bitmap2freeslotidx[blk->m_alloc_bitmap[idx]] + idx * 8;
    }

    MemBlock *deallocate_with_hint(void *ptr, MemBlock *hint);

    void deallocate_from_block(void *ptr, MemBlock *blk);

public:
    static constexpr size_t calc_block_size_from_block_bytes(
        size_t block_bytes,
        size_t obj_size) {
        
        //size_t block_size = 8ull * (block_bytes - m_bitmap_offset) / (1 + 8 * obj_size);
        if (block_bytes - m_bitmap_offset < 16) return 1;
        size_t block_size = 
            (8 * (block_bytes - m_bitmap_offset) - 127) / (1 + 8 * obj_size);
        if (block_size == 0) return 1;
        return block_size;
    }

private:
    size_t                          m_block_bytes;

    static constexpr ptrdiff_t      m_bitmap_offset = offsetof(MemBlock, m_alloc_bitmap);

    UINT4                           m_maxpool_order;

    size_t                          m_pool_size[64];

    ptrdiff_t                       m_payload_offset[64];

    size_t                          m_memblock_size[64];

    size_t                          m_bitmap_num_bytes[64];

    MemBlock                        *m_full_blocks[64];

    MemBlock                        *m_free_blocks[64];

    std_map<uintptr_t, uintptr_t>   m_block_end_addr;

    std_set<uintptr_t>              m_large_objs;
}; // generic_mempool_memmgr

//class dummy_memmgr: public memmgr {
//public:
//    dummy_memmgr(memmgr *parent_mmgr)
//        : memmgr(parent_mmgr) {}
//    
//    // ignores all child memmgrs: they will be destructed in the owner of this fake root
//    virtual ~dummy_memmgr() {}
//
//    virtual void *allocate(size_t n, const void *hint) {
//        return m_parent->allocate(n, hint);
//    }
//
//    virtual void deallocate(void *ptr) {
//        m_parent->deallocate(ptr);
//    }
//
//    virtual void deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset) {
//        m_parent->deallocate_ptrs_in_list(ptr, next_ptr_offset);
//    }
//
//    virtual void deallocate_ptrs_in_array(void **ptr, size_t n) {
//        m_parent->deallocate_ptrs_in_array(ptr, n);
//    }
//};

extern basic_memmgr *g_root_mmgr;
extern basic_tracked_memmgr *g_tracked_mmgr;

void create_root_memmgrs();

void destroy_root_memmgrs();

template<class MEMMGR, class... Args>
MEMMGR *create_memmgr(memmgr *parent_mmgr, Args&&... args) {
    RT_ASSERT(parent_mmgr);
    void *buf = parent_mmgr->allocate(sizeof(MEMMGR));
    MEMMGR *mmgr = new (buf) MEMMGR(
            parent_mmgr,
            std::forward<Args>(args)...);
    return mmgr;
}

void destroy_memmgr(memmgr *mmgr);

#endif //MEMMGR_H
