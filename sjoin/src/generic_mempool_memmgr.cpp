#include <memmgr.h>
#include <utils.h>
#include <cstddef>

generic_mempool_memmgr::generic_mempool_memmgr(
    memmgr *parent_mmgr,
    size_t block_bytes)
    : memmgr(parent_mmgr),
    m_block_bytes(block_bytes),
    m_maxpool_order(0),
    m_pool_size(),
    m_payload_offset(),
    m_memblock_size(),
    m_bitmap_num_bytes(),
    m_full_blocks(),
    m_free_blocks(),
    m_block_end_addr(parent_mmgr),
    m_large_objs(parent_mmgr) {

    
    /*if (obj_size <= 4) {
        if (obj_size <= 2) {
            m_aligned_obj_size = obj_size;
        }
        else if (obj_size <= 4) {
            m_aligned_obj_size = 4;
        }
    } else {
        m_aligned_obj_size = ((obj_size + 7) / 8) * 8;
    }
    
    while (((m_pool_size % 16) * (m_aligned_obj_size % 16)) % 16 != 0) {
        ++m_pool_size;
    } */

    //size_t bitmap_size = (m_pool_size + 7) / 8;
    //m_bitmap_num_bytes = bitmap_size;
    //m_payload_offset = (m_bitmap_offset + bitmap_size + 15) / 16 * 16;
    //m_memblock_size = m_payload_offset + m_pool_size * m_aligned_obj_size;
    
    memset(m_full_blocks, 0, sizeof(MemBlock*) * 64);
    memset(m_free_blocks, 0, sizeof(MemBlock*) * 64);

    for (UINT4 i = 0; i != 64; ++i) {
        size_t obj_size = 1ull << i;
        size_t pool_size = calc_block_size_from_block_bytes(m_block_bytes, obj_size);
        if (pool_size == 1) {
            m_maxpool_order = i - 1;
            break;
        }
        size_t bitmap_size = (pool_size + 7) / 8;

        m_pool_size[i] = pool_size;
        m_bitmap_num_bytes[i] = bitmap_size;
        m_payload_offset[i] = (m_bitmap_offset + bitmap_size + 15) / 16 * 16;
        m_memblock_size[i] = m_payload_offset[i] + obj_size * pool_size;
        RT_ASSERT(m_memblock_size[i] <= m_block_bytes);
    }
}

generic_mempool_memmgr::~generic_mempool_memmgr() {
    for (UINT4 i = 0; i <= m_maxpool_order; ++i) {
        m_parent->deallocate_ptrs_in_list(m_full_blocks[i], offsetof(MemBlock, m_next));
        m_parent->deallocate_ptrs_in_list(m_free_blocks[i], offsetof(MemBlock, m_next));
    }

    for (uintptr_t ptr_value: m_large_objs) {
        void *ptr = (void*) ptr_value;
        m_parent->deallocate(ptr);
    }
}

MemBlock *generic_mempool_memmgr::find_containing_memblock(const void *ptr) {
    auto iter = m_block_end_addr.upper_bound((uintptr_t) ptr);
    if (iter == m_block_end_addr.end()) return nullptr;
    return (MemBlock *)(iter->second);
};

void *generic_mempool_memmgr::allocate(size_t n, const void *hint) {
    UINT4 order = ceil_log2(n);
    RT_ASSERT(n);
    
    // large obj is allocated directly from parent
    if (order > m_maxpool_order) {
        void *ptr = m_parent->allocate(n, hint);
        m_large_objs.emplace((uintptr_t) ptr);
        return ptr;
    }
    
    // smaller obj are allocated from one of the pools
    //size_t obj_size = 1ull << order;
    RT_ASSERT((n <= 1 && order == 0) || 
        (n > (1ull >> (order - 1)) && n <= (1ull << order)));
    MemBlock *curblk = nullptr,
             *blk_for_alloc = nullptr;
    
    if (hint) {
        MemBlock *blk = find_containing_memblock(hint);
        if (!blk) {
            hint = nullptr;
        } else {
            if (blk->m_order == order &&
                is_ptr_in_memblock(hint, blk) &&
                !blk->m_in_full_chain) {
                
                blk_for_alloc = blk;
            } else {
                hint = nullptr;
            }
        }
    }
    
    if (hint) {
        goto do_allocation; 
    } else {
        curblk = m_free_blocks[order];
    }
    while (curblk) {
        blk_for_alloc = curblk;
        curblk = curblk->m_next;

do_allocation:
        size_t idx = find_free_obj<true>(blk_for_alloc, order);
        if (idx >= m_pool_size[order]) {
            // this block is full
            blk_for_alloc->m_in_full_chain = true;
            blk_for_alloc->disconnect_from_list(&m_free_blocks[order]);
            blk_for_alloc->prepend_to_list(&m_full_blocks[order]);

            if (hint) {
                hint = nullptr;
                curblk = m_free_blocks[order];
            }
        } else {
            mark_allocated(blk_for_alloc, idx);
            return (void *) (payload(blk_for_alloc, order) + (idx << order));
        }
    };

    // no free blocks, create a new one
    RT_ASSERT(m_free_blocks[order] == nullptr);
    blk_for_alloc = (MemBlock *) m_parent->allocate(m_block_bytes);
    memset(blk_for_alloc, 0, m_block_bytes);
    blk_for_alloc->m_order = order;
    //blk_for_alloc->m_in_full_chain = false;
    //blk_for_alloc->m_hint_for_alloc = 0;
    //blk_for_alloc->m_prev = nullptr;
    //blk_for_alloc->m_next = nullptr;
    m_free_blocks[order] = blk_for_alloc;
    m_block_end_addr.emplace(
        ((uintptr_t) blk_for_alloc) + m_memblock_size[order],
        (uintptr_t) blk_for_alloc);

    goto do_allocation;
}

void generic_mempool_memmgr::deallocate(void *ptr) {
    if (!ptr) return ;
    MemBlock *blk = find_containing_memblock(ptr);    
    deallocate_from_block(ptr, blk);
}

void generic_mempool_memmgr::deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset) {
    MemBlock *hint = nullptr;
    while (ptr) {
        void *next_ptr = *reinterpret_cast<void**>(reinterpret_cast<BYTE*>(ptr) + next_ptr_offset);
        hint = deallocate_with_hint(ptr, hint);
        ptr = next_ptr;
    }
}

void generic_mempool_memmgr::deallocate_ptrs_in_array(void **ptr, size_t n) {
    MemBlock *hint = nullptr;
    for (size_t i = 0; i < n; ++i) {
        if (ptr[i]) {
            hint = deallocate_with_hint(ptr[i], hint);
        }
    }
}

MemBlock *generic_mempool_memmgr::deallocate_with_hint(void *ptr, MemBlock *hint) {
    MemBlock *blk;
    if (hint && is_ptr_in_memblock(ptr, hint)) {
        blk = hint;
    } else {
        blk = find_containing_memblock(ptr);
    }

    deallocate_from_block(ptr, blk);
    return blk;
}

void generic_mempool_memmgr::deallocate_from_block(void *ptr, MemBlock *blk) {

    if (!blk || !is_ptr_in_memblock(ptr, blk)) {
        m_large_objs.erase((uintptr_t) ptr);
        m_parent->deallocate(ptr);
        return ;
    }

    size_t idx;
    UINT4 order;
    order = blk->m_order;

    idx = (((uintptr_t) ptr) - (uintptr_t) payload(blk, order)) >> order;
    mark_unallocated(blk, idx);

    if (blk->m_in_full_chain) {
        blk->m_in_full_chain = false;
        blk->m_hint_for_alloc = idx / 8;
        blk->disconnect_from_list(&m_full_blocks[order]);
        blk->prepend_to_list(&m_free_blocks[order]);
    }
}

void generic_mempool_memmgr::deallocate_all() {
    for (UINT4 order = 0; order <= m_maxpool_order; ++order) {
        MemBlock *blk = m_full_blocks[order];
        if (blk) {
            for (;;) {
                memset(blk->m_alloc_bitmap, 0, m_bitmap_num_bytes[order]);
                blk->m_in_full_chain = false;
                blk->m_hint_for_alloc = 0;

                if (blk->m_next) {
                    blk = blk->m_next;
                } else {
                    break;
                }
            }

            blk->m_next = m_free_blocks[order];
        }
        blk = m_free_blocks[order];
        
        while (blk) {
            memset(blk->m_alloc_bitmap, 0, m_bitmap_num_bytes[order]);
            blk->m_hint_for_alloc = 0;
            blk = blk->m_next;
        }

        if (m_full_blocks[order]) {
            m_free_blocks[order] = m_full_blocks[order];
            m_full_blocks[order] = nullptr;
        }
    }

    for (uintptr_t ptr_value: m_large_objs) {
        void *ptr = (void*) ptr_value;
        m_parent->deallocate(ptr);
    }
    m_large_objs.clear();
}

