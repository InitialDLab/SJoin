#include <memmgr.h>
#include <cstddef>

mempool_memmgr::mempool_memmgr(
    memmgr *parent_mmgr,
    size_t obj_size,
    size_t pool_size)
    : memmgr(parent_mmgr),
    m_aligned_obj_size(0),
    m_pool_size(pool_size),
    m_payload_offset(0),
    m_memblock_size(0),
    m_bitmap_num_bytes(0),
    m_full_blocks(nullptr),
    m_free_blocks(nullptr),
    m_block_end_addr(parent_mmgr) {
    
    if (obj_size <= 4) {
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
    }

    size_t bitmap_size = (m_pool_size + 7) / 8;
    m_bitmap_num_bytes = bitmap_size;
    m_payload_offset = (m_bitmap_offset + bitmap_size + 15) / 16 * 16;
    m_memblock_size = m_payload_offset + m_pool_size * m_aligned_obj_size;
}

mempool_memmgr::~mempool_memmgr() {
    m_parent->deallocate_ptrs_in_list(m_full_blocks, offsetof(MemBlock, m_next));
    m_parent->deallocate_ptrs_in_list(m_free_blocks, offsetof(MemBlock, m_next));
}

MemBlock *mempool_memmgr::find_containing_memblock(const void *ptr) {
    auto iter = m_block_end_addr.upper_bound((uintptr_t) ptr);
    if (iter == m_block_end_addr.end()) return nullptr;
    return (MemBlock *)(*iter - m_memblock_size);
};

void *mempool_memmgr::allocate(size_t n, const void *hint) {
    MemBlock *curblk = nullptr,
             *blk_for_alloc = nullptr;
    
    if (n > m_aligned_obj_size) return nullptr;

    if (hint) {
        MemBlock *blk = find_containing_memblock(hint);
        if (is_ptr_in_memblock(hint, blk)) {
            if (!blk->m_in_full_chain) {
                blk_for_alloc = blk;
            } else hint = nullptr;
        } else hint = nullptr;
    }
    
    if (hint) {
        //blk_for_alloc = curblk;
        goto do_allocation; 
    } else {
        curblk = m_free_blocks;
    }
    while (curblk) {
        blk_for_alloc = curblk;
        curblk = curblk->m_next;

do_allocation:
        size_t idx = find_free_obj<true>(blk_for_alloc);
        if (idx >= m_pool_size) {
            // this block is full
            blk_for_alloc->m_in_full_chain = true;
            blk_for_alloc->disconnect_from_list(&m_free_blocks);
            blk_for_alloc->prepend_to_list(&m_full_blocks);

            if (hint) {
                hint = nullptr;
                curblk = m_free_blocks;
            }
        } else {
            mark_allocated(blk_for_alloc, idx);
            return (void *) (payload(blk_for_alloc) + idx * m_aligned_obj_size);
        }
    };

    // no free blocks, create a new one
    RT_ASSERT(m_free_blocks == nullptr);
    blk_for_alloc = (MemBlock *) m_parent->allocate(m_memblock_size);
    memset(blk_for_alloc, 0, m_memblock_size);
    //blk_for_alloc->m_prev = nullptr;
    //blk_for_alloc->m_next = nullptr;
    //blk_for_alloc->m_in_full_chain = false;
    //blk_for_alloc->m_hint_for_alloc = 0;
    m_free_blocks = blk_for_alloc;
    m_block_end_addr.insert(((uintptr_t) blk_for_alloc) + m_memblock_size);

    goto do_allocation;
}

void mempool_memmgr::deallocate(void *ptr) {
    if (!ptr) return ;
    MemBlock *blk = find_containing_memblock(ptr);    
    deallocate_from_block(ptr, blk);
}

void mempool_memmgr::deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset) {
    MemBlock *hint = nullptr;
    while (ptr) {
        void *next_ptr = *reinterpret_cast<void**>(reinterpret_cast<BYTE*>(ptr) + next_ptr_offset);
        hint = deallocate_with_hint(ptr, hint);
        ptr = next_ptr;
    }
}

void mempool_memmgr::deallocate_ptrs_in_array(void **ptr, size_t n) {
    MemBlock *hint = nullptr;
    for (size_t i = 0; i < n; ++i) {
        if (ptr[i]) {
            hint = deallocate_with_hint(ptr[i], hint);
        }
    }
}

MemBlock *mempool_memmgr::deallocate_with_hint(void *ptr, MemBlock *hint) {
    MemBlock *blk;
    if (is_ptr_in_memblock(ptr, hint)) {
        blk = hint;
    } else {
        blk = find_containing_memblock(ptr);
    }

    deallocate_from_block(ptr, blk);
    return blk;
}

void mempool_memmgr::deallocate_from_block(void *ptr, MemBlock *blk) {
    size_t idx;

    RT_ASSERT(is_ptr_in_memblock(ptr, blk));
    idx = (((uintptr_t) ptr) - (uintptr_t) payload(blk)) / m_aligned_obj_size;
    mark_unallocated(blk, idx);

    if (blk->m_in_full_chain) {
        blk->m_in_full_chain = false;
        blk->m_hint_for_alloc = idx / 8;
        blk->disconnect_from_list(&m_full_blocks);
        blk->prepend_to_list(&m_free_blocks);
    }
}

void mempool_memmgr::deallocate_all() {
    MemBlock *blk = m_full_blocks;
    if (blk) {
        for (;;) {
            memset(blk->m_alloc_bitmap, 0, m_bitmap_num_bytes);
            blk->m_in_full_chain = false;
            blk->m_hint_for_alloc = 0;

            if (blk->m_next) {
                blk = blk->m_next;
            } else {
                break;
            }
        }

        blk->m_next = m_free_blocks;
    }

    blk = m_free_blocks;
    
    while (blk) {
        memset(blk->m_alloc_bitmap, 0, m_bitmap_num_bytes);
        blk->m_hint_for_alloc = 0;
        blk = blk->m_next;
    }

    if (m_full_blocks) {
        m_free_blocks = m_full_blocks;
        m_full_blocks = nullptr;
    }
}

