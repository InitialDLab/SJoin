#include <memmgr.h>
#include <algorithm>
#include <cstdio>

//#define MEMMGR_VERBOSE

basic_memmgr *g_root_mmgr = nullptr;
basic_tracked_memmgr *g_tracked_mmgr = nullptr;

memmgr::~memmgr() {
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->~memmgr()\n", (uintptr_t) this);
#endif
}

memmgr::memmgr(memmgr *parent_mmgr)
    : m_parent(parent_mmgr),
    m_child_mmgr(),
    m_child_mmgr_cleaned(false) {
    
    if (parent_mmgr) {
        parent_mmgr->add_child(this);
    }
}

void memmgr::clean_child_mmgr() { 
    if (!m_child_mmgr_cleaned) {
        m_child_mmgr_cleaned = true;
        for (memmgr *mmgr: m_child_mmgr) {
            mmgr->~memmgr();
            this->deallocate(mmgr);
        }
        m_child_mmgr.clear();
    }
}

void memmgr::add_child(memmgr *mmgr) {
    m_child_mmgr.insert(mmgr);
    mmgr->m_parent = this;
}

void memmgr::remove_child(memmgr *mmgr) {
    m_child_mmgr.erase(mmgr);
}

void memmgr::deallocate_ptrs_in_list(void *ptr, ptrdiff_t next_ptr_offset) {
    while (ptr) {
        void *next_ptr = *reinterpret_cast<void**>(reinterpret_cast<BYTE*>(ptr) + next_ptr_offset);
        deallocate(ptr);
        ptr = next_ptr;
    }
}

void memmgr::deallocate_ptrs_in_array(void **ptr, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        deallocate(ptr[i]);
    }
}

basic_memmgr::basic_memmgr(memmgr *parent_mmgr)
    : memmgr(parent_mmgr) {}

basic_memmgr::~basic_memmgr() {
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->~basic_memmgr()\n", (uintptr_t) this);
#endif
    clean_child_mmgr(); 
}

basic_tracked_memmgr::basic_tracked_memmgr(memmgr *parent)
    : memmgr(parent),
    m_tracked_ptrs(parent, 1024) {}

basic_tracked_memmgr::~basic_tracked_memmgr() {
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->~basic_tracked_memmgr()\n", (uintptr_t) this);
#endif
    clean_child_mmgr();

    for (auto &ptr_dfunc : m_tracked_ptrs) {
        if (ptr_dfunc.second) {
            ptr_dfunc.second(ptr_dfunc.first);
        }
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->deallocate(             0x%016lu)\n",
            (uintptr_t) this,
            (uintptr_t) ptr_dfunc.first);
#endif
        delete[] (BYTE*) ptr_dfunc.first;
    }
    m_tracked_ptrs.clear();
}

void *basic_tracked_memmgr::allocate(size_t n, const void *hint) {
    void *buf = new BYTE[n];
    m_tracked_ptrs.emplace(buf, std::function<void(void*)>());
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->allocate(%8lu) == 0x%016lu\n",
            (uintptr_t) this, n,
            (uintptr_t) buf);
#endif
    return buf;
}

void basic_tracked_memmgr::deallocate(void *ptr) {
    if (!ptr) return ;
    auto num_removed = m_tracked_ptrs.erase(ptr);
    RT_ASSERT(num_removed == 1, "0x%016lX not found in basic_tracked_memmgr", (uintptr_t) ptr);
    (void) num_removed;
#ifdef MEMMGR_VERBOSE
    printf("0x%016lu->deallocate(             0x%016lu)\n",
            (uintptr_t) this,
            (uintptr_t) ptr);
#endif
    delete[] (BYTE *) ptr;
}

void basic_tracked_memmgr::register_destroy_func(void *p, std::function<void(void*)> dfunc) {
    RT_ASSERT(m_tracked_ptrs.find(p) != m_tracked_ptrs.end());
    m_tracked_ptrs[p] = dfunc;
}

void create_root_memmgrs() {
    RT_ASSERT(!g_root_mmgr);
    g_root_mmgr = new basic_memmgr(nullptr);
    g_tracked_mmgr = g_root_mmgr->newobj<basic_tracked_memmgr>(g_root_mmgr);
}

void destroy_root_memmgrs() {
    RT_ASSERT(g_root_mmgr);
    delete g_root_mmgr;
    g_tracked_mmgr = nullptr;
    g_root_mmgr = nullptr;
}

void destroy_memmgr(memmgr *mmgr) {
    memmgr *parent_mmgr = mmgr->get_parent_mmgr();

    RT_ASSERT(parent_mmgr);
    parent_mmgr->remove_child(mmgr);
    mmgr->~memmgr();
    parent_mmgr->deallocate((void*)mmgr);
}

