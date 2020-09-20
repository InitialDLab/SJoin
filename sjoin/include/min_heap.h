#ifndef MIN_HEAP_H
#define MIN_HEAP_H

#include <config.h>
#include <basictypes.h>
#include <std_containers.h>
#include <utility>

template<class T, class KeyFunc>
void make_min_heap(
    std_vector<T>       &heap,
    UINT8               n,
    KeyFunc             key_func) {
    
    for (UINT8 i = n >> 1; i >= 1; --i) {
        push_down_min_heap(
            heap,
            i,
            n,
            key_func);
    }
}

template<class T, class KeyFunc>
void push_down_min_heap(
    std_vector<T>       &heap,
    UINT8               i,
    UINT8               n,
    KeyFunc             key_func) {
    
    T item_i0 = heap[i];
    auto key_i0 = key_func(item_i0);
    UINT8 j = i << 1;
    while (j <= n) {
        auto key_j = key_func(heap[j]);
        if (j < n) {
            UINT8 k = j + 1;
            auto key_k = key_func(heap[k]);
            if (key_k < key_j) {
                if (key_i0 > key_k) {
                    heap[i] = heap[k];
                    i = k;
                    goto continue_loop;
                } else break;
            }
        }
        if (key_i0 > key_j) {
            heap[i] = heap[j];
            i = j;
        } else break;
continue_loop:
        j = i << 1;
    }
    
    heap[i] = item_i0;
}

template<class T, class KeyFunc>
void push_up_min_heap(
    std_vector<T>       &heap,
    UINT8               i,
    UINT8               n,
    KeyFunc             key_func) {
    
    T item_i0 = heap[i];
    auto key_i0 = key_func(item_i0);
    UINT8 j = i >> 1;
    while (j >= 1) {
        auto key_j = key_func(heap[j]);
        if (key_i0 < key_j) {
            heap[i] = heap[j];
            i = j;
            j = i >> 1;
        } else break;
    }
    
    heap[i] = item_i0;
}

template<class T, class KeyFunc, class IdxFunc>
void make_min_heap_with_inverted_index(
    std_vector<T>       &heap,
    std_vector<UINT8>   &inverted_index,
    UINT8               n,
    KeyFunc             key_func,
    IdxFunc             idx_func) {
    
    UINT8 i = n;
    for (; i > (n >> 1); --i) {
        inverted_index[idx_func(heap[i])] = i; 
    }
    for (; i >= 1; --i) {
        push_down_min_heap_with_inverted_index(
            heap,
            inverted_index,
            i,
            n,
            key_func,
            idx_func);
    }
}

template<class T, class KeyFunc, class IdxFunc>
void push_down_min_heap_with_inverted_index(
    std_vector<T>       &heap,
    std_vector<UINT8>   &inverted_index,
    UINT8               i,
    UINT8               n,
    KeyFunc             key_func,
    IdxFunc             idx_func) {
    
    T item_i0 = heap[i];
    auto key_i0 = key_func(item_i0);
    UINT8 j = i << 1;
    while (j <= n) {
        auto key_j = key_func(heap[j]);
        if (j < n) {
            UINT8 k = j + 1;
            auto key_k = key_func(heap[k]);
            if (key_k < key_j) {
                if (key_i0 > key_k) {
                    heap[i] = heap[k];
                    inverted_index[idx_func(heap[i])] = i;
                    i = k;
                    goto continue_loop;
                } else break;
            }
        }
        if (key_i0 > key_j) {
            heap[i] = heap[j];
            inverted_index[idx_func(heap[i])] = i;
            i = j;
        } else break;
continue_loop:
        j = i << 1;
    }
    
    heap[i] = item_i0;
    inverted_index[idx_func(item_i0)] = i;
}

template<class T, class KeyFunc, class IdxFunc>
void push_up_min_heap_with_inverted_index(
    std_vector<T>       &heap,
    std_vector<UINT8>   &inverted_index,
    UINT8               i,
    UINT8               n,
    KeyFunc             key_func,
    IdxFunc             idx_func) {
    
    T item_i0 = heap[i];
    auto key_i0 = key_func(item_i0);
    UINT8 j = i >> 1;
    while (j >= 1) {
        auto key_j = key_func(heap[j]);
        if (key_i0 < key_j) {
            heap[i] = heap[j];
            inverted_index[idx_func(heap[i])] = i;
            i = j;
            j = i >> 1;
        } else break;
    }
    
    heap[i] = item_i0;
    inverted_index[idx_func(item_i0)] = i;
}

#endif
