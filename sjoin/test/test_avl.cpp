#define IN_TEST_AVL
#include <vector>
#include <utility>
#include <iostream>
#include <random>
#include <algorithm>
#include <chrono>
#include <ctime>

#include <memmgr.h>
#include <avl.h>
//#include "reservoir.h"
//#include "memory_pool.h"
//#include "linear_road.h"
//#include "fenwick_tree.h"

#define N 1000000
#define M 10000

#define BLOCK_BYTES (1024 * 1024)

void test_avl1(memmgr *mmgr, bool mmgr_is_shared) {
    size_t node_size = get_avl_node_size<int, int>();
    size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size);
    mempool_memmgr *mempool = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size,
        block_size);

    avl_t<int, int> tree(mempool, mmgr_is_shared);

    tree.emplace(0, 1);
    //tree.print(std::cout); std::cout << std::endl;
    tree.emplace(10, 2);
    //tree.print(std::cout); std::cout << std::endl;
    tree.emplace(20, 3);
    //tree.print(std::cout); std::cout << std::endl;
    tree.emplace(15, 4);
    //tree.print(std::cout); std::cout << std::endl;
    tree.emplace(13, 5);
    tree.emplace(12, 6);
    tree.print(std::cout); std::cout << std::endl;
    
    for (auto iter = tree.begin(); iter != tree.end(); ++iter) {
        std::cout << iter->key << ' ' << iter->value << std::endl;
    }
    
    std::cout << std::endl;
    auto start = tree.lower_bound(9);
    auto end = tree.upper_bound(15);
    for (auto iter = start; iter != end; ++iter) {
        std::cout << iter->key << ' ' << iter->value << std::endl;
    }
    
    // it is safe to call cleaned avl with invalid mmgr
    tree.clear();
    destroy_memmgr(mempool);
}

void test_avl2(memmgr *mmgr, bool mmgr_is_shared) {
    size_t node_size = get_avl_node_size<float, int>();
    size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size);
    mempool_memmgr *mempool = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size,
        block_size);

    avl_t<float, int> tree(mempool, mmgr_is_shared);
    std::vector<std::pair<float, int>> v;

    std::mt19937_64 rgen(time(0));
    std::uniform_real_distribution<float> unif(0.0, 1.0);
    for (int i = 0; i < N; ++i) {
        v.emplace_back(unif(rgen), i);
        tree.emplace(v.back().first, v.back().second);
        if ((i + 1) % 100000 == 0) {
            std::cout << i + 1 << std::endl;
        }
    }
    
    std::cout << "start iterating" << std::endl;
    std::sort(v.begin(), v.end());
    auto viter = v.begin();
    auto vend = v.end();
    auto titer = tree.begin();
    auto tend = tree.end();
    while (viter != vend && titer != tend) {
        if (viter->first != titer->key) {
            std::cout << viter->first << ' ' << viter->second << ' '
                << titer->key << ' ' << titer->value << std::endl;
        }
        ++viter;
        ++titer;
    }

    if (viter != vend) {
        std::cout << "viter != vend" << std::endl;
    }

    if (titer != tend) {
        std::cout << "titer != tend" << std::endl;
    }

    std::cout << "testing get_nth..." << std::endl;
    for (int i = 0; i < N; ++i) {
        auto iter = tree.get_nth((unsigned long) i, count_agg);
        if (iter->value != v[i].second) {
            std::cout << i << ' '
                << iter->key << ' ' << iter->value << ' '
                << v[i].first << ' ' << v[i].second << std::endl;
        }
    }

    tree.clear();
    destroy_memmgr(mempool);
}

void test_avl3(memmgr *mmgr, bool mmgr_is_shared) {
    size_t node_size = get_avl_node_size<int, int>();
    size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size);
    mempool_memmgr *mempool = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size,
        block_size);

    avl_t<int, int> tree(mempool, mmgr_is_shared);
    std::vector<std::pair<int, int>> v;

    std::mt19937_64 rgen(time(0));
    std::uniform_int_distribution<int> unif(0, 100);
    for (int i = 0; i < N; ++i) {
        v.emplace_back(unif(rgen), i);
        tree.emplace(v.back().first, v.back().second);
        /*if ((i + 1) % 100000 == 0) {
            std::cout << i + 1 << std::endl;
        } */
    }

    std::cout << "testing find..." << std::endl;
    std::sort(v.begin(), v.end());
    std::uniform_int_distribution<int> unif2(-50, 150);
    for (int i = 0; i < M; ++i) {
        int lower = unif2(rgen);
        int upper = unif2(rgen);
        long tree_cnt = tree.sum_range_ii(lower, upper, count_agg);

        auto liter = std::lower_bound(v.begin(), v.end(), std::make_pair(lower, 0));
        auto riter = std::upper_bound(v.begin(), v.end(), std::make_pair(upper, N));
        long v_cnt = riter - liter;
        if (v_cnt < 0) v_cnt = 0;

        if (tree_cnt != v_cnt) {
            std::cout << lower << ' ' << upper << ' ' << tree_cnt << ' ' << v_cnt
                << std::endl;
        }
    }

    tree.clear();
    destroy_memmgr(mempool);
}

void test_avl7(memmgr *mmgr, bool mmgr_is_shared) {
    size_t node_size = get_avl_node_size<int, int>();
    size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size);
    mempool_memmgr *mempool = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size,
        block_size);

    avl_t<int, int> tree(mempool, mmgr_is_shared);
    std::vector<int> v;
    
    constexpr int num_distinct_values = 100;
    v.resize(num_distinct_values);

    std::mt19937_64 rgen(time(0));
    std::uniform_int_distribution<int> unif(0, num_distinct_values - 1);
    for (int i = 0; i < N; ++i) {
        int key = unif(rgen);
        tree.emplace(key, v[key]++);
        if ((i + 1) % 100000 == 0) {
            std::cout << i + 1 << std::endl;
        }
    }
    
    std::cout << "start iterating" << std::endl;
    auto titer = tree.begin();
    auto tend = tree.end();
    int prev_key = -1;
    int prev_value = 0;
    while (titer != tend) {
        if (titer->key != prev_key) {
            prev_key = titer->key;
        } else {
            if (prev_value >= titer->value) {
                std::cout << "prev_value >= titer->value "
                    << titer->key << ' ' << prev_value << ' ' << titer->value
                    << std::endl;
            }
        }
        prev_value = titer->value;
        --v[prev_key];
        ++titer;
    }

    for (int i = 0; i != num_distinct_values; ++i) {
        if (v[i] != 0) {
            std::cout << "v[i] == " << v[i] << std::endl;
        }
    }

    tree.clear();
    destroy_memmgr(mempool);
}

void test_avl8(memmgr *mmgr, bool mmgr_is_shared) {
    constexpr size_t node_size = get_avl_node_size<int, int>();
    constexpr size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size);
    mempool_memmgr *mempool = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size,
        block_size);


    avl_t<int, int> tree(mempool, mmgr_is_shared);

    constexpr size_t node_size2 = get_avl_node_size<int, std::vector<int>>();
    constexpr size_t block_size2 = mempool_memmgr::calc_block_size_from_block_bytes(
        BLOCK_BYTES, node_size2);
    mempool_memmgr *mempool2 = create_memmgr<mempool_memmgr>(
        mmgr,
        node_size2,
        block_size2);

    avl_t<int, std::vector<int>> tree2(mempool2, mmgr_is_shared);

    std::mt19937_64 rgen(time(0));
    std::uniform_int_distribution<int> unif(0, 100);
    for (int i = 0; i < N; ++i) {
        int key = unif(rgen);
        tree.emplace(key, i);
        auto iter = tree2.find_for_update(key);
        if (iter.is_null()) {
            tree2.emplace_at(iter, key);
        }
        iter->value.push_back(i);
        if ((i + 1) % 100000 == 0) {
            std::cout << i + 1 << std::endl;
        }
    }

    auto titer1 = tree.begin(),
         tend1 = tree.end();
    auto titer2 = tree2.begin(),
         tend2 = tree2.end();

    while (titer2 != tend2) {
        for (int i : titer2->value) {
            if (titer1 == tend1) {
                std::cout << "missing values in tree 1 (multi-map) " 
                    << titer2->key << ' ' << i << std::endl;
                return ;
            }
            if (titer1->key != titer2->key || titer1->value != i) {
                std::cout << titer1->key << ' ' << titer1->value << ' '
                    << titer2->key << ' ' << i << std::endl;
                return ;
            }
            ++titer1;
        }
        ++titer2;
    }

    if (titer1 != tend1) {
        std::cout << "titer1 != tend1" << std::endl;
    }

    tree.clear();
    tree2.clear();
    destroy_memmgr(mempool);
    destroy_memmgr(mempool2);
}

void test_avl(memmgr *mmgr, bool mmgr_is_shared) {
    test_avl1(mmgr, mmgr_is_shared);
    test_avl2(mmgr, mmgr_is_shared);
    test_avl3(mmgr, mmgr_is_shared);
    test_avl7(mmgr, mmgr_is_shared);
    test_avl8(mmgr, mmgr_is_shared);
}
