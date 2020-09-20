#ifndef STD_CONTAINERS_H
#define STD_CONTAINERS_H

#include <config.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <deque>
#include <functional>


class memmgr;
template<class T> class memmgr_std_allocator;

namespace std_container_with_memmgr_impl {

template<class C>
using value_type_of = typename C::value_type;

template<template<class ...> class C, class... CArgs>
using memmgr_alloc_for = memmgr_std_allocator<value_type_of<C<CArgs...>>>;

template<template<class ...> class C, class... CArgs>
using container_with_memmgr = C<CArgs..., memmgr_alloc_for<C, CArgs...>>;


template<
    template<class... CArgs1> class Container,
    class... CArgs2>
struct _C: public container_with_memmgr<Container, CArgs2...> {

    template<class... Args>
    _C(memmgr *mmgr, Args... args)
        : container_with_memmgr<Container, CArgs2...>(
            std::forward<Args>(args)...,
            memmgr_alloc_for<Container, CArgs2...>(mmgr)) {}
};

}

template<class T>
using std_vector = std_container_with_memmgr_impl::_C<std::vector, T>;

template<
    class Key,
    class Compare = std::less<Key>>
using std_set = std_container_with_memmgr_impl::_C<std::set, Key, Compare>;

template<
    class Key,
    class T,
    class Compare = std::less<Key>>
using std_map = std_container_with_memmgr_impl::_C<std::map, Key, T, Compare>;

template<
    class Key,
    class T,
    class Compare = std::less<Key>>
using std_multimap = std_container_with_memmgr_impl::_C<
    std::multimap, Key, T, Compare>;

template<
    class Key,
    class Compare = std::less<Key>>
using std_multiset = std_container_with_memmgr_impl::_C<
    std::multiset, Key, Compare>;

template<
    class Key,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>>
using std_unordered_set = std_container_with_memmgr_impl::_C<
    std::unordered_set, Key, Hash, KeyEqual>;

template<
    class Key,
    class T,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>>
using std_unordered_map = std_container_with_memmgr_impl::_C<
    std::unordered_map, Key, T, Hash, KeyEqual>;

template<
    class Key,
    class T,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>>
using std_unordered_multimap = std_container_with_memmgr_impl::_C<
    std::unordered_multimap, Key, T, Hash, KeyEqual>;

template<class T>
using std_deque = std_container_with_memmgr_impl::_C<std::deque, T>;

#endif

