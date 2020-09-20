#ifndef UNION_SET_H
#define UNION_SET_H

#include "basic_defs.h"

template<class VecUINT8>
void union_set_initialize(
    VecUINT8 &vec,
    UINT8 n) {
    
    for (UINT8 i = 0; i < n; ++i) {
        vec[i] = i;
    }
}

template<class VecUINT8>
UINT8 union_set_get_parent(
    VecUINT8 &vec,
    UINT8 i) {

    if (vec[vec[i]] != i) {
        vec[i] = union_set_get_parent(vec, vec[i]);
    }
    return vec[i];
}

template<class VecUINT8>
bool union_set_is_in_same_set(
    VecUINT8 &vec,
    UINT8 i,
    UINT8 j) {
    return union_set_get_parent(vec, i) == union_set_get_parent(vec, j);
}

template<class VecUINT8>
void union_set_merge(
    VecUINT8 &vec,
    UINT8 i,
    UINT8 j) {
    
    vec[union_set_get_parent(vec, i)] = union_set_get_parent(vec, j);
}

#endif // UNION_SET_H
