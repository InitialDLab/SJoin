#ifndef ADDRESS_H
#define ADDRESS_H

// assuming we are using x86_64 where we have 48-bit virtual addresses
// we use a non-canonical [1] address to encode flags (e.g., an insertion handle
// in iterator)
// [1] https://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details
template<int bit, typename pointer>
inline bool is_bit47_extended(pointer ptr) {
    return ((reinterpret_cast<std::uintptr_t>(ptr) >> bit) & 1) == 
        ((reinterpret_cast<std::uintptr_t>(ptr) >> 47) & 1);
}

template<int bit, typename pointer>
inline bool is_not_bit47_extended(pointer ptr) {
    return ((reinterpret_cast<std::uintptr_t>(ptr) >> bit) & 1) != 
        ((reinterpret_cast<std::uintptr_t>(ptr) >> 47) & 1);
}

template<int bit, typename pointer>
inline pointer xor_bit(pointer ptr) {
    return reinterpret_cast<pointer>(reinterpret_cast<std::uintptr_t>(ptr) ^ (1ul << bit)); 
}

#endif
