#ifndef BASIC_TYPES_H
#define BASIC_TYPES_H

#include <config.h>
#include <debug.h>

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <cstdlib>
#include <cstring>
#include <limits>

using std::memcpy;

typedef std::int16_t INT2;
typedef std::uint16_t UINT2;
typedef std::int32_t INT4;
typedef std::uint32_t UINT4;
typedef std::int64_t INT8;
typedef std::uint64_t UINT8;
typedef __int128 INT16;
typedef unsigned __int128 UINT16;
typedef unsigned char UCHAR;
typedef UCHAR UINT1;
typedef UCHAR BYTE;
typedef double DOUBLE;

using std::size_t;
using std::uintptr_t;
using std::ptrdiff_t;

typedef UINT2 COLID; // column id
typedef UINT2 SID; // stream id
typedef size_t ROWID;

constexpr const SID INVALID_SID = ~(SID)0;
constexpr const COLID INVALID_COLID = ~(COLID)0;
constexpr const ROWID INVALID_ROWID = ~(ROWID)0;

typedef UINT4 INDEXID;
constexpr INDEXID INVALID_INDEXID = ~(INDEXID)0;

typedef UINT4 PAGE_OFFSET;

typedef ptrdiff_t PayloadOffset;
typedef UINT2 IndexId;

typedef INT8 WEIGHT;
constexpr WEIGHT MAX_WEIGHT = std::numeric_limits<WEIGHT>::max();

typedef INT8 TIMESTAMP;
constexpr TIMESTAMP TS_INFTY = std::numeric_limits<TIMESTAMP>::max();

template<typename btype, typename Enabled = void>
struct btypefuncs {};

template<typename btype>
struct btypefuncs<btype, typename std::enable_if<std::is_arithmetic_v<btype>>::type> {
    static btype read(BYTE *buf
            DARG(PAGE_OFFSET buflen)) {
        btype v;

        RT_ASSERT(sizeof(btype) <= buflen);
        memcpy(reinterpret_cast<BYTE*>(&v), buf, sizeof(btype));
        return v;
    }

    static void write(BYTE *buf,
            btype value
            DARG(PAGE_OFFSET buflen)) {

        RT_ASSERT(sizeof(btype) <= buflen);
        memcpy(buf, reinterpret_cast<BYTE*>(&value), sizeof(btype));
    }
};


#endif

