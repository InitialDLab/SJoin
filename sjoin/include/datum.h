#ifndef DATUM_H
#define DATUM_H

#include <config.h>
#include <basictypes.h>

struct StrRef {
    const char *m_str;
    PAGE_OFFSET m_len;
};

union DATUM {
    INT4        m_int4;
    INT8        m_int8;
    StrRef      m_str;
    UINT4       m_uint4;
    DOUBLE      m_double;
};

typedef DATUM *Datum;
typedef const DATUM *const_Datum;

#define DATUM_from_INT4(value) (DATUM{.m_int4 = (value)})
#define DATUM_from_INT8(value) (DATUM{.m_int8 = (value)})
#define DATUM_from_STR(value, len) (DATUM{.m_str = {.m_str = (value), .m_len = (len) }})
#define DATUM_from_UINT4(value) (DATUM{.m_uint4 = (value)})
#define DATUM_from_DOUBLE(value) (DATUM{.m_double = (value)})
#define DATUM_NONE (DATUM{})


#endif // DATUM_H
