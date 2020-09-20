#ifndef DATATYPES_H
#define DATATYPES_H

#include <basictypes.h>
#include <datum.h>

#include <functional>
#include <string_view>

#include <iostream>

enum dtype : BYTE {
    DTYPE_INT                           = 0,
    DTYPE_LONG                          = 1,
    DTYPE_VARCHAR                       = 2,
    DTYPE_DATE                          = 3,
    DTYPE_DOUBLE                        = 4,

    NUM_DTYPES
};

namespace dtype_impl {
    constexpr bool dtype_is_varlen[NUM_DTYPES] = { false,      // int
        false,      // long
        true,       // varchar
        false,      // date
        false,      // double
    };

    constexpr PAGE_OFFSET dtype_fixed_size[NUM_DTYPES] = {
        sizeof(INT4),       // int
        sizeof(INT8),       // long
        sizeof(StrRef),     // varchar
        sizeof(UINT4),      // date
        sizeof(DOUBLE),     // double
    };
}

class typeinfo {
public:
    typeinfo(dtype type)
        :  m_type(type) {}

    dtype get_type() const { return m_type; }
    
    bool is_varlen() const { return dtype_impl::dtype_is_varlen[m_type]; }
    
    /*
     * Fixed-len type: return its size
     * Var-len type: size in DATUM
     */
    PAGE_OFFSET get_size() const {
        return dtype_impl::dtype_fixed_size[m_type];
    }

    PAGE_OFFSET get_alignment() const {
        PAGE_OFFSET datum_size = get_size();
        if (datum_size == 1) return 1;
        if (datum_size == 2) return 2;
        if (datum_size <= 4) return 4;
        if (datum_size <= 8) return 8;
        return 16;
    }
    
    /*
     * Fixed-len type: return its size
     * Var-len type: return the max size (in bytes)
     */
    virtual PAGE_OFFSET get_max_size() const {
        return dtype_impl::dtype_fixed_size[m_type];
    }

    virtual size_t hash(const_Datum datum) const = 0;

    virtual int compare(const_Datum datum1, const_Datum datum2) const = 0;

    virtual void add(Datum res, Datum left, Datum right) const {
        RT_ASSERT(false, "N/A");
    }

    virtual void minus(Datum res, Datum left, Datum right) const {
        RT_ASSERT(false, "N/A");
    }

    virtual void copy(Datum tgt, Datum src) const = 0;

    //virtual Datum get_column(COLID colid) = 0;

protected:
    dtype           m_type;
};

class typeinfo_int: public typeinfo {
public:
    typeinfo_int(): typeinfo(DTYPE_INT) {}

    virtual size_t hash(const_Datum datum) const {
        return (size_t) datum->m_int4;
    }

    virtual int compare(const_Datum datum1, const_Datum datum2) const {
	return (datum1->m_int4 < datum2->m_int4) ? -1 :
            ((datum1->m_int4 == datum2->m_int4) ? 0 : 1);
    }

    virtual void add(Datum datum, Datum left, Datum right) const {
        datum->m_int4 = left->m_int4 + right->m_int4;
    }

    virtual void minus(Datum datum, Datum left, Datum right) const {
        datum->m_int4 = left->m_int4 - right->m_int4;
    }

    virtual void copy(Datum tgt, Datum src) const {
        tgt->m_int4 = src->m_int4;
    }
};

class typeinfo_long: public typeinfo {
public:
    typeinfo_long(): typeinfo(DTYPE_LONG) {}

    virtual size_t hash(const_Datum datum) const {
        return (size_t) datum->m_int8;
    }

    virtual int compare(const_Datum datum1, const_Datum datum2) const {
	return (datum1->m_int8 < datum2->m_int8) ? -1 :
            ((datum1->m_int8 == datum2->m_int8) ? 0 : 1);
    }

    virtual void add(Datum datum, Datum left, Datum right) const {
        datum->m_int8 = left->m_int8 + right->m_int8;
    }

    virtual void minus(Datum datum, Datum left, Datum right) const {
        datum->m_int8 = left->m_int8 - right->m_int8;
    }

    virtual void copy(Datum tgt, Datum src) const {
        tgt->m_int8 = src->m_int8;
    }
};

class typeinfo_varchar: public typeinfo {
public:
    typeinfo_varchar(PAGE_OFFSET maxlen)
        : typeinfo(DTYPE_VARCHAR),
        m_maxlen(maxlen) {}

    virtual PAGE_OFFSET get_max_size() const {
        return m_maxlen;
    }

    virtual size_t hash(const_Datum datum) const {
        return std::hash<std::string_view>()(std::string_view(datum->m_str.m_str));
    }

    virtual int compare(const_Datum datum1, const_Datum datum2) const {
        return strcmp(datum1->m_str.m_str, datum2->m_str.m_str);
    }

    virtual void copy(Datum tgt, Datum src) const {
        tgt->m_str = src->m_str;
    }
private:
    PAGE_OFFSET         m_maxlen;
};

class typeinfo_date: public typeinfo {
public:
    typeinfo_date(): typeinfo(DTYPE_DATE) {}

    virtual size_t hash(const_Datum datum) const {
        return (size_t) datum->m_uint4;
    }

    virtual int compare(const_Datum datum1, const_Datum datum2) const {
        UINT4 y1, m1, d1, y2, m2, d2;
        y1 = datum1->m_uint4 >> 9;
        y2 = datum2->m_uint4 >> 9;
        if (y1 < y2) return -1;
        if (y1 > y2) return 1;

        m1 = (datum1->m_uint4 >> 5) & 0xf; // + 1 is the month
        m2 = (datum2->m_uint4 >> 5) & 0xf;
        if (m1 < m2) return -1;
        if (m1 > m2) return 1;

        d1 = datum1->m_uint4 & 0x1f;
        d2 = datum2->m_uint4 & 0x1f;
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
    }

    virtual void copy(Datum tgt, Datum src) const {
        tgt->m_uint4 = src->m_uint4;
    }
};

class typeinfo_double: public typeinfo {
public:
    typeinfo_double(): typeinfo(DTYPE_DOUBLE) {}

    virtual size_t hash(const_Datum datum) const {
        return std::hash<DOUBLE>()(datum->m_double);
    }

    virtual int compare(const_Datum datum1, const_Datum datum2) const {
	return (datum1->m_double < datum2->m_double) ? -1 :
            ((datum1->m_double == datum2->m_double) ? 0 : 1);
    }

    virtual void copy(Datum tgt, Datum src) const {
        tgt->m_double = src->m_double;
    }
};

namespace dtype_impl {
    extern typeinfo *fixed_len_ti[NUM_DTYPES];
}

extern void init_typeinfos();

inline typeinfo *get_fixedlen_ti(dtype type) {
    RT_ASSERT(!dtype_impl::dtype_is_varlen[type]);
    return dtype_impl::fixed_len_ti[type];
}

#endif // DATATYPES_H

