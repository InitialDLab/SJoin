#include <record.h>
#include <cstdio>
#include <cstring>
#include <memory>
#include <cstdlib>

using namespace std;

Record::Record()
    : m_buf(nullptr),
    m_buflen(0),
    m_schema(nullptr),
    m_last_set_varlen_field_slot(INVALID_COLID) {}

void Record::set_buf(BYTE *buf) {
    m_buf = buf;
    m_buflen = ~(PAGE_OFFSET) 0;
}

PAGE_OFFSET Record::get_buflen() const {
    if (m_schema->is_fixed_len()) return m_schema->get_max_len();
    return btypefuncs<PAGE_OFFSET>::read(m_buf  DARG(m_buflen));
}

void Record::set_buf(BYTE *buf, PAGE_OFFSET buflen) {
    m_buf = buf;
    m_buflen = buflen;
}

void Record::set_varchar_seq(COLID colid, const char *value, size_t len) {
    PAGE_OFFSET field_offset;
    PAGE_OFFSET varlen_beg;
    COLID slot;
    PAGE_OFFSET end_marker_offset;

    RT_ASSERT(m_buf);
    RT_ASSERT(m_schema->get_type(colid) == DTYPE_VARCHAR);

    slot = (COLID) m_schema->get_offset_raw(colid);
    RT_ASSERT(
        ((slot == INVALID_COLID || slot == 0)
          && m_last_set_varlen_field_slot == INVALID_COLID) ||
        (slot != 0 && m_last_set_varlen_field_slot == slot - 1));
    m_last_set_varlen_field_slot = slot;
    
    varlen_beg = m_schema->get_varlen_beg();
    if (slot == INVALID_COLID) {
        field_offset = m_schema->get_varlen_payload();
    } else {
        field_offset = btypefuncs<PAGE_OFFSET>::read(
            m_buf + varlen_beg + sizeof(PAGE_OFFSET) * slot
            DARG(m_buflen - varlen_beg - sizeof(PAGE_OFFSET) * slot));
    }
    
    len = min((size_t) m_schema->get_ti(colid)->get_max_size(), len);
    RT_ASSERT(field_offset + len <= m_buflen);
    memcpy(m_buf + field_offset, value, len);
    
    if (slot == INVALID_COLID) {
        if (m_schema->get_num_varlen_fields() == 1) {
            end_marker_offset = 0;
        } else {
            end_marker_offset = varlen_beg;
        }
    } else if (slot + 2 == m_schema->get_num_varlen_fields()) {
        end_marker_offset = 0;
    } else {
        end_marker_offset = varlen_beg + sizeof(PAGE_OFFSET) * (slot + 1);
    }
    btypefuncs<PAGE_OFFSET>::write(
        m_buf + end_marker_offset,
        field_offset + len
        DARG(m_buflen - end_marker_offset));

}

bool Record::parse_line(const char *line, char delim) {
    const char *p = line;
    COLID colid = 0;

    reset_for_creation();
    while (colid < m_schema->get_num_cols() && *p) {
        const char *p2 = p;
        const char *p3;
        if (*p2 == '"') {
            ++p2;
            while (*p2 && *p2 != '"') ++p2;
            p3 = p2 + 1;
            RT_ASSERT(!*p3 || *p3 == delim);
            //--p2;
            ++p;
        } else {
            while (*p2 && *p2 != delim) ++p2;
            p3 = p2;
        }


        switch (m_schema->get_type(colid)) {
        case DTYPE_INT:
        {
            if (p2 == p) {
                //XXX see below
                set_int(colid, 0);
            } else {
                INT4 value = (INT4) stoi(std::string(p, p2 - p));
                set_int(colid, value);
            }
        }
            break;
        case DTYPE_LONG:
        {
            if (p2 == p) {
                //XXX should be NULL.
                // Now defaults to 0 and this is a required behavior for edgar_input_converter to work
                set_long(colid, 0l);
            } else {
                INT8 value = (INT8) stoll(std::string(p, p2 - p));
                set_long(colid, value);
            }
        }
            break;
        case DTYPE_VARCHAR:
            set_varchar_seq(colid, p, p2 - p);
            break;
        case DTYPE_DATE:
        {
	    if (p2 == p) {
		//XXX should be NULL
		// Now defaults to representation 0 which appears in TPC-DS
		set_date(colid, 0);
	    } else {
		std::string s(p, p2 - p);
		UINT4 y, m, d, value;
		sscanf(s.c_str(), "%u-%u-%u", &y, &m, &d);
		if (y > 9999) return false;
		if (m == 0 || m > 12) return false;
		if (d == 0 || d > 31) return false;
		value = (y << 9) | ((m-1) << 5) | (d - 1);
		set_date(colid, value);
	    }
        }
            break;

        case DTYPE_DOUBLE:
        {
            if (p2 == p) {
                set_double(colid, 0.0);
            } else {
                DOUBLE value = stod(std::string(p, p2 - p));
                set_double(colid, value);
            }
        }
            break;
        default:
            RT_ASSERT(false);
        }

        ++colid;
        if (!*p3) {
            p = p3;
            break;
        }
        p = p3 + 1;
    }
    return colid == m_schema->get_num_cols();
}

std::string Record::to_string(COLID colid) {
    switch (m_schema->get_type(colid)) {
    case DTYPE_INT:
        return std::to_string(get_int(colid));
    case DTYPE_LONG:
        return std::to_string(get_long(colid));
    case DTYPE_VARCHAR:
        {
            PAGE_OFFSET len;
            char *p = get_varchar(colid, &len);
	    return std::string(p, len);
        }
    case DTYPE_DATE:
        {
            UINT4 value = get_date(colid);
            UINT4 y, m, d;
            y = value >> 9;
            m = ((value >> 5) & 0xf) + 1;
            d = (value & 0x1f) + 1;
            std::string ret(10, '\0');
            snprintf(ret.data(), 11, "%04u-%02u-%02u", y, m, d);
            return std::move(ret);
        }
    case DTYPE_DOUBLE:
        return std::to_string(get_double(colid));
    default:
        ;
    }

    RT_ASSERT(false);
    return "";
}

std::string Record::to_string(char delim) {
    std::string s;
    s.append(to_string((COLID) 0));
    for (COLID colid = 1; colid < m_schema->get_num_cols(); ++colid) {
        s.push_back(delim);
        s.append(to_string(colid));
    }
    return std::move(s);
}

DATUM Record::get_column(COLID colid) {
    switch (m_schema->get_type(colid)) {
    case DTYPE_INT:
        return DATUM_from_INT4(get_int(colid));
    case DTYPE_LONG:
        return DATUM_from_INT8(get_long(colid));
    case DTYPE_VARCHAR:
        {
            PAGE_OFFSET len;
            char *p = get_varchar(colid, &len);
            return DATUM_from_STR(p, len);
        }
    case DTYPE_DATE:
        return DATUM_from_UINT4(get_date(colid));
    case DTYPE_DOUBLE:
        return DATUM_from_DOUBLE(get_double(colid));
    default:
        ;
    }

    RT_ASSERT(false);
    return DATUM_NONE;
}

void Record::copy_column(Datum target, COLID colid) {
    switch (m_schema->get_type(colid)) {
    case DTYPE_INT:
        target->m_int4 = get_int(colid);
        break;
    case DTYPE_LONG:
        target->m_int8 = get_long(colid);
        break;
    case DTYPE_VARCHAR:
        target->m_str.m_str = get_varchar(colid, &target->m_str.m_len);
        break;
    case DTYPE_DATE:
        target->m_uint4 = get_date(colid);
        break;
    case DTYPE_DOUBLE:
        target->m_double = get_double(colid);
        break;
    default:
        RT_ASSERT(false);
    }
}

