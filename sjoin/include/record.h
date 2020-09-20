#ifndef RECORD_H
#define RECORD_H

#include <schema.h>

struct RecordId {
    SID         m_sid;
    ROWID       m_rowid;
};

class Record {
public:
    
    Record();

    ~Record() {}

    void set_buf(BYTE *buf, PAGE_OFFSET buflen);

    void set_buf(BYTE *buf);

    void reset_for_creation() {
        m_last_set_varlen_field_slot = INVALID_COLID;
    }

    BYTE *get_buf() const { return m_buf; }

    PAGE_OFFSET get_buflen() const;

    void set_schema(const Schema *schema) { m_schema = schema; }

    const Schema *get_schema() const { return m_schema; }

    PAGE_OFFSET get_len() const;
    
    INT4 get_int(COLID colid) const {
        PAGE_OFFSET offset;
        
        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_INT);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        return btypefuncs<INT4>::read(m_buf + offset  DARG(m_buflen - offset));
    }

    void set_int(COLID colid, INT4 value) {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_INT);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        return btypefuncs<INT4>::write(m_buf + offset, value
                DARG(m_buflen - offset));
    }

    INT8 get_long(COLID colid) const {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_LONG);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));
        return btypefuncs<INT8>::read(m_buf + offset  DARG(m_buflen - offset));
    }

    void set_long(COLID colid, INT8 value) {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_LONG);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        return btypefuncs<INT8>::write(m_buf + offset, value
                DARG(m_buflen - offset));
    }

    char *get_varchar(COLID colid, PAGE_OFFSET *plen) const {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_VARCHAR);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            plen
            DARG(m_buflen));
        return (char *)(m_buf + offset);
    }

    void set_varchar_seq(COLID colid, const char *value, size_t len);
    
    UINT4 get_date(COLID colid) const {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_DATE);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        return btypefuncs<UINT4>::read(m_buf + offset  DARG(m_buflen - offset));
    }

    void set_date(COLID colid, UINT4 value) {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_DATE);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));
        
        btypefuncs<UINT4>::write(m_buf + offset, value
            DARG(m_buflen - offset));
    }

    DOUBLE get_double(COLID colid) const {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_DOUBLE);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        return btypefuncs<DOUBLE>::read(m_buf + offset  DARG(m_buflen - offset));
    }

    void set_double(COLID colid, DOUBLE value) {
        PAGE_OFFSET offset;

        RT_ASSERT(m_buf);
        RT_ASSERT(m_schema->get_type(colid) == DTYPE_DOUBLE);
        offset = m_schema->get_offset(
            colid,
            m_buf,
            NULL
            DARG(m_buflen));

        btypefuncs<DOUBLE>::write(m_buf + offset, value
            DARG(m_buflen - offset));
    }

    TIMESTAMP get_ts() const {
        return (TIMESTAMP) get_long(m_schema->get_ts_colid());
    }
    
    bool parse_line(const char *line, char delim);

    DATUM get_column(COLID colid);

    void copy_column(Datum target, COLID colid);

    std::string to_string(COLID colid);

    std::string to_string(char delim);

private:
    BYTE                *m_buf;

    PAGE_OFFSET         m_buflen;

    const Schema        *m_schema;
    
    COLID               m_last_set_varlen_field_slot;
};

#endif

