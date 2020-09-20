#ifndef SCHEMA_H
#define SCHEMA_H

#include <dtypes.h>
#include <debug.h>
#include <memmgr.h>
#include <unordered_map>
#include <string_view>

#include <iostream>

/*
 * If all fields are fixed len, (format 0)
 * 
 * 0
 * offset(i_0)      offset(i_1)         offset(i_2)              offset(i_{N-1})
 * |                   |                   |                           |
 * |                   |                   |                           |
 * v                   v                   v                           v
 * |fixed-len field i_0|fixed-len field i_1|...|fixed-len field i_{N-1}|
 *
 * If some of them are var len, (format 1)
 *
 *          4
 * 0        offset(i_0)      offset(i_2)         offset(i_2)           offset(i_{N-1})
 * |        |                   |                   |                           |
 * |        |                   |                   |   |-----------------------|
 * v UINT4  v                   v                   v   v                        
 * | reclen |fixed-len field i_0|fixed-len field i_1|...|fixed-len field i_{N-1}|
 * | var_offsets |varlen field j_0|varlen field j_1|...|varlen field j_{M-1}|
 * ^             ^                ^                ^   ^                    ^
 * |             |                |                |   |                    |
 * |             |                |                |   |                    |
 * m_varlen_beg  A + (m_num_varlen_fields-1) * 4   |  ...                 reclen
 * (A)           (B)              |                |
 *          (m_varlen_payload)    |
 *                          B + var_offsets[0] B + var_offsets[1]
 *                          (offset(j_1) == 0) (offset(j_2) == 1)
 *
 * offset(j_0) should be INVALID_COLID and never used.
 *
 * alignment issue?
 */
class Schema {
public:
    Schema(
        memmgr *mmgr,
        COLID ncols,
        typeinfo * const ti[],
        const char * const names[],
        COLID ts_colid = INVALID_COLID,
        COLID primary_key_colid = INVALID_COLID);

    Schema(const Schema &) = delete;
    Schema(Schema &&) = delete;

    Schema &operator=(const Schema &) = delete;
    Schema &operator=(Schema &&) = delete;
    
    ~Schema();

    COLID get_num_cols() const { return m_ncols; }

    dtype get_type(COLID colid) const { return m_ti[colid]->get_type(); }

    typeinfo *get_ti(COLID colid) const { return m_ti[colid]; }

    const char *get_name(COLID colid) const { return m_names[colid]; }

    COLID get_colid_from_name(std::string_view colname) const {
        auto iter = m_name_to_colid.find(colname);
        if (iter == m_name_to_colid.end()) return INVALID_COLID;
	return iter->second;
    }

    PAGE_OFFSET get_offset_raw(COLID colid) const { return m_offset[colid]; }

    PAGE_OFFSET get_offset(
        COLID colid,
        BYTE *buf,
        PAGE_OFFSET *pcollen
        DARG(PAGE_OFFSET buflen)) const;

    PAGE_OFFSET get_max_len() const { return m_maxlen; }

    COLID get_num_varlen_fields() const { return m_num_varlen_fields; }

    bool is_fixed_len() const { return get_num_varlen_fields() == 0; }
    
    PAGE_OFFSET get_varlen_beg() const { return m_varlen_beg; }

    PAGE_OFFSET get_varlen_payload() const { return m_varlen_payload; }

    COLID get_ts_colid() const { return m_ts_colid; }

    COLID get_primary_key_colid() const { return m_primary_key_colid; }

private:
    memmgr              *m_mmgr;

    typeinfo            **m_ti;

    char                **m_names;

    PAGE_OFFSET         *m_offset;

    COLID               m_ncols;

    COLID               m_num_varlen_fields;

    PAGE_OFFSET         m_varlen_beg;

    PAGE_OFFSET         m_varlen_payload;

    PAGE_OFFSET         m_maxlen;

    COLID               m_ts_colid;

    COLID               m_primary_key_colid;

    std_unordered_map<std::string_view, COLID>
                        m_name_to_colid;
        
};

#endif // SCHEMA_H

