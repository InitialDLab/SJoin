#include <schema.h>
#include <utils.h>
#include <cstring>
#include <string>

#include <iostream>

using namespace std;

Schema::Schema(
    memmgr *mmgr,
    COLID ncols,
    typeinfo * const ti[],
    const char * const names[], // can be NULL
    COLID ts_colid,
    COLID primary_key_colid)
    : m_mmgr(mmgr),
      m_ti(NULL),
      m_names(NULL),
      m_offset(NULL), 
      m_ncols(ncols),
      m_num_varlen_fields(0),
      m_varlen_beg(0),
      m_varlen_payload(0),
      m_maxlen(0),
      m_ts_colid(ts_colid),
      m_primary_key_colid(primary_key_colid),
      m_name_to_colid(mmgr, (decltype(m_name_to_colid.size()))(ncols * 1.5)) {

    m_ti = m_mmgr->allocate_array<typeinfo*>(ncols);
    m_names = m_mmgr->allocate_array<char*>(ncols);
    m_offset = m_mmgr->allocate_array<PAGE_OFFSET>(ncols);
    for (COLID i = 0; i != m_ncols; ++i) {
        m_ti[i] = ti[i];
        if (!names || !names[i]) {
            string tmpname = "col";
            tmpname += to_string(i);
            m_names[i] = mmgr_strdup(m_mmgr, tmpname.c_str());
        } else {
	    m_names[i] = mmgr_strdup(m_mmgr, names[i]);
	}
        m_name_to_colid.emplace(std::string_view(m_names[i], strlen(m_names[i])), i);

        if (m_ti[i]->is_varlen()) {
            if (m_num_varlen_fields != 0) {
                m_offset[i] = (m_num_varlen_fields) - 1;
            } else m_offset[i] = INVALID_COLID;
            ++m_num_varlen_fields;
            m_maxlen += m_ti[i]->get_max_size();
        }
    }
    
    if (m_num_varlen_fields) {
        m_varlen_beg = sizeof(PAGE_OFFSET);
    } else {
        m_varlen_beg = 0;
    }
    for (COLID i = 0; i != m_ncols; ++i) {
        if (!m_ti[i]->is_varlen()) {
            m_offset[i] = m_varlen_beg;
            m_varlen_beg += m_ti[i]->get_size();
        }
    }

    if (m_num_varlen_fields) {
        m_varlen_payload = m_varlen_beg + (m_num_varlen_fields - 1) * sizeof(PAGE_OFFSET);
        m_maxlen += m_varlen_payload;
    } else {
        m_varlen_payload = m_varlen_beg;
        m_maxlen = m_varlen_beg;
    }
}

Schema::~Schema() {
    for (COLID i = 0; i != m_ncols; ++i) {
        m_mmgr->deallocate(m_names[i]);
    }
    m_mmgr->deallocate(m_names);
    m_mmgr->deallocate(m_ti);
    m_mmgr->deallocate(m_offset);
}


PAGE_OFFSET Schema::get_offset(
    COLID colid,
    BYTE *buf,
    PAGE_OFFSET *pcollen
    DARG(PAGE_OFFSET buflen)) const {

    PAGE_OFFSET offset;
    size_t collen;

    RT_ASSERT(colid >= 0 && colid < m_ncols);

    if (m_num_varlen_fields) {
        // format 1
        
        if (m_ti[colid]->is_varlen()) {
            PAGE_OFFSET offset2;
            if (m_offset[colid] == INVALID_COLID) {
                offset = m_varlen_payload;
                
                if (m_num_varlen_fields == 1) {
                    offset2 = btypefuncs<PAGE_OFFSET>::read(buf
                        DARG(buflen));
                } else {
                    offset2 = m_varlen_beg;
                    offset2 = btypefuncs<PAGE_OFFSET>::read(buf + offset2
                        DARG(buflen - offset2));
                }
            } else {
                offset = m_varlen_beg + m_offset[colid] * sizeof(PAGE_OFFSET);
                offset = btypefuncs<PAGE_OFFSET>::read(buf + offset
                    DARG(buflen - offset));

                if (m_offset[colid] + 2 == m_num_varlen_fields) {
                    offset2 = btypefuncs<PAGE_OFFSET>::read(buf
                        DARG(buflen));
                } else {
                    offset2 = m_varlen_beg + (m_offset[colid] + 1) * sizeof(PAGE_OFFSET);
                    offset2 = btypefuncs<PAGE_OFFSET>::read(buf + offset2 
                        DARG(buflen - offset2));
                }
            }

            collen = offset2 - offset;
        } else {
            offset = m_offset[colid];
            collen = m_ti[colid]->get_size();
        }
    } else {
        // format 0
        offset = m_offset[colid];
        collen = m_ti[colid]->get_size();
    }
    
    if (pcollen) *pcollen = collen;
    return offset;
}

