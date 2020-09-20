#ifndef PREDICATE_H
#define PREDICATE_H

#include <basictypes.h>
#include <dtypes.h>
#include <datum.h>

class Predicate {
public:
    enum Type {
        EQUI,
        BAND,
    };

    enum ForeignKeySpec {
        NO_FKEY,
        LEFT_IS_FKEY,
        RIGHT_IS_FKEY
    };

    Predicate(
        Type type,
        SID  lsid,
        COLID lcolid,
        SID rsid,
        COLID rcolid,
        typeinfo *ti,
        Datum value,
        ForeignKeySpec fkey_spec = NO_FKEY)
    : m_type(type),
      m_lsid(lsid),
      m_rsid(rsid),
      m_lcolid(lcolid),
      m_rcolid(rcolid),
      m_ti(ti),
      m_value(),
      m_fkey_spec(fkey_spec) {
        
        if (value) {
            m_ti->copy(&m_value, value);
        }
    }

    Type get_type() const { return m_type; }

    SID get_lsid() const { return m_lsid; }

    COLID get_lcolid() const { return m_lcolid; }

    SID get_rsid() const { return m_rsid; }

    COLID get_rcolid() const { return m_rcolid; }

    Datum get_value() { return &m_value; }
    
    typeinfo *get_ti() const { return m_ti; }

    SID get_the_other_sid(SID sid) const {
        return (sid == m_lsid) ? m_rsid : m_lsid;
    }

    COLID get_my_colid(SID sid) const {
        return (sid == m_lsid) ? m_lcolid : m_rcolid;
    }

    COLID get_the_other_colid(SID sid) const {
        return (sid == m_lsid) ? m_rcolid : m_lcolid;
    }
    
    void swap_left_right();

    void compute_range_ii(
        SID                 my_sid,
        Datum               my_value,
        Datum               other_value_low,
        Datum               other_value_high);

    void compute_low_i(
        SID                 my_sid,
        Datum               my_value,
        Datum               other_value_low);

    void compute_high_i(
        SID                 my_sid,
        Datum               my_value,
        Datum               other_value_high);

    ForeignKeySpec get_fkey_spec() const {
        return m_fkey_spec;
    }

private:
    Type                m_type;
    
    SID                 m_lsid;

    SID                 m_rsid;

    COLID               m_lcolid;

    COLID               m_rcolid;

    typeinfo            *m_ti;

    DATUM               m_value;

    ForeignKeySpec      m_fkey_spec;
};


#endif // RPEDICATE_H
