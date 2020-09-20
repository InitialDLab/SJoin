#include <join.h>
#include <debug.h>
#include <algorithm>

Join::Join(memmgr *mmgr)
    : m_mmgr(mmgr),
    m_schemas(mmgr),
    m_predicates(mmgr),
    m_window_non_overlapping(false) {
}


Join::~Join() {
}

SID Join::add_stream(Schema *schema) {
    RT_ASSERT(
        schema->get_ts_colid() == INVALID_COLID ||
        schema->get_type(schema->get_ts_colid()) == DTYPE_LONG);
    m_schemas.emplace_back(schema);

    return (SID)(m_schemas.size() - 1);
}

void Join::add_equi_join(
    SID lsid,
    COLID lcolid,
    SID rsid,
    COLID rcolid,
    Predicate::ForeignKeySpec fkey_spec) {

    RT_ASSERT(m_schemas[lsid]->get_type(lcolid) == m_schemas[rsid]->get_type(rcolid));
    if (fkey_spec == Predicate::LEFT_IS_FKEY) {
        RT_ASSERT(m_schemas[rsid]->get_primary_key_colid() == rcolid);
    }
    if (fkey_spec == Predicate::RIGHT_IS_FKEY) {
        RT_ASSERT(m_schemas[lsid]->get_primary_key_colid() == lcolid);
    }
    m_predicates.emplace_back(
        Predicate::EQUI,
        lsid,
        lcolid,
        rsid,
        rcolid,
        m_schemas[lsid]->get_ti(lcolid),
        nullptr,
        fkey_spec);
}

void Join::add_band_join(SID lsid, COLID lcolid, SID rsid, COLID rcolid, Datum value) {
    RT_ASSERT(m_schemas[lsid]->get_type(lcolid) == m_schemas[rsid]->get_type(rcolid));
    m_predicates.emplace_back(
        Predicate::BAND,
        lsid,
        lcolid,
        rsid,
        rcolid,
        m_schemas[lsid]->get_ti(lcolid),
        value);
}

