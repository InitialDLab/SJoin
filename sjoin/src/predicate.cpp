#include <predicate.h>

void Predicate::swap_left_right() {
    m_lsid ^= m_rsid;
    m_rsid ^= m_lsid;
    m_lsid ^= m_rsid;

    m_lcolid ^= m_rcolid;
    m_rcolid ^= m_lcolid;
    m_lcolid ^= m_rcolid;
}

// XXX we assume both sides and m_value have the same type
void Predicate::compute_range_ii(
    SID                 my_sid,
    Datum               my_value,
    Datum               other_value_low,
    Datum               other_value_high) {
    
    switch (m_type) {
    case EQUI:
        m_ti->copy(other_value_low, my_value);
        m_ti->copy(other_value_high, my_value);
        break;

    case BAND:
        m_ti->minus(other_value_low, my_value, &m_value);
        m_ti->add(other_value_high, my_value, &m_value);
        break;

    default:
        RT_ASSERT(false);
    }
}

void Predicate::compute_low_i(
    SID                 my_sid,
    Datum               my_value,
    Datum               other_value_low) {
    
    switch (m_type) {
    case EQUI:
        m_ti->copy(other_value_low, my_value);
        break;

    case BAND:
        m_ti->minus(other_value_low, my_value, &m_value);
        break;

    default:
        RT_ASSERT(false);
    }
}

void Predicate::compute_high_i(
    SID                 my_sid,
    Datum               my_value,
    Datum               other_value_high) {
    
    switch (m_type) {
    case EQUI:
        m_ti->copy(other_value_high, my_value);
        break;

    case BAND:
        m_ti->add(other_value_high, my_value, &m_value);
        break;

    default:
        RT_ASSERT(false);
    }
}

