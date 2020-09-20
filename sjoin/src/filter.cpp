#include <filter.h>

bool Filter::match(Record *rec) {
	DATUM my_value = rec->get_column(m_colid);
	switch (m_type) {
		case EQUI:
			if (m_ti->compare(&my_value, &m_value) == 0) {
				return true;
			} else {
				return false;
			}
			break;

		case BAND:
			DATUM upper_bound;
			DATUM lower_bound;
			m_ti->add(&upper_bound, &m_operand, &m_value);
			m_ti->minus(&lower_bound, &m_operand, &m_value);
			if (m_ti->compare(&my_value, &lower_bound) != -1 && m_ti->compare(&my_value, &upper_bound) != 1) {
				//  abs(my_value - operand) <= value
				return true;
			} else {
				return false;
			}
			break;

		case NEQ:
			if (m_ti->compare(&my_value, &m_value) != 0) {
				return true;
			} else {
				return false;
			}
			break;
		default:
			RT_ASSERT(false);
			return false;							
	}
	RT_ASSERT(false);
	return false;
}

