#ifndef FILTER_H
#define FILTER_H

#include <basictypes.h>
#include <dtypes.h>
#include <datum.h>
#include <record.h>

class Filter {
public:
	enum Type {
		EQUI,
		BAND,
		NEQ
	};
	
	Filter(
		Type type,
		SID sid,
		COLID colid,
		typeinfo *ti,
		Datum operand,
		Datum value)
	: m_type(type),
	  m_sid(sid),
	  m_colid(colid),
	  m_ti(ti),
	  m_operand(),
	  m_value() {
		if (operand) {
			m_ti->copy(&m_operand, operand);
		}
		if (value) {
			m_ti->copy(&m_value, value);
		}
	}

	Type get_type() const { return m_type; }
	
	SID get_sid() const { return m_sid; }

	COLID get_colid() const { return m_colid; }
	
	typeinfo *get_ti() const { return m_ti; }

	Datum get_operand() { return &m_operand; }

	Datum get_value() { return &m_value; }
	
	bool match(Record *rec);
private:
	Type 		m_type;
	SID 		m_sid;
	COLID 		m_colid;
	typeinfo	*m_ti;
	DATUM		m_operand;
	DATUM		m_value;
};

#endif
