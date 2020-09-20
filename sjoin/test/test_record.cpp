#include <cstdio>
#include <record.h>
#include <iostream>
using namespace std;

void test_schema1(memmgr *mmgr) {
    cout << "test_schema1()" << endl;
    typeinfo *ti[] =
        {
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_LONG),
            g_tracked_mmgr->newobj<typeinfo_varchar>(100),
            get_fixedlen_ti(DTYPE_DATE),
            get_fixedlen_ti(DTYPE_DOUBLE)
        };
    Schema *schema = mmgr->newobj<Schema>(
        mmgr,
        5,
        INVALID_COLID,
        ti,
        nullptr);

    PAGE_OFFSET maxlen = schema->get_max_len();
    RT_ASSERT(maxlen == 128);
    
    BYTE *buf = mmgr->allocate_array<BYTE>(maxlen);
    Record rec;
    rec.set_schema(schema);
    rec.set_buf(buf, maxlen); 
    
    for (const char * line: {
        "51|20309037|blah blah|2012-09-07|4.03",
        "51|20379038|abcdef ghijk|2012-09-08|4.3"
        }) {
        if (!rec.parse_line(line, '|')) {
            RT_ASSERT(false, "parsing line");
        }

        for (COLID colid = 0; colid < schema->get_num_cols(); ++colid) {
            std::string str = rec.to_string(colid);
            cout << str;
            if (colid == schema->get_num_cols() - 1) {
                cout << endl;
            } else {
                cout << '|';
            }
        }
        cout << line << endl;
        cout << endl;
    }

    mmgr->deallocate(buf);
}

void test_schema2(memmgr *mmgr) {
    cout << "test_schema2()" << endl;
    typeinfo *ti[] = 
        {
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_DATE),
            get_fixedlen_ti(DTYPE_DOUBLE)
        };
    Schema *schema = mmgr->newobj<Schema>(
        mmgr,
        4,
        INVALID_COLID,
        ti,
        nullptr);

    PAGE_OFFSET maxlen = schema->get_max_len();
    RT_ASSERT(maxlen == 24);
    
    BYTE *buf = mmgr->allocate_array<BYTE>(maxlen);
    Record rec;
    rec.set_schema(schema);
    rec.set_buf(buf, maxlen); 
    
    for (const char * line: {
        "51|20309037|2012-09-07|4.03",
        "51|20379038|2012-09-08|4.3"
        }) {
        if (!rec.parse_line(line, '|')) {
            RT_ASSERT(false, "parsing line");
        }

        for (COLID colid = 0; colid < schema->get_num_cols(); ++colid) {
            std::string str = rec.to_string(colid);
            cout << str;
            if (colid == schema->get_num_cols() - 1) {
                cout << endl;
            } else {
                cout << '|';
            }
        }
        cout << line << endl;
        cout << endl;
    }
}

void test_schema3(memmgr *mmgr) {
    cout << "test_schema3()" << endl;
    typeinfo *ti[] = 
        {
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_LONG),
            g_tracked_mmgr->newobj<typeinfo_varchar>(100),
            g_tracked_mmgr->newobj<typeinfo_varchar>(5),
            get_fixedlen_ti(DTYPE_DATE),
            get_fixedlen_ti(DTYPE_DOUBLE),
            g_tracked_mmgr->newobj<typeinfo_varchar>(2)
        };
    Schema *schema = mmgr->newobj<Schema>(
        mmgr,
        7,
        INVALID_COLID,
        ti,
        nullptr);

    PAGE_OFFSET maxlen = schema->get_max_len();
    RT_ASSERT(maxlen == 143);
    
    BYTE *buf = mmgr->allocate_array<BYTE>(maxlen);
    Record rec;
    rec.set_schema(schema);
    rec.set_buf(buf, maxlen); 
    
    for (const char * line: {
        "51|20309037|blah blah|aaaaa|2012-09-07|4.03|ab_to_be_truncated",
        "51|20379038|abcdef ghijk|bbbbb|2012-09-08|4.3|cd_to_be_truncated"
        }) {
        if (!rec.parse_line(line, '|')) {
            RT_ASSERT(false, "parsing line");
        }

        for (COLID colid = 0; colid < schema->get_num_cols(); ++colid) {
            std::string str = rec.to_string(colid);
            cout << str;
            if (colid == schema->get_num_cols() - 1) {
                cout << endl;
            } else {
                cout << '|';
            }
        }
        cout << line << endl;
        cout << endl;
    }

    mmgr->deallocate(buf);
}

void test_record() {
    memmgr *mmgr = create_memmgr<basic_tracked_memmgr>(g_root_mmgr);
        
    test_schema1(mmgr);

    test_schema2(mmgr);
    
    test_schema3(mmgr);

    destroy_memmgr(mmgr);
}
