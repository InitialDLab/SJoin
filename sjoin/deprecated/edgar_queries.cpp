#include <config.h>
#include <edgar_queries.h>

static Schema *edgar_schema = nullptr;

Schema *get_edgar_schema() {
    if (edgar_schema) return edgar_schema;
    
    const char *names[] = {
        "ts", // ingestion timestamp
        "ip",
        "ip/24",
        "original_ts",
        "date",
        "time",
        "zone",
        "cik",
        "accession",
        "doc",
        "code",
        "size",
        "idx",
        "norefer",
        "noagent",
        "find",
        "crawler",
        "browser"
    };
    typeinfo *ti[] = {
        get_fixedlen_ti(DTYPE_LONG),
        get_fixedlen_ti(DTYPE_LONG),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_LONG),
        get_fixedlen_ti(DTYPE_DATE),
        g_tracked_mmgr->newobj<typeinfo_varchar>(8),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_DOUBLE),
        g_tracked_mmgr->newobj<typeinfo_varchar>(20),
        g_tracked_mmgr->newobj<typeinfo_varchar>(100),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_LONG),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        get_fixedlen_ti(DTYPE_INT),
        g_tracked_mmgr->newobj<typeinfo_varchar>(3)
    };
    edgar_schema = g_tracked_mmgr->newobj<Schema>(
        g_tracked_mmgr,
        18,
        ti,
        names,
        0 /* ts_colid */);

    return edgar_schema;
}

