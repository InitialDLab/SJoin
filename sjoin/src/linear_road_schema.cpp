#include <config.h>
#include <linear_road.h>
#include <memmgr.h>
#include <dtypes.h>

static Schema *sch_linear_road = nullptr;

Schema *get_linear_road_schema() {
    if (!sch_linear_road) {
        const char *names[] = {
            "type",
            "time",
            "carid",
            "speed",
            "xway",
            "lane",
            "dir",
            "seg",
            "pos",
            "qid",
            "sinit",
            "send",
            "dow",
            "tod",
            "day"
        };
        typeinfo *ti[] = {
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_LONG),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT),
            get_fixedlen_ti(DTYPE_INT)
        };
        sch_linear_road = g_tracked_mmgr->newobj<Schema>(
            g_tracked_mmgr,
            15,
            ti,
            names,
            1 /* ts_colid */
        );
    }

    return sch_linear_road;
}

