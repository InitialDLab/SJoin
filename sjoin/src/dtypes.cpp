#include <dtypes.h>
#include <memmgr.h>

namespace dtype_impl {
    typeinfo *fixed_len_ti[NUM_DTYPES];
}

void init_typeinfos() {
    /*for (int i = 0; i < (int) NUM_DTYPES; ++i) {
        dtype type = (dtype) i;
        if (!dtype_impl::dtype_is_varlen[type]) {
            dtype_impl::fixed_len_ti[i] = g_tracked_mmgr->newobj<typeinfo>(type);
        } else dtype_impl::fixed_len_ti[i] = nullptr;
    } */
    typeinfo **ti = dtype_impl::fixed_len_ti;
    ti[DTYPE_INT] = g_tracked_mmgr->newobj<typeinfo_int>();
    ti[DTYPE_LONG] = g_tracked_mmgr->newobj<typeinfo_long>();
    ti[DTYPE_VARCHAR] = nullptr;
    ti[DTYPE_DATE] = g_tracked_mmgr->newobj<typeinfo_date>();
    ti[DTYPE_DOUBLE] = g_tracked_mmgr->newobj<typeinfo_double>();
}

