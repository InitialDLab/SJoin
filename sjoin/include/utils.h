#include <config.h>
#include <basictypes.h>
#include <memmgr.h>
#include <csignal>

char *mmgr_strdup(memmgr *mmgr, const char *s);

#define LogTable256_LT(n) ,n,n,n,n, n,n,n,n, n,n,n,n, n,n,n,n
constexpr const UINT4 LogTable256[256] = {
// log2(0) := 0
    (UINT4)-1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3 // 0..15
    LogTable256_LT(4) // 16..31
    LogTable256_LT(5) // 32..47
    LogTable256_LT(5) // 48..63
    LogTable256_LT(6) // 64..79
    LogTable256_LT(6) // 80..95
    LogTable256_LT(6) // 96..111
    LogTable256_LT(6) // 112..127
    LogTable256_LT(7) // 128..143
    LogTable256_LT(7) // 144..159
    LogTable256_LT(7) // 160..175
    LogTable256_LT(7) // 176..191
    LogTable256_LT(7) // 192..207
    LogTable256_LT(7) // 208..223
    LogTable256_LT(7) // 224..239
    LogTable256_LT(7) // 240..255
};

// see https://graphics.stanford.edu/~seander/bithacks.html#IntegerLogLookup
// extended to 64-bit integers
// floor(log_2 v) for v >= 1
// max_uint4 for v == 0
//
inline constexpr UINT4 log2(UINT8 v) {
    unsigned t = v >> 32;
    if (t) {
        if ((v = t >> 16)) {
            if ((t = v >> 8)) {
                return 56 + LogTable256[t]; 
            } else {
                return 48 + LogTable256[v];
            }
        } else {
            if ((v = t >> 8)) {
                return 40 + LogTable256[v];
            } else {
                return 32 + LogTable256[t];
            }
        }
    } else {
        if ((t = v >> 16)) {
            if ((v = t >> 8)) {
                return 24 + LogTable256[v];
            } else {
                return 16 + LogTable256[t];
            }
        } else {
            if ((t = v >> 8)) {
                return 8 + LogTable256[t];
            } else {
                return LogTable256[v];
            }
        }
    }
}

// ceil(log_2 v)
inline constexpr UINT4 ceil_log2(UINT8 v) {
    return 1 + log2(v-1);
}

UINT8 get_current_vmpeak();


extern volatile std::sig_atomic_t g_terminated;

void setup_signal_handlers();

void sigterm_handler(int signum);



#ifdef __GNUC__
// We have been using GCC only.
#define intentional_fallthrough __attribute__((fallthrough))
#else
// TODO other compilers?
#define intentional_fallthrough
#endif

