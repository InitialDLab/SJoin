#include <utils.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>

using namespace std;

char *mmgr_strdup(memmgr *mmgr, const char *s) {
    auto len = strlen(s);
    char *copy = mmgr->allocate_array<char>(len + 1);
    strcpy(copy, s);
    return copy;
}

UINT8 get_current_vmpeak() {
    FILE *file = fopen("/proc/self/status", "r");
    char line[128];

    while (fgets(line, 128, file) != NULL) {
        if (!strncmp(line, "VmPeak:", 7)) {
            return strtoull(line + 7, nullptr, 0);
        }
    }

    return 0;
}

volatile std::sig_atomic_t g_terminated = 0;

void setup_signal_handlers() {
    std::signal(SIGTERM, sigterm_handler);
}

void sigterm_handler(int handle) {
    g_terminated = 1;
}

