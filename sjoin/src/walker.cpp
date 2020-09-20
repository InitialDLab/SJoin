#include <walker.h>

static void construct(
    DOUBLE *E, // probabilities
    DOUBLE *B, // temporary space
    DOUBLE *F, // cutoff  values
    UINT4 *IA, // aliases
    UINT4 N) { // size of prob. space

    DOUBLE err = 1e-10;
    
    // init IA, F, B
    for (UINT4 i = 0; i < N; ++i) {
        IA[i] = i; 
        F[i] = 0.0;
        B[i] = E[i] - 1.0 / N;
    }
    
    // find largest + and - diff and their positions in B
    for (UINT4 i = 0; i < N; ++i) {
        // test if the sum of diff has become significant
        DOUBLE sum = 0.0;
        for (UINT4 m = 0; m < N; ++m) {
            sum += fabs(B[m]);
        }
        if (sum < err) {
            break;
        }

        DOUBLE C = 0.0;
        DOUBLE D = 0.0;
        UINT4 k, l;
        for (UINT4 j = 0; j < N; ++j) {
            if (B[j] < C) {
                C = B[j];
                k = j;
            }
            else if (B[j] > D) {
                D = B[j];
                l = j;
            }
        }

#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
        IA[k] = l;
        F[k] = 1.0 + C * N;
        B[k] = 0.0;
        B[l] = C + D;
#pragma GCC diagnostic warning "-Wmaybe-uninitialized"
    }
}

WalkerParam *walker_construct(memmgr *mmgr, DOUBLE *prob, UINT4 N) {
    WalkerParam *p = (WalkerParam *) mmgr->allocate(sizeof(WalkerParam));
    
    p->mmgr = mmgr;
    p->N = N;
    p->IA = mmgr->allocate_array<UINT4>(N);
    p->F = mmgr->allocate_array<DOUBLE>(N);
    DOUBLE *B = mmgr->allocate_array<DOUBLE>(N);

    construct(prob, B, p->F, p->IA, N);
    mmgr->deallocate(B);

    new (&p->unif_a) std::uniform_int_distribution<INT4>(0, N-1);
    new (&p->unif_b) std::uniform_real_distribution<DOUBLE>(0.0, 1.0);

    return p;
}

// UA, UB: r.v. uniformly distributed over (0,1)
// IA: alias
// F: cutoff values
// N: number of values
//static UINT4 sample(
//    DOUBLE UA,
//    DOUBLE UB, 
//    const UINT4 *IA,
//    const DOUBLE *F,
//    UINT4 N) {
    
//    UINT4 IX = (int)(UA * N);
//    if (UB > F[IX]) IX = IA[IX];
//    return IX;
//}

