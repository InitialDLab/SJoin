#ifndef WALKER_H
#define WALKER_H

#include <config.h>
#include <basictypes.h>
#include <memmgr.h>
#include <random>

struct WalkerParam {
    ~WalkerParam() {
        mmgr->deallocate(IA);
        mmgr->deallocate(F);

        (&unif_a)->~uniform_int_distribution();
        (&unif_b)->~uniform_real_distribution();
    }

    memmgr *mmgr;

    UINT4   N; 
    UINT4   *IA;
    DOUBLE  *F;

    std::uniform_int_distribution<UINT4> unif_a;
    std::uniform_real_distribution<DOUBLE> unif_b;
};

WalkerParam *walker_construct(memmgr *mmgr, DOUBLE *prob, UINT4 N);

template<typename RNG>
inline UINT4 walker_sample(WalkerParam* param, RNG &rng) {
    
    UINT4 IX = param->unif_a(rng);
    if (param->unif_b(rng) >= param->F[IX]) {
        return param->IA[IX];
    }
    return IX;
}

#endif
