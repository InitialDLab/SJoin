#ifndef DEBUG_H
#define DEBUG_H

#include <cstdio>

extern void my_abort();

#define CONCAT_HELPER(_1, _2) _1 ## _2 
#define CONCAT(_1, _2) CONCAT_HELPER(_1, _2)

#define CONCAT3_HELPER(_1, _2, _3) _1 ## _2 ## _3
#define CONCAT3(_1, _2, _3) CONCAT3_HELPER(_1, _2, _3)

#define STRINGIFY_HELPER(_1) #_1
#define STRINGIFY(_1) STRINGIFY_HELPER(_1)

// the garbage argument is required until c++20 and for C until the latest
#define GET_FIRST___VA_ARGS__(...) GET_FIRST___VA_ARGS___HELPER(__VA_ARGS__, garbage)
#define GET_FIRST___VA_ARGS___HELPER(first, ...) first

#define GET_SECOND___VA_ARGS__(...) GET_SECOND___VA_ARGS___HELPER(__VA_ARGS__, garbage, garbage)
#define GET_SECOND___VA_ARGS___HELPER(first, second, ...) second

#define NEGATE_OF(arg) CONCAT(NEGATE_OF_, arg)
#define NEGATE_OF_yes no
#define NEGATE_OF_no yes

#define HAS_ONLY_ONE___VA_ARGS__(...) \
    HAS_ONLY_ONE___VA_ARGS___HELPER(__VA_ARGS__,\
        no, no, no, no, no, no, no, no, no, no, \
        no, no, no, no, no, no, no, no, no, no, \
        no, no, no, no, no, no, no, no, no, no, \
        no, yes, garbage)
#define HAS_ONLY_ONE___VA_ARGS___HELPER(\
   a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, \
   a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, \
   a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, \
   a31, a32, a33, ...) a33

#define HAS_AT_LEAST_TWO___VA_ARGS__(...) \
        NEGATE_OF(HAS_ONLY_ONE___VA_ARGS__(__VA_ARGS__))

#define HAS_AT_MOST_TWO___VA_ARGS__(...) \
    HAS_AT_MOST_TWO___VA_ARGS___HELPER(__VA_ARGS__,\
        no, no, no, no, no, no, no, no, no, no, \
        no, no, no, no, no, no, no, no, no, no, \
        no, no, no, no, no, no, no, no, no, no, \
        yes, yes, garbage)
#define HAS_AT_MOST_TWO___VA_ARGS___HELPER(\
   a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, \
   a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, \
   a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, \
   a31, a32, a33, ...) a33

#define TAKE_REST_OF___VA_ARGS___EXCLUDING_FIRST_TWO(...) \
    CONCAT(TAKE_REST_OF___VA_ARGS___EXCLUDING_FIRST_TWO_HAS_AT_MOST_TWO_,\
        HAS_AT_MOST_TWO___VA_ARGS__(__VA_ARGS__))(__VA_ARGS__)

#define TAKE_REST_OF___VA_ARGS___EXCLUDING_FIRST_TWO_HAS_AT_MOST_TWO_yes(...)

#define TAKE_REST_OF___VA_ARGS___EXCLUDING_FIRST_TWO_HAS_AT_MOST_TWO_no(first, second, ...) , __VA_ARGS__

#ifdef NDEBUG
#define RT_ASSERT(...)


#define DBGONLY_ARG0(...)
#define DBGONLY_ARG(...)

#else

#define RT_ASSERT RT_ASSERT_EX

#define RT_ASSERT_EX(...) \
    do { \
        if (!(GET_FIRST___VA_ARGS__(__VA_ARGS__))) { \
            fprintf(stderr, \
                RT_ASSERT_EX_FMT_STRING(__VA_ARGS__), \
                __FILE__, __LINE__ \
                TAKE_REST_OF___VA_ARGS___EXCLUDING_FIRST_TWO(__VA_ARGS__)); \
            my_abort(); \
        } \
    } while(0)

#define RT_ASSERT_EX_FMT_STRING(...) \
    "%s:%d: " RT_ASSERT_EX_FMT_STRING_HELPER(\
    HAS_AT_LEAST_TWO___VA_ARGS__(__VA_ARGS__), __VA_ARGS__) "\n"

#define RT_ASSERT_EX_FMT_STRING_HELPER(have_two, ...) \
    CONCAT(RT_ASSERT_EX_FMT_STRING_HAVE_USER_SPECIFIED_,have_two)(__VA_ARGS__)

#define RT_ASSERT_EX_FMT_STRING_HAVE_USER_SPECIFIED_no(...) \
    "assertion \"" STRINGIFY(GET_FIRST___VA_ARGS__(__VA_ARGS__)) "\" failed"

#define RT_ASSERT_EX_FMT_STRING_HAVE_USER_SPECIFIED_yes(...) \
    GET_SECOND___VA_ARGS__(__VA_ARGS__)


#define DBGONLY_ARG0(...) __VA_ARGS__
#define DBGONLY_ARG(...) , __VA_ARGS__

#endif // NDEBUG

#define DARG0(...) DBGONLY_ARG0(__VA_ARGS__)
#define DARG(...) DBGONLY_ARG(__VA_ARGS__)

#endif // DEBUG_H

