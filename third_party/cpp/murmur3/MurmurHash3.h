//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_

namespace voltdb {

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER)

typedef unsigned char uint8_t;
typedef unsigned long uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else	// defined(_MSC_VER)

#include <stdint.h>

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------

void MurmurHash3_x64_128 ( const void * key, int len, uint32_t seed, void * out );

inline int64_t MurmurHash3_x64_128 ( int64_t value, uint32_t seed) {
    int64_t out[2];
    MurmurHash3_x64_128( &value, 8, seed, out);
    return out[0];
}

inline int64_t MurmurHash3_x64_128 ( int64_t value) {
    return MurmurHash3_x64_128(value, 0);
}

//-----------------------------------------------------------------------------

}
#endif // _MURMURHASH3_H_
