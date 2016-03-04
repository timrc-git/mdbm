
#ifdef  SGI
#ifdef  __STDC__
        #pragma weak mdbm_hash = _mdbm_hash0
        #pragma weak mdbm_hash0 = _mdbm_hash0
        #pragma weak mdbm_hash1 = _mdbm_hash1
        #pragma weak mdbm_hash2 = _mdbm_hash2
        #pragma weak mdbm_hash3 = _mdbm_hash3
        #pragma weak mdbm_hash4 = _mdbm_hash4
        #pragma weak mdbm_hash5 = _mdbm_hash5
#endif
#endif

#ifdef  SGI
#include "synonyms.h"
#endif

#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "mdbm.h"

/* #define  WHATSTR(X)      static const char what[] = X
   WHATSTR("@(#)hash.c 1.4"); 
 */

static uint32_t  crc32_table[256] = {
    0x0,        0x4c11db7,  0x9823b6e,  0xd4326d9,
    0x130476dc, 0x17c56b6b, 0x1a864db2, 0x1e475005,
    0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
    0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
    0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9,
    0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
    0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011,
    0x791d4014, 0x7ddc5da3, 0x709f7b7a, 0x745e66cd,
    0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
    0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
    0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81,
    0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
    0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49,
    0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
    0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
    0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d,
    0x34867077, 0x30476dc0, 0x3d044b19, 0x39c556ae,
    0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
    0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16,
    0x18aeb13,  0x54bf6a4,  0x808d07d,  0xcc9cdca,
    0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde,
    0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02,
    0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1, 0x53dc6066,
    0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
    0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e,
    0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692,
    0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6,
    0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a,
    0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e,
    0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
    0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686,
    0xd5b88683, 0xd1799b34, 0xdc3abded, 0xd8fba05a,
    0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637,
    0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
    0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f,
    0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
    0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47,
    0x36194d42, 0x32d850f5, 0x3f9b762c, 0x3b5a6b9b,
    0x315d626,  0x7d4cb91,  0xa97ed48,  0xe56f0ff,
    0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
    0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7,
    0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
    0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f,
    0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
    0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
    0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b,
    0x9b3660c6, 0x9ff77d71, 0x92b45ba8, 0x9675461f,
    0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
    0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640,
    0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
    0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8,
    0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24,
    0x119b4be9, 0x155a565e, 0x18197087, 0x1cd86d30,
    0x29f3d35,  0x65e2082,  0xb1d065b,  0xfdc1bec,
    0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088,
    0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654,
    0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0,
    0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c,
    0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18,
    0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
    0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0,
    0x9abc8bd5, 0x9e7d9662, 0x933eb0bb, 0x97ffad0c,
    0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668,
    0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4,
};

mdbm_ubig_t
mdbm_hash0(uint8_t *buf, int len)
{
    uint8_t *p, *ep;
    uint32_t        crc = ~0;

    p = buf; ep = p + len;
    while (p < ep)
        crc = (crc << 8) ^ crc32_table[(crc >> 24) ^ *p++];
    return (mdbm_ubig_t) crc;
}

/*
** This came from ejb's hsearch.
*/
#define PRIME1          37
#define PRIME2          1048583
mdbm_ubig_t
mdbm_hash1(uint8_t *buf, int len)
{
    uint8_t *key;
    mdbm_ubig_t h;

    for (key = buf, h = 0; len--; ) {
        h = h * PRIME1 ^ (*key++ - ' ');
    }
    h %= PRIME2;

    return h;
}

/*
** Phong's linear congruential hash
*/
#define dcharhash(h, c) ((h) = 0x63c63cd9*(h) + 0x9c39c33d + (c))
mdbm_ubig_t
mdbm_hash2(uint8_t *buf, int len)
{
    uint8_t *e, *key;
    mdbm_ubig_t h;
    uint8_t c;

    key = buf;
    e = key + len;
    for (h = 0; key != e; ) {
        c = *key++;
        if (!c && key > e)
            break;
        dcharhash(h, c);
    }

    return h;
}

/*
** OZ's original sdbm hash
*/
#define HASHC   h = *key++ + 65599 * h
mdbm_ubig_t
mdbm_hash3(uint8_t *buf, int len)
{
    uint8_t *key;
    int loop;
    mdbm_ubig_t h;

    h = 0;
    key = buf;
    if (len > 0) {
        loop = (len + 8 - 1) >> 3;

        switch (len & (8 - 1)) {
            case 0:
              do {
                HASHC;
            case 7:
                HASHC;
            case 6:
                HASHC;
            case 5:
                HASHC;
            case 4:
                HASHC;
            case 3:
                HASHC;
            case 2:
                HASHC;
            case 1:
                HASHC;
              } while (--loop);
        }
    }

    return h;
}

/*
** Hash function from Chris Torek.
*/
#define HASH4a   h = (h << 5) - h + *key++;
#define HASH4b   h = (h << 5) + h + *key++;
#define HASH4 HASH4b
mdbm_ubig_t
mdbm_hash4(uint8_t *buf, int len)
{
    uint8_t *key;
    int loop;
    mdbm_ubig_t h;

    h = 0;
    key = buf;
    if (len > 0) {
        loop = (len + 8 - 1) >> 3;

        switch (len & (8 - 1)) {
            case 0:
                do {
                        HASH4
            case 7:
                        HASH4
            case 6:
                        HASH4
            case 5:
                        HASH4
            case 4:
                        HASH4
            case 3:
                        HASH4
            case 2:
                        HASH4
            case 1:
                        HASH4
                } while (--loop);
        }
    }
    return h;
}

/*
 * Fowler/Noll/Vo hash
 * Public Domain, Originally from chongo (Landon Noll)
 *   http://www.isthe.com/chongo/tech/comp/fnv/index.html
 *
 * The magic is in the interesting relationship between the special
 * prime FNV_PRIME and the two numbers 2^N 2^8 (N is either 32 or 64)
 * e.g. in the 32-bit case FNV_PRIME = 16777619 (2^24 + 403)
 *
 * Based on ariel's tests on several input sets (words, URLs, integers),
 * this hash runs about 10% faster and performs better (less collisions)
 * than CRC-32
 */

/*
 * 32/64 bit magic FNV primes
 * The main secret of the algorithm is in these numbers.
 */
#define FNV_32_PRIME ((u_int32_t) 0x01000193UL)
#define FNV_64_PRIME ((u_int64_t) 0x100000001b3ULL)
 
/*
 * The init value is quite arbitrary, but these seem to perform
 * well on both web2 and sequential integers represented as strings.
 */
#define FNV1_32_INIT ((u_int32_t) 33554467UL)
#define FNV1_64_INIT ((u_int64_t) 0xcbf29ce484222325ULL)                        

#ifdef  SUPPORT_64BIT
#  define FNV_PRIME FNV_64_PRIME
#  define FNV_INIT FNV1_64_INIT
#else
#  define FNV_PRIME FNV_32_PRIME
#  define FNV_INIT  FNV1_32_INIT
#endif

mdbm_ubig_t
mdbm_hash5(uint8_t *buf, int len)
{
    mdbm_ubig_t     val;                    /* current hash value */
    uint8_t   *buf_end = buf + len;

    for (val = FNV_INIT; buf < buf_end; ++buf) {
        val *= FNV_PRIME;
        val ^= *buf;
    }
    return val;
}


mdbm_ubig_t
mdbm_hash6(uint8_t *buf, int len)
{
    unsigned long h = 0;
    uint8_t* end = buf + len;
    for ( ; buf < end; ++buf)
        h = 5*h + *buf;
    return h;
}

#ifndef PROVIDE_SSL_HASHES
/* Allow use of OpenSSL version of MD5. */
#  ifdef USE_OPENSSL
#    include <openssl/md5.h>
#  else /* USE_OPENSSL */
#    include <md5.h>
#  endif /* USE_OPENSSL */
#else /* PROVIDE_SSL_HASHES */
#  include "hash-ssl.c"
#endif /* PROVIDE_SSL_HASHES */


mdbm_ubig_t
mdbm_hash7(uint8_t *buf, int len)
{
    mdbm_ubig_t rv = 0;
    MD5_CTX c;
    uint8_t digest[16];

    MD5Init(&c);
    MD5Update(&c,buf,len);
    MD5Final(digest,&c);
    memcpy(&rv, digest, sizeof(mdbm_ubig_t));
    return rv;
}

/* Allow use of OpenSSL version of SHA. */
#ifndef PROVIDE_SSL_HASHES
#  ifdef USE_OPENSSL
#    include <openssl/sha.h>
#  else /* USE_OPENSSL */
#    include <sha.h>
#  endif /* USE_OPENSSL */
#endif /* PROVIDE_SSL_HASHES */

mdbm_ubig_t
mdbm_hash8(uint8_t *buf, int len)
{
    mdbm_ubig_t rv = 0;
    SHA_CTX c;
    uint8_t digest[20];

    SHA1_Init(&c);
    SHA1_Update(&c,buf,len);
    SHA1_Final(digest,&c);
    memcpy(&rv, digest, sizeof(mdbm_ubig_t));
    return rv;
}



typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bits set, and the deltas of all three
  high bits or all three low bits, whether the original value of a,b,c
  is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
  have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
  2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a 
  structure that could supported 2x parallelism, like so:
      a -= b; 
      a -= c; x = (c>>13);
      b -= c; a ^= x;
      b -= a; x = (a<<8);
      c -= a; b ^= x;
      c -= b; x = (b>>13);
      ...
  Unfortunately, superscalar Pentiums and Sparcs can't take advantage 
  of that parallelism.  They've also turned some of those single-cycle
  latency instructions into multi-cycle latency instructions.  Still,
  this is the fastest good hash I could find.  There were about 2^^68
  to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/
#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  len     : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 6*len+35 instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

mdbm_ubig_t jenkins_hash (uint8_t* k, int length)
{
   register ub4 a,b,c,len;
   ub4 initval = 0xaec49ac0;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += (k[0] +((ub4)k[1]<<8) +((ub4)k[2]<<16) +((ub4)k[3]<<24));
      b += (k[4] +((ub4)k[5]<<8) +((ub4)k[6]<<16) +((ub4)k[7]<<24));
      c += (k[8] +((ub4)k[9]<<8) +((ub4)k[10]<<16)+((ub4)k[11]<<24));
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=((ub4)k[10]<<24);
   case 10: c+=((ub4)k[9]<<16);
   case 9 : c+=((ub4)k[8]<<8);
      /* the first byte of c is reserved for the length */
   case 8 : b+=((ub4)k[7]<<24);
   case 7 : b+=((ub4)k[6]<<16);
   case 6 : b+=((ub4)k[5]<<8);
   case 5 : b+=k[4];
   case 4 : a+=((ub4)k[3]<<24);
   case 3 : a+=((ub4)k[2]<<16);
   case 2 : a+=((ub4)k[1]<<8);
   case 1 : a+=k[0];
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
}

/*
 * SuperFastHash by Paul Hseih:
 *   http://www.azillionmonkeys.com/qed/hash.html
 *
 * Licensed under BSD according to the following clause from:
 *   http://www.azillionmonkeys.com/qed/weblicense.html
 * "If your code is compatible with the old style BSD license and you wish 
 *  to avoid the burden of explicitely protecting code you obtained from 
 *  here from misrepresentation then you can simply cover it with 
 *  the old-style BSD license."
 */

#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
  || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#endif

#if !defined (get16bits)
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

uint32_t SuperFastHash (const char * data, int len) {
    uint32_t hash = len, tmp;
    int rem;

    if (len <= 0 || data == NULL) return 0;

    rem = len & 3;
    len >>= 2;

    /* Main loop */
    for (;len > 0; len--) {
        hash  += get16bits (data);
        tmp    = (get16bits (data+2) << 11) ^ hash;
        hash   = (hash << 16) ^ tmp;
        data  += 2*sizeof (uint16_t);
        hash  += hash >> 11;
    }

    /* Handle end cases */
    switch (rem) {
        case 3: hash += get16bits (data);
                hash ^= hash << 16;
                hash ^= data[sizeof (uint16_t)] << 18;
                hash += hash >> 11;
                break;
        case 2: hash += get16bits (data);
                hash ^= hash << 11;
                hash += hash >> 17;
                break;
        case 1: hash += *data;
                hash ^= hash << 10;
                hash += hash >> 1;
    }

    /* Force "avalanching" of final 127 bits */
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;

    return hash;
}


/* table to translate between from hash number to hash function pointer */
mdbm_hash_t mdbm_hash_funcs[] = {
    (mdbm_hash_t)mdbm_hash0,            /* 0 */
    (mdbm_hash_t)mdbm_hash1,            /* 1 */
    (mdbm_hash_t)mdbm_hash2,            /* 2 */
    (mdbm_hash_t)mdbm_hash3,            /* 3 */
    (mdbm_hash_t)mdbm_hash4,            /* 4 */
    (mdbm_hash_t)mdbm_hash5,            /* 5 */
    (mdbm_hash_t)mdbm_hash6,
    (mdbm_hash_t)mdbm_hash7,
    (mdbm_hash_t)mdbm_hash8,
    (mdbm_hash_t)jenkins_hash,
    (mdbm_hash_t)SuperFastHash
};

