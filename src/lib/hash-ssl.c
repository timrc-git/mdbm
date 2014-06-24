/* MD5 Hash algorithm. Algorithm due to Ron Rivest.               */
/* Written by Colin Plumb in 1993, and released to public domain. */

/* set byte order, NO-OP for Intel/little-endian                  */
/* NOTE: for big-endian, you need to reverse each set of 4 bytes  */
#define orderW32Bytes(buf, len)
/*
void orderW32Bytes(uint8_t *buf, uint32_t nw32) {
    uint32_t t;
    do {
        t = (uint32_t)((unsigned)buf[3] << 8 | buf[2]) << 16 | ((unsigned) buf[1] << 8 | buf[0]);
        *(uint32_t *) buf = t;
        buf += 4;
    } while (--nw32);
}
*/

/* The four core functions - F1 is optimized somewhat */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))
/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) \
               (w += f(x, y, z) + data, w = w<<s | w>>(32-s), w += x)

#define MD5_DIGEST_SIZE 16 /* DON'T CHANGE THIS */
typedef struct MD5_CTX {
  uint32_t state[4];  /* state (ABCD) */
  uint32_t bits[2];   /* number of bits, modulo 2^64 (lsb first) */
  uint8_t in[64];     /* input buffer */
  /* uint8_t digest[MD5_DIGEST_SIZE]; // generated cryptographic hash */
  int finished;
} MD5_CTX;

static const uint32_t CK_MD5_INIT_STATE[4] = { 0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476 };

void MD5Init(MD5_CTX* ctx) {
  ctx->state[0] = 0x67452301;
  ctx->state[1] = 0xefcdab89;
  ctx->state[2] = 0x98badcfe;
  ctx->state[3] = 0x10325476;
  ctx->bits[0] = 0;
  ctx->bits[1] = 0;
  ctx->finished = 0;
  memset(ctx->in, 0, sizeof(ctx->in)); 
}
void MD5Transform(MD5_CTX* ctx) {
  uint32_t *state = ctx->state;
  uint32_t *in = (uint32_t*)ctx->in; /* pretend it's 32-bit ints */
  uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
  MD5STEP(F1, a, b, c, d, in[ 0] + 0xd76aa478, 7);
  MD5STEP(F1, d, a, b, c, in[ 1] + 0xe8c7b756, 12);
  MD5STEP(F1, c, d, a, b, in[ 2] + 0x242070db, 17);
  MD5STEP(F1, b, c, d, a, in[ 3] + 0xc1bdceee, 22);
  MD5STEP(F1, a, b, c, d, in[ 4] + 0xf57c0faf, 7);
  MD5STEP(F1, d, a, b, c, in[ 5] + 0x4787c62a, 12);
  MD5STEP(F1, c, d, a, b, in[ 6] + 0xa8304613, 17);
  MD5STEP(F1, b, c, d, a, in[ 7] + 0xfd469501, 22);
  MD5STEP(F1, a, b, c, d, in[ 8] + 0x698098d8, 7);
  MD5STEP(F1, d, a, b, c, in[ 9] + 0x8b44f7af, 12);
  MD5STEP(F1, c, d, a, b, in[10] + 0xffff5bb1, 17);
  MD5STEP(F1, b, c, d, a, in[11] + 0x895cd7be, 22);
  MD5STEP(F1, a, b, c, d, in[12] + 0x6b901122, 7);
  MD5STEP(F1, d, a, b, c, in[13] + 0xfd987193, 12);
  MD5STEP(F1, c, d, a, b, in[14] + 0xa679438e, 17);
  MD5STEP(F1, b, c, d, a, in[15] + 0x49b40821, 22);

  MD5STEP(F2, a, b, c, d, in[ 1] + 0xf61e2562, 5);
  MD5STEP(F2, d, a, b, c, in[ 6] + 0xc040b340, 9);
  MD5STEP(F2, c, d, a, b, in[11] + 0x265e5a51, 14);
  MD5STEP(F2, b, c, d, a, in[ 0] + 0xe9b6c7aa, 20);
  MD5STEP(F2, a, b, c, d, in[ 5] + 0xd62f105d, 5);
  MD5STEP(F2, d, a, b, c, in[10] + 0x02441453, 9);
  MD5STEP(F2, c, d, a, b, in[15] + 0xd8a1e681, 14);
  MD5STEP(F2, b, c, d, a, in[ 4] + 0xe7d3fbc8, 20);
  MD5STEP(F2, a, b, c, d, in[ 9] + 0x21e1cde6, 5);
  MD5STEP(F2, d, a, b, c, in[14] + 0xc33707d6, 9);
  MD5STEP(F2, c, d, a, b, in[ 3] + 0xf4d50d87, 14);
  MD5STEP(F2, b, c, d, a, in[ 8] + 0x455a14ed, 20);
  MD5STEP(F2, a, b, c, d, in[13] + 0xa9e3e905, 5);
  MD5STEP(F2, d, a, b, c, in[ 2] + 0xfcefa3f8, 9);
  MD5STEP(F2, c, d, a, b, in[ 7] + 0x676f02d9, 14);
  MD5STEP(F2, b, c, d, a, in[12] + 0x8d2a4c8a, 20);

  MD5STEP(F3, a, b, c, d, in[ 5] + 0xfffa3942, 4);
  MD5STEP(F3, d, a, b, c, in[ 8] + 0x8771f681, 11);
  MD5STEP(F3, c, d, a, b, in[11] + 0x6d9d6122, 16);
  MD5STEP(F3, b, c, d, a, in[14] + 0xfde5380c, 23);
  MD5STEP(F3, a, b, c, d, in[ 1] + 0xa4beea44, 4);
  MD5STEP(F3, d, a, b, c, in[ 4] + 0x4bdecfa9, 11);
  MD5STEP(F3, c, d, a, b, in[ 7] + 0xf6bb4b60, 16);
  MD5STEP(F3, b, c, d, a, in[10] + 0xbebfbc70, 23);
  MD5STEP(F3, a, b, c, d, in[13] + 0x289b7ec6, 4);
  MD5STEP(F3, d, a, b, c, in[ 0] + 0xeaa127fa, 11);
  MD5STEP(F3, c, d, a, b, in[ 3] + 0xd4ef3085, 16);
  MD5STEP(F3, b, c, d, a, in[ 6] + 0x04881d05, 23);
  MD5STEP(F3, a, b, c, d, in[ 9] + 0xd9d4d039, 4);
  MD5STEP(F3, d, a, b, c, in[12] + 0xe6db99e5, 11);
  MD5STEP(F3, c, d, a, b, in[15] + 0x1fa27cf8, 16);
  MD5STEP(F3, b, c, d, a, in[ 2] + 0xc4ac5665, 23);

  MD5STEP(F4, a, b, c, d, in[ 0] + 0xf4292244, 6);
  MD5STEP(F4, d, a, b, c, in[ 7] + 0x432aff97, 10);
  MD5STEP(F4, c, d, a, b, in[14] + 0xab9423a7, 15);
  MD5STEP(F4, b, c, d, a, in[ 5] + 0xfc93a039, 21);
  MD5STEP(F4, a, b, c, d, in[12] + 0x655b59c3, 6);
  MD5STEP(F4, d, a, b, c, in[ 3] + 0x8f0ccc92, 10);
  MD5STEP(F4, c, d, a, b, in[10] + 0xffeff47d, 15);
  MD5STEP(F4, b, c, d, a, in[ 1] + 0x85845dd1, 21);
  MD5STEP(F4, a, b, c, d, in[ 8] + 0x6fa87e4f, 6);
  MD5STEP(F4, d, a, b, c, in[15] + 0xfe2ce6e0, 10);
  MD5STEP(F4, c, d, a, b, in[ 6] + 0xa3014314, 15);
  MD5STEP(F4, b, c, d, a, in[13] + 0x4e0811a1, 21);
  MD5STEP(F4, a, b, c, d, in[ 4] + 0xf7537e82, 6);
  MD5STEP(F4, d, a, b, c, in[11] + 0xbd3af235, 10);
  MD5STEP(F4, c, d, a, b, in[ 2] + 0x2ad7d2bb, 15);
  MD5STEP(F4, b, c, d, a, in[ 9] + 0xeb86d391, 21);

  state[0] += a; state[1] += b; state[2] += c; state[3] += d;
}
void MD5Update(MD5_CTX* ctx, const uint8_t* buf, uint32_t len) {
  uint8_t *in = ctx->in;
  uint32_t *bits = ctx->bits;
  uint32_t t = bits[0];
  if ((bits[0] = t + ((uint32_t) len << 3)) < t) { bits[1]++; }
  bits[1] += len >> 29;
  t = (t >> 3) & 0x3f;
  if (t) {
     uint8_t *p = (uint8_t *)in + t;
     t = 64 - t;
     if (len<t) { memcpy(p, buf, len); return; }
     memcpy(p, buf, t);
     orderW32Bytes(in, 16);
     MD5Transform(ctx);
     buf += t;
     len -= t;
  }
  while (len >= 64) {
     memcpy(in, buf, 64);
     orderW32Bytes(in, 16);
     MD5Transform(ctx);
     buf += 64;
     len -= 64;
  }
  memcpy(in, buf, len);          /* Handle any remaining bytes of data. */
}
void MD5Final(uint8_t *digest, MD5_CTX* ctx) {
  uint32_t *state = ctx->state;
  uint32_t *bits = ctx->bits;
  uint8_t *in = ctx->in;
  /* Set the first char of padding to 0x80.  Safe as always at least one byte is free */
  uint32_t count = (bits[0] >> 3) & 0x3F; /* Compute number of bytes mod 64 */
  uint8_t *p = in + count;
  *p++ = 0x80;
  count = 64 - 1 - count; /* Bytes of padding needed to make 64 bytes */
  if (count < 8) {        /* Pad out to 56 mod 64 */
      /* Two lots of padding: Pad the first block to 64 bytes. */
      memset(p, 0, count);
      orderW32Bytes(in, 16);
      MD5Transform(ctx);
      /* Now fill the next block with 56 bytes */
      memset(in, 0, 56);
  } else { memset(p, 0, count - 8); } /* Pad block to 56 bytes */
  orderW32Bytes(in, 14);
  /* Append length in bits and transform */
  ((uint32_t *) in)[14] = bits[0];
  ((uint32_t *) in)[15] = bits[1];
  MD5Transform(ctx);
  orderW32Bytes((uint8_t *) state, 4);
  memcpy(digest, state, 16);
  /* erase, in case it's sensitive */
  MD5Init(ctx);
}


/********************************************************************************/
/********************************************************************************/

/* This code is public-domain - it is based on libcrypt            */
/* placed in the public domain by Wei Dai and other contributors.  */

#include <stdint.h>
#include <string.h>


#ifdef __BIG_ENDIAN__
# define SHA_BIG_ENDIAN
#elif defined __LITTLE_ENDIAN__
/* override */
#elif defined __BYTE_ORDER
# if __BYTE_ORDER__ ==  __ORDER_BIG_ENDIAN__
# define SHA_BIG_ENDIAN
# endif
#else /* ! defined __LITTLE_ENDIAN__ */
# include <endian.h> /* machine/endian.h */
# if __BYTE_ORDER__ ==  __ORDER_BIG_ENDIAN__
#  define SHA_BIG_ENDIAN
# endif
#endif


/* header */
#define HASH_LENGTH 20
#define BLOCK_LENGTH 64

typedef struct SHA_CTX {
    uint32_t buffer[BLOCK_LENGTH/4];
    uint32_t state[HASH_LENGTH/4];
    uint32_t byteCount;
    uint8_t bufferOffset;
    uint8_t keyBuffer[BLOCK_LENGTH];
    uint8_t innerHash[HASH_LENGTH];
} SHA_CTX;


/* code */
#define SHA1_K0  0x5a827999
#define SHA1_K20 0x6ed9eba1
#define SHA1_K40 0x8f1bbcdc
#define SHA1_K60 0xca62c1d6

void SHA1_Init(SHA_CTX *s) {
    s->state[0] = 0x67452301;
    s->state[1] = 0xefcdab89;
    s->state[2] = 0x98badcfe;
    s->state[3] = 0x10325476;
    s->state[4] = 0xc3d2e1f0;
    s->byteCount = 0;
    s->bufferOffset = 0;
}

uint32_t sha1_rol32(uint32_t number, uint8_t bits) {
    return ((number << bits) | (number >> (32-bits)));
}

void sha1_hashBlock(SHA_CTX *s) {
    uint8_t i;
    uint32_t a,b,c,d,e,t;

    a=s->state[0];
    b=s->state[1];
    c=s->state[2];
    d=s->state[3];
    e=s->state[4];
    for (i=0; i<80; i++) {
        if (i>=16) {
            t = s->buffer[(i+13)&15] ^ s->buffer[(i+8)&15] ^ s->buffer[(i+2)&15] ^ s->buffer[i&15];
            s->buffer[i&15] = sha1_rol32(t,1);
        }
        if (i<20) {
            t = (d ^ (b & (c ^ d))) + SHA1_K0;
        } else if (i<40) {
            t = (b ^ c ^ d) + SHA1_K20;
        } else if (i<60) {
            t = ((b & c) | (d & (b | c))) + SHA1_K40;
        } else {
            t = (b ^ c ^ d) + SHA1_K60;
        }
        t+=sha1_rol32(a,5) + e + s->buffer[i&15];
        e=d;
        d=c;
        c=sha1_rol32(b,30);
        b=a;
        a=t;
    }
    s->state[0] += a;
    s->state[1] += b;
    s->state[2] += c;
    s->state[3] += d;
    s->state[4] += e;
}

void sha1_addUncounted(SHA_CTX *s, uint8_t data) {
    uint8_t * const b = (uint8_t*) s->buffer;
#ifdef SHA_BIG_ENDIAN
    b[s->bufferOffset] = data;
#else
    b[s->bufferOffset ^ 3] = data;
#endif
    s->bufferOffset++;
    if (s->bufferOffset == BLOCK_LENGTH) {
        sha1_hashBlock(s);
        s->bufferOffset = 0;
    }
}

void sha1_pad(SHA_CTX *s) {
    /* Implement SHA-1 padding (fips180-2 5.1.1) */
    /* Pad with 0x80 followed by 0x00 until the end of the block */
    sha1_addUncounted(s, 0x80);
    while (s->bufferOffset != 56) sha1_addUncounted(s, 0x00);

    /* Append length in the last 8 bytes */
    sha1_addUncounted(s, 0); /* We're only using 32 bit lengths */
    sha1_addUncounted(s, 0); /* But SHA-1 supports 64 bit lengths */
    sha1_addUncounted(s, 0); /* So zero pad the top bits */
    sha1_addUncounted(s, s->byteCount >> 29); /* Shifting to multiply by 8 */
    sha1_addUncounted(s, s->byteCount >> 21); /* as SHA-1 supports bitstreams as well as */
    sha1_addUncounted(s, s->byteCount >> 13); /* byte. */
    sha1_addUncounted(s, s->byteCount >> 5);
    sha1_addUncounted(s, s->byteCount << 3);
}

void SHA1_Final(uint8_t *digest, SHA_CTX *s) {
    int i; (void)i;
    /* Pad to complete the last block */
    sha1_pad(s);

#ifndef SHA_BIG_ENDIAN
    /* Swap byte order back */
    for (i=0; i<5; i++) {
        s->state[i]= 
              (((s->state[i])<<24)& 0xff000000)
            | (((s->state[i])<<8) & 0x00ff0000)
            | (((s->state[i])>>8) & 0x0000ff00)
            | (((s->state[i])>>24)& 0x000000ff);
    }
#endif

    /* Return pointer to hash (20 characters) */
    memcpy(digest, s->state, sizeof(s->state));
}

void SHA1_Update(SHA_CTX *s, uint8_t *data, size_t len) {
    for (;len--;) {
      ++s->byteCount;
      sha1_addUncounted(s, (uint8_t) *data++);
    }
}

