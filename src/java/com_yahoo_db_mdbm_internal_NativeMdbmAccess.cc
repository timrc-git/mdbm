/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
#include <jni.h>
#include <errno.h>
#include <string.h>
#include <mdbm.h>
#include <mdbm_handle_pool.h>

#include "jni_helper_defines.h"
#include "com_yahoo_db_mdbm_internal_NativeMdbmAccess.h"

#define MDBM_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmLoadException"
#define MDBM_LOAD_FAILED_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmLoadException"
#define MDBM_LOCK_FAILED_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmLockFailedException"
#define MDBM_UNLOCK_FAILED_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmUnlockFailedException"
#define MDBM_STORE_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmStoreException"
#define MDBM_DELETE_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmDeleteException"
#define MDBM_FETCH_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmFetchException"
#define MDBM_NOENTRY_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmNoEntryException"
#define MDBM_CREATE_POOL_FAILED_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmCreatePoolException"
#define MDBM_POOL_ACQUIRE_HANDLE_FAILED_EXCEPTION "com/yahoo/db/mdbm/exceptions/MdbmPoolAcquireHandleFailedException"
#define UNSUPPORTED_OPERATION_EXCEPTION "java/lang/UnsupportedOperationException"

DECLARE_CACHED_CLASS(nativeMdbmImplementationClass, "com/yahoo/db/mdbm/internal/NativeMdbmImplementation")
DECLARE_CACHED_METHOD_ID(nativeMdbmImplementationClass, nativeMdbmImplementationCtorId, "<init>", "(JLjava/lang/String;I)V")
DECLARE_CACHED_METHOD_ID(nativeMdbmImplementationClass, nativeMdbmImplementationGetPointerId, "getPointer", "()J")
DECLARE_CACHED_METHOD_ID(nativeMdbmImplementationClass, nativeMdbmImplementationGetPathId, "getPath", "()Ljava/lang/String;")

DECLARE_CACHED_CLASS(uncloseableMdbmClass, "com/yahoo/db/mdbm/internal/UncloseableMdbm")
DECLARE_CACHED_METHOD_ID(uncloseableMdbmClass, uncloseableMdbmCtorId, "<init>", "(JLjava/lang/String;I)V")
DECLARE_CACHED_METHOD_ID(uncloseableMdbmClass, uncloseableMdbmGetPointerId, "getPointer", "()J")
DECLARE_CACHED_METHOD_ID(uncloseableMdbmClass, uncloseableMdbmGetPathId, "getPath", "()Ljava/lang/String;")

DECLARE_CACHED_CLASS(pooledMdbmHandleClass, "com/yahoo/db/mdbm/internal/PooledMdbmHandle")
DECLARE_CACHED_METHOD_ID(pooledMdbmHandleClass, pooledMdbmHandleCtorId, "<init>", "(Lcom/yahoo/db/mdbm/MdbmPoolInterface;Lcom/yahoo/db/mdbm/internal/UncloseableMdbm;)V")
DECLARE_CACHED_METHOD_ID(pooledMdbmHandleClass, pooledMdbmHandleGetPointerId, "getPointer", "()J")
DECLARE_CACHED_METHOD_ID(pooledMdbmHandleClass, pooledMdbmHandleGetPathId, "getPath", "()Ljava/lang/String;")

DECLARE_CACHED_CLASS(nativeMdbmPoolImplementationClass, "com/yahoo/db/mdbm/internal/NativeMdbmPoolImplementation")
DECLARE_CACHED_METHOD_ID(nativeMdbmPoolImplementationClass, nativeMdbmPoolImplementationCtorId, "<init>", "(Lcom/yahoo/db/mdbm/MdbmInterface;JI)V")
DECLARE_CACHED_METHOD_ID(nativeMdbmPoolImplementationClass, nativeMdbmPoolImplementationGetPointerId, "getPointer", "()J")
DECLARE_CACHED_METHOD_ID(nativeMdbmPoolImplementationClass, nativeMdbmPoolImplementationGetPathId, "getPath", "()Ljava/lang/String;")

DECLARE_CACHED_CLASS(nativeMdbmIteratorClass, "com/yahoo/db/mdbm/internal/NativeMdbmIterator")
DECLARE_CACHED_METHOD_ID(nativeMdbmIteratorClass, nativeMdbmIteratorCtorId, "<init>", "(J)V")
DECLARE_CACHED_METHOD_ID(nativeMdbmIteratorClass, nativeMdbmIteratorGetPointerId, "getPointer", "()J")

DECLARE_CACHED_CLASS(mdbmDatumClass, "com/yahoo/db/mdbm/MdbmDatum")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumDefaultCtorId, "<init>", "()V")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumSizeCtorId, "<init>", "(I)V")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumByteArrayCtorId, "<init>", "([B)V")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumByteBufferCtorId, "<init>", "(Ljava/nio/ByteBuffer;)V")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumGetSizeId, "getSize", "()I")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumSetSizeId, "setSize", "(I)V")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumGetDataId, "getData", "()[B")
DECLARE_CACHED_METHOD_ID(mdbmDatumClass, mdbmDatumSetDataId, "setData", "([B)V")

DECLARE_CACHED_CLASS(mdbmKvPairClass, "com/yahoo/db/mdbm/MdbmKvPair")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairDefaultCtorId, "<init>", "()V")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairDatumCtorId, "<init>", "(Lcom/yahoo/db/mdbm/MdbmDatum;Lcom/yahoo/db/mdbm/MdbmDatum;)V")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairGetValueId, "getValue", "()Lcom/yahoo/db/mdbm/MdbmDatum;")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairSetValueId, "setValue", "(Lcom/yahoo/db/mdbm/MdbmDatum;)V")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairGetKeyId, "getKey", "()Lcom/yahoo/db/mdbm/MdbmDatum;")
DECLARE_CACHED_METHOD_ID(mdbmKvPairClass, mdbmKvPairSetKeyId, "setKey", "(Lcom/yahoo/db/mdbm/MdbmDatum;)V")

DECLARE_CACHED_CLASS(mdbmExceptionClass, MDBM_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmExceptionClass, mdbmExceptionCtorId, "<init>", "(Ljava/lang/String;)V")
DECLARE_CACHED_METHOD_ID(mdbmExceptionClass, mdbmExceptionSetPathId, "setPath", "(Ljava/lang/String;)V")
DECLARE_CACHED_METHOD_ID(mdbmExceptionClass, mdbmExceptionSetInfoId, "setInfo", "(Ljava/lang/String;)V")

DECLARE_CACHED_CLASS(mdbmDeleteExceptionClass, MDBM_DELETE_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmDeleteExceptionClass, mdbmDeleteExceptionCtorId, "<init>", "(Ljava/lang/String;)V")

DECLARE_CACHED_CLASS(mdbmFetchExceptionClass, MDBM_FETCH_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmFetchExceptionClass, mdbmFetchExceptionCtorId, "<init>", "(Ljava/lang/String;)V")

DECLARE_CACHED_CLASS(mdbmStoreExceptionClass, MDBM_STORE_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmStoreExceptionClass, mdbmStoreExceptionCtorId, "<init>", "(Ljava/lang/String;)V")

DECLARE_CACHED_CLASS(mdbmNoEntryExceptionClass, MDBM_NOENTRY_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmNoEntryExceptionClass, mdbmNoEntryExceptionCtorId, "<init>", "(Ljava/lang/String;)V")

DECLARE_CACHED_CLASS(mdbmCreatePoolFailedExceptionClass, MDBM_CREATE_POOL_FAILED_EXCEPTION)
DECLARE_CACHED_METHOD_ID(mdbmCreatePoolFailedExceptionClass, mdbmCreatePoolFailedExceptionCtorId, "<init>", "(Ljava/lang/String;)V")

static void zeroDatum(datum &d) {
    d.dptr = NULL;
    d.dsize = 0;
}

static void zeroKv(kvpair &kv) {
    zeroDatum(kv.key);
    zeroDatum(kv.val);
}

class ScopedMdbmLock {
public:
    ScopedMdbmLock(JNIEnv *jenv, MDBM *db, const datum *key, int flags) :
            jenv(jenv), _db(db), _key(key), _flags(flags) {
        lock();
    }

    ScopedMdbmLock(JNIEnv *jenv, MDBM *db) :
            jenv(jenv), _db(db), _key(NULL), _flags(0) {
        lock();
    }

    inline void set(MDBM *db, const datum *key, int flags) {
        _db = db;
        _key = key;
        _flags = flags;
    }

    inline int lock() {
        if (NULL == _db) {
            ThrowException(jenv, NULL_POINTER_EXCEPTION,
                    "null db in ScopedLock::lock");
            return -1;
        }

        if (NULL == _key) {
            return checkAndThrow(mdbm_lock(_db), false, 0, "");
        }

        return checkAndThrow(mdbm_lock_smart(_db, _key, _flags), true, _flags,
                "");
    }

    inline int unlock() {
        int ret = 1;

        if (NULL == _db) {
            _key = NULL;
            ThrowException(jenv, NULL_POINTER_EXCEPTION,
                    "null db in ScopedLock::unlock");
            return -1;
        }

        if (NULL == _key) {
            ret = checkAndThrow(mdbm_unlock(_db), false, 0, "un");
        } else {
            ret = checkAndThrow(mdbm_unlock_smart(_db, _key, _flags), true,
                    _flags, "un");
        }

        _db = NULL;
        _key = NULL;

        return ret;
    }

    virtual ~ScopedMdbmLock() {
        unlock();
    }

private:
    inline int checkAndThrow(int ret, bool smart, int flags, const char *type) {
        if (1 != ret) {
            char mesg[2048] = { 0, };
            if (smart) {
                snprintf(mesg, sizeof(mesg), "mdbm_%slock failed errno=%s (%d)",
                        type, strerror(errno), errno);
            } else {
                snprintf(mesg, sizeof(mesg),
                        "mdbm_smart_%slock(db,key,0x%x failed errno=%s (%d)",
                        type, flags, strerror(errno), errno);
            }
            ThrowException(jenv, MDBM_LOCK_FAILED_EXCEPTION, mesg);
        }
        return ret;
    }

    // Disabled
    ScopedMdbmLock(const ScopedMdbmLock &);
    void operator=(const ScopedMdbmLock &);

    JNIEnv *jenv;
    MDBM *_db;
    const datum *_key;
    int _flags;
};

class ScopedDatum {
public:
    ScopedDatum() :
            valid(false), mdbmDatum(NULL) {
        zeroDatum(d);
    }

    // STORE
    ScopedDatum(JNIEnv *jenv, jobject mdbmDatum) :
            valid(false), mdbmDatum(NULL) {
        zeroDatum(d);

        RETURN_AND_THROW_IF_NULL(mdbmDatum, "null mdbmDatum object");

        setMdbmDatum(jenv, mdbmDatum);
    }

    // FETCH
    ScopedDatum(JNIEnv *jenv, datum &v) :
            valid(false), mdbmDatum(NULL) {
        zeroDatum(d);

        if (0 == v.dsize || NULL == v.dptr) {
            //nothing to do.
            return;
        }

        setDatum(jenv, v);
    }

    inline bool isValid() const {
        return valid;
    }

    inline datum* getDatum() {
        return &d;
    }

    inline jobject getMdbmDatum() {
        return mdbmDatum;
    }

    inline void setDatum(JNIEnv *jenv, datum &v) {
        valid = false;
        // given an mdbm datum, setup our datum using it's byte[] and size
        // this is typically used for a key
        GET_CACHED_CLASS(jenv, mdbmDatumClass);
        GET_CACHED_METHOD_ID(jenv, mdbmDatumByteArrayCtorId);
        if (NULL == mdbmDatumClass || NULL == mdbmDatumByteArrayCtorId) {
            // valid is set to false so bail.
            return;
        }

        jbyteArray bytes = jenv->NewByteArray(v.dsize);
        RETURN_IF_EXCEPTION_OR_NULL(bytes);

        jenv->SetByteArrayRegion(bytes, 0, v.dsize, (jbyte*) v.dptr);
        RETURN_IF_EXCEPTION();

        mdbmDatum = jenv->NewObject(mdbmDatumClass, mdbmDatumByteArrayCtorId,
                bytes);
        if (NULL == mdbmDatum || jenv->ExceptionCheck()) {
            mdbmDatum = NULL;
            return;
        }

        valid = true;
    }

    inline bool setMdbmDatum(JNIEnv *jenv, jobject pmdbmDatum) {
        // given an mdbm datum, setup our datum using it's byte[] and size
        // this is typically used for a key
        GET_CACHED_CLASS(jenv, mdbmDatumClass);
        GET_CACHED_METHOD_ID(jenv, mdbmDatumGetSizeId);
        GET_CACHED_METHOD_ID(jenv, mdbmDatumGetDataId);
        if (NULL == pmdbmDatum || NULL == mdbmDatumGetSizeId
                || NULL == mdbmDatumGetDataId) {
            // valid is set to false so bail.
            return false;
        }

        d.dsize = jenv->CallIntMethod(pmdbmDatum, mdbmDatumGetSizeId);
        RETURN_FALSE_IF_EXCEPTION();

        jbyteArray bytes = (jbyteArray) jenv->CallObjectMethod(pmdbmDatum,
                mdbmDatumGetDataId);
        RETURN_FALSE_IF_EXCEPTION();

        // get the byte array elements.
        javaBytes.set(jenv, bytes);
        RETURN_FALSE_IF_EXCEPTION();

        d.dptr = (char*) javaBytes.get();

        valid = true;
        mdbmDatum = pmdbmDatum;

        return valid;
    }

private:
    // only true if we completed setup.
    bool valid;

    ///// STORE //////
    // This is what mdbm deals with
    datum d;

    // This holds a reference back to the backing byte array so changes are reflected.
    ScopedByteArray javaBytes;

    ///// FETCH //////
    jobject mdbmDatum;
};

class ScopedKvPair {
public:
    // STORE
    ScopedKvPair(JNIEnv *jenv, jobject mdbmKvPair) :
            valid(false), mdbmKvPair(NULL) {
        zeroKv(kv);
        RETURN_AND_THROW_IF_NULL(mdbmKvPair, "null mdbmKvPair object");

        // given an mdbm datum, setup our datum using it's byte[] and size
        // this is typically used for a key
        GET_CACHED_CLASS(jenv, mdbmKvPairClass);
        GET_CACHED_METHOD_ID(jenv, mdbmKvPairGetKeyId);
        GET_CACHED_METHOD_ID(jenv, mdbmKvPairGetValueId);
        if (NULL == mdbmKvPair || NULL == mdbmKvPairGetKeyId
                || NULL == mdbmKvPairGetValueId) {
            // valid is set to false so bail.
            return;
        }

        jobject keyObject = jenv->CallObjectMethod(mdbmKvPair,
                mdbmKvPairGetKeyId);
        RETURN_IF_EXCEPTION_OR_NULL(keyObject);
        RETURN_IF_EXCEPTION_OR_NULL(key.setMdbmDatum(jenv, keyObject));

        jobject valueObject = jenv->CallObjectMethod(mdbmKvPair,
                mdbmKvPairGetValueId);
        RETURN_IF_EXCEPTION_OR_NULL(valueObject);
        RETURN_IF_EXCEPTION_OR_NULL(val.setMdbmDatum(jenv, valueObject));

        valid = true;
    }

    // FETCH
    ScopedKvPair(JNIEnv *jenv, kvpair &in_kv) :
            valid(false), mdbmKvPair(NULL) {
        zeroKv(kv);

        if ((0 == in_kv.key.dsize || NULL == in_kv.key.dptr)
                && (0 == in_kv.val.dsize || NULL == in_kv.val.dptr)) {
            //nothing to do.
            return;
        }

        GET_CACHED_CLASS(jenv, mdbmKvPairClass);
        GET_CACHED_METHOD_ID(jenv, mdbmKvPairDatumCtorId);
        if (NULL == mdbmKvPairClass || NULL == mdbmKvPairDatumCtorId) {
            // valid is set to false so bail.
            return;
        }

        // now we need to create the datums to fill out.
        key.setDatum(jenv, in_kv.key);
        RETURN_IF_EXCEPTION_OR_NULL(key.getMdbmDatum());

        val.setDatum(jenv, in_kv.val);
        RETURN_IF_EXCEPTION_OR_NULL(val.getMdbmDatum());

        // now that key and val are setup, we can create the java object using them.
        mdbmKvPair = jenv->NewObject(mdbmKvPairClass, mdbmKvPairDatumCtorId,
                key.getMdbmDatum(), val.getMdbmDatum());
        if (NULL == mdbmKvPair || jenv->ExceptionCheck()) {
            mdbmKvPair = NULL;
            return;
        }

        valid = true;
    }

    inline bool isValid() const {
        return valid;
    }

    inline kvpair* getKv() {
        kv.key = *key.getDatum();
        kv.val = *val.getDatum();
        return &kv;
    }

    inline jobject getMdbmKvPair() {
        return mdbmKvPair;
    }

private:
    // only true if we completed setup.
    bool valid;

    ScopedDatum key;
    ScopedDatum val;
    kvpair kv;

    ///// FETCH //////
    jobject mdbmKvPair;
};

static MDBM *getMdbmPointer(JNIEnv *jenv, jobject thisObject) {
    RETURN_NULL_AND_THROW_IF_NULL(thisObject, "null thisObject object in getMdbmPointer");

    GET_CACHED_CLASS(jenv, nativeMdbmImplementationClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmImplementationClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmImplementationGetPointerId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmImplementationGetPointerId);

    jlong pointer = jenv->CallLongMethod(thisObject,
            nativeMdbmImplementationGetPointerId);
    RETURN_NULL_IF_0_OR_EXCEPTION(pointer);

    POINTER_TO_CONTEXT (MDBM);

    return context;
}

static MDBM *getPooledMdbmPointer(JNIEnv *jenv, jobject thisObject) {
    RETURN_NULL_AND_THROW_IF_NULL(thisObject, "null thisObject object in getPooledMdbmPointer");

    GET_CACHED_CLASS(jenv, pooledMdbmHandleClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (pooledMdbmHandleClass);

    GET_CACHED_METHOD_ID(jenv, pooledMdbmHandleGetPointerId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (pooledMdbmHandleGetPointerId);

    jlong pointer = jenv->CallLongMethod(thisObject,
            pooledMdbmHandleGetPointerId);
    RETURN_NULL_IF_0_OR_EXCEPTION(pointer);

    POINTER_TO_CONTEXT (MDBM);

    return context;
}
static MDBM_ITER *getIterPointer(JNIEnv *jenv, jobject thisObject) {

    if (NULL == thisObject) {
        // it's perfectly valid to have a null iterator
        return NULL;
    }

    GET_CACHED_CLASS(jenv, nativeMdbmIteratorClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmIteratorClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmIteratorGetPointerId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmIteratorGetPointerId);

    jlong pointer = jenv->CallLongMethod(thisObject,
            nativeMdbmIteratorGetPointerId);
    RETURN_NULL_IF_0_OR_EXCEPTION(pointer);

    POINTER_TO_CONTEXT (MDBM_ITER);

    return context;
}

static mdbm_pool_t *getPoolPointer(JNIEnv *jenv, jobject thisObject) {

    if (NULL == thisObject) {
        ThrowException(jenv, NULL_POINTER_EXCEPTION, "null thisObject object in getPoolPointer");
        return NULL;
    }

    GET_CACHED_CLASS(jenv, nativeMdbmPoolImplementationClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmPoolImplementationGetPointerId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationGetPointerId);

    jlong pointer = jenv->CallLongMethod(thisObject,
            nativeMdbmPoolImplementationGetPointerId);
    RETURN_NULL_IF_0_OR_EXCEPTION(pointer);

    POINTER_TO_CONTEXT (mdbm_pool_t);

    return context;
}

static void ThrowMdbmException(JNIEnv *jenv, const char *exceptionClass,
        jclass exceptionClassId, jmethodID ctorId, const char *message,
        jobject thisObject) {

    if (!jenv || !exceptionClass) {
        return;
    }

    jenv->ExceptionClear();

    GET_CACHED_METHOD_ID(jenv, mdbmExceptionSetPathId);

    GET_CACHED_CLASS(jenv, nativeMdbmImplementationClass);
    GET_CACHED_METHOD_ID(jenv, nativeMdbmImplementationGetPathId);

    if (NULL == exceptionClassId || //
            NULL == ctorId || //
            NULL == mdbmExceptionSetPathId || //
            NULL == nativeMdbmImplementationClass || //
            NULL == nativeMdbmImplementationGetPathId) {

        ThrowException(jenv, exceptionClass, message);
        return;
    }

    jobject exception = jenv->NewObject(exceptionClassId, ctorId, message);
    if (NULL != thisObject) {
        jobject path = jenv->CallObjectMethod(thisObject,
                nativeMdbmImplementationGetPathId);
        if (NULL != path) {
            jenv->CallVoidMethod(thisObject, mdbmExceptionSetPathId,
                    (jstring) path);
        }
    }

    jenv->Throw((jthrowable) exception);
}

static bool checkZeroIsOkReturn(JNIEnv *jenv, int ret, jobject thisObject,
        const char *exception, jclass exceptionClassId, jmethodID ctorId,
        const char *mdbm_function_name, int flags, const void *iter) {
    // deal with errors first.
    if (0 != ret) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg),
                "%s, flags(0x%x), iter(%s) failed errno=%s (%d)",
                mdbm_function_name, flags,
                ((NULL == iter) ? "NULL" : "non-null"), strerror(errno), errno);

        ThrowMdbmException(jenv, exception, exceptionClassId, ctorId, mesg,
                thisObject);
        return false;
    }

    return true;
}

static bool checkZeroIsOkReturn(JNIEnv *jenv, int ret, const char *exception,
        const char *mdbm_function_name, int flags, const void *iter) {
    // deal with errors first.
    if (0 != ret) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg),
                "%s, flags(0x%x), iter(%s) failed errno=%s (%d)",
                mdbm_function_name, flags,
                ((NULL == iter) ? "NULL" : "non-null"), strerror(errno), errno);
        ThrowException(jenv, exception, mesg);
        return false;
    }

    return true;
}

static bool checkOneIsOkReturn(JNIEnv *jenv, int ret, const char *exception,
        const char *mdbm_function_name, int flags, const void *iter) {
    // deal with errors first.
    if (1 != ret) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg),
                "%s, flags(0x%x), iter(%s) failed errno=%s (%d)",
                mdbm_function_name, flags,
                ((NULL == iter) ? "NULL" : "non-null"), strerror(errno), errno);
        ThrowException(jenv, exception, mesg);
        return false;
    }

    return true;
}

typedef int (mdbm_fetch_fptr)(MDBM *db, datum *key, datum *val,
        MDBM_ITER *iter);

static jobject mdbm_fetch_wrapper(JNIEnv *jenv, jclass thisClass,
        jobject thisObject, jobject keyObject, jobject iterObject, MDBM *mdbm,
        mdbm_fetch_fptr *mdbm_function, const char *mdbm_function_name) {
    RETURN_FALSE_AND_THROW_IF_NULL(keyObject, "keyObject was null");

    if (NULL == mdbm) {
        mdbm = getMdbmPointer(jenv, thisObject);
        RETURN_FALSE_AND_THROW_IF_NULL(mdbm, "mdbm was null");
    }

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    ScopedDatum keyDatum(jenv, keyObject);
    if (!keyDatum.isValid()) {
        // somewhere during construction it threw an exception, so propagate that.
        return NULL;
    }

    datum value = { 0, };
    int ret = (*mdbm_function)(mdbm, keyDatum.getDatum(), &value, iter);

    if ( ret == -1 ) {
        if ( errno == ENOENT ) {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_NOENTRY_EXCEPTION,
                    mdbmNoEntryExceptionClass, mdbmNoEntryExceptionCtorId,
                    mdbm_function_name, 0, iter);
            return NULL;
        } else {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_FETCH_EXCEPTION,
                    mdbmFetchExceptionClass, mdbmFetchExceptionCtorId,
                    mdbm_function_name, 0, iter);
            return NULL;
        }
    }

    // deal with success.
    // this means copying the data into the new datum Object and returning that.
    ScopedDatum valueDatum(jenv, value);

    return valueDatum.getMdbmDatum();
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    createIter
 * Signature: ()Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_createIter(
        JNIEnv *jenv, jclass thisClass) {
    GET_CACHED_CLASS(jenv, nativeMdbmIteratorClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmIteratorClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmIteratorCtorId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmIteratorCtorId);

    MDBM_ITER *mdbmIter = (MDBM_ITER*) calloc(1, sizeof(MDBM_ITER));
    MDBM_ITER_INIT(mdbmIter);

    CONTEXT_TO_POINTER(mdbmIter);

    return jenv->NewObject(nativeMdbmIteratorClass, nativeMdbmIteratorCtorId,
            pointer);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    freeIter
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_freeIter
(JNIEnv *jenv, jclass thisClass, jlong pointer)
{
    if (0 == pointer) {
        return;
    }

    POINTER_TO_CONTEXT (MDBM_ITER);
    free (context);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_open
 * Signature: (Ljava/lang/String;IIII)Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1open(
        JNIEnv *jenv, jclass thisClass, jstring file, jint flags, jint mode,
        jint psize, jint presize) {

    GET_CACHED_CLASS(jenv, nativeMdbmImplementationClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmImplementationClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmImplementationCtorId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmImplementationCtorId);

    ScopedStringUTFChars fileString(jenv, file);

    MDBM *mdbm = mdbm_open(fileString.getChars(), flags, mode, psize, presize);
    if (NULL == mdbm) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg),
                "mdbm_open(%s, 0x%x, 0x%x, %d, %d) failed  errno=%s (%d)",
                fileString.getChars(), flags, mode, psize, presize,
                strerror(errno), errno);
        ThrowException(jenv, MDBM_LOAD_FAILED_EXCEPTION, mesg);

        return NULL;
    }

    // otherwise we have an open db, create it.
    CONTEXT_TO_POINTER(mdbm);

    return jenv->NewObject(nativeMdbmImplementationClass,
            nativeMdbmImplementationCtorId, pointer, file, flags);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1close
(JNIEnv *jenv, jclass thisClass, jlong pointer)
{
    if (0 == pointer) {
        return;
    }

    POINTER_TO_CONTEXT(MDBM);

    mdbm_close(context);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_store_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1store_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jobject valueObject, jint flags, jobject iterObject) {
    RETURN_FALSE_AND_THROW_IF_NULL(keyObject, "keyObject was null");
    RETURN_FALSE_AND_THROW_IF_NULL(valueObject, "valueObject was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_FALSE_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    ScopedDatum keyDatum(jenv, keyObject);
    if (!keyDatum.isValid()) {
        // somewhere during construction it threw an exception, so propogate that.
        return JNI_FALSE;
    }

    ScopedDatum valDatum(jenv, valueObject);
    if (!valDatum.isValid()) {
        // somewhere during construction it threw an exception, so propagate that.
        return JNI_FALSE;
    }

    int ret = mdbm_store_r(mdbm, keyDatum.getDatum(), valDatum.getDatum(),
            flags, iter);

    switch (ret) {
    case 0:
        // success
        return JNI_TRUE;

    case 1:
        // failed Flag MDBM_INSERT was specified, and the key already exists
        return JNI_FALSE;

    default:
        checkZeroIsOkReturn(jenv, ret, MDBM_STORE_EXCEPTION, "mdbm_store_r",
                flags, iter);
        return JNI_FALSE;
    }

    return JNI_FALSE;
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_fetch_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmDatum;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1fetch_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jobject iterObject) {

    return mdbm_fetch_wrapper(jenv, thisClass, thisObject, keyObject,
            iterObject, NULL, mdbm_fetch_r, "mdbm_fetch_r");
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_fetch_r_locked
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;Lcom/yahoo/db/mdbm/internal/NativeMdbmIter
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1fetch_1r_1locked(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jobject iterObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedMdbmLock lock(jenv, mdbm);

    return mdbm_fetch_wrapper(jenv, thisClass, thisObject, keyObject,
            iterObject, mdbm, mdbm_fetch_r, "mdbm_fetch_r");
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_fetch_dup_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmDatum;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1fetch_1dup_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jobject iterObject) {

    return mdbm_fetch_wrapper(jenv, thisClass, thisObject, keyObject,
            iterObject, NULL, mdbm_fetch_dup_r, "mdbm_fetch_dup_r");
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_delete
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1delete(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject)
{
    RETURN_AND_THROW_IF_NULL(keyObject, "keyObject was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    if (!keyDatum.isValid()) {
        // somewhere during construction it threw an exception, so propagate that.
        return;
    }

    int ret = mdbm_delete (mdbm, *keyDatum.getDatum());

    if ( ret == -1 ) {
        if ( errno == ENOENT ) {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_NOENTRY_EXCEPTION,
                    mdbmNoEntryExceptionClass, mdbmNoEntryExceptionCtorId,
                    "mdbm_delete", 0, NULL);
        } else {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_DELETE_EXCEPTION,
                    mdbmDeleteExceptionClass, mdbmDeleteExceptionCtorId,
                    "mdbm_delete", 0, NULL);
        }
    }
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_delete_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1delete_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject iterObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // Not sure if we should check for null iter here.
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    int ret = mdbm_delete_r (mdbm, iter);

    if ( ret == -1 ) {
        if ( errno == ENOENT ) {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_NOENTRY_EXCEPTION,
                    mdbmNoEntryExceptionClass, mdbmNoEntryExceptionCtorId,
                    "mdbm_delete_r", 0, iter);
        } else {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_DELETE_EXCEPTION,
                    mdbmDeleteExceptionClass, mdbmDeleteExceptionCtorId,
                    "mdbm_delete_r", 0, iter);
        }
    }
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_lock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1lock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_lock (mdbm);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_LOCK_FAILED_EXCEPTION, "mdbm_lock", 0,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_unlock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1unlock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_unlock (mdbm);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_UNLOCK_FAILED_EXCEPTION, "mdbm_unlock", 0,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_plock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1plock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject, jint flags)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    int ret = mdbm_plock (mdbm, keyDatum.getDatum(), flags);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_LOCK_FAILED_EXCEPTION, "mdbm_plock", flags,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_tryplock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1tryplock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jint flags) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_NULL_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    // don't throw from try*
    return mdbm_tryplock(mdbm, keyDatum.getDatum(), flags);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_punlock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1punlock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject, jint flags)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    int ret = mdbm_punlock (mdbm, keyDatum.getDatum(), flags);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_UNLOCK_FAILED_EXCEPTION, "mdbm_punlock", flags,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_lock_shared
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1lock_1shared(
        JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_lock_shared (mdbm);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_LOCK_FAILED_EXCEPTION, "mdbm_lock_shared", 0,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_trylock_shared
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1trylock_1shared(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // don't throw from try*
    return mdbm_trylock_shared(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_lock_smart
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1lock_1smart(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject, jint flags)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    int ret = mdbm_lock_smart (mdbm, keyDatum.getDatum(), flags);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_LOCK_FAILED_EXCEPTION, "mdbm_lock_smart", flags,
            NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_trylock_smart
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1trylock_1smart(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject,
        jint flags) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_NULL_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    return mdbm_trylock_smart(mdbm, keyDatum.getDatum(), flags);

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_unlock_smart
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/MdbmDatum;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1unlock_1smart(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject keyObject, jint flags) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedDatum keyDatum(jenv, keyObject);
    RETURN_AND_THROW_IF_NULL(keyDatum.isValid(), "invalid key");

    int ret = mdbm_unlock_smart(mdbm, keyDatum.getDatum(), flags);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_UNLOCK_FAILED_EXCEPTION, "mdbm_unlock_smart",
            flags, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_sethash
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1sethash(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jint hashid) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_sethash(mdbm, hashid);

    // deal with errors first.
    checkOneIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_sethash",
            hashid, NULL);
}
/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_sync
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1sync(
        JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_sync(mdbm);

    // deal with errors first.
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_sync",
            0, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_limit_size_v3
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;JLcom/yahoo/db/mdbm/ShakeCallback;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1limit_1size_1v3(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jlong, jobject,
        jobject)
{
    ThrowException(jenv, UNSUPPORTED_OPERATION_EXCEPTION, "mdbm_limit_size_v3 isn't supported");
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_compress_tree
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1compress_1tree
(JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    mdbm_compress_tree(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_close_fd
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1close_1fd
(JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    mdbm_close_fd(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_truncate
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1truncate
(JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    mdbm_truncate(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_replace_file
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1replace_1file(
        JNIEnv *jenv, jclass thisClass, jstring oldFile, jstring newFile)
{
    RETURN_AND_THROW_IF_NULL(oldFile, "oldFile was null");
    RETURN_AND_THROW_IF_NULL(newFile, "newFile was null");

    ScopedStringUTFChars oldFileString(jenv, oldFile);
    ScopedStringUTFChars newFileString(jenv, newFile);

    int ret = mdbm_replace_file(oldFileString.getChars(), newFileString.getChars());
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_replace_file", 0, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_replace_db
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1replace_1db(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jstring newFile)
{
    RETURN_AND_THROW_IF_NULL(newFile, "newFile was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedStringUTFChars newFileString(jenv, newFile);

    int ret = mdbm_replace_db(mdbm, newFileString.getChars());
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_replace_db", 0, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_pre_split
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;J)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1pre_1split(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jlong size)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_pre_split(mdbm, size);
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_pre_split", size, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_setspillsize
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1setspillsize(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jint size)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_setspillsize(mdbm, size);
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_setspillsize", size, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_store_str
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Ljava/lang/String;Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1store_1str(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jstring keyObject, jstring valObject,
        jint flags)
{
    RETURN_AND_THROW_IF_NULL(keyObject, "key was null");
    RETURN_AND_THROW_IF_NULL(valObject, "val was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedStringUTFChars keyString(jenv, keyObject);
    ScopedStringUTFChars valString(jenv, valObject);

    int ret = mdbm_store_str(mdbm, keyString.getChars(), valString.getChars(), flags);

    // deal with errors first.
    checkZeroIsOkReturn(jenv, ret, MDBM_STORE_EXCEPTION,
            "mdbm_store_str", 0, NULL);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_fetch_str
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1fetch_1str(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jstring keyObject) {
    RETURN_FALSE_AND_THROW_IF_NULL(keyObject, "key was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedStringUTFChars keyString(jenv, keyObject);

    char *ret = mdbm_fetch_str(mdbm, keyString.getChars());
    if (NULL == ret) {
        return NULL;
    }

    return jenv->NewStringUTF(ret);

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_delete_str
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1delete_1str(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jstring keyObject)
{
    RETURN_AND_THROW_IF_NULL(keyObject, "key was null");

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    ScopedStringUTFChars keyString(jenv, keyObject);

    int ret = mdbm_delete_str(mdbm, keyString.getChars());

    if ( ret == -1 ) {
        if ( errno == ENOENT ) {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_NOENTRY_EXCEPTION,
                    mdbmNoEntryExceptionClass, mdbmNoEntryExceptionCtorId,
                    "mdbm_delete_str", 0, NULL);
        } else {
            checkZeroIsOkReturn(jenv, -1, thisObject, MDBM_DELETE_EXCEPTION,
                    mdbmDeleteExceptionClass, mdbmDeleteExceptionCtorId,
                    "mdbm_delete_str", 0, NULL);
        }
    }

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_prune
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/PruneCallback;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1prune
(JNIEnv *jenv, jclass thisClass, jobject thisObject, jobject)
{
    ThrowException(jenv, UNSUPPORTED_OPERATION_EXCEPTION, "mdbm_limit_size_v3 isn't supported");
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_purge
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1purge
(JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    mdbm_purge(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_fsync
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1fsync(
        JNIEnv *jenv, jclass thisClass, jobject thisObject)
{
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    int ret = mdbm_fsync(mdbm);

    // deal with errors first.
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION,
            "mdbm_fsync", 0, NULL);

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_trylock
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1trylock(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_trylock(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_islocked
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)Z
 */
JNIEXPORT jboolean
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1islocked(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return (mdbm_islocked(mdbm) == 1) ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_isowned
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)Z
 */
JNIEXPORT jboolean
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1isowned(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {
    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return (mdbm_isowned(mdbm) == 1) ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_first_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmKvPair;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1first_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject,
        jobject iterObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    kvpair retKv = mdbm_first_r(mdbm, iter);

    // need to parse a kvpair now and return the value
    ScopedKvPair kvPair(jenv, retKv);

    if (!kvPair.isValid()) {
        return NULL;
    }

    return kvPair.getMdbmKvPair();
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_next_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmKvPair;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1next_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject,
        jobject iterObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    kvpair retKv = mdbm_next_r(mdbm, iter);

    // need to parse a kvpair now and return the value
    ScopedKvPair kvPair(jenv, retKv);

    if (!kvPair.isValid()) {
        return NULL;
    }

    return kvPair.getMdbmKvPair();

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_firstkey_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmDatum;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1firstkey_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject,
        jobject iterObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    datum d = mdbm_firstkey_r(mdbm, iter);

    // need to parse a kvpair now and return the value
    ScopedDatum datum(jenv, d);

    if (!datum.isValid()) {
        return NULL;
    }

    return datum.getMdbmDatum();

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_nextkey_r
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;Lcom/yahoo/db/mdbm/internal/NativeMdbmIterator;)Lcom/yahoo/db/mdbm/MdbmDatum;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1nextkey_1r(
        JNIEnv *jenv, jclass thisClass, jobject thisObject,
        jobject iterObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    // it's fine for iter to be null
    MDBM_ITER *iter = getIterPointer(jenv, iterObject);

    datum d = mdbm_nextkey_r(mdbm, iter);

    // need to parse a kvpair now and return the value
    ScopedDatum datum(jenv, d);

    if (!datum.isValid()) {
        return NULL;
    }

    return datum.getMdbmDatum();

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_size
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1size(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_size(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_limit_size
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1limit_1size(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_limit_size(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_page_size
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)I
 */
JNIEXPORT jint
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1page_1size(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_page_size(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_hash
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)I
 */
JNIEXPORT jint
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1hash(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_hash(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_alignment
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)I
 */
JNIEXPORT jint
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1alignment(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_alignment(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_version
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1version(
        JNIEnv *jenv, jclass thisClass, jobject thisObject) {

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    return mdbm_get_version(mdbm);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_dup_handle
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;I)Lcom/yahoo/db/mdbm/internal/NativeMdbmImplementation;
 */
JNIEXPORT jobject
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1dup_1handle(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jint) {
    ThrowException(jenv, UNSUPPORTED_OPERATION_EXCEPTION,
            "mdbm_dup_handle isn't supported");

    return NULL;
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    mdbm_get_hash_value
 * Signature: (Lcom/yahoo/db/mdbm/MdbmDatum;I)J
 */
JNIEXPORT jlong
JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_mdbm_1get_1hash_1value(
        JNIEnv *jenv, jclass thisClass, jobject keyObject,
        jint hashFunctionCode) {
    RETURN_FALSE_AND_THROW_IF_NULL(keyObject, "keyObject was null");

    ScopedDatum keyDatum(jenv, keyObject);
    if (!keyDatum.isValid()) {
        // somewhere during construction it threw an exception, so propagate that.
        return 0;
    }

    uint32_t hashValue = 0;
    int ret = mdbm_get_hash_value(*keyDatum.getDatum(), hashFunctionCode,
            &hashValue);
    checkZeroIsOkReturn(jenv, ret, MDBM_EXCEPTION, "mdbm_get_hash_value", 0,
            NULL);

    return hashValue;
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    create_pool
 * Signature: (Lcom/yahoo/db/mdbm/MdbmInterface;I)Lcom/yahoo/db/mdbm/MdbmPoolInterface;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_create_1pool(
        JNIEnv *jenv, jclass thisClass, jobject thisObject, jint poolSize) {
    RETURN_NULL_AND_THROW_IF_NULL(thisObject, "thisObject was null");
    RETURN_NULL_AND_THROW_IF_NULL(poolSize, "poolSize was null");

    GET_CACHED_CLASS(jenv, nativeMdbmPoolImplementationClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmPoolImplementationCtorId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationCtorId);

    MDBM *mdbm = getMdbmPointer(jenv, thisObject);
    RETURN_NULL_AND_THROW_IF_NULL(mdbm, "mdbm was null");

    mdbm_pool_t* pool = mdbm_pool_create_pool(mdbm, poolSize);
    if (NULL == pool) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg), "mdbm_pool_create_pool, failed errno=%s (%d)",
                strerror(errno), errno);

        ThrowMdbmException(jenv, MDBM_CREATE_POOL_FAILED_EXCEPTION,
                mdbmCreatePoolFailedExceptionClass,
                mdbmCreatePoolFailedExceptionCtorId, mesg, thisObject);
        return NULL;
    }

    CONTEXT_TO_POINTER(pool);

    return jenv->NewObject(nativeMdbmPoolImplementationClass,
            nativeMdbmPoolImplementationCtorId, thisObject, pointer, poolSize);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    destroy_pool
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_destroy_1pool
(JNIEnv *jenv, jclass thisClass, jlong pointer)
{
    if (0 == pointer) {
        return;
    }

    POINTER_TO_CONTEXT (mdbm_pool_t);
    mdbm_pool_destroy_pool(context);
}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    acquire_handle
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmPoolImplementation;)Lcom/yahoo/db/mdbm/MdbmInterface;
 */
JNIEXPORT jobject JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_acquire_1handle(
        JNIEnv *jenv, jclass thisClass, jobject poolObject) {
    RETURN_NULL_AND_THROW_IF_NULL(poolObject, "poolObject was null");

    GET_CACHED_CLASS(jenv, uncloseableMdbmClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (uncloseableMdbmClass);

    GET_CACHED_METHOD_ID(jenv, uncloseableMdbmCtorId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (uncloseableMdbmCtorId);

    GET_CACHED_CLASS(jenv, pooledMdbmHandleClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (pooledMdbmHandleClass);

    GET_CACHED_METHOD_ID(jenv, pooledMdbmHandleCtorId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (pooledMdbmHandleCtorId);

    GET_CACHED_CLASS(jenv, nativeMdbmPoolImplementationClass);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationClass);

    GET_CACHED_METHOD_ID(jenv, nativeMdbmPoolImplementationGetPathId);
    RETURN_NULL_IF_EXCEPTION_OR_NULL (nativeMdbmPoolImplementationGetPathId);

    mdbm_pool_t *pool = getPoolPointer(jenv, poolObject);
    if (NULL == pool) {
        return NULL;
    }

    MDBM *mdbm = mdbm_pool_acquire_handle(pool);
    if (NULL == mdbm) {
        char mesg[2048] = { 0, };
        snprintf(mesg, sizeof(mesg),
                "mdbm_pool_acquire_handle, failed errno=%s (%d)", strerror(errno),
                errno);

        ThrowException(jenv, MDBM_POOL_ACQUIRE_HANDLE_FAILED_EXCEPTION, mesg);
        return NULL;
    }

    // we also need the path.
    jobject path = jenv->CallObjectMethod(poolObject,
            nativeMdbmPoolImplementationGetPathId);

    // otherwise we have an open db, create it.
    CONTEXT_TO_POINTER(mdbm);

    jobject uncloseableMdbm = jenv->NewObject(uncloseableMdbmClass,
            uncloseableMdbmCtorId, pointer, path, 0);
    RETURN_NULL_IF_EXCEPTION_OR_NULL(uncloseableMdbm);

    return jenv->NewObject(pooledMdbmHandleClass, pooledMdbmHandleCtorId,
            poolObject, uncloseableMdbm);

}

/*
 * Class:     com_yahoo_db_mdbm_internal_NativeMdbmAccess
 * Method:    release_handle
 * Signature: (Lcom/yahoo/db/mdbm/internal/NativeMdbmPoolImplementation;Lcom/yahoo/db/mdbm/MdbmInterface;)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_db_mdbm_internal_NativeMdbmAccess_release_1handle
(JNIEnv *jenv, jclass thisClass, jobject poolObject, jobject thisObject)
{
    RETURN_AND_THROW_IF_NULL(poolObject, "poolObject was null");
    RETURN_AND_THROW_IF_NULL(thisObject, "thisObject was null");

    mdbm_pool_t *pool = getPoolPointer(jenv, poolObject);
    if (NULL == pool) {
        return;
    }

    MDBM *mdbm = getPooledMdbmPointer(jenv, thisObject);
    if (NULL == mdbm) {
        return;
    }

    mdbm_pool_release_handle(pool, mdbm);
}
