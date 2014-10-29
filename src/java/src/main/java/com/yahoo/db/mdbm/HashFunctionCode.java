/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import com.yahoo.db.mdbm.exceptions.MdbmException;

public enum HashFunctionCode {
    MDBM_HASH_CRC32(0), // < table based 32bit crc
    MDBM_HASH_EJB(1), // < from hsearch
    MDBM_HASH_PHONG(2), // < congruential hash
    MDBM_HASH_OZ(3), // < from sdbm
    MDBM_HASH_TOREK(4), // < from Berkeley db
    MDBM_HASH_FNV(5), // < Fowler/Vo/Noll hash
    MDBM_HASH_STL(6), // < STL string hash
    MDBM_HASH_MD5(7), // < MD5
    MDBM_HASH_SHA_1(8), // < SHA_1
    MDBM_HASH_JENKINS(9), // < JENKINS
    MDBM_HASH_HSIEH(10), // < HSIEH SuperFastHash
    MDBM_MAX_HASH(10); // bump up if adding more

    public final int v;

    private HashFunctionCode(int v) {
        this.v = v;
    }

    public static HashFunctionCode valueOf(int hashInt) throws MdbmException {
        switch (hashInt) {
            case 0:
                return MDBM_HASH_CRC32;

            case 1:
                return MDBM_HASH_EJB;

            case 2:
                return MDBM_HASH_PHONG;

            case 3:
                return MDBM_HASH_OZ;

            case 4:
                return MDBM_HASH_TOREK;

            case 5:
                return MDBM_HASH_FNV;

            case 6:
                return MDBM_HASH_STL;

            case 7:
                return MDBM_HASH_MD5;

            case 8:
                return MDBM_HASH_SHA_1;

            case 9:
                return MDBM_HASH_JENKINS;

            case 10:
                return MDBM_HASH_HSIEH;

            default:
                throw new MdbmException("Unknown hashFunction: " + hashInt);
        }
    }
}
