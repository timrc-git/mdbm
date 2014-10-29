/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import java.nio.ByteBuffer;

public class BufferFun {
    static ByteBuffer bb;
    static int size = 10;

    public static void main(String args[]) {
        bb = ByteBuffer.allocateDirect(size);

        for (int i = 0; i < size; i++) {
            bb.put((byte) i);
        }

        dump(1);
        dump(2);
        arrayDump(3);
    }

    static void dump(int rev) {
        bb.rewind();
        for (int i = 0; i < bb.capacity(); i++) {
            System.out.println("Got: [" + rev + ":" + i + "]: " + bb.get());
        }
    }

    static void arrayDump(int rev) {
        bb.rewind();
        byte[] dst = new byte[size];
        bb.get(dst, 0, size);

        bb.rewind();
        for (int i = 0; i < dst.length; i++) {
            System.out.println("Got: [" + rev + ":" + i + "]: " + bb.get());
        }
        dump(4);
    }
}
