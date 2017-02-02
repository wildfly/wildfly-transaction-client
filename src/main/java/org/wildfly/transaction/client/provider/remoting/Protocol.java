/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2016 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wildfly.transaction.client.provider.remoting;

import static org.jboss.remoting3.util.StreamUtils.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import javax.transaction.xa.Xid;

import org.wildfly.transaction.client.SimpleXid;
import org.xnio.streams.LimitedInputStream;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class Protocol {
    public static final boolean SIGNED = true;
    public static final boolean UNSIGNED = false;

    public static final int VERSION_MIN = 0;
    public static final int VERSION_MAX = 0;

    // all msgs are initiated by the client
    // msg format
    // byte 0-1: inv ID
    // byte 2: M_ message type
    // byte 3...: P_ parameters

    // parameter format
    // byte 0: param ID
    // byte 1-x: length (packed integer)
    // byte x...: parameter payload of exactly length bytes

    // client -> server

    // Add capabilities; server only replies with acknowledged capabilities (may be empty) (error not raised on unrecognized)
    public static final int M_CAPABILITY    = 0x00; // P_*

    // unused                               = 0x01
    // Roll back the transaction with the given XID
    public static final int M_XA_ROLLBACK   = 0x02; // P_XID(gtid) [ P_SEC_CONTEXT ]
    // Prepare the transaction with the given XID
    public static final int M_XA_PREPARE    = 0x03; // P_XID(gtid) [ P_SEC_CONTEXT ]
    // Commit the transaction with the given XID
    public static final int M_XA_COMMIT     = 0x04; // P_XID(gtid) [ P_SEC_CONTEXT ] [ P_ONE_PHASE ]
    // Forget the transaction with the given XID
    public static final int M_XA_FORGET     = 0x05; // P_XID(gtid) [ P_SEC_CONTEXT ]
    // Execute before-completion for the transaction with the given XID
    public static final int M_XA_BEFORE     = 0x06; // P_XID(gtid) [ P_SEC_CONTEXT ]
    // Get a list of XIDs to recover
    public static final int M_XA_RECOVER    = 0x07; // [ P_SEC_CONTEXT ] [ P_PARENT_NAME ]
    // Mark the XA transaction as rollback-only; used if the resource was called with TMFAIL
    public static final int M_XA_RB_ONLY    = 0x08; // P_XID(gtid) [ P_SEC_CONTEXT ]
    // Unused
    // unused                               = 0x09;
    // TXN_CONTEXT is released (even for error)
    public static final int M_UT_COMMIT     = 0x0A; // P_TXN_CONTEXT [ P_SEC_CONTEXT ]
    // TXN_CONTEXT is released (even for error)
    public static final int M_UT_ROLLBACK   = 0x0B; // P_TXN_CONTEXT [ P_SEC_CONTEXT ]

    // server -> client

    public static final int M_RESP_CAPABILITY   = 0x00; // P_*

    public static final int M_RESP_XA_BEGIN     = 0x11; // [ P_XA_ERROR | P_SEC_EXC ]
    public static final int M_RESP_XA_ROLLBACK  = 0x12; // [ P_XA_ERROR | P_SEC_EXC ]
    public static final int M_RESP_XA_PREPARE   = 0x13; // [ P_XA_RDONLY | P_XA_ERROR | P_SEC_EXC ]
    public static final int M_RESP_XA_COMMIT    = 0x14; // [ P_XA_ERROR | P_SEC_EXC ]
    public static final int M_RESP_XA_FORGET    = 0x15; // [ P_XA_ERROR | P_SEC_EXC ]
    public static final int M_RESP_XA_BEFORE    = 0x16; // [ P_XA_ERROR | P_SEC_EXC ]

    public static final int M_RESP_XA_RECOVER   = 0x17; // P_XID... | P_XA_ERROR | P_SEC_EXC

    public static final int M_RESP_UT_BEGIN     = 0x18; // [ P_UT_SYS_EXC | P_SEC_EXC ]
    public static final int M_RESP_UT_COMMIT    = 0x19; // [ P_UT_RB_EXC | P_UT_HME_EXC | P_UT_HRE_EXC | P_UT_SYS_EXC | P_SEC_EXC ]
    public static final int M_RESP_UT_ROLLBACK  = 0x1A; // [ P_UT_SYS_EXC | P_SEC_EXC ]

    public static final int M_RESP_PARAM_ERROR  = 0xFE; // empty (missing required or found unknown parameter)
    public static final int M_RESP_ERROR        = 0xFF; // empty (unknown request code)

    // parameters

    // unused                                 0x00
    public static final int P_XID           = 0x01; // body = XID
    public static final int P_ONE_PHASE     = 0x02; // len=0
    public static final int P_PARENT_NAME   = 0x03; // body = utf8
    // unused                                 0x04
    // unused                                 0x05
    public static final int P_TXN_TIMEOUT   = 0x06; // body = packed-int timeout (seconds)
    public static final int P_XA_RDONLY     = 0x07; // len=0

    public static final int P_UT_RB_EXC     = 0x10; // RollbackException
    public static final int P_UT_HME_EXC    = 0x11; // HeuristicMixedException
    public static final int P_UT_HRE_EXC    = 0x12; // HeuristicRollbackException
    public static final int P_UT_SYS_EXC    = 0x13; // SystemException
    public static final int P_UT_IS_EXC     = 0x14; // IllegalStateException

    public static final int P_SEC_EXC       = 0x20; // SecurityException

    public static final int P_XA_ERROR      = 0x30; // body = packed-int (signed) XA error code

    public static final int P_VERSION_ERROR = 0x40; // additional capabilities must be negotiated (s -> c)

    public static final int P_SEC_CONTEXT   = 0xF0; // uint32 security context association ID
    public static final int P_TXN_CONTEXT   = 0xF1; // uint32 transaction context association ID

    public static void writeParam(int param, OutputStream os, byte val, @SuppressWarnings("unused") boolean signed) throws IOException {
        writeInt8(os, param);
        if (val == 0) {
            writeInt8(os, 0);
        } else {
            writeInt8(os, 1);
            writeInt8(os, val);
        }
    }


    public static void writeParam(int param, OutputStream os, short val, boolean signed) throws IOException {
        writeInt8(os, param);
        final int len;
        if (signed && val < 0) {
            // always write one sign byte if needed
            len = 32 - Integer.numberOfLeadingZeros(~val) + 8 >> 3;
        } else {
            len = 32 - Integer.numberOfLeadingZeros(val) + 7 >> 3;
        }
        writeInt8(os, len);
        switch (len) {
            case 2: writeInt8(os, val >> 8);
            case 1: writeInt8(os, val >> 0);
        }
    }

    public static void writeParam(int param, OutputStream os, int val, boolean signed) throws IOException {
        writeInt8(os, param);
        final int len;
        if (signed && val < 0) {
            // always write one sign byte if needed
            len = 32 - Integer.numberOfLeadingZeros(~val) + 8 >> 3;
        } else {
            len = 32 - Integer.numberOfLeadingZeros(val) + 7 >> 3;
        }
        writeInt8(os, len);
        switch (len) {
            case 4: writeInt8(os, val >> 24);
            case 3: writeInt8(os, val >> 16);
            case 2: writeInt8(os, val >> 8);
            case 1: writeInt8(os, val >> 0);
        }
    }

    public static void writeParam(int param, OutputStream os, long val, boolean signed) throws IOException {
        writeInt8(os, param);
        final int len;
        if (signed && val < 0) {
            // always write one byte for -1
            len = 64 - Long.numberOfLeadingZeros(~val) + 8 >> 3;
        } else {
            len = 64 - Long.numberOfLeadingZeros(val) + 7 >> 3;
        }
        writeInt8(os, len);
        switch (len) {
            case 8: writeInt8(os, val >> 56);
            case 7: writeInt8(os, val >> 48);
            case 6: writeInt8(os, val >> 40);
            case 5: writeInt8(os, val >> 32);
            case 4: writeInt8(os, val >> 24);
            case 3: writeInt8(os, val >> 16);
            case 2: writeInt8(os, val >> 8);
            case 1: writeInt8(os, val >> 0);
        }
    }

    public static void writeParam(int param, OutputStream os, byte[] bytes) throws IOException {
        writeInt8(os, param);
        if (bytes == null || bytes.length == 0) {
            writeInt8(os, 0);
        } else {
            writePackedUnsignedInt32(os, bytes.length);
            os.write(bytes);
        }
    }

    public static void writeParam(int param, OutputStream os, String str) throws IOException {
        if (str == null || str.isEmpty()) {
            writeParam(param, os);
        } else {
            writeParam(param, os, str.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void writeParam(int param, OutputStream os) throws IOException {
        writeInt8(os, param);
        writeInt8(os, 0);
    }

    public static void writeParam(int param, OutputStream os, Xid xid) throws IOException {
        if (xid == null) {
            writeParam(param, os);
            return;
        }
        final int formatId = xid.getFormatId();
        final byte[] gtid = xid.getGlobalTransactionId();
        final byte[] bq = xid.getBranchQualifier();
        if (gtid.length > Xid.MAXGTRIDSIZE || bq.length > Xid.MAXBQUALSIZE) {
            throw new IOException("Cannot write invalid XID");
        }
        final int len = gtid.length + bq.length + 5;
        //noinspection PointlessBooleanExpression,ConstantConditions
        assert Xid.MAXGTRIDSIZE <= 64 && Xid.MAXBQUALSIZE <= 64 && len < 256;
        // might be > 127
        writeInt8(os, param);
        writePackedUnsignedInt32(os, len);
        writeInt32BE(os, formatId);
        writeInt8(os, gtid.length);
        os.write(gtid);
        os.write(bq);
    }

    public static int readIntParam(InputStream is, int len) throws IOException {
        int t = 0;
        for (int i = 0; i < len; i ++) {
            t = t << 8 | readInt8(is) & 0xff;
        }
        return t;
    }

    public static String readStringParam(InputStream is, int len) throws IOException {
        byte[] b = new byte[len];
        readFully(is, b);
        return new String(b, StandardCharsets.UTF_8);
    }

    public static SimpleXid readXid(InputStream is, int len) throws IOException {
        final LimitedInputStream lis = new LimitedInputStream(is, len);
        final int formatId = readPackedUnsignedInt32(lis);
        final byte[] gtid = new byte[readInt8(lis)];
        readFully(lis, gtid);
        final byte[] bq = new byte[len - 5 - gtid.length];
        readFully(lis, bq);
        return new SimpleXid(formatId, gtid, bq);
    }
}
