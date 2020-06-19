/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015 Red Hat, Inc., and individual contributors
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

package org.wildfly.transaction.client;

import org.wildfly.transaction.client._private.Log;

import static java.lang.Integer.signum;
import static java.lang.Math.min;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import javax.transaction.xa.Xid;

/**
 * A special simplified XID implementation which can only be compared with itself.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class SimpleXid implements Xid, Comparable<SimpleXid> {

    /**
     * An empty byte array used when there is an empty XID component.
     */
    public static final byte[] NO_BYTES = new byte[0];

    /**
     * An empty XID array.
     */
    public static final Xid[] NO_XIDS = new Xid[0];

    /**
     * An empty {@code SimpleXid} array.
     */
    public static final SimpleXid[] NO_SIMPLE_XIDS = new SimpleXid[0];

    /**
     * A completely empty {@code SimpleXid}, which sorts below all other {@code SimpleXid} instances.
     */
    public static final SimpleXid EMPTY = new SimpleXid(0, NO_BYTES, NO_BYTES, false);

    private static final char DEFAULT_SEPARATOR = ':';

    private final int formatId;
    private final byte[] globalId;
    private final byte[] branchId;
    private final int hashCode;

    public SimpleXid(final int formatId, final byte[] gtId, final byte[] bq) {
        this(formatId, gtId, bq, true);
    }

    private SimpleXid(final int formatId, final byte[] gtId, final byte[] bq, boolean clone) {
        this.formatId = formatId;
        final int globalIdLength = gtId.length;
        final int branchIdLength = bq.length;
        globalId = globalIdLength > 0 ? clone ? gtId.clone() : gtId : NO_BYTES;
        branchId = branchIdLength > 0 ? clone ? bq.clone() : bq : NO_BYTES;
        this.hashCode = (formatId * 31 + Arrays.hashCode(globalId)) * 31 + Arrays.hashCode(branchId);
    }

    public int getFormatId() {
        return formatId;
    }

    public byte[] getGlobalTransactionId() {
        byte[] globalId = this.globalId;
        return globalId.length == 0 ? globalId : globalId.clone();
    }

    public byte[] getBranchQualifier() {
        byte[] branchId = this.branchId;
        return branchId.length == 0 ? branchId : branchId.clone();
    }

    public boolean equals(final Object obj) {
        return obj instanceof SimpleXid && equals((SimpleXid) obj);
    }

    public boolean equals(final SimpleXid obj) {
        return obj != null && hashCode == obj.hashCode && formatId == obj.formatId && Arrays.equals(globalId, obj.globalId) && Arrays.equals(branchId, obj.branchId);
    }

    public SimpleXid withoutBranch() {
        if (branchId.length == 0) {
            return this;
        } else {
            return new SimpleXid(formatId, globalId, NO_BYTES, false);
        }
    }

    public int hashCode() {
        return hashCode;
    }

    public static SimpleXid of(final Xid xid) {
        return xid instanceof SimpleXid ? (SimpleXid) xid : new SimpleXid(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
    }

    public static SimpleXid of(final String xidHexString, char separator) {
        String[] xidParts = xidHexString.split(String.valueOf(separator));
        if (xidParts.length != 3) throw Log.log.failToConvertHexadecimalFormatToSimpleXid(xidHexString, String.valueOf(separator));
        return new SimpleXid(Integer.parseUnsignedInt(xidParts[0],16), hexStringToByteArray(xidParts[1]), hexStringToByteArray(xidParts[2]));
    }

    public static SimpleXid of(final String xidHexString) {
        return SimpleXid.of(xidHexString, DEFAULT_SEPARATOR);
    }

    public int compareTo(final SimpleXid o) {
        int res = signum(formatId - o.formatId);
        if (res == 0) res = compareByteArrays(globalId, o.globalId);
        if (res == 0) res = compareByteArrays(branchId, o.branchId);
        assert (res == 0) == equals(o);
        return res;
    }

    public String toHexString() {
        return toHexString(DEFAULT_SEPARATOR);
    }

    public String toHexString(char separator) {
        StringBuilder b = new StringBuilder();
        toHexString(b, separator);
        return b.toString();
    }

    private void toHexString(StringBuilder builder, char separator) {
        builder.append(Integer.toHexString(formatId)).append(separator);
        for (final byte x : globalId) {
            final int v = x & 0xff;
            if (v < 16) {
                builder.append('0');
            }
            builder.append(Integer.toHexString(v));
        }
        builder.append(separator);
        for (final byte x : branchId) {
            final int v = x & 0xff;
            if (v < 16) {
                builder.append('0');
            }
            builder.append(Integer.toHexString(v));
        }
    }

    private static int compareByteArrays(byte[] a1, byte[] a2) {
        final int l1 = a1.length;
        final int l2 = a2.length;
        int minLen = min(l1, l2);
        int res;
        for (int i = 0; i < minLen; i ++) {
            res = signum((a1[i] & 0xff) - (a2[i] & 0xff));
            if (res != 0) return res;
        }
        return signum(l1 - l2);
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len/2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("XID [");
        toHexString(b, DEFAULT_SEPARATOR);
        b.append(']');
        return b.toString();
    }
}
