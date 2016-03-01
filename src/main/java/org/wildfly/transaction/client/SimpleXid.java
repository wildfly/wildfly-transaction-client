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

import java.util.Arrays;

import javax.transaction.xa.Xid;

/**
 * A special simplified XID implementation which can only be compared with itself.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class SimpleXid implements Xid {

    /**
     * An empty XID array.
     */
    public static final Xid[] NO_XIDS = new Xid[0];

    /**
     * An empty {@code SimpleXid} array.
     */
    public static final SimpleXid[] NO_SIMPLE_XIDS = new SimpleXid[0];

    private static final byte[] NO_BYTES = new byte[0];

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
}
