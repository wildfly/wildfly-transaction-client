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

package org.wildfly.transaction.client;

import static java.lang.Math.max;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.Xid;

import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;

/**
 * A handle for the outflow of an XA transactional resource.
 * <p>
 * In certain circumstances, if all outflowing transactions forget the enlistment or it is owned by another node,
 * the transaction may be resolved at prepare directly with an {@code XA_RDONLY} status, or it may be completely
 * removed from the transaction before it completes, allowing more efficient transaction resolution as well as
 * correct behavior in the presence of outflow cycles.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface XAOutflowHandle {
    /**
     * Get an estimated remaining timeout for this transaction.
     *
     * @return the timeout, in seconds (0 indicates that the timeout has likely already expired)
     */
    int getRemainingTime();

    /**
     * Signal that this enlistment may be forgotten.
     */
    void forgetEnlistment();

    /**
     * Signal that the peer already had the transaction in question.  It may have been created previously by this
     * node, or by another node.  When there are no outstanding handles, if the corresponding subordinate enlistment
     * has not been verified, and this method was called at least once, then the enlistment may be assumed to be
     * owned by someone else and subsequent outstanding handles will not prevent the transaction from completing.
     */
    void nonMasterEnlistment();

    /**
     * Signal that this enlistment should be activated effective immediately.  If any outflowing transactions verify
     * the enlistment, then the enlistment is verified for all.
     *
     * @throws RollbackException if the transaction was rolled back before the enlistment could be activated
     * @throws SystemException if the enlistment failed for an unexpected reason
     */
    void verifyEnlistment() throws RollbackException, SystemException;

    /**
     * Get the XID of the transaction.
     *
     * @return the XID of the transaction (must not be {@code null})
     */
    @NotNull
    Xid getXid();

    /**
     * Create a simple, no-operation outflow handle.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @param timeout the estimated remaining time, in seconds
     * @return the outflow handle (not {@code null})
     */
    static XAOutflowHandle createSimple(Xid xid, int timeout) {
        Assert.checkNotNullParam("xid", xid);
        Assert.checkMinimumParameter("timeout", 0, timeout);
        return new XAOutflowHandle() {
            private long start = System.nanoTime();
            @NotNull
            public Xid getXid() {
                return xid;
            }

            public int getRemainingTime() {
                long elapsed = System.nanoTime() - start;
                return timeout - (int) max(timeout, elapsed / 1_000_000_000L);
            }

            public void forgetEnlistment() {
            }

            public void nonMasterEnlistment() {
            }

            public void verifyEnlistment() {
            }
        };
    }
}
