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

package org.wildfly.transaction.client.spi;

import jakarta.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.wildfly.common.annotation.NotNull;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface RemoteTransactionPeer {
    /**
     * Look up an outflow handle for a remote transaction with the given XID.  The remote transaction is only looked up; no
     * remote action should take place as a result of this call beyond correlating the XID in a protocol-specific manner.
     * The given XID should only be examined for the global transaction ID; the branch ID should be ignored (and will
     * usually be empty in any event).
     *
     * @param xid the transaction ID
     * @return the handle for the remote transaction
     * @throws XAException if the lookup failed for some reason
     */
    @NotNull
    SubordinateTransactionControl lookupXid(Xid xid) throws XAException;

    /**
     * Acquire a list of all unresolved subordinate transactions from the location associated with this provider.
     *
     * @param flag the recovery flag
     * @param parentName the parent node name
     * @return the (possibly empty) XID array
     * @throws XAException if an error occurs
     */
    @NotNull
    Xid[] recover(int flag, String parentName) throws XAException;

    /**
     * Establish a remote user-controlled transaction without local enlistment.
     *
     * @param timeout the timeout of the transaction in seconds
     * @return the transaction handle (must not be {@code null})
     * @throws SystemException if an unexpected error occurs
     */
    @NotNull
    SimpleTransactionControl begin(int timeout) throws SystemException;

    /**
     * Get the provider interface with the given type for this peer.
     *
     * @param clazz the provider interface type class (must not be {@code null})
     * @param <T> the provider interface type
     * @return the provider interface with the given type, or {@code null} if no such interface is supported
     */
    default <T> T getProviderInterface(Class<T> clazz) {
        return null;
    }
}
