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

import javax.resource.spi.XATerminator;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * The API which must be implemented by transaction providers that support transaction inflow.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface XAImporter extends XATerminator, XARecoverable {
    /**
     * Import a transaction.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @param timeout the remaining transaction timeout
     * @return the imported transaction (must not be {@code null})
     * @throws XAException if the import failed for some reason
     */
    Transaction beginImported(Xid xid, int timeout) throws XAException;

    /**
     * Perform before-completion processing.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @throws XAException if the import failed for some reason
     */
    void beforeComplete(Xid xid) throws XAException;

    /**
     * Commit an imported (typically prepared) transaction.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @param onePhase {@code true} to perform prepare and commit in one operation, {@code false} otherwise
     * @throws XAException if the operation fails for some reason
     */
    void commit(Xid xid, boolean onePhase) throws XAException;

    /**
     * Forget an imported, prepared transaction.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @throws XAException if the operation fails for some reason
     */
    void forget(Xid xid) throws XAException;

    /**
     * Prepare an imported transaction.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @return {@link XAResource#XA_OK} if the prepare is successful or {@link XAResource#XA_RDONLY} if there is no
     *  commit phase necessary
     * @throws XAException if the operation fails for some reason
     */
    int prepare(Xid xid) throws XAException;

    /**
     * Roll back an imported transaction.
     *
     * @param xid the transaction ID (must not be {@code null})
     * @throws XAException if the operation fails for some reason
     */
    void rollback(Xid xid) throws XAException;

    /**
     * Initiate a recovery scan.
     *
     * @param flag one of {@link XAResource#TMSTARTRSCAN}, {@link XAResource#TMNOFLAGS}, or {@link XAResource#TMENDRSCAN}
     * @return the array of recovered XIDs (may be {@link SimpleXid#NO_XIDS}, must not be {@code null})
     * @throws XAException if the operation fails for some reason
     */
    Xid[] recover(int flag) throws XAException;
}
