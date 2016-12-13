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

import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;

/**
 * The API which must be implemented by transaction providers that support transaction inflow.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface XAImporter extends XATerminator, XARecoverable {
    /**
     * Import a transaction.  If the transaction already exists, it should be returned, otherwise a new transaction
     * should be initiated with the given timeout (in seconds).
     *
     * @param xid the transaction ID (must not be {@code null})
     * @param timeout the remaining transaction timeout, or 0 if the default should be used
     * @return the imported transaction (must not be {@code null})
     * @throws XAException if the import failed for some reason
     */
    @NotNull
    ImportResult findOrImportTransaction(Xid xid, int timeout) throws XAException;

    /**
     * Find an existing transaction on this system.  If no such transaction exists, {@code null} is returned.  Normally
     * the transaction is located only by global ID.
     *
     * @param xid the XID to search for (must not be {@code null})
     * @return the transaction object, or {@code null} if the transaction is not known
     * @throws XAException if the transaction manager failed for some reason
     */
    Transaction findExistingTransaction(Xid xid) throws XAException;

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
    @NotNull
    Xid[] recover(int flag) throws XAException;

    /**
     * Class representing the result of a transaction import.
     */
    class ImportResult {
        private final Transaction transaction;
        private final boolean isNew;

        /**
         * Construct a new instance.
         *
         * @param transaction the new transaction (must not be {@code null})
         * @param isNew {@code true} if the transaction was just now imported, {@code false} if the transaction already existed
         */
        public ImportResult(final Transaction transaction, final boolean isNew) {
            this.transaction = Assert.checkNotNullParam("transaction", transaction);
            this.isNew = isNew;
        }

        /**
         * Get the transaction.
         *
         * @return the transaction (not {@code null})
         */
        public Transaction getTransaction() {
            return transaction;
        }

        /**
         * Determine whether this import resulted in a new transaction.
         *
         * @return {@code true} if the transaction was new, {@code false} otherwise
         */
        public boolean isNew() {
            return isNew;
        }
    }
}
