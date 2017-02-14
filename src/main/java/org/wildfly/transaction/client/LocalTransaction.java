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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client._private.Log;

/**
 * A transaction from a local transaction provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class LocalTransaction extends AbstractTransaction {
    private final LocalTransactionContext owner;
    private final Transaction transaction;

    LocalTransaction(final LocalTransactionContext owner, final Transaction transaction) {
        super();
        this.owner = owner;
        this.transaction = transaction;
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
        if (isImported()) {
            throw Log.log.commitOnImported();
        }
        // ensure we're not associated with the transaction
        try {
            final TransactionManager transactionManager = owner.getProvider().getTransactionManager();
            if (transaction.equals(transactionManager.getTransaction())) {
                transactionManager.commit();
            } else {
                owner.getProvider().commitLocal(transaction);
            }
        } finally {
            final XAOutflowedResources outflowedResources = RemoteTransactionContext.getOutflowedResources(this);
            if (outflowedResources == null || outflowedResources.getEnlistedSubordinates() == 0) {
                // we can drop the mapping, since we are both a master and have no enlisted subordinates
                owner.getProvider().dropLocal(transaction);
            }
        }
    }

    public void rollback() throws IllegalStateException, SystemException {
        if (isImported()) {
            throw Log.log.rollbackOnImported();
        }
        // ensure we're not associated with the transaction
        final TransactionManager transactionManager = owner.getProvider().getTransactionManager();
        if (transaction.equals(transactionManager.getTransaction())) {
            transactionManager.rollback();
        } else {
            owner.getProvider().rollbackLocal(transaction);
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        transaction.setRollbackOnly();
    }

    public int getStatus() throws SystemException {
        return transaction.getStatus();
    }

    public int getTransactionTimeout() {
        return owner.getProvider().getTimeout(transaction);
    }

    public boolean enlistResource(final XAResource xaRes) throws RollbackException, IllegalStateException, SystemException {
        Assert.checkNotNullParam("xaRes", xaRes);
        return transaction.enlistResource(xaRes);
    }

    public boolean delistResource(final XAResource xaRes, final int flag) throws IllegalStateException, SystemException {
        Assert.checkNotNullParam("xaRes", xaRes);
        return transaction.delistResource(xaRes, flag);
    }

    public void registerSynchronization(final Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
        Assert.checkNotNullParam("sync", sync);
        transaction.registerSynchronization(sync);
    }

    /**
     * Get the name of the node which initiated the transaction.
     *
     * @return the name of the node which initiated the transaction, or {@code null} if it could not be determined
     */
    public String getParentName() {
        return owner.getProvider().getNameFromXid(owner.getProvider().getXid(transaction));
    }

    /**
     * Get the XID of the local transaction.
     *
     * @return the transaction XID (not {@code null})
     */
    public Xid getXid() {
        return owner.getProvider().getXid(transaction);
    }

    void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
        Assert.checkNotNullParam("sync", sync);
        owner.getProvider().registerInterposedSynchronization(transaction, sync);
    }

    Object getResource(final Object key) throws NullPointerException {
        return owner.getProvider().getResource(transaction, Assert.checkNotNullParamWithNullPointerException("key", key));
    }

    void putResource(final Object key, final Object value) throws NullPointerException {
        owner.getProvider().putResource(transaction, Assert.checkNotNullParamWithNullPointerException("key", key), value);
    }

    Object getKey() {
        return owner.getProvider().getKey(transaction);
    }

    boolean getRollbackOnly() {
        return owner.getProvider().getRollbackOnly(transaction);
    }

    void suspend() throws SystemException {
        notifyAssociationListeners(false);
        TransactionManager transactionManager = owner.getProvider().getTransactionManager();
        if (! transaction.equals(transactionManager.getTransaction())) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManager.getTransaction());
        }
        final Transaction transactionManagerTransaction = transactionManager.suspend();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
    }

    void resume() throws SystemException {
        TransactionManager transactionManager = owner.getProvider().getTransactionManager();
        try {
            transactionManager.resume(transaction);
        } catch (InvalidTransactionException e) {
            // should be impossible
            throw Log.log.invalidTxnState();
        }
        final Transaction transactionManagerTransaction = transactionManager.getTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
        notifyAssociationListeners(true);
    }

    void verifyAssociation() {
        TransactionManager transactionManager = owner.getProvider().getTransactionManager();
        try {
            final Transaction transactionManagerTransaction = transactionManager.getTransaction();
            if (! transaction.equals(transactionManagerTransaction)) {
                throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
            }
        } catch (SystemException e) {
            // should be impossible
            throw Log.log.invalidTxnState();
        }
    }

    /**
     * Determine if this transaction was imported.
     *
     * @return {@code true} if the transaction was imported, {@code false} if it was initiated locally
     */
    public boolean isImported() {
        return owner.getProvider().isImported(transaction);
    }

    public <T> T getProviderInterface(final Class<T> providerInterfaceType) {
        return owner.getProvider().getProviderInterface(transaction, providerInterfaceType);
    }

    public int hashCode() {
        return transaction.hashCode();
    }

    public boolean equals(final Object obj) {
        return obj instanceof LocalTransaction && equals((LocalTransaction) obj);
    }

    private boolean equals(final LocalTransaction obj) {
        return this == obj || obj != null && transaction.equals(obj.transaction);
    }

    public String toString() {
        return String.format("Local transaction (delegate=%s, owner=%s)", transaction, owner);
    }
}
