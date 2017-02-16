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

import static java.security.AccessController.doPrivileged;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.common.context.ContextManager;
import org.wildfly.common.context.Contextual;
import org.wildfly.transaction.TransactionPermission;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;

/**
 * The local transaction context, which manages the creation and usage of local transactions.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class LocalTransactionContext implements Contextual<LocalTransactionContext> {
    private static final ContextManager<LocalTransactionContext> CONTEXT_MANAGER = new ContextManager<LocalTransactionContext>(LocalTransactionContext.class, "org.wildfly.transaction.client.context.local");
    private static final Supplier<LocalTransactionContext> PRIVILEGED_SUPPLIER = doPrivileged((PrivilegedAction<Supplier<LocalTransactionContext>>) CONTEXT_MANAGER::getPrivilegedSupplier);

    private static final Object LOCAL_TXN_KEY = new Object();
    private static final TransactionPermission CREATION_LISTENER_PERMISSION = TransactionPermission.forName("registerCreationListener");
    private static final TransactionPermission SUSPEND_REQUESTS_PERMISSION = TransactionPermission.forName("suspendRequests");
    private static final TransactionPermission RESUME_REQUESTS_PERMISSION = TransactionPermission.forName("resumeRequests");

    static {
        doPrivileged((PrivilegedAction<?>) () -> {
            CONTEXT_MANAGER.setGlobalDefault(new LocalTransactionContext(LocalTransactionProvider.EMPTY));
            return null;
        });
    }

    private final LocalTransactionProvider provider;

    private final List<CreationListener> creationListeners = new CopyOnWriteArrayList<>();

    private volatile boolean requestsSuspended;

    /**
     * Construct a new instance.  The given provider will be used to manage local transactions.
     *
     * @param provider the local transaction provider
     */
    public LocalTransactionContext(final LocalTransactionProvider provider) {
        Assert.checkNotNullParam("provider", provider);
        this.provider = provider;
    }

    /**
     * Get the context manager.
     *
     * @return the context manager (not {@code null})
     */
    @NotNull
    public static ContextManager<LocalTransactionContext> getContextManager() {
        return CONTEXT_MANAGER;
    }

    /**
     * Get the context manager; delegates to {@link #getContextManager()}.
     *
     * @return the context manager (not {@code null})
     */
    @NotNull
    public ContextManager<LocalTransactionContext> getInstanceContextManager() {
        return getContextManager();
    }

    /**
     * Get the current local transaction context.  The default context does not support initiating new transactions.
     *
     * @return the current local transaction context (not {@code null})
     */
    @NotNull
    public static LocalTransactionContext getCurrent() {
        return PRIVILEGED_SUPPLIER.get();
    }

    private LocalTransaction notifyCreationListeners(LocalTransaction transaction) {
        for (CreationListener creationListener : creationListeners) {
            try {
                creationListener.transactionCreated(transaction);
            } catch (Throwable t) {
                Log.log.trace("Transaction creation listener throws an exception", t);
            }
        }
        return transaction;
    }

    /**
     * Register a transaction creation listener.
     *
     * @param creationListener the creation listener (must not be {@code null})
     */
    public void registerCreationListener(CreationListener creationListener) {
        Assert.checkNotNullParam("creationListener", creationListener);
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(CREATION_LISTENER_PERMISSION);
        }
        creationListeners.add(creationListener);
    }

    /**
     * Remove a transaction creation listener.
     *
     * @param creationListener the creation listener (must not be {@code null})
     */
    public void removeCreationListener(CreationListener creationListener) {
        Assert.checkNotNullParam("creationListener", creationListener);
        creationListeners.removeIf(c -> c == creationListener);
    }

    /**
     * Begin a new, local transaction on behalf of a local peer.
     *
     * @param timeout the transaction timeout to use for this transaction
     * @return the local transaction (not {@code null})
     * @throws SystemException if the transaction creation failed for some reason, one of the possible reasons being
     *                         suspended server
     * @throws SecurityException if the caller is not authorized to create a local transaction in this context
     */
    @NotNull
    public LocalTransaction beginTransaction(final int timeout) throws SystemException, SecurityException {
        return beginTransaction(timeout, false);
    }

    /**
     * Begin a new, local transaction on behalf of a local or remote peer.
     *
     * @param timeout the transaction timeout to use for this transaction
     * @param remote {@code true} if the peer is remote, {@code false} if the peer is local
     * @return the local transaction (not {@code null})
     * @throws SystemException if the transaction creation failed for some reason, one of the possible reasons being
     *                         suspended server
     * @throws SecurityException if the caller is not authorized to create a local transaction in this context
     */
    @NotNull
    public LocalTransaction beginTransaction(final int timeout, final boolean remote) throws SystemException, SecurityException {
        Assert.checkMinimumParameter("timeout", 0, timeout);
        if (remote && requestsSuspended) {
            throw Log.log.suspendedCannotCreateNew();
        }
        final Transaction newTransaction = provider.createNewTransaction(timeout);
        //noinspection ConstantConditions
        if (newTransaction == null) {
            throw Log.log.providerCreatedNullTransaction();
        }
        return notifyCreationListeners(new LocalTransaction(this, newTransaction));
    }

    /**
     * Attempt to import a transaction, which subsequently may be controlled by its XID or by the returned handle.
     *
     * @param xid the XID of the transaction to import (must not be {@code null})
     * @param timeout the transaction timeout to use, if new
     * @param doNotImport {@code true} to indicate that a non-existing transaction should not be imported, {@code false} otherwise
     * @return the transaction import result, or {@code null} if (and only if) {@code doNotImport} is {@code true} and the transaction didn't exist locally
     * @throws XAException if a problem occurred while importing the transaction
     */
    public ImportResult<LocalTransaction> findOrImportTransaction(Xid xid, int timeout, boolean doNotImport) throws XAException {
        Assert.checkNotNullParam("xid", xid);
        Assert.checkMinimumParameter("timeout", 0, timeout);
        XAImporter xaImporter = provider.getXAImporter();
        final boolean requestsSuspended = this.requestsSuspended;
        final ImportResult<?> result = xaImporter.findOrImportTransaction(xid, timeout, doNotImport || requestsSuspended);
        if (result == null) {
            if (! doNotImport) {
                if (requestsSuspended) {
                    throw Log.log.suspendedCannotImportXa(XAException.XAER_RMERR);
                }
                throw Log.log.providerCreatedNullTransaction();
            }
            return null;
        }
        final ImportResult<LocalTransaction> finalResult = result.withTransaction(getOrAttach(result.getTransaction()));
        if (finalResult.isNew()) {
            notifyCreationListeners(finalResult.getTransaction());
        }
        return finalResult;
    }

    /**
     * Attempt to import a transaction, which subsequently may be controlled by its XID or by the returned handle.
     *
     * @param xid the XID of the transaction to import (must not be {@code null})
     * @param timeout the transaction timeout to use, if new
     * @return the transaction import result (not {@code null})
     * @throws XAException if a problem occurred while importing the transaction
     */
    @NotNull
    public ImportResult<LocalTransaction> findOrImportTransaction(Xid xid, int timeout) throws XAException {
        return Assert.assertNotNull(findOrImportTransaction(xid, timeout, false));
    }

    /**
     * Attempt to import a provider's current transaction as a local transaction.
     *
     * @return {@code true} if the transaction was associated, {@code false} if the provider had no current transaction
     * @throws SystemException if an error occurred acquiring the current transaction from the provider
     * @throws NotSupportedException if the thread is already associated with a transaction
     */
    public boolean importProviderTransaction() throws SystemException, NotSupportedException {
        final ContextTransactionManager.State state = ContextTransactionManager.INSTANCE.getStateRef().get();
        if (state.transaction != null) {
            throw Log.log.nestedNotSupported();
        }
        final Transaction transaction = provider.getTransactionManager().getTransaction();
        if (transaction == null) {
            return false;
        }
        state.transaction = getOrAttach(transaction);
        return true;
    }

    LocalTransaction getOrAttach(Transaction transaction) {
        LocalTransaction txn = (LocalTransaction) provider.getResource(transaction, LOCAL_TXN_KEY);
        boolean isNew = false;
        if (txn == null) {
            // use LOCAL_TXN_KEY so we can be reasonably assured that there will be no deadlock
            synchronized (LOCAL_TXN_KEY) {
                txn = (LocalTransaction) provider.getResource(transaction, LOCAL_TXN_KEY);
                if (txn == null) {
                    provider.putResource(transaction, LOCAL_TXN_KEY, txn = new LocalTransaction(this, transaction));
                    isNew = true;
                }
            }
        }
        if (isNew) {
            notifyCreationListeners(txn);
        }
        return txn;
    }

    /**
     * Get the recovery interface for this context.  The recovery interface can be used to recover transactions which
     * were imported into this context via {@link #findOrImportTransaction(Xid, int)}.
     *
     * @return the recovery interface for this context (not {@code null})
     */
    @NotNull
    public XARecoverable getRecoveryInterface() {
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(TransactionPermission.forName("getRecoveryInterface"));
        }

        final XAImporter xaImporter = provider.getXAImporter();
        return new XARecoverable() {
            public Xid[] recover(final int flag, final String parentName) throws XAException {
                return xaImporter.recover(flag, parentName);
            }

            public void commit(final Xid xid, final boolean onePhase) throws XAException {
                xaImporter.commit(xid, onePhase);
            }

            public void forget(final Xid xid) throws XAException {
                xaImporter.forget(xid);
            }
        };
    }

    /**
     * Cause requests to create new transactions to be refused.
     *
     * @throws SecurityException if a security manager is present and the caller does not have the {@code suspendRequests} {@link TransactionPermission}
     */
    public void suspendRequests() throws SecurityException {
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(SUSPEND_REQUESTS_PERMISSION);
        }
        requestsSuspended = true;
    }

    /**
     * Cause requests to create new transactions to be allowed after a previous call to {@link #suspendRequests()}.
     *
     * @throws SecurityException if a security manager is present and the caller does not have the {@code resumeRequests} {@link TransactionPermission}
     */
    public void resumeRequests() throws SecurityException {
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(RESUME_REQUESTS_PERMISSION);
        }
        requestsSuspended = false;
    }

    LocalTransactionProvider getProvider() {
        return provider;
    }

    public String toString() {
        return String.format("Local transaction context for provider %s", provider);
    }
}
