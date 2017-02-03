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
import static java.lang.Math.min;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.wildfly.common.Assert;
import org.wildfly.transaction.TransactionPermission;

/**
 * A managed transaction.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractTransaction implements Transaction {
    private static final TransactionPermission ASSOCIATION_LISTENER_PERMISSION = TransactionPermission.forName("registerAssociationListener");

    private final Object outflowLock = new Object();
    private final long start = System.nanoTime();
    final Set<AssociationListener> associationListeners = new CopyOnWriteArraySet<>();

    AbstractTransaction() {
    }

    abstract void registerInterposedSynchronization(Synchronization synchronization) throws IllegalStateException;

    abstract Object getResource(Object key) throws NullPointerException;

    abstract void putResource(Object key, Object value) throws NullPointerException;

    abstract Object getKey();

    boolean getRollbackOnly() {
        try {
            return getStatus() == Status.STATUS_MARKED_ROLLBACK;
        } catch (SystemException e) {
            return false;
        }
    }

    abstract void suspend() throws SystemException;

    abstract void resume() throws SystemException;

    abstract void verifyAssociation();

    /**
     * Get the transaction timeout that was in force when the transaction began.
     *
     * @return the transaction timeout
     */
    public abstract int getTransactionTimeout();

    /**
     * Get an estimate of the amount of time remaining in this transaction.
     *
     * @return the estimate in seconds, or 0 if the transaction is estimated to have expired
     */
    public final int getEstimatedRemainingTime() {
        final long elapsed = System.nanoTime() - start;
        final int transactionTimeout = getTransactionTimeout();
        return transactionTimeout - (int) min((max(elapsed, 0L) + 999_999_999L) / 1_000_000_000L, transactionTimeout);
    }

    /**
     * Get a provider-specific interface from this transaction.
     *
     * @param providerInterfaceType the provider interface type class (must not be {@code null})
     * @param <T> the provider interface type
     * @return the provider interface, or {@code null} if the given type isn't supported by this transaction's provider
     */
    public <T> T getProviderInterface(Class<T> providerInterfaceType) {
        return null;
    }

    /**
     * Register an association listener for this transaction, which will be called any time this thread is suspended
     * or resumed.
     *
     * @param associationListener the association listener (must not be {@code null})
     */
    public void registerAssociationListener(AssociationListener associationListener) {
        Assert.checkNotNullParam("associationListener", associationListener);
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(ASSOCIATION_LISTENER_PERMISSION);
        }

        associationListeners.add(associationListener);
    }

    Object getOutflowLock() {
        return outflowLock;
    }
}
