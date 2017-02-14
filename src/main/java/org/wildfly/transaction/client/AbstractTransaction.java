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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.wildfly.common.Assert;
import org.wildfly.common.function.ExceptionBiConsumer;
import org.wildfly.common.function.ExceptionBiFunction;
import org.wildfly.common.function.ExceptionConsumer;
import org.wildfly.common.function.ExceptionFunction;
import org.wildfly.common.function.ExceptionObjIntConsumer;
import org.wildfly.common.function.ExceptionRunnable;
import org.wildfly.common.function.ExceptionSupplier;
import org.wildfly.common.function.ExceptionToIntBiFunction;
import org.wildfly.common.function.ExceptionToIntFunction;
import org.wildfly.transaction.TransactionPermission;
import org.wildfly.transaction.client._private.Log;

/**
 * A managed transaction.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractTransaction implements Transaction {
    private static final TransactionPermission ASSOCIATION_LISTENER_PERMISSION = TransactionPermission.forName("registerAssociationListener");

    private final Object outflowLock = new Object();
    private final long start = System.nanoTime();
    final List<AssociationListener> associationListeners = new CopyOnWriteArrayList<>();

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

    void notifyAssociationListeners(final boolean associated) {
        for (AssociationListener associationListener : associationListeners) {
            try {
                associationListener.associationChanged(this, associated);
            } catch (Throwable t) {
                Log.log.tracef(t, "An association listener %s threw an exception on transaction %s", associationListener, this);
            }
        }
    }

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

    interface SysExTry extends AutoCloseable {
        void close() throws SystemException;
    }

    private static void doNothing() {}

    private static SysExTry whileSuspended() throws SystemException {
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        final AbstractTransaction suspended = tm.suspend();
        return suspended == null ? AbstractTransaction::doNothing : () -> tm.resume(suspended);
    }

    private static SysExTry whileResumed(AbstractTransaction transaction) throws SystemException {
        if (transaction == null) {
            return AbstractTransaction::doNothing;
        }
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        tm.resume(transaction);
        return () -> {
            final AbstractTransaction suspended = tm.suspend();
            if (transaction != suspended) {
                throw Log.log.unexpectedProviderTransactionMismatch(transaction, suspended);
            }
        };
    }

    public <T, U, R, E extends Exception> R performFunction(ExceptionBiFunction<T, U, R, E> function, T param1, U param2) throws E, SystemException {
        try (SysExTry ignored = whileSuspended()) {
            try (SysExTry ignored2 = whileResumed(this)) {
                return function.apply(param1, param2);
            }
        }
    }

    public <T, R, E extends Exception> R performFunction(ExceptionFunction<T, R, E> function, T param) throws E, SystemException {
        return performFunction(ExceptionFunction::apply, function, param);
    }

    public <R, E extends Exception> R performSupplier(ExceptionSupplier<R, E> function) throws E, SystemException {
        return performFunction(ExceptionSupplier::get, function);
    }

    public <T, E extends Exception> void performConsumer(ExceptionObjIntConsumer<T, E> consumer, T param1, int param2) throws E, SystemException {
        try (SysExTry ignored = whileSuspended()) {
            try (SysExTry ignored2 = whileResumed(this)) {
                consumer.accept(param1, param2);
            }
        }
    }

    public <T, U, E extends Exception> void performConsumer(ExceptionBiConsumer<T, U, E> consumer, T param1, U param2) throws E, SystemException {
        try (SysExTry ignored = whileSuspended()) {
            try (SysExTry ignored2 = whileResumed(this)) {
                consumer.accept(param1, param2);
            }
        }
    }

    public <T, E extends Exception> void performConsumer(ExceptionConsumer<T, E> consumer, T param) throws E, SystemException {
        performConsumer(ExceptionConsumer::accept, consumer, param);
    }

    public <T, U, E extends Exception> int performToIntFunction(ExceptionToIntBiFunction<T, U, E> function, T param1, U param2) throws E, SystemException {
        try (SysExTry ignored = whileSuspended()) {
            try (SysExTry ignored2 = whileResumed(this)) {
                return function.apply(param1, param2);
            }
        }
    }

    public <T, E extends Exception> int performToIntFunction(ExceptionToIntFunction<T, E> function, T param) throws E, SystemException {
        return performToIntFunction(ExceptionToIntFunction::apply, function, param);
    }

    public <E extends Exception> void performAction(ExceptionRunnable<E> action) throws E, SystemException {
        performConsumer((ExceptionConsumer<ExceptionRunnable<E>, E>) ExceptionRunnable::run, action);
    }

    Object getOutflowLock() {
        return outflowLock;
    }
}
