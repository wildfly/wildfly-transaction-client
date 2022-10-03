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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;

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

    private static final Object RB_EX_KEY = new Object();

    private final Object outflowLock = new Object();
    private final long start = System.nanoTime();

    final List<AssociationListener> associationListeners = new CopyOnWriteArrayList<>();

    AbstractTransaction() {
    }

    abstract void registerInterposedSynchronization(Synchronization synchronization) throws IllegalStateException;

    public abstract Object getResource(Object key) throws NullPointerException;

    public abstract void putResource(Object key, Object value) throws NullPointerException;

    public abstract Object putResourceIfAbsent(Object key, Object value) throws IllegalArgumentException;

    abstract Object getKey();

    boolean getRollbackOnly() {
        try {
            return getStatus() == Status.STATUS_MARKED_ROLLBACK;
        } catch (SystemException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        List<Throwable> list = (List<Throwable>) getResource(RB_EX_KEY);
        if (list == null) {
            List<Throwable> appearing = (List<Throwable>) putResourceIfAbsent(RB_EX_KEY, list = new ArrayList<>());
            if (appearing != null) {
                list = appearing;
            }
        }
        synchronized (list) {
            list.add(Log.log.markedRollbackOnly());
        }
    }

    @SuppressWarnings("unchecked")
    void addRollbackExceptions(Exception ex) {
        List<Throwable> list = (List<Throwable>) getResource(RB_EX_KEY);
        if (list != null) {
            for (Throwable throwable : list) {
                ex.addSuppressed(throwable);
            }
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
        return transactionTimeout - (int) min(max(elapsed, 0L) / 1_000_000_000L, transactionTimeout);
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

    public abstract void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException;

    abstract void commitAndDissociate() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException;

    public abstract void rollback() throws IllegalStateException, SystemException;

    abstract void rollbackAndDissociate() throws IllegalStateException, SystemException;

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
        return tm::suspend;
    }

    public <T, U, R, E extends Exception> R performFunction(ExceptionBiFunction<T, U, R, E> function, T param1, U param2) throws E, SystemException {
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        if (Objects.equals(tm.getStateRef().get().transaction, this)) {
            return function.apply(param1, param2);
        }
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
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        if (Objects.equals(tm.getStateRef().get().transaction, this)) {
            consumer.accept(param1, param2);
            return;
        }
        try (SysExTry ignored = whileSuspended()) {
            try (SysExTry ignored2 = whileResumed(this)) {
                consumer.accept(param1, param2);
            }
        }
    }

    public <T, U, E extends Exception> void performConsumer(ExceptionBiConsumer<T, U, E> consumer, T param1, U param2) throws E, SystemException {
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        if (Objects.equals(tm.getStateRef().get().transaction, this)) {
            consumer.accept(param1, param2);
            return;
        }
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
        final ContextTransactionManager tm = ContextTransactionManager.INSTANCE;
        if (Objects.equals(tm.getStateRef().get().transaction, this)) {
            return function.apply(param1, param2);
        }
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

    abstract boolean importBacking() throws SystemException;

    abstract void unimportBacking();

    final class AssociatingSynchronization implements Synchronization {
        private final Synchronization sync;

        AssociatingSynchronization(final Synchronization sync) {
            this.sync = sync;
        }

        public void beforeCompletion() {
            try {
                if (importBacking()) try {
                    sync.beforeCompletion();
                } finally {
                    unimportBacking();
                } else {
                    performConsumer(Synchronization::beforeCompletion, sync);
                }
            } catch (SystemException e) {
                throw new SynchronizationException(e);
            }
        }

        public void afterCompletion(final int status) {
            try {
                if (importBacking()) try {
                    sync.afterCompletion(status);
                } finally {
                    unimportBacking();
                } else {
                    performConsumer(Synchronization::afterCompletion, sync, status);
                }
            } catch (SystemException e) {
                throw new SynchronizationException(e);
            }
        }
    }
}
