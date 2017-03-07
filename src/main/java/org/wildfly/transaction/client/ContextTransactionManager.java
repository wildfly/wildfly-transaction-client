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

import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.wildfly.common.Assert;
import org.wildfly.transaction.TransactionPermission;
import org.wildfly.transaction.client._private.Log;

/**
 * The singleton, global transaction manager for the local instance.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ContextTransactionManager implements TransactionManager {
    static final ContextTransactionManager INSTANCE = new ContextTransactionManager();

    private static final AtomicInteger defaultTimeoutRef = new AtomicInteger(LocalTransactionContext.DEFAULT_TXN_TIMEOUT);
    private static final TransactionPermission SET_TIMEOUT_PERMISSION = TransactionPermission.forName("setGlobalDefaultTransactionTimeout");

    final ThreadLocal<State> stateRef = ThreadLocal.withInitial(State::new);

    private ContextTransactionManager() {
    }

    public void begin() throws NotSupportedException, SystemException {
        final State state = stateRef.get();
        if (state.transaction != null) {
            throw Log.log.nestedNotSupported();
        }
        resume(LocalTransactionContext.getCurrent().beginTransaction(state.getTimeout()));
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        final State state = stateRef.get();
        try {
            if (state.transaction == null) {
                throw Log.log.noTransaction();
            }
            state.transaction.commitAndDissociate();
        } finally {
            state.transaction = null;
        }
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        final State state = stateRef.get();
        try {
            if (state.transaction == null) {
                throw Log.log.noTransaction();
            }
            state.transaction.rollbackAndDissociate();
        } finally {
            state.transaction = null;
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        final State state = stateRef.get();
        if (state.transaction == null) {
            throw Log.log.noTransaction();
        }
        state.transaction.setRollbackOnly();
    }

    public int getStatus() throws SystemException {
        final State state = stateRef.get();
        return state == null || state.transaction == null ? Status.STATUS_NO_TRANSACTION : state.transaction.getStatus();
    }

    public AbstractTransaction getTransaction() {
        final AbstractTransaction transaction = stateRef.get().transaction;
        if (transaction != null) transaction.verifyAssociation();
        return transaction;
    }

    public void setTransactionTimeout(final int timeout) {
        Assert.checkMinimumParameter("timeout", 0, timeout);
        stateRef.get().setTimeout(timeout);
    }

    public int getTransactionTimeout() {
        return stateRef.get().getTimeout();
    }

    public AbstractTransaction suspend() throws SystemException {
        final State state = stateRef.get();
        AbstractTransaction transaction = state.transaction;
        if (transaction == null) {
            return null;
        }
        try {
            transaction.suspend();
            return transaction;
        } finally {
            state.transaction = null;
        }
    }

    public void resume(final Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException {
        if (transaction != null && ! (transaction instanceof AbstractTransaction)) {
            throw Log.log.notSupportedTransaction(transaction);
        }
        resume((AbstractTransaction) transaction);
    }

    void resume(final AbstractTransaction transaction) throws IllegalStateException, SystemException {
        final State state = stateRef.get();
        if (state.transaction != null) throw Log.log.alreadyAssociated();
        state.transaction = transaction;
        if (transaction != null) try {
            transaction.resume();
        } catch (Throwable t) {
            state.transaction = null;
            throw t;
        }
    }

    boolean isAvailable() {
        return stateRef.get().available;
    }

    boolean setAvailable(boolean available) {
        return stateRef.get().available = available;
    }

    /**
     * Get the transaction manager instance.
     *
     * @return the transaction manager instance (not {@code null})
     */
    public static ContextTransactionManager getInstance() {
        return INSTANCE;
    }

    /**
     * Get the global default transaction timeout.
     *
     * @return the global default transaction timeout in seconds (>= 1)
     */
    public static int getGlobalDefaultTransactionTimeout() {
        return defaultTimeoutRef.get();
    }

    /**
     * Set the global default transaction timeout, returning the original value.
     *
     * @param newTimeout the new timeout value in seconds (must be >= 1)
     * @return the previous timeout in seconds (>= 1)
     */
    public static int setGlobalDefaultTransactionTimeout(int newTimeout) {
        Assert.checkMinimumParameter("newTimeout", 1, newTimeout);
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(SET_TIMEOUT_PERMISSION);
        }
        return defaultTimeoutRef.getAndSet(newTimeout);
    }

    /**
     * Set the minimum global default transaction timeout, returning the original value.  The new timeout will not be
     * less than the given minimum.
     *
     * @param minimumTimeout the minimum timeout value in seconds (must be >= 1)
     * @return the previous timeout in seconds (>= 1)
     */
    public static int setMinimumGlobalDefaultTransactionTimeout(int minimumTimeout) {
        Assert.checkMinimumParameter("minimumTimeout", 1, minimumTimeout);
        int oldVal = defaultTimeoutRef.get();
        if (oldVal >= minimumTimeout) {
            return oldVal;
        }
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(SET_TIMEOUT_PERMISSION);
        }
        while (! defaultTimeoutRef.compareAndSet(oldVal, minimumTimeout) && oldVal < minimumTimeout) {
            oldVal = defaultTimeoutRef.get();
        }
        return oldVal;
    }

    /**
     * Set the maximum global default transaction timeout, returning the original value.  The new timeout will not be
     * greater than the given maximum.
     *
     * @param maximumTimeout the maximum timeout value in seconds (must be >= 1)
     * @return the previous timeout in seconds (>= 1)
     */
    public static int setMaximumGlobalDefaultTransactionTimeout(int maximumTimeout) {
        Assert.checkMinimumParameter("maximumTimeout", 1, maximumTimeout);
        int oldVal = defaultTimeoutRef.get();
        if (oldVal <= maximumTimeout) {
            return oldVal;
        }
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(SET_TIMEOUT_PERMISSION);
        }
        while (! defaultTimeoutRef.compareAndSet(oldVal, maximumTimeout) && oldVal > maximumTimeout) {
            oldVal = defaultTimeoutRef.get();
        }
        return oldVal;
    }

    ThreadLocal<State> getStateRef() {
        return stateRef;
    }

    static final class State {
        AbstractTransaction transaction;
        private int timeout = 0;
        boolean available = true;

        int getTimeout() {
            final int timeout = this.timeout;
            return timeout == 0 ? defaultTimeoutRef.get() : timeout;
        }

        void setTimeout(int timeout) {
            this.timeout = timeout;
        }
    }
}
