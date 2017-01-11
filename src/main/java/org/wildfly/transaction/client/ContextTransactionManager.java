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
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client._private.Log;

/**
 * The singleton, global transaction manager for the local instance.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ContextTransactionManager implements TransactionManager {
    static final ContextTransactionManager INSTANCE = new ContextTransactionManager();

    final ThreadLocal<State> stateRef = ThreadLocal.withInitial(State::new);

    private ContextTransactionManager() {
    }

    public void begin() throws NotSupportedException, SystemException {
        final State state = stateRef.get();
        if (state.transaction != null) {
            throw Log.log.nestedNotSupported();
        }
        resume(LocalTransactionContext.getCurrent().beginTransaction(state.timeout));
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        final State state = stateRef.get();
        try {
            if (state.transaction == null) {
                throw Log.log.noTransaction();
            }
            state.transaction.commit();
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
            state.transaction.rollback();
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
        stateRef.get().timeout = timeout;
    }

    int getTransactionTimeout() {
        return stateRef.get().timeout;
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
        Assert.checkNotNullParam("transaction", transaction);
        if (! (transaction instanceof AbstractTransaction)) {
            throw Log.log.notSupportedTransaction(transaction);
        }
        resume((AbstractTransaction) transaction);
    }

    void resume(final AbstractTransaction transaction) throws IllegalStateException, SystemException {
        final State state = stateRef.get();
        if (state.transaction != null) throw Log.log.alreadyAssociated();
        transaction.resume();
        state.transaction = transaction;
    }

    /**
     * Get the transaction manager instance.
     *
     * @return the transaction manager instance (not {@code null})
     */
    public static ContextTransactionManager getInstance() {
        return INSTANCE;
    }

    ThreadLocal<State> getStateRef() {
        return stateRef;
    }

    static final class State {
        AbstractTransaction transaction;
        int timeout;
    }
}
