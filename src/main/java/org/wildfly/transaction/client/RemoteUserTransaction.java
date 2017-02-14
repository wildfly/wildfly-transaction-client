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

import java.io.Serializable;
import java.net.URI;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;

/**
 * A remote {@code UserTransaction} which controls the transaction state of a remote system.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemoteUserTransaction implements UserTransaction, Serializable {
    private static final long serialVersionUID = 8612109476723652825L;
    private static final int DEFAULT_TIMEOUT = 43200;

    private final ThreadLocal<State> stateRef = ThreadLocal.withInitial(State::new);
    private final URI location;

    RemoteUserTransaction(final URI location) {
        this.location = location;
    }

    public void begin() throws NotSupportedException, SystemException {
        final ContextTransactionManager transactionManager = ContextTransactionManager.getInstance();
        if (transactionManager.getTransaction() != null) {
            throw Log.log.nestedNotSupported();
        }
        final RemoteTransactionContext context = RemoteTransactionContext.getInstancePrivate();
        final RemoteTransactionProvider provider = context.getProvider(location);
        if (provider == null) {
            throw Log.log.noProviderForUri(location);
        }
        final SimpleTransactionControl control = provider.getPeerHandle(location).begin();
        final int timeout = stateRef.get().timeout;
        transactionManager.resume(context.notifyCreationListeners(new RemoteTransaction(control, location, timeout)));
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        ContextTransactionManager transactionManager = ContextTransactionManager.getInstance();
        final RemoteTransaction remoteTransaction = getMatchingTransaction();
        if (remoteTransaction == null) {
            throw Log.log.invalidTxnState();
        } else {
            transactionManager.commit();
        }
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        ContextTransactionManager transactionManager = ContextTransactionManager.getInstance();
        final RemoteTransaction remoteTransaction = getMatchingTransaction();
        if (remoteTransaction == null) {
            throw Log.log.invalidTxnState();
        } else {
            transactionManager.rollback();
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        ContextTransactionManager transactionManager = ContextTransactionManager.getInstance();
        final RemoteTransaction remoteTransaction = getMatchingTransaction();
        if (remoteTransaction == null) {
            throw Log.log.noTransaction();
        } else {
            transactionManager.setRollbackOnly();
        }
    }

    public int getStatus() {
        final RemoteTransaction remoteTransaction = getMatchingTransaction();
        return remoteTransaction == null ? Status.STATUS_NO_TRANSACTION : remoteTransaction.getStatus();
    }

    /**
     * Get the location of this object.
     *
     * @return the location of this object
     */
    public URI getLocation() {
        return location;
    }

    RemoteTransaction getMatchingTransaction() {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getTransaction();
        if (! (transaction instanceof RemoteTransaction)) {
            return null;
        }
        final RemoteTransaction remoteTransaction = (RemoteTransaction) transaction;
        if (! remoteTransaction.getLocation().equals(location)) {
            return null;
        }
        return remoteTransaction;
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {
        if (seconds < 0) throw Log.log.negativeTxnTimeout();
        stateRef.get().timeout = seconds == 0 ? DEFAULT_TIMEOUT : seconds;
    }

    Object writeReplace() {
        return new SerializedUserTransaction(location);
    }

    static final class State {
        int timeout = DEFAULT_TIMEOUT;
    }
}
