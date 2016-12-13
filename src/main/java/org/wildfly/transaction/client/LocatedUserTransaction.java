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
import java.util.function.Function;

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
 * A remote {@code UserTransaction} which is located on a remote system.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class LocatedUserTransaction implements UserTransaction, Serializable {
    private static final long serialVersionUID = 8612109476723652825L;

    private final ThreadLocal<State> stateRef = ThreadLocal.withInitial(State::new);
    private final URI location;

    LocatedUserTransaction(final URI location) {
        this.location = location;
    }

    public void begin() throws NotSupportedException, SystemException {
        final State state = stateRef.get();
        if (state.status != Status.STATUS_NO_TRANSACTION) {
            throw Log.log.nestedNotSupported();
        }
        final RemoteTransactionProvider provider = RemoteTransactionContext.getInstancePrivate().getProvider(location, Function.identity());
        if (provider == null) {
            throw Log.log.noProviderForUri(location);
        }
        state.transactionHandle = provider.getPeerHandle(location).begin(state.timeout);
        state.status = Status.STATUS_ACTIVE;
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        final State state = stateRef.get();
        final int status = state.status;
        if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
            state.status = Status.STATUS_NO_TRANSACTION;
            state.transactionHandle.commit();
            state.transactionHandle = null;
        } else {
            throw Log.log.invalidTxnState();
        }
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        final State state = stateRef.get();
        final int status = state.status;
        if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
            state.status = Status.STATUS_NO_TRANSACTION;
            state.transactionHandle.rollback();
            state.transactionHandle = null;
        } else {
            throw Log.log.invalidTxnState();
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        final State state = stateRef.get();
        final int status = state.status;
        if (status == Status.STATUS_MARKED_ROLLBACK) {
            return;
        } else if (status == Status.STATUS_ACTIVE) {
            state.status = Status.STATUS_MARKED_ROLLBACK;
            state.transactionHandle.setRollbackOnly();
        } else {
            throw Log.log.noTransaction();
        }
    }

    public int getStatus() {
        return stateRef.get().status;
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {
        if (seconds < 0) throw Log.log.negativeTxnTimeout();
        stateRef.get().timeout = seconds;
    }

    Object writeReplace() {
        return new SerializedUserTransaction(location);
    }

    static final class State {
        int status = Status.STATUS_NO_TRANSACTION;
        int timeout = 0;
        SimpleTransactionControl transactionHandle;
    }
}
