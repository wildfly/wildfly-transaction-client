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

package org.wildfly.transaction.client.provider.remoting;

import static org.wildfly.transaction.client._private.Log.log;

import java.io.IOException;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexMap;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.LocalTransaction;

/**
 * The per-connection transaction server.  This can be used to resolve a local transaction for a given transaction ID.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemotingTransactionServer {

    private final RemotingTransactionService transactionService;
    private final IntIndexMap<LocalTxn> txns = new IntIndexHashMap<LocalTxn>(LocalTxn::getId);

    RemotingTransactionServer(final RemotingTransactionService transactionService, final Connection connection) {
        this.transactionService = transactionService;
        connection.addCloseHandler(this::handleClosed);
    }

    @NotNull
    public LocalTransaction requireTransaction(int id) throws SystemException {
        final LocalTxn txn = txns.get(id);
        if (txn == null) {
            throw log.noTransactionForId(id);
        }
        return txn.getTransaction();
    }

    @NotNull
    public LocalTransaction getOrBeginTransaction(int id, int timeout) throws SystemException {
        final LocalTxn txn = txns.get(id);
        if (txn != null) {
            return txn.getTransaction();
        }
        boolean ok = false;
        LocalTransaction transaction = transactionService.getTransactionContext().beginTransaction(timeout, true);
        try {
            final LocalTxn appearing = txns.putIfAbsent(new LocalTxn(id, transaction));
            if (appearing != null) {
                return appearing.getTransaction();
            }
            ok = true;
            return transaction;
        } finally {
            if (! ok) {
                safeRollback(transaction);
            }
        }
    }

    public LocalTransaction getTransactionIfExists(int id) {
        final LocalTxn txn = txns.get(id);
        return txn == null ? null : txn.getTransaction();
    }

    void handleClosed(Connection connection, IOException ignored) {
        for (LocalTxn txn : txns) {
            safeRollback(txn.getTransaction());
        }
    }

    static void safeRollback(final Transaction transaction) {
        if (transaction != null) try {
            transaction.rollback();
        } catch (SystemException e) {
            log.trace("Got exception during rollback-on-disconnect", e);
        }
    }

    IntIndexMap<LocalTxn> getTxnMap() {
        return txns;
    }

    /**
     * Get the transaction service for this server.
     *
     * @return the transaction service for this server (not {@code null})
     */
    public RemotingTransactionService getTransactionService() {
        return transactionService;
    }

    TransactionServerChannel openChannel(Channel channel) {
        final TransactionServerChannel transactionServerChannel = new TransactionServerChannel(this, channel, transactionService.getTransactionContext());
        transactionServerChannel.start();
        return transactionServerChannel;
    }

    // tracked transactions

    static final class LocalTxn {
        private final LocalTransaction transaction;
        private final int id;

        LocalTxn(final int id, final LocalTransaction transaction) {
            this.id = id;
            this.transaction = transaction;
        }

        LocalTransaction getTransaction() {
            return transaction;
        }

        int getId() {
            return id;
        }
    }
}
