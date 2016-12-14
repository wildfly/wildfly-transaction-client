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

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.wildfly.transaction.client._private.Log.log;

import java.io.IOException;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexMap;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.common.function.ExceptionSupplier;
import org.wildfly.transaction.client.ImportResult;
import org.wildfly.transaction.client.LocalTransaction;
import org.wildfly.transaction.client.LocalTransactionContext;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * The per-connection transaction server.  This can be used to resolve a local transaction for a given transaction ID.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemotingTransactionServer {

    private final RemotingTransactionService transactionService;
    private final Connection connection;
    private final IntIndexMap<Txn> txns = new IntIndexHashMap<Txn>(Txn::getId);

    RemotingTransactionServer(final RemotingTransactionService transactionService, final Connection connection) {
        this.transactionService = transactionService;
        this.connection = connection;
        connection.addCloseHandler(this::handleClosed);
    }

    @NotNull
    public LocalTransaction requireTransaction(int id) throws SystemException {
        final Txn txn = txns.get(id);
        if (txn == null) {
            throw log.noTransactionForId(id);
        }
        return txn.getTransaction();
    }

    @NotNull
    public LocalTransaction getOrBeginTransaction(int id, int timeout) throws SystemException {
        final Txn txn = txns.get(id);
        if (txn != null) {
            return txn.getTransaction();
        }
        boolean ok = false;
        LocalTransaction transaction = transactionService.getTransactionContext().beginTransaction(timeout);
        try {
            final Txn appearing = txns.putIfAbsent(new LocalTxn(id, transaction));
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
        final Txn txn = txns.get(id);
        return txn == null ? null : txn.getTransaction();
    }

    void handleClosed(Connection connection, IOException ignored) {
        for (Txn txn : txns) {
            if (txn instanceof LocalTxn) {
                safeRollback(txn.getTransaction());
            }
        }
    }

    static void safeRollback(final Transaction transaction) {
        if (transaction != null) try {
            transaction.rollback();
        } catch (SystemException e) {
            log.trace("Got exception during rollback-on-disconnect", e);
        }
    }

    IntIndexMap<Txn> getTxnMap() {
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

    abstract static class Txn {
        private final int id;

        Txn(final int id) {
            this.id = id;
        }

        int getId() {
            return id;
        }

        abstract LocalTransaction getTransaction();

        abstract ExceptionSupplier<LocalTransaction, XAException> getTransactionSupplier();
    }

    static final class LocalTxn extends Txn {
        private final LocalTransaction transaction;

        LocalTxn(final int id, final LocalTransaction transaction) {
            super(id);
            this.transaction = transaction;
        }

        LocalTransaction getTransaction() {
            return transaction;
        }

        ExceptionSupplier<LocalTransaction, XAException> getTransactionSupplier() {
            return this::getTransaction;
        }
    }

    static class ImportedTxn extends Txn {
        private final LocalTransactionContext transactionContext;
        private final Xid xid;
        private final long startTime;
        private final int timeout;
        private volatile ImportResult importResult;

        ImportedTxn(final int id, final LocalTransactionContext transactionContext, final Xid xid, final int timeout) {
            super(id);
            this.transactionContext = transactionContext;
            this.xid = xid;
            startTime = System.nanoTime();
            this.timeout = timeout;
        }

        Xid getXid() {
            return xid;
        }

        LocalTransaction getTransaction() {
            final ImportResult importResult = this.importResult;
            return importResult == null ? null : importResult.getTransaction();
        }

        ImportResult getOrImport() throws XAException {
            ImportResult importResult = this.importResult;
            if (importResult != null) {
                return importResult;
            }
            synchronized (this) {
                importResult = this.importResult;
                if (importResult != null) {
                    return importResult;
                }
                long elapsed = max(0L, System.nanoTime() - startTime) / 1_000_000_000L;
                if (elapsed >= timeout) {
                    // don't even import it
                    throw log.transactionTimedOut(XAException.XA_RBTIMEOUT);
                }
                // don't set if there's an exception on import
                importResult = transactionContext.findOrImportTransaction(xid, max(1, timeout - (int) min((long) Integer.MAX_VALUE, elapsed)));
                this.importResult = importResult;
                return importResult;
            }
        }

        SubordinateTransactionControl getControl() throws XAException {
            return getOrImport().getControl();
        }

        ExceptionSupplier<LocalTransaction, XAException> getTransactionSupplier() {
            return () -> getOrImport().getTransaction();
        }
    }
}
