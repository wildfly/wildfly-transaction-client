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

package org.wildfly.transaction.client.provider.jboss;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.resource.spi.XATerminator;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.jboss.tm.ExtendedJBossXATerminator;
import org.jboss.tm.ImportedTransaction;
import org.jboss.tm.TransactionImportResult;
import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XAImporter;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;

/**
 * The local transaction provider for JBoss application servers.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class JBossLocalTransactionProvider implements LocalTransactionProvider {
    private final int staleTransactionTime;
    private final ExtendedJBossXATerminator ext;
    private final XATerminator xt;
    private final TransactionManager tm;
    private final TransactionSynchronizationRegistry tsr;
    private final XAImporter xi = new XAImporterImpl();

    JBossLocalTransactionProvider(final int staleTransactionTime, final XATerminator xt, final ExtendedJBossXATerminator ext, final TransactionManager tm, final TransactionSynchronizationRegistry tsr) {
        this.staleTransactionTime = staleTransactionTime;
        this.ext = Assert.checkNotNullParam("ext", ext);
        this.xt = Assert.checkNotNullParam("xt", xt);
        this.tm = Assert.checkNotNullParam("tm", tm);
        this.tsr = Assert.checkNotNullParam("tsr", tsr);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NotNull
    public TransactionManager getTransactionManager() {
        return tm;
    }

    @NotNull
    public XAImporter getXAImporter() {
        return xi;
    }

    @NotNull
    public Transaction createNewTransaction(final int timeout) throws SystemException, SecurityException {
        final Transaction suspended = tm.suspend();
        try {
            tm.begin();
            return tm.suspend();
        } catch (NotSupportedException e) {
            throw Log.log.unexpectedFailure(e);
        } catch (Throwable t) {
            try {
                tm.resume(suspended);
            } catch (InvalidTransactionException e) {
                e.addSuppressed(t);
                throw Log.log.unexpectedFailure(e);
            }
            throw t;
        }
    }

    public boolean isImported(@NotNull final Transaction transaction) throws IllegalArgumentException {
        return transaction instanceof ImportedTransaction;
    }

    public void registerInterposedSynchronization(@NotNull final Transaction transaction, @NotNull final Synchronization sync) throws IllegalArgumentException {
        tsr.registerInterposedSynchronization(sync);
    }

    public Object getResource(@NotNull final Transaction transaction, @NotNull final Object key) {
        // we know this will always be true
        assert safeGetTransaction() != transaction;
        return tsr.getResource(key);
    }

    private Transaction safeGetTransaction() {
        try {
            return tm.getTransaction();
        } catch (SystemException e) {
            // should be impossible with Arjuna/Narayana
            throw Log.log.unexpectedFailure(e);
        }
    }

    public void putResource(@NotNull final Transaction transaction, @NotNull final Object key, final Object value) throws IllegalArgumentException {
        // we know this will always be true
        assert safeGetTransaction() != transaction;
        tsr.putResource(key, value);
    }

    public boolean getRollbackOnly(@NotNull final Transaction transaction) throws IllegalArgumentException {
        // we know this will always be true
        assert safeGetTransaction() != transaction;
        return tsr.getRollbackOnly();
    }

    @NotNull
    public Object getKey(@NotNull final Transaction transaction) throws IllegalArgumentException {
        // we know this will always be true
        assert safeGetTransaction() != transaction;
        return tsr.getTransactionKey();
    }

    static final class Entry {
        private final SimpleXid gtid;
        private final Transaction transaction;
        private final int timeout;
        private final long start = System.nanoTime();

        Entry(final SimpleXid gtid, final Transaction transaction, final int timeout) {
            this.gtid = gtid;
            this.transaction = transaction;
            this.timeout = timeout;
        }

        SimpleXid getGtid() {
            return gtid;
        }

        Transaction getTransaction() {
            return transaction;
        }

        int getTimeout() {
            return timeout;
        }

        long getRemainingNanos() {
            return max(0L, timeout * 1_000_000_000L - max(0, System.nanoTime() - start));
        }
    }

    final class XAImporterImpl implements XAImporter {
        @SuppressWarnings("serial")
        private final Map<SimpleXid, Entry> knownImported = Collections.synchronizedMap(new LinkedHashMap<SimpleXid, Entry>() {
            protected boolean removeEldestEntry(final Map.Entry<SimpleXid, Entry> eldest) {
                return eldest.getValue().getRemainingNanos() == 0L;
            }
        });

        @NotNull
        public ProviderImportResult findOrImportTransaction(final Xid xid, final int timeout) throws XAException {
            final SimpleXid gtid = SimpleXid.of(xid).withoutBranch();
            Entry entry = knownImported.get(gtid);
            if (entry != null) {
                return new ProviderImportResult(entry.getTransaction(), false);
            }
            final TransactionImportResult result = ext.importTransaction(xid, timeout);
            final Transaction transaction = result.getTransaction();
            knownImported.putIfAbsent(gtid, new Entry(gtid, transaction, min(staleTransactionTime, timeout)));
            return new ProviderImportResult(transaction, result.isNewImportedTransaction());
        }

        public Transaction findExistingTransaction(final Xid xid) throws XAException {
            final SimpleXid gtid = SimpleXid.of(xid).withoutBranch();
            Entry entry = knownImported.get(gtid);
            if (entry != null) {
                return entry.getTransaction();
            }
            final Transaction transaction = ext.getTransaction(xid);
            if (transaction == null) {
                return null;
            }
            // TODO: get the transaction timeout value, somehow
            knownImported.putIfAbsent(gtid, new Entry(gtid, transaction, staleTransactionTime));
            return transaction;
        }

        public void beforeComplete(final Xid xid) throws XAException {
            final ImportedTransaction importedTransaction = requireTxn(xid);
            try {
                if (! importedTransaction.doBeforeCompletion()) {
                    throw Log.log.beforeCompletionFailed(null, null);
                }
            } catch (SystemException e) {
                throw Log.log.beforeCompletionFailed(e, null);
            }
        }

        public void commit(final Xid xid, final boolean onePhase) throws XAException {
            xt.commit(xid, onePhase);
        }

        public void forget(final Xid xid) throws XAException {
            xt.forget(xid);
        }

        public int prepare(final Xid xid) throws XAException {
            return xt.prepare(xid);
        }

        public void rollback(final Xid xid) throws XAException {
            xt.rollback(xid);
        }

        @NotNull
        public Xid[] recover(final int flag) throws XAException {
            // TODO probably not right... check against old code
            return xt.recover(flag);
        }

        private ImportedTransaction requireTxn(final Xid xid) throws XAException {
            final ImportedTransaction importedTransaction = ext.getImportedTransaction(xid);
            if (importedTransaction == null) {
                throw Log.log.notActiveXA(XAException.XAER_NOTA);
            }
            return importedTransaction;
        }
    }

    /**
     * A builder for a JBoss local transaction provider.
     */
    public static final class Builder {
        private int staleTransactionTime = 600;
        private ExtendedJBossXATerminator extendedJBossXATerminator;
        private XATerminator xaTerminator;
        private TransactionManager transactionManager;
        private TransactionSynchronizationRegistry transactionSynchronizationRegistry;

        Builder() {
        }

        /**
         * Get the stale transaction time, in seconds.
         *
         * @return the stale transaction time, in seconds
         */
        public int getStaleTransactionTime() {
            return staleTransactionTime;
        }

        /**
         * Set the stale transaction time, in seconds.  The time must be no less than one second.
         *
         * @param staleTransactionTime the stale transaction time, in seconds
         */
        public Builder setStaleTransactionTime(final int staleTransactionTime) {
            Assert.checkMinimumParameter("staleTransactionTime", 1, staleTransactionTime);
            this.staleTransactionTime = staleTransactionTime;
            return this;
        }

        /**
         * Get the extended JBoss XA terminator.
         *
         * @return the extended JBoss XA terminator
         */
        public ExtendedJBossXATerminator getExtendedJBossXATerminator() {
            return extendedJBossXATerminator;
        }

        /**
         * Set the extended JBoss XA terminator.
         *
         * @param ext the extended JBoss XA terminator (must not be {@code null})
         */
        public Builder setExtendedJBossXATerminator(final ExtendedJBossXATerminator ext) {
            Assert.checkNotNullParam("ext", ext);
            this.extendedJBossXATerminator = ext;
            return this;
        }

        /**
         * Get the XA terminator.
         *
         * @return the XA terminator
         */
        public XATerminator getXATerminator() {
            return xaTerminator;
        }

        /**
         * Set the XA terminator.
         *
         * @param xt the XA terminator (must not be {@code null})
         */
        public Builder setXATerminator(final XATerminator xt) {
            Assert.checkNotNullParam("xt", xt);
            this.xaTerminator = xt;
            return this;
        }

        /**
         * Get the transaction manager.
         *
         * @return the transaction manager
         */
        public TransactionManager getTransactionManager() {
            return transactionManager;
        }

        /**
         * Set the transaction manager.
         *
         * @param tm the transaction manager
         */
        public Builder setTransactionManager(final TransactionManager tm) {
            Assert.checkNotNullParam("tm", tm);
            this.transactionManager = tm;
            return this;
        }

        /**
         * Get the transaction synchronization registry.
         *
         * @return the transaction synchronization registry
         */
        public TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
            return transactionSynchronizationRegistry;
        }

        /**
         * Set the transaction synchronization registry.
         *
         * @param tsr the transaction synchronization registry
         */
        public Builder setTransactionSynchronizationRegistry(final TransactionSynchronizationRegistry tsr) {
            Assert.checkNotNullParam("tsr", tsr);
            this.transactionSynchronizationRegistry = tsr;
            return this;
        }

        /**
         * Build this provider.  If any required properties are {@code null}, an exception is thrown.
         *
         * @return the built provider (not {@code null})
         * @throws IllegalArgumentException if a required property is {@code null}
         */
        public JBossLocalTransactionProvider build() {
            Assert.checkNotNullParam("extendedJBossXATerminator", extendedJBossXATerminator);
            Assert.checkNotNullParam("xaTerminator", xaTerminator);
            Assert.checkNotNullParam("transactionManager", transactionManager);
            Assert.checkNotNullParam("transactionSynchronizationRegistry", transactionSynchronizationRegistry);
            return new JBossLocalTransactionProvider(staleTransactionTime, xaTerminator, extendedJBossXATerminator, transactionManager, transactionSynchronizationRegistry);
        }
    }
}
