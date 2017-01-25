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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.spi.XATerminator;
import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.arjuna.ats.arjuna.AtomicAction;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import org.jboss.tm.ExtendedJBossXATerminator;
import org.jboss.tm.ImportedTransaction;
import org.jboss.tm.TransactionImportResult;
import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.ImportResult;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XAImporter;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

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
            if (suspended != null) try {
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
        final Transaction transactionManagerTransaction = safeGetTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
        tsr.registerInterposedSynchronization(sync);
    }

    public Object getResource(@NotNull final Transaction transaction, @NotNull final Object key) {
        final Transaction transactionManagerTransaction = safeGetTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
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
        final Transaction transactionManagerTransaction = safeGetTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
        tsr.putResource(key, value);
    }

    public boolean getRollbackOnly(@NotNull final Transaction transaction) throws IllegalArgumentException {
        final Transaction transactionManagerTransaction = safeGetTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
        return tsr.getRollbackOnly();
    }

    @NotNull
    public Object getKey(@NotNull final Transaction transaction) throws IllegalArgumentException {
        final Transaction transactionManagerTransaction = safeGetTransaction();
        if (! transaction.equals(transactionManagerTransaction)) {
            throw Log.log.unexpectedProviderTransactionMismatch(transaction, transactionManagerTransaction);
        }
        return tsr.getTransactionKey();
    }

    @NotNull
    public String getNodeName() {
        final String nodeIdentifier = arjPropertyManager.getCoreEnvironmentBean().getNodeIdentifier();
        if (nodeIdentifier == null) {
            throw Log.log.noLocalTransactionProviderNodeName();
        }
        return nodeIdentifier;
    }

    private static final int UID_LEN = 28;

    public String getNameFromXid(@NotNull final Xid xid) {
        final int formatId = xid.getFormatId();
        if (formatId == 0x20000 || formatId == 0x20005 || formatId == 0x20008) {
            final byte[] gtid = xid.getGlobalTransactionId();
            final int length = gtid.length;
            if (length <= UID_LEN) {
                // no parent name encoded there
                return null;
            }
            return new String(gtid, UID_LEN, length - UID_LEN, StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    public <T> T getProviderInterface(final Transaction transaction, final Class<T> providerInterfaceType) {
        // access to underlying txn
        return providerInterfaceType.isInstance(transaction) ? providerInterfaceType.cast(transaction) : null;
    }

    final class Entry implements SubordinateTransactionControl {
        private final SimpleXid xid;
        private final ImportedTransaction transaction;
        private final AtomicInteger timeoutRef;
        private final long start = System.nanoTime();

        Entry(final SimpleXid xid, final ImportedTransaction transaction, final int timeout) {
            this.xid = xid;
            this.transaction = transaction;
            this.timeoutRef = new AtomicInteger(timeout);
        }

        SimpleXid getXid() {
            return xid;
        }

        ImportedTransaction getTransaction() {
            return transaction;
        }

        int getTimeout() {
            return timeoutRef.get();
        }

        long getRemainingNanos() {
            return max(0L, timeoutRef.get() * 1_000_000_000L - max(0, System.nanoTime() - start));
        }

        void growTimeout(final int newTimeout) {
            final AtomicInteger timeoutRef = this.timeoutRef;
            int timeout;
            do {
                timeout = timeoutRef.get();
                if (newTimeout <= timeout) {
                    return;
                }
            } while (! timeoutRef.compareAndSet(timeout, newTimeout));
        }

        void shrinkTimeout(final int newTimeout) {
            final AtomicInteger timeoutRef = this.timeoutRef;
            int timeout;
            do {
                timeout = timeoutRef.get();
                if (newTimeout >= timeout) {
                    return;
                }
            } while (! timeoutRef.compareAndSet(timeout, newTimeout));
        }

        public void rollback() throws XAException {
            final ImportedTransaction transaction = this.transaction;
            if (transaction.activated()) try {
                transaction.doRollback();
            } catch (HeuristicCommitException e) {
                throw new XAException(XAException.XA_HEURCOM);
            } catch (HeuristicMixedException e) {
                throw new XAException(XAException.XA_HEURMIX);
            } catch (HeuristicRollbackException e) {
                throw new XAException(XAException.XA_HEURRB);
            } catch (SystemException | RuntimeException e) {
                throw new XAException(XAException.XAER_RMERR);
            } finally {
                ext.removeImportedTransaction(xid);
            }
        }

        public void end(final int flags) throws XAException {
            if (flags == XAResource.TMFAIL) {
                try {
                    transaction.setRollbackOnly();
                } catch (IllegalStateException e) {
                    throw new XAException(XAException.XAER_NOTA);
                } catch (RuntimeException | SystemException e) {
                    throw new XAException(XAException.XAER_RMERR);
                }
            }
        }

        public void beforeCompletion() throws XAException {
            try {
                if (! transaction.doBeforeCompletion()) {
                    throw new XAException(XAException.XAER_RMERR);
                }
            } catch (IllegalStateException e) {
                throw new XAException(XAException.XAER_NOTA);
            } catch (RuntimeException | SystemException e) {
                throw new XAException(XAException.XAER_RMERR);
            }
        }

        public int prepare() throws XAException {
            final ImportedTransaction transaction = this.transaction;
            final int tpo = transaction.doPrepare();
            switch (tpo) {
                case PREPARE_READONLY:
                    ext.removeImportedTransaction(xid);
                    return XAResource.XA_RDONLY;

                case PREPARE_OK:
                    return XAResource.XA_OK;

                case PREPARE_NOTOK:
                    try {
                        transaction.doRollback();
                    } catch (HeuristicCommitException | HeuristicMixedException | HeuristicRollbackException | SystemException e) {
                        // TODO: the old code removes the transaction here, but JBTM-427 implies that the TM should do this explicitly later; for now keep old behavior
                        ext.removeImportedTransaction(xid);
                        // JBTM-427; JTA doesn't allow heuristic codes on prepare :(
                        throw initializeSuppressed(new XAException(XAException.XAER_RMERR), transaction, e);
                    }
                    throw initializeSuppressed(new XAException(XAException.XA_RBROLLBACK), transaction, null);

                case INVALID_TRANSACTION:
                    throw new XAException(XAException.XAER_NOTA);

                default:
                    throw new XAException(XAException.XA_RBOTHER);
            }
        }

        public void forget() throws XAException {
            try {
                transaction.doForget();
            } catch (IllegalStateException e) {
                throw new XAException(XAException.XAER_NOTA);
            } catch (RuntimeException e) {
                throw new XAException(XAException.XAER_RMERR);
            }
        }

        public void commit(final boolean onePhase) throws XAException {
            final ImportedTransaction transaction = this.transaction;
            try {
                if (onePhase) {
                    transaction.doOnePhaseCommit();
                } else {
                    if (! transaction.doCommit()) {
                        throw new XAException(XAException.XA_RETRY);
                    }
                }
            } catch (HeuristicMixedException e) {
                throw initializeSuppressed(new XAException(XAException.XA_HEURMIX), transaction, e);
            } catch (RollbackException e) {
                throw initializeSuppressed(new XAException(XAException.XA_RBROLLBACK), transaction, e);
            } catch (HeuristicCommitException e) {
                throw initializeSuppressed(new XAException(XAException.XA_HEURCOM), transaction, e);
            } catch (HeuristicRollbackException e) {
                throw initializeSuppressed(new XAException(XAException.XA_HEURRB), transaction, e);
            } catch (IllegalStateException e) {
                throw initializeSuppressed(new XAException(XAException.XAER_NOTA), transaction, e);
            } catch (RuntimeException | SystemException e) {
                throw initializeSuppressed(new XAException(XAException.XAER_RMERR), transaction, e);
            }
        }

        private XAException initializeSuppressed(final XAException ex, final ImportedTransaction transaction, final Throwable cause) {
            if (cause != null) ex.initCause(cause);
            try {
                if (transaction instanceof AtomicAction) {
                    for (Throwable t : ((AtomicAction) transaction).getDeferredThrowables()) {
                        ex.addSuppressed(t);
                    }
                }
            } catch (NoClassDefFoundError ignored) {
                // skip attaching the deferred throwables
            }
            return ex;
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
        public ImportResult<ImportedTransaction> findOrImportTransaction(final Xid xid, final int timeout) throws XAException {
            final SimpleXid simpleXid = SimpleXid.of(xid);
            final SimpleXid gtid = simpleXid.withoutBranch();
            final int status;
            Entry entry = knownImported.get(gtid);
            if (entry != null) {
                final ImportedTransaction transaction = entry.getTransaction();
                try {
                    status = transaction.getStatus();
                } catch (SystemException e) {
                    throw new XAException(XAException.XAER_RMFAIL);
                }
                if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
                    entry.growTimeout(timeout);
                } else {
                    entry.shrinkTimeout(min(staleTransactionTime, timeout));
                }
                return new ImportResult<ImportedTransaction>(transaction, entry, false);
            }
            final TransactionImportResult result = ext.importTransaction(xid, timeout);
            final ImportedTransaction transaction = result.getTransaction();
            try {
                status = transaction.getStatus();
            } catch (SystemException e) {
                throw new XAException(XAException.XAER_RMFAIL);
            }
            final int newTimeout;
            if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
                newTimeout = timeout;
            } else {
                newTimeout = min(staleTransactionTime, timeout);
            }
            entry = new Entry(simpleXid, transaction, newTimeout);
            knownImported.putIfAbsent(gtid, entry);
            return new ImportResult<ImportedTransaction>(transaction, entry, result.isNewImportedTransaction());
        }

        public Transaction findExistingTransaction(final Xid xid) throws XAException {
            final SimpleXid simpleXid = SimpleXid.of(xid);
            final SimpleXid gtid = simpleXid.withoutBranch();
            final int status;
            Entry entry = knownImported.get(gtid);
            if (entry != null) {
                final Transaction transaction = entry.getTransaction();
                try {
                    status = transaction.getStatus();
                } catch (SystemException e) {
                    throw new XAException(XAException.XAER_RMFAIL);
                }
                if (status != Status.STATUS_ACTIVE && status != Status.STATUS_MARKED_ROLLBACK) {
                    entry.shrinkTimeout(staleTransactionTime);
                }
                return entry.getTransaction();
            }
            final Transaction transaction = ext.getTransaction(xid);
            if (transaction == null) {
                return null;
            }
            if (! (transaction instanceof ImportedTransaction)) {
                throw new XAException(XAException.XAER_NOTA);
            }
            knownImported.putIfAbsent(gtid, new Entry(simpleXid, (ImportedTransaction) transaction, staleTransactionTime));
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
            Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
            if (entry != null) {
                entry.commit(onePhase);
            } else {
                throw new XAException(XAException.XAER_NOTA);
            }
        }

        public void forget(final Xid xid) throws XAException {
            Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
            if (entry != null) {
                entry.forget();
            } else {
                throw new XAException(XAException.XAER_NOTA);
            }
        }

        public int prepare(final Xid xid) throws XAException {
            Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
            if (entry != null) {
                return entry.prepare();
            } else {
                throw new XAException(XAException.XAER_NOTA);
            }
        }

        public void rollback(final Xid xid) throws XAException {
            Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
            if (entry != null) {
                entry.rollback();
            } else {
                throw new XAException(XAException.XAER_NOTA);
            }
        }

        @NotNull
        public Xid[] recover(final int flag, final String parentNodeName) throws XAException {
            try {
                return ext.doRecover(null, parentNodeName);
            } catch (NotSupportedException e) {
                throw new XAException(XAException.XAER_RMFAIL);
            }
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

    // Prepare result codes; see com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome for more info
    private static final int PREPARE_OK = 0;
    private static final int PREPARE_NOTOK = 1;
    private static final int PREPARE_READONLY = 2;
    private static final int HEURISTIC_ROLLBACK = 3;
    private static final int HEURISTIC_COMMIT = 4;
    private static final int HEURISTIC_MIXED = 5;
    private static final int HEURISTIC_HAZARD = 6;
    private static final int FINISH_OK = 7;
    private static final int FINISH_ERROR = 8;
    private static final int NOT_PREPARED = 9;
    private static final int ONE_PHASE_ERROR = 10;
    private static final int INVALID_TRANSACTION = 11;
    private static final int PREPARE_ONE_PHASE_COMMITTED = 12;
}
