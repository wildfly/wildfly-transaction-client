/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
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

import static com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome.INVALID_TRANSACTION;
import static com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome.PREPARE_NOTOK;
import static com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome.PREPARE_OK;
import static com.arjuna.ats.arjuna.coordinator.TwoPhaseOutcome.PREPARE_READONLY;
import static java.lang.Long.signum;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

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

import com.arjuna.ats.arjuna.common.arjPropertyManager;
import org.jboss.tm.ExtendedJBossXATerminator;
import org.jboss.tm.ImportedTransaction;
import org.jboss.tm.TransactionImportResult;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.ImportResult;
import org.wildfly.transaction.client.LocalTransaction;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XAImporter;
import org.wildfly.transaction.client.XAResourceRegistry;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * The local transaction provider for JBoss application servers.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class JBossLocalTransactionProvider implements LocalTransactionProvider {
    private static final Object ENTRY_KEY = new Object();

    private final int staleTransactionTime;
    private final ExtendedJBossXATerminator ext;
    private final TransactionManager tm;
    private final XAImporterImpl xi = new XAImporterImpl();
    private final ConcurrentSkipListSet<XidKey> timeoutSet = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<SimpleXid, Entry> known = new ConcurrentHashMap<>();
    private final FileSystemXAResourceRegistry fileSystemXAResourceRegistry;

    JBossLocalTransactionProvider(final ExtendedJBossXATerminator ext, final int staleTransactionTime, final TransactionManager tm,
                                  final XAResourceRecoveryRegistry registry, final Path xaRecoveryDirRelativeToPath) {
        Assert.checkMinimumParameter("setTransactionTimeout", 0, staleTransactionTime);
        this.staleTransactionTime = staleTransactionTime;
        this.ext = Assert.checkNotNullParam("ext", ext);
        this.tm = Assert.checkNotNullParam("tm", tm);

        try {
            ext.doRecover(null, null);
        } catch (Exception e) {
            // the recover method is called to load transactions from Narayana object store at startup
            // if it fails we ignore, troubles will be adjusted during runtime
            Log.log.doRecoverFailureOnIntialization(e);
        }
        this.fileSystemXAResourceRegistry = new FileSystemXAResourceRegistry(this, xaRecoveryDirRelativeToPath);
        registry.addXAResourceRecovery(fileSystemXAResourceRegistry::getInDoubtXAResources);
    }

    /**
     * Create a builder for the transaction provider.
     *
     * @return the builder
     */
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
        final TransactionManager tm = this.tm;
        final int oldTimeout = getTransactionManagerTimeout();
        tm.setTransactionTimeout(timeout);
        try {
            final Transaction suspended = tm.suspend();
            try {
                tm.begin();
                final Transaction transaction = tm.suspend();
                SimpleXid gtid = SimpleXid.of(getXid(transaction)).withoutBranch();
                known.put(gtid, getEntryFor(transaction, gtid));
                // Narayana doesn't actually throw exceptions here so this should be fine
                tm.setTransactionTimeout(oldTimeout);
                return transaction;
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
        } catch (Throwable t) {
            try {
                tm.setTransactionTimeout(oldTimeout);
            } catch (Throwable t2) {
                // Narayana doesn't actually throw exceptions here so this should be fine
                t2.addSuppressed(t);
                throw t2;
            }
            throw t;
        }
    }

    @Override
    public XAResourceRegistry getXAResourceRegistry(LocalTransaction transaction) throws SystemException {
        return fileSystemXAResourceRegistry.getXAResourceRegistryFile(transaction);
    }

    abstract int getTransactionManagerTimeout() throws SystemException;

    public boolean isImported(@NotNull final Transaction transaction) throws IllegalArgumentException {
        return transaction instanceof ImportedTransaction;
    }

    public abstract void registerInterposedSynchronization(@NotNull Transaction transaction, @NotNull Synchronization sync) throws IllegalArgumentException;

    @NotNull
    public abstract Object getKey(@NotNull Transaction transaction) throws IllegalArgumentException;

    public void dropLocal(@NotNull final Transaction transaction) {
        final Xid xid = getXid(transaction);
        final SimpleXid gtid = SimpleXid.of(xid).withoutBranch();
        final Entry entry = known.remove(gtid);
        if (entry != null) {
            timeoutSet.remove(entry.getXidKey());
        }
    }

    public abstract int getTimeout(@NotNull Transaction transaction);

    @NotNull
    public abstract Xid getXid(@NotNull Transaction transaction);

    @NotNull
    public String getNodeName() {
        final String nodeIdentifier = arjPropertyManager.getCoreEnvironmentBean().getNodeIdentifier();
        if (nodeIdentifier == null) {
            throw Log.log.noLocalTransactionProviderNodeName();
        }
        return nodeIdentifier;
    }

    Entry getEntryFor(Transaction transaction, SimpleXid gtid) {
        Entry entry = (Entry) getResource(transaction, ENTRY_KEY);
        if (entry != null) {
            return entry;
        }
        final XidKey xidKey;
        synchronized (transaction) {
            entry = (Entry) getResource(transaction, ENTRY_KEY);
            if (entry != null) {
                return entry;
            }
            int lifetime = getTimeout(transaction) + staleTransactionTime;
            final long timeTick = getTimeTick();
            // this is the maximum amount of time we expect any potential incoming peer might know about this transaction ID
            xidKey = new XidKey(gtid, timeTick + lifetime * 1_000_000_000L);
            putResource(transaction, ENTRY_KEY, entry = new Entry(gtid, transaction, xidKey));
        }
        timeoutSet.add(xidKey);
        if (! isStatusInactive(transaction)) {
            try {
                registerInterposedSynchronization(transaction, new Synchronization() {
                    public void beforeCompletion() {
                        // no operation
                    }

                    public void afterCompletion(final int status) {
                        // let the TM do some heavy lifting for us
                        final long timeTick = getTimeTick();
                        // clear off all expired entries
                        final ConcurrentMap<SimpleXid, Entry> known = JBossLocalTransactionProvider.this.known;
                        final Iterator<XidKey> iterator = timeoutSet.headSet(new XidKey(SimpleXid.EMPTY, timeTick)).iterator();
                        while (iterator.hasNext()) {
                            SimpleXid xidToRemove = iterator.next().getId();
                            Entry knownEntry = known.remove(xidToRemove);

                            if (knownEntry == null) {
                                Log.log.unknownXidToBeRemovedFromTheKnownTransactionInstances(xidToRemove);
                            } else {
                                final Transaction transaction = knownEntry.getTransaction();
                                if (transaction instanceof ImportedTransaction) {
                                    try {
                                        ext.removeImportedTransaction(getXid(transaction));
                                    } catch (XAException xae) {
                                        Log.log.cannotRemoveImportedTransaction(xidToRemove, xae);
                                    }
                                }
                            }

                            iterator.remove();
                        }
                    }
                });
            } catch (IllegalStateException e) {
                if (! isStatusInactive(transaction)) {
                    // we don't know why it happened because the tx is not inactive; could be unknown or something weird
                    throw e;
                }
            }
        }
        return entry;
    }

    boolean isStatusInactive(Transaction transaction) {
        switch (getStatus(transaction)) {
            case Status.STATUS_PREPARING:
            case Status.STATUS_PREPARED:
            case Status.STATUS_COMMITTING:
            case Status.STATUS_COMMITTED:
            case Status.STATUS_ROLLING_BACK:
            case Status.STATUS_ROLLEDBACK: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    int getStatus(Transaction transaction) {
        try {
            return transaction.getStatus();
        } catch (SystemException e) {
            return Status.STATUS_UNKNOWN;
        }
    }

    private static final long TIME_START = System.nanoTime();

    long getTimeTick() {
        return System.nanoTime() - TIME_START;
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

    static final class XidKey implements Comparable<XidKey> {
        private final SimpleXid gtid;
        private final long expiration;

        XidKey(final SimpleXid gtid, final long expiration) {
            this.gtid = gtid;
            this.expiration = expiration;
        }

        public int compareTo(final XidKey o) {
            final int res = signum(expiration - o.expiration);
            return res == 0 ? gtid.compareTo(o.gtid) : res;
        }

        SimpleXid getId() {
            return gtid;
        }
    }

    final class Entry implements SubordinateTransactionControl {
        private final SimpleXid gtid;
        private final Transaction transaction;
        private final XidKey xidKey;

        Entry(final SimpleXid gtid, final Transaction transaction, final XidKey xidKey) {
            this.gtid = gtid;
            this.transaction = transaction;
            this.xidKey = xidKey;
        }

        XidKey getXidKey() {
            return xidKey;
        }

        Transaction getTransaction() {
            return transaction;
        }

        void rollbackLocal() throws SystemException {
            if (transaction instanceof ImportedTransaction) {
                throw Log.log.rollbackOnImported();
            }
            transaction.rollback();
        }

        void commitLocal() throws HeuristicRollbackException, RollbackException, HeuristicMixedException, SystemException {
            if (transaction instanceof ImportedTransaction) {
                throw Log.log.commitOnImported();
            }
            transaction.commit();
        }

        public void rollback() throws XAException {
            final Transaction transaction = this.transaction;
            try {
                final int status = transaction.getStatus();
                if (status == Status.STATUS_ROLLING_BACK || status == Status.STATUS_ROLLEDBACK) {
                    // no harm here
                    return;
                }
            } catch (SystemException e) {
                // can't determine status; fall out
            }
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            if (importedTransaction.activated()) try {
                importedTransaction.doRollback();
            } catch (HeuristicCommitException e) {
                throw Log.log.heuristicCommitXa(XAException.XA_HEURCOM, e);
            } catch (HeuristicMixedException e) {
                throw Log.log.heuristicMixedXa(XAException.XA_HEURMIX, e);
            } catch (HeuristicRollbackException e) {
                throw Log.log.heuristicRollbackXa(XAException.XA_HEURRB, e);
            } catch (IllegalStateException e) {
                throw Log.log.illegalStateXa(XAException.XAER_NOTA, e);
            } catch (Throwable /* RuntimeException | SystemException */ e) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e);
            } finally {
                ext.removeImportedTransaction(gtid);
            }
        }

        public void end(final int flags) throws XAException {
            if (flags != XAResource.TMFAIL) {
                return;
            }
            final Transaction transaction = this.transaction;
            try {
                final int status = transaction.getStatus();
                if (status == Status.STATUS_MARKED_ROLLBACK || status == Status.STATUS_ROLLING_BACK || status == Status.STATUS_ROLLEDBACK) {
                    // no harm here
                    return;
                }
            } catch (SystemException e) {
                // can't determine status; fall out
            }
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            try {
                transaction.setRollbackOnly();
            } catch (IllegalStateException e) {
                throw Log.log.illegalStateXa(XAException.XAER_NOTA, e);
            } catch (Throwable /* RuntimeException | SystemException */ e) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e);
            }
        }

        public void beforeCompletion() throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            try {
                if (! importedTransaction.doBeforeCompletion()) {
                    throw new XAException(XAException.XAER_RMERR);
                }
            } catch (IllegalStateException e) {
                throw Log.log.illegalStateXa(XAException.XAER_NOTA, e);
            } catch (Throwable /* RuntimeException | SystemException */ e) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e);
            }
        }

        public int prepare() throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            final int tpo = importedTransaction.doPrepare();
            switch (tpo) {
                case PREPARE_READONLY:
                    ext.removeImportedTransaction(gtid);
                    return XAResource.XA_RDONLY;

                case PREPARE_OK:
                    return XAResource.XA_OK;

                case PREPARE_NOTOK:
                    //noinspection TryWithIdenticalCatches
                    try {
                        importedTransaction.doRollback();
                    } catch (HeuristicCommitException | HeuristicMixedException | HeuristicRollbackException | SystemException e) {
                        // TODO: the old code removes the transaction here, but JBTM-427 implies that the TM should do this explicitly later; for now keep old behavior
                        ext.removeImportedTransaction(gtid);
                        // JBTM-427; JTA doesn't allow heuristic codes on prepare :(
                        throw initializeSuppressed(Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e), importedTransaction);
                    } catch (Throwable t) {
                        // maybe still remove the transaction...
                        ext.removeImportedTransaction(gtid);
                        throw initializeSuppressed(Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, t), importedTransaction);
                    }
                    throw initializeSuppressed(new XAException(XAException.XA_RBROLLBACK), importedTransaction);

                case INVALID_TRANSACTION:
                    throw new XAException(XAException.XAER_NOTA);

                default:
                    throw new XAException(XAException.XA_RBOTHER);
            }
        }

        public void forget() throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            try {
                importedTransaction.doForget();
            } catch (IllegalStateException e) {
                throw Log.log.illegalStateXa(XAException.XAER_NOTA, e);
            } catch (Throwable /* RuntimeException | SystemException */ e) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e);
            }
        }

        public void commit(final boolean onePhase) throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            try {
                if (onePhase) {
                    importedTransaction.doOnePhaseCommit();
                } else {
                    if (! importedTransaction.doCommit()) {
                        dropLocal(importedTransaction);
                        ext.doRecover(null, null);
                        throw new XAException(XAException.XA_RETRY);
                    }
                }
            } catch (XAException e) {
                throw initializeSuppressed(e, importedTransaction);
            } catch (HeuristicMixedException e) {
                throw initializeSuppressed(Log.log.heuristicMixedXa(XAException.XA_HEURMIX, e), importedTransaction);
            } catch (RollbackException e) {
                throw initializeSuppressed(Log.log.rollbackXa(XAException.XA_RBROLLBACK, e), importedTransaction);
            } catch (HeuristicCommitException e) {
                throw initializeSuppressed(Log.log.heuristicCommitXa(XAException.XA_HEURCOM, e), importedTransaction);
            } catch (HeuristicRollbackException e) {
                throw initializeSuppressed(Log.log.heuristicRollbackXa(XAException.XA_HEURRB, e), importedTransaction);
            } catch (IllegalStateException e) {
                throw initializeSuppressed(Log.log.illegalStateXa(XAException.XAER_NOTA, e), importedTransaction);
            } catch (Throwable e) {
                throw initializeSuppressed(Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e), importedTransaction);
            }
        }

        private XAException initializeSuppressed(final XAException ex, final ImportedTransaction transaction) {
            if(ex != null && transaction.supportsDeferredThrowables()) {
                for(Throwable suppressedThrowable: transaction.getDeferredThrowables()) {
                    ex.addSuppressed(suppressedThrowable);
                }
            }
            return ex;
        }
    }

    final class XAImporterImpl implements XAImporter {

        public ImportResult<Transaction> findOrImportTransaction(final Xid xid, final int timeout, final boolean doNotImport) throws XAException {
            try {
                final SimpleXid simpleXid = SimpleXid.of(xid);
                final SimpleXid gtid = simpleXid.withoutBranch();
                final ConcurrentMap<SimpleXid, Entry> known = JBossLocalTransactionProvider.this.known;
                Entry entry = known.get(gtid);
                if (entry != null) {
                    return new ImportResult<Transaction>(entry.getTransaction(), entry, false);
                }
                final boolean imported;
                Transaction transaction;
                if (doNotImport) {
                    imported = false;
                    transaction = ext.getTransaction(xid);

                    if (transaction == null) {
                        return null;
                    }
                } else {
                    final TransactionImportResult result = ext.importTransaction(xid, timeout);
                    transaction = result.getTransaction();
                    imported = result.isNewImportedTransaction();
                }
                entry = getEntryFor(transaction, gtid);
                final Entry appearing = known.putIfAbsent(gtid, entry);
                if (appearing != null) {
                    // even if someone else beat us to the map, we still might have imported first... preserve the original Entry for economy though
                    return new ImportResult<Transaction>(transaction, appearing, imported);
                } else {
                    return new ImportResult<Transaction>(transaction, entry, imported);
                }
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        public Transaction findExistingTransaction(final Xid xid) throws XAException {
            try {
                final SimpleXid simpleXid = SimpleXid.of(xid);
                final SimpleXid gtid = simpleXid.withoutBranch();
                final ConcurrentMap<SimpleXid, Entry> known = JBossLocalTransactionProvider.this.known;
                Entry entry = known.get(gtid);
                if (entry != null) {
                    return entry.getTransaction();
                }
                final Transaction transaction = ext.getTransaction(xid);
                if (transaction == null) {
                    return null;
                }
                return known.computeIfAbsent(gtid, g -> getEntryFor(transaction, g)).getTransaction();
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        public void commit(final Xid xid, final boolean onePhase) throws XAException {
            try {
                Entry entry = known.get(SimpleXid.of(xid).withoutBranch());
                if (entry != null) {
                    entry.commit(onePhase);
                } else {
                    throw new XAException(XAException.XAER_NOTA);
                }
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        public void forget(final Xid xid) throws XAException {
            try {
                Entry entry = known.get(SimpleXid.of(xid).withoutBranch());
                if (entry != null) {
                    entry.forget();
                } else {
                    throw new XAException(XAException.XAER_NOTA);
                }
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        @NotNull
        public Xid[] recover(final int flag, final String parentNodeName) throws XAException {
            try {
                try {
                    final Xid[] xids = ext.doRecover(null, parentNodeName);
                    return xids == null ? SimpleXid.NO_XIDS : xids;
                } catch (NotSupportedException e) {
                    throw new XAException(XAException.XAER_RMFAIL);
                }
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
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
        private XAResourceRecoveryRegistry xaResourceRecoveryRegistry;
        private Path xaRecoveryLogDirRelativeToPath;

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
        @Deprecated
        public XATerminator getXATerminator() {
            return xaTerminator;
        }

        /**
         * Set the XA terminator.
         *
         * @param xt the XA terminator (must not be {@code null})
         */
        @Deprecated
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
        @Deprecated
        public TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
            return transactionSynchronizationRegistry;
        }

        /**
         * Set the transaction synchronization registry.
         *
         * @param tsr the transaction synchronization registry
         */
        @Deprecated
        public Builder setTransactionSynchronizationRegistry(final TransactionSynchronizationRegistry tsr) {
            Assert.checkNotNullParam("tsr", tsr);
            this.transactionSynchronizationRegistry = tsr;
            return this;
        }

        /**
         * Set the xa resource recovery registry.
         *
         * @param reg xa resource recovery registry (must not be {@code null})
         */
        public Builder setXAResourceRecoveryRegistry(final XAResourceRecoveryRegistry reg) {
            Assert.checkNotNullParam("reg", reg);
            this.xaResourceRecoveryRegistry = reg;
            return this;
        }

        /**
         * Set the xa recovery log dir relative to path
         *
         * @param path the xa recovery log file relative to path (must not be {@code null}
         */
        public Builder setXARecoveryLogDirRelativeToPath(Path path) {
            Assert.checkNotNullParam("path", path);
            this.xaRecoveryLogDirRelativeToPath = path;
            return this;
        }

        /**
         * Build this provider.  If any required properties are {@code null}, an exception is thrown.
         *
         * @return the built provider (not {@code null})
         * @throws IllegalArgumentException if a required property is {@code null}
         */
        public JBossLocalTransactionProvider build() {
            ExtendedJBossXATerminator extendedJBossXATerminator = this.extendedJBossXATerminator;
            TransactionManager transactionManager = this.transactionManager;
            int staleTransactionTime = this.staleTransactionTime;
            Assert.checkNotNullParam("extendedJBossXATerminator", extendedJBossXATerminator);
            Assert.checkNotNullParam("transactionManager", transactionManager);
            Assert.checkMinimumParameter("staleTransactionTime", 0, staleTransactionTime);
            Assert.checkNotNullParam("xaResourceRecoveryRegistry", xaResourceRecoveryRegistry);
            if (transactionManager instanceof com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple
             || transactionManager instanceof com.arjuna.ats.jbossatx.jta.TransactionManagerDelegate) {
                return new JBossJTALocalTransactionProvider(staleTransactionTime, extendedJBossXATerminator,
                        transactionManager, xaResourceRecoveryRegistry, xaRecoveryLogDirRelativeToPath);
            } else if (transactionManager instanceof com.arjuna.ats.internal.jta.transaction.jts.TransactionManagerImple
             || transactionManager instanceof com.arjuna.ats.jbossatx.jts.TransactionManagerDelegate) {
                return new JBossJTSLocalTransactionProvider(staleTransactionTime, extendedJBossXATerminator,
                        transactionManager, xaResourceRecoveryRegistry, xaRecoveryLogDirRelativeToPath);
            } else {
                throw Log.log.unknownTransactionManagerType(transactionManager.getClass());
            }
        }
    }
}
