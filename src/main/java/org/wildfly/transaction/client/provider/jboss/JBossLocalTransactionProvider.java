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

import static java.lang.Long.signum;

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
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple;
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
    private static final Object ENTRY_KEY = new Object();

    private final int staleTransactionTime;
    private final ExtendedJBossXATerminator ext;
    private final XATerminator xt;
    private final TransactionManager tm;
    private final TransactionSynchronizationRegistry tsr;
    private final XAImporterImpl xi = new XAImporterImpl();

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
            final Transaction transaction = tm.suspend();
            SimpleXid gtid = SimpleXid.of(((TransactionImple) transaction).getTxId()).withoutBranch();
            xi.registerNew(gtid, transaction, timeout);
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
        return ((TransactionImple) transaction).getTxLocalResource(key);
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
        ((TransactionImple) transaction).putTxLocalResource(key, value);
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
        return ((TransactionImple) transaction).get_uid();
    }

    public void commitLocal(@NotNull final Transaction transaction) throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).commitLocal();
    }

    public void rollbackLocal(@NotNull final Transaction transaction) throws IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).rollbackLocal();
    }

    public int getTimeout(@NotNull final Transaction transaction) {
        return ((TransactionImple) transaction).getTimeout();
    }

    @NotNull
    public Xid getXid(@NotNull final Transaction transaction) {
        return ((TransactionImple) transaction).getTxId();
    }

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
        if (entry == null) {
            synchronized (ENTRY_KEY) {
                entry = (Entry) getResource(transaction, ENTRY_KEY);
                if (entry == null) {
                    putResource(transaction, ENTRY_KEY, entry = new Entry(gtid, transaction));
                }
            }
        }
        return entry;
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

    static final int BIT_BEFORE_COMP = 1 << 0;
    static final int BIT_PREPARE_OR_ROLLBACK = 1 << 1;
    static final int BIT_COMMIT_OR_FORGET = 1 << 2;

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
    }

    final class Entry implements SubordinateTransactionControl {
        private final SimpleXid gtid;
        private final Transaction transaction;
        private final AtomicInteger completionBits = new AtomicInteger(0);
        private final long start = System.nanoTime();

        Entry(final SimpleXid gtid, final Transaction transaction) {
            this.gtid = gtid;
            this.transaction = transaction;
        }

        SimpleXid getGtid() {
            return gtid;
        }

        Transaction getTransaction() {
            return transaction;
        }

        int getTimeout() {
            return ((TransactionImple) transaction).getTimeout();
        }

        void rollbackLocal() throws SystemException {
            if (transaction instanceof ImportedTransaction) {
                throw Log.log.rollbackOnImported();
            }
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_PREPARE_OR_ROLLBACK) != 0) {
                    throw Log.log.invalidTxnState();
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
            transaction.rollback();
        }

        void commitLocal() throws HeuristicRollbackException, RollbackException, HeuristicMixedException, SystemException {
            if (transaction instanceof ImportedTransaction) {
                throw Log.log.commitOnImported();
            }
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_PREPARE_OR_ROLLBACK) != 0 || (oldVal & BIT_COMMIT_OR_FORGET) != 0) {
                    throw Log.log.invalidTxnState();
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_COMMIT_OR_FORGET | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
            transaction.commit();
        }

        public void rollback() throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_PREPARE_OR_ROLLBACK) != 0) {
                    throw Log.log.invalidTxStateXa(XAException.XAER_NOTA);
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
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
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            if (flags == XAResource.TMFAIL) {
                if (false /* JBTM-2846 */) try {
                    transaction.setRollbackOnly();
                } catch (IllegalStateException e) {
                    throw Log.log.illegalStateXa(XAException.XAER_NOTA, e);
                } catch (Throwable /* RuntimeException | SystemException */ e) {
                    throw Log.log.resourceManagerErrorXa(XAException.XAER_RMERR, e);
                }
            }
        }

        public void beforeCompletion() throws XAException {
            final Transaction transaction = this.transaction;
            if (! (transaction instanceof ImportedTransaction)) {
                throw Log.log.notImportedXa(XAException.XAER_NOTA);
            }
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_BEFORE_COMP) != 0) {
                    throw Log.log.invalidTxStateXa(XAException.XAER_NOTA);
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_BEFORE_COMP));
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
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_PREPARE_OR_ROLLBACK) != 0) {
                    throw Log.log.invalidTxStateXa(XAException.XAER_NOTA);
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
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
            int oldVal;
            do {
                oldVal = completionBits.get();
                if ((oldVal & BIT_COMMIT_OR_FORGET) != 0) {
                    throw Log.log.invalidTxStateXa(XAException.XAER_NOTA);
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_COMMIT_OR_FORGET | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
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
            int oldVal;
            do {
                oldVal = completionBits.get();
                if (onePhase && (oldVal & BIT_PREPARE_OR_ROLLBACK) != 0 || (oldVal & BIT_COMMIT_OR_FORGET) != 0) {
                    throw Log.log.invalidTxStateXa(XAException.XAER_NOTA);
                }
            } while (! completionBits.compareAndSet(oldVal, oldVal | BIT_COMMIT_OR_FORGET | BIT_PREPARE_OR_ROLLBACK | BIT_BEFORE_COMP));
            final ImportedTransaction importedTransaction = (ImportedTransaction) transaction;
            try {
                if (onePhase) {
                    importedTransaction.doOnePhaseCommit();
                } else {
                    if (! importedTransaction.doCommit()) {
                        throw new XAException(XAException.XA_RETRY);
                    }
                }
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
        // TODO: use a concurrent navigable collection, with a periodic flush
        private final Map<SimpleXid, Entry> knownImported = Collections.synchronizedMap(new LinkedHashMap<SimpleXid, Entry>());

        @NotNull
        public ImportResult<Transaction> findOrImportTransaction(final Xid xid, final int timeout) throws XAException {
            try {
                final SimpleXid simpleXid = SimpleXid.of(xid);
                final SimpleXid gtid = simpleXid.withoutBranch();
                final int status;
                final Map<SimpleXid, Entry> knownImported = this.knownImported;
                synchronized (knownImported) {
                    Entry entry = knownImported.get(gtid);
                    if (entry != null) {
                        return new ImportResult<Transaction>(entry.getTransaction(), entry, false);
                    }
                    final TransactionImportResult result = ext.importTransaction(xid, timeout);
                    final ImportedTransaction transaction = result.getTransaction();
                    final int newTimeout = timeout + staleTransactionTime;
                    entry = getEntryFor(transaction, gtid);
                    knownImported.put(gtid, entry);
                    return new ImportResult<Transaction>(transaction, entry, result.isNewImportedTransaction());
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
                final int status;
                Entry entry = knownImported.get(gtid);
                if (entry != null) {
                    return entry.getTransaction();
                }
                final Transaction transaction = ext.getTransaction(xid);
                if (transaction == null) {
                    return null;
                }
                knownImported.putIfAbsent(gtid, entry = getEntryFor(transaction, gtid));
                return entry.getTransaction();
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        public void commit(final Xid xid, final boolean onePhase) throws XAException {
            try {
                Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
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
                Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
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

        public int prepare(final Xid xid) throws XAException {
            try {
                Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
                if (entry != null) {
                    return entry.prepare();
                } else {
                    throw new XAException(XAException.XAER_NOTA);
                }
            } catch (XAException e) {
                throw e;
            } catch (Throwable t) {
                throw Log.log.resourceManagerErrorXa(XAException.XAER_RMFAIL, t);
            }
        }

        public void rollback(final Xid xid) throws XAException {
            try {
                Entry entry = knownImported.get(SimpleXid.of(xid).withoutBranch());
                if (entry != null) {
                    entry.rollback();
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

        private ImportedTransaction requireTxn(final Xid xid) throws XAException {
            final ImportedTransaction importedTransaction = ext.getImportedTransaction(xid);
            if (importedTransaction == null) {
                throw Log.log.notActiveXA(XAException.XAER_NOTA);
            }
            return importedTransaction;
        }

        void registerNew(final SimpleXid gtid, final Transaction transaction, final int timeout) {
            knownImported.put(gtid, getEntryFor(transaction, gtid));
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
