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
import org.wildfly.transaction.client.XAImporter;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;

/**
 * The local transaction provider for JBoss application servers.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class JBossLocalTransactionProvider implements LocalTransactionProvider {
    private final ExtendedJBossXATerminator ext;
    private final XATerminator xt;
    private final TransactionManager tm;
    private final TransactionSynchronizationRegistry tsr;
    private final XAImporter xi = new XAImporterImpl();

    public JBossLocalTransactionProvider(final XATerminator xt, final ExtendedJBossXATerminator ext, final TransactionManager tm, final TransactionSynchronizationRegistry tsr) {
        this.ext = Assert.checkNotNullParam("ext", ext);
        this.xt = Assert.checkNotNullParam("xt", xt);
        this.tm = Assert.checkNotNullParam("tm", tm);
        this.tsr = Assert.checkNotNullParam("tsr", tsr);
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

    final class XAImporterImpl implements XAImporter {
        @NotNull
        public ProviderImportResult findOrImportTransaction(final Xid xid, final int timeout) throws XAException {
            final TransactionImportResult result = ext.importTransaction(xid, timeout);
            return new ProviderImportResult(result.getTransaction(), result.isNewImportedTransaction());
        }

        public Transaction findExistingTransaction(final Xid xid) throws XAException {
            return ext.getTransaction(xid);
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
}
