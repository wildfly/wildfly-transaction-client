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

package org.wildfly.transaction.client;

import jakarta.resource.spi.XATerminator;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.wildfly.transaction.client._private.Log;

class ContextXATerminator implements XATerminator {
    private final LocalTransactionContext transactionContext;

    ContextXATerminator(final LocalTransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        getImportedTransaction(xid).getControl().commit(onePhase);
    }

    public void forget(final Xid xid) throws XAException {
        getImportedTransaction(xid).getControl().forget();
    }

    public int prepare(final Xid xid) throws XAException {
        return getImportedTransaction(xid).getControl().prepare();
    }

    public void rollback(final Xid xid) throws XAException {
        getImportedTransaction(xid).getControl().rollback();
    }

    public Xid[] recover(final int flag) throws XAException {
        return transactionContext.getRecoveryInterface().recover(flag);
    }

    private ImportResult<LocalTransaction> getImportedTransaction(final Xid xid) throws XAException {
        final ImportResult<LocalTransaction> result = transactionContext.findOrImportTransaction(xid, 0, true);
        if (result == null) {
            throw Log.log.noTransactionXa(XAException.XAER_NOTA);
        }
        final LocalTransaction transaction = result.getTransaction();
        if (! transaction.isImported()) {
            throw Log.log.noTransactionXa(XAException.XAER_NOTA);
        }
        return result;
    }
}
