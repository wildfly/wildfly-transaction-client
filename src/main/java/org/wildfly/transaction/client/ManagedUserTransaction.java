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

import java.net.URI;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.UserTransaction;

import org.wildfly.transaction.client._private.Log;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class ManagedUserTransaction implements UserTransaction {
    private final URI location;

    ManagedUserTransaction(final URI location) {
        this.location = location;
    }

    public void begin() throws NotSupportedException, SystemException {
        // we need to enlist; check for existing transaction
        TransactionManager transactionManager = this.transactionManager;
        Transaction transaction = transactionManager.getTransaction();
        boolean controlled = false;
        if (transaction == null) {

            transactionManager.begin();
            controlled = true;
            transaction = transactionManager.getTransaction();
        }
        RemoteTransactionXAResource resource = new RemoteTransactionXAResource(location, controlled);
        try {
            if (! transaction.enlistResource(resource)) {
                throw Log.log.remoteTransactionEnlistmentFailed(null);
            }
        } catch (RollbackException e) {
            throw Log.log.remoteTransactionEnlistmentFailed(e);
        }
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {

    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {

    }

    public int getStatus() throws SystemException {
        return 0;
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {

    }
}
