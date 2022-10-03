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

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.UserTransaction;

import org.wildfly.transaction.TransactionPermission;
import org.wildfly.transaction.client._private.Log;

/**
 * A {@code UserTransaction} instance that controls the transaction state of the current local provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class LocalUserTransaction implements UserTransaction, Serializable {
    private static final long serialVersionUID = -8082008006243656822L;

    private static final LocalUserTransaction instance = new LocalUserTransaction();

    private LocalUserTransaction() {
    }

    public void begin() throws NotSupportedException, SystemException {
        checkTransactionStateAvailability();
        ContextTransactionManager.getInstance().begin(CreationListener.CreatedBy.USER_TRANSACTION);
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        checkTransactionStateAvailability();
        ContextTransactionManager.getInstance().commit();
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        checkTransactionStateAvailability();
        ContextTransactionManager.getInstance().rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        checkTransactionStateAvailability();
        ContextTransactionManager.getInstance().setRollbackOnly();
    }

    public int getStatus() throws SystemException {
        checkTransactionStateAvailability();
        return ContextTransactionManager.getInstance().getStatus();
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {
        checkTransactionStateAvailability();
        ContextTransactionManager.getInstance().setTransactionTimeout(seconds);
    }

    public void setAvailability(boolean available) {
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(TransactionPermission.forName("modifyUserTransactionAvailability"));
        }

        ContextTransactionManager.getInstance().setAvailable(available);
    }

    public boolean isAvailable() {
        return ContextTransactionManager.getInstance().isAvailable();
    }

    Object readResolve() {
        return instance;
    }

    Object writeReplace() {
        return instance;
    }

    /**
     * Get the singleton instance.
     *
     * @return the singleton instance
     */
    public static LocalUserTransaction getInstance() {
        return instance;
    }

    /**
     * UserTransaction is could not be available within particular scopes
     * e.g. for CDI @Transactional and a TxType other than NOT_SUPPORTED or NEVER
     */
    private void checkTransactionStateAvailability() {
        if(!isAvailable()) {
            throw Log.log.forbiddenContextForUserTransaction();
        }
    }
}
