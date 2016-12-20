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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

/**
 * A {@code UserTransaction} instance that controls the transaction state of the current local provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class LocalUserTransaction implements UserTransaction {
    private static final LocalUserTransaction instance = new LocalUserTransaction();

    private LocalUserTransaction() {
    }

    public void begin() throws NotSupportedException, SystemException {
        ContextTransactionManager.getInstance().begin();
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        ContextTransactionManager.getInstance().commit();
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        ContextTransactionManager.getInstance().rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        ContextTransactionManager.getInstance().setRollbackOnly();
    }

    public int getStatus() throws SystemException {
        return ContextTransactionManager.getInstance().getStatus();
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {
        ContextTransactionManager.getInstance().setTransactionTimeout(seconds);
    }

    /**
     * Get the singleton instance.
     *
     * @return the singleton instance
     */
    public static LocalUserTransaction getInstance() {
        return instance;
    }
}
