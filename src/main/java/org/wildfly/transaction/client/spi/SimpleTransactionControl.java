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

package org.wildfly.transaction.client.spi;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client._private.Log;

/**
 * An interface implemented by transaction handles that provide direct/simple commit and rollback functionality.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface SimpleTransactionControl {
    /**
     * Commit this transaction.  Any provider-specific thread association with this transaction is cancelled regardless
     * of the outcome of this method.
     *
     * @throws RollbackException if the transaction was rolled back rather than committed
     * @throws HeuristicMixedException if a heuristic decision was made resulting in a mix of committed and rolled back resources
     * @throws HeuristicRollbackException if a heuristic decision was made and all resources were rolled back
     * @throws SecurityException if the current thread or user is not authorized to modify the transaction
     * @throws SystemException if there is an unexpected error condition
     */
    void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException;

    /**
     * Roll back this transaction.  Any provider-specific thread association with this transaction is cancelled regardless
     * of the outcome of this method.
     *
     * @throws SecurityException if the current thread or user is not authorized to modify the transaction
     * @throws SystemException if there is an unexpected error condition
     */
    void rollback() throws SecurityException, SystemException;

    /**
     * Set the transaction to be rollback-only.  The transaction system is generally guaranteed to only call {@link
     * #rollback()}
     * after this method, so its implementation is optional, however it may be useful to hint to the remote system that
     * the transaction will only be rolled back.
     *
     * @throws SystemException if an unexpected error occurs
     */
    default void setRollbackOnly() throws SystemException {
    }

    /**
     * Safely roll back a transaction without throwing an exception; useful in cases where rollback failure is unrecoverable.
     */
    default void safeRollback() {
        try {
            rollback();
        } catch (SecurityException | SystemException e) {
            Log.log.rollbackFailed(e);
        }
    }

    /**
     * A simple transaction control facade over a transaction manager {@code Transaction} object.
     *
     * @param transaction the transaction (must not be {@code null})
     * @return the simple transaction control facade (not {@code null})
     */
    static SimpleTransactionControl of(Transaction transaction) {
        Assert.checkNotNullParam("transaction", transaction);
        return new SimpleTransactionControl() {
            public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
                transaction.commit();
            }

            public void rollback() throws SecurityException, SystemException {
                transaction.rollback();
            }

            public void setRollbackOnly() throws SystemException {
                transaction.setRollbackOnly();
            }

            public <T> T getProviderInterface(final Class<T> providerInterfaceType) {
                return providerInterfaceType.isInstance(transaction) ? providerInterfaceType.cast(transaction) : null;
            }
        };
    }

    /**
     * Get a provider-specific interface from this transaction controller.
     *
     * @param providerInterfaceType the provider interface type class (must not be {@code null})
     * @param <T> the provider interface type
     * @return the provider interface, or {@code null} if the given type isn't supported by this transaction's provider
     */
    <T> T getProviderInterface(Class<T> providerInterfaceType);
}
