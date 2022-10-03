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

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import javax.transaction.xa.Xid;

/**
 * A transaction creation listener, which is called when a new transaction is begun or imported.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@FunctionalInterface
public interface CreationListener {
    /**
     * A transaction has been created.
     *
     * @param transaction the transaction that was created (not {@code null})
     * @param createdBy the creator of the transaction (not {@code null})
     */
    void transactionCreated(AbstractTransaction transaction, CreatedBy createdBy);

    /**
     * The enumeration of possible initiators of a transaction.
     */
    enum CreatedBy {
        /**
         * The transaction was created by way of {@link UserTransaction#begin()}.
         */
        USER_TRANSACTION,
        /**
         * The transaction was created by way of {@link TransactionManager#begin()}.
         */
        TRANSACTION_MANAGER,
        /**
         * The transaction was imported by way of {@link LocalTransactionContext#findOrImportTransaction(Xid, int)} or
         * {@link LocalTransactionContext#findOrImportTransaction(Xid, int, boolean)}.
         */
        IMPORT,
        /**
         * The transaction was merged by way of {@link LocalTransactionContext#importProviderTransaction()}.
         */
        MERGE,
    }
}
