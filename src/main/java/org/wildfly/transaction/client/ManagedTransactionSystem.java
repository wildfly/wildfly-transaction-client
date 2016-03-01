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

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class ManagedTransactionSystem extends RemoteTransactionContext {
    private final TransactionManager transactionManager;

    ManagedTransactionSystem(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public UserTransaction getUserTransaction(final URI location) {
        return new ManagedUserTransaction(transactionManager, transactionSynchronizationRegistry, location);
    }

    TransactionManager getTransactionManager() {
        return transactionManager;
    }


}
