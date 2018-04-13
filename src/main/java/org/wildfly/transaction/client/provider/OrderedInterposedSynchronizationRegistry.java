/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018 Red Hat, Inc., and individual contributors
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

package org.wildfly.transaction.client.provider;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.Synchronization;
import javax.transaction.TransactionSynchronizationRegistry;

import org.wildfly.transaction.client.AbstractTransaction;
import org.wildfly.transaction.client.ContextTransactionManager;
import org.wildfly.transaction.client.ContextTransactionSynchronizationRegistry;
import org.wildfly.transaction.client.CreationListener;
import org.wildfly.transaction.client._private.Log;

/**
 * A registry for supporting disagreeable specifications that require interposed synchronizations to run in a specific
 * order.
 */
public final class OrderedInterposedSynchronizationRegistry {
    private final CreationListener creationListener = new CreationListenerImpl();
    private final List<Registry> registries = new ArrayList<>();

    /**
     * Construct a new instance.
     */
    public OrderedInterposedSynchronizationRegistry() {
    }

    /**
     * Get the transaction creation listener.  The listener must be registered with each transaction context that
     * may be active when the sub-registries may be used.
     *
     * @return the transaction creation listener (not {@code null})
     */
    public CreationListener getCreationListener() {
        return creationListener;
    }

    /**
     * Create a new transaction synchronization registry.  Registries are processed in the order they are created.
     *
     * @return the transaction synchronization registry (not {@code null})
     */
    public TransactionSynchronizationRegistry createRegistry() {
        final Registry registry = new Registry();
        final List<Registry> registries = this.registries;
        synchronized (registries) {
            registries.add(registry);
        }
        return registry;
    }

    final class CreationListenerImpl implements CreationListener {
        CreationListenerImpl() {
        }

        public void transactionCreated(final AbstractTransaction transaction, final CreatedBy createdBy) {
            final List<Registry> registries = OrderedInterposedSynchronizationRegistry.this.registries;
            synchronized (registries) {
                for (Registry registry : registries) {
                    final TransactionSynchronizationList list = new TransactionSynchronizationList();
                    transaction.putResource(registry.getResourceKey(), list);
                    ContextTransactionSynchronizationRegistry.getInstance().registerInterposedSynchronization(list);
                }
            }
        }
    }

    final class Registry implements TransactionSynchronizationRegistry {
        private final Object resourceKey = new Object();

        Registry() {
        }

        Object getResourceKey() {
            return resourceKey;
        }

        public Object getTransactionKey() {
            return ContextTransactionSynchronizationRegistry.getInstance().getTransactionKey();
        }

        public int getTransactionStatus() {
            return ContextTransactionSynchronizationRegistry.getInstance().getTransactionStatus();
        }

        public boolean getRollbackOnly() throws IllegalStateException {
            return ContextTransactionSynchronizationRegistry.getInstance().getRollbackOnly();
        }

        public void setRollbackOnly() throws IllegalStateException {
            ContextTransactionSynchronizationRegistry.getInstance().setRollbackOnly();
        }

        public void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
            final AbstractTransaction transaction = ContextTransactionManager.getInstance().getTransaction();
            TransactionSynchronizationList list = (TransactionSynchronizationList) transaction.getResource(resourceKey);
            if (list == null) {
                throw Log.log.unknownTransaction();
            }
            list.registerSynchronization(sync);
        }

        public Object getResource(final Object key) throws IllegalStateException {
            return ContextTransactionSynchronizationRegistry.getInstance().getResource(key);
        }

        public void putResource(final Object key, final Object value) throws IllegalStateException {
            ContextTransactionSynchronizationRegistry.getInstance().putResource(key, value);
        }
    }
}
