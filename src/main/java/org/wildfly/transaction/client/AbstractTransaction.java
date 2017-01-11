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

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

/**
 * A managed transaction.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public abstract class AbstractTransaction implements Transaction {
    private final Object outflowLock = new Object();

    AbstractTransaction() {
    }

    abstract void registerInterposedSynchronization(Synchronization synchronization) throws IllegalStateException;

    abstract Object getResource(Object key) throws NullPointerException;

    abstract void putResource(Object key, Object value) throws NullPointerException;

    abstract Object getKey();

    boolean getRollbackOnly() {
        try {
            return getStatus() == Status.STATUS_MARKED_ROLLBACK;
        } catch (SystemException e) {
            return false;
        }
    }

    abstract void suspend() throws SystemException;

    abstract void resume() throws SystemException;

    abstract void verifyAssociation();

    /**
     * Get a provider-specific interface from this transaction.
     *
     * @param providerInterfaceType the provider interface type class (must not be {@code null})
     * @param <T> the provider interface type
     * @return the provider interface, or {@code null} if the given type isn't supported by this transaction's provider
     */
    public <T> T getProviderInterface(Class<T> providerInterfaceType) {
        return null;
    }

    Object getOutflowLock() {
        return outflowLock;
    }
}
