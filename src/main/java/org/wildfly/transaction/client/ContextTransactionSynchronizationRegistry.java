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
import javax.transaction.TransactionSynchronizationRegistry;

import org.wildfly.transaction.client._private.Log;

/**
 * A {@code TransactionSynchronizationRegistry} which operates against the current local provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ContextTransactionSynchronizationRegistry implements TransactionSynchronizationRegistry {
    private static final ContextTransactionSynchronizationRegistry instance = new ContextTransactionSynchronizationRegistry();

    private ContextTransactionSynchronizationRegistry() {
    }

    public static ContextTransactionSynchronizationRegistry getInstance() {
        return instance;
    }

    public Object getTransactionKey() {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getStateRef().get().transaction;
        return transaction == null ? null : transaction.getKey();
    }

    public int getTransactionStatus() {
        try {
            return ContextTransactionManager.getInstance().getStatus();
        } catch (SystemException e) {
            return Status.STATUS_UNKNOWN;
        }
    }

    public boolean getRollbackOnly() throws IllegalStateException {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getStateRef().get().transaction;
        if (transaction == null) {
            throw Log.log.noTransaction();
        }
        return transaction.getRollbackOnly();
    }

    public void setRollbackOnly() throws IllegalStateException {
        try {
            ContextTransactionManager.getInstance().setRollbackOnly();
        } catch (SystemException e) {
            // ignored
        }
    }

    public void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getStateRef().get().transaction;
        if (transaction == null) {
            throw Log.log.noTransaction();
        }
        transaction.registerInterposedSynchronization(sync);
    }

    public Object getResource(final Object key) throws IllegalStateException {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getStateRef().get().transaction;
        if (transaction == null) {
            throw Log.log.noTransaction();
        }
        return transaction.getResource(key);
    }

    public void putResource(final Object key, final Object value) throws IllegalStateException {
        final AbstractTransaction transaction = ContextTransactionManager.getInstance().getStateRef().get().transaction;
        if (transaction == null) {
            throw Log.log.noTransaction();
        }
        transaction.putResource(key, value);
    }
}
