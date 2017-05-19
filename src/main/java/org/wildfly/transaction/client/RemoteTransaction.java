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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.wildfly.common.Assert;
import org.wildfly.transaction.TransactionPermission;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemoteTransaction extends AbstractTransaction {
    private final AtomicReference<State> stateRef;
    private final ConcurrentMap<Object, Object> resources = new ConcurrentHashMap<>();
    private final URI location;
    private final Object key = new Key();
    private final SimpleTransactionControl control;
    private final int timeout;

    RemoteTransaction(final SimpleTransactionControl control, final URI location, final int timeout) {
        super();
        stateRef = new AtomicReference<>(new Active());
        this.location = location;
        this.control = control;
        this.timeout = timeout;
    }

    Object getResource(final Object key) throws NullPointerException {
        Assert.checkNotNullParamWithNullPointerException("key", key);
        return resources.get(key);
    }

    void putResource(final Object key, final Object value) throws NullPointerException {
        Assert.checkNotNullParamWithNullPointerException("key", key);
        if (value == null) resources.remove(key); else resources.put(key, value);
    }

    Object getKey() {
        return key;
    }

    void suspend() {
        notifyAssociationListeners(false);
        // no operation
    }

    void resume() {
        notifyAssociationListeners(true);
        // no operation
    }

    void verifyAssociation() {
        // no operation
    }

    public int getTransactionTimeout() {
        return timeout;
    }

    public <T> T getProviderInterface(final Class<T> providerInterfaceType) {
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(TransactionPermission.forName("getProviderInterface"));
        }
        return control.getProviderInterface(providerInterfaceType);
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
        stateRef.get().commit();
    }

    void commitAndDissociate() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
        try {
            commit();
        } finally {
            suspend();
        }
    }

    public void rollback() throws IllegalStateException, SystemException {
        stateRef.get().rollback();
    }

    void rollbackAndDissociate() throws IllegalStateException, SystemException {
        try {
            rollback();
        } finally {
            suspend();
        }
    }

    boolean importBacking() {
        return false;
    }

    void unimportBacking() {
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        stateRef.get().setRollbackOnly();
    }

    public int getStatus() {
        return stateRef.get().getStatus();
    }

    public boolean enlistResource(final XAResource xaRes) {
        Assert.checkNotNullParam("xaRes", xaRes);
        return false;
    }

    public boolean delistResource(final XAResource xaRes, final int flag) {
        Assert.checkNotNullParam("xaRes", xaRes);
        return false;
    }

    public void registerSynchronization(final Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
        Assert.checkNotNullParam("sync", sync);
        stateRef.get().registerSynchronization(new AssociatingSynchronization(sync));
    }

    void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
        Assert.checkNotNullParam("sync", sync);
        stateRef.get().registerInterposedSynchronization(new AssociatingSynchronization(sync));
    }

    public int hashCode() {
        return System.identityHashCode(this);
    }

    public boolean equals(final Object obj) {
        return obj == this;
    }

    public String toString() {
        return String.format("Remote transaction \"%s\" (delegate=%s)", location, control);
    }

    /**
     * Get the location of this remote transaction.
     *
     * @return the location of this remote transaction (not {@code null})
     */
    public URI getLocation() {
        return location;
    }

    abstract static class State {
        abstract void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException;

        abstract void rollback() throws IllegalStateException, SystemException;

        abstract void setRollbackOnly() throws IllegalStateException, SystemException;

        abstract int getStatus();

        abstract void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException, SystemException;

        abstract void registerInterposedSynchronization(Synchronization sync) throws IllegalStateException;
    }

    abstract class Unresolved extends State {

        Unresolved() {
        }

        void rollback() throws IllegalStateException, SystemException {
            final AtomicReference<State> stateRef = RemoteTransaction.this.stateRef;
            if (! stateRef.compareAndSet(this, InactiveState.ROLLING_BACK)) {
                stateRef.get().rollback();
                return;
            }
            try {
                control.rollback();
            } catch (SecurityException e) {
                stateRef.set(this);
                throw e;
            } catch (Throwable t) {
                stateRef.set(InactiveState.UNKNOWN);
                throw t;
            }
            stateRef.set(InactiveState.ROLLED_BACK);
        }

        void registerSynchronization(final Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
            throw Log.log.registerSynchRemoteTransaction();
        }

        void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
            throw Log.log.registerSynchRemoteTransaction();
        }
    }

    final class Active extends Unresolved {
        Active() {
        }

        void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
            final AtomicReference<State> stateRef = RemoteTransaction.this.stateRef;
            if (! stateRef.compareAndSet(this, InactiveState.COMMITTING)) {
                stateRef.get().commit();
                return;
            }
            try {
                control.commit();
            } catch (SecurityException e) {
                stateRef.set(this);
                throw e;
            } catch (RollbackException | HeuristicRollbackException e) {
                stateRef.set(InactiveState.ROLLED_BACK);
                throw e;
            } catch (Throwable t) {
                stateRef.set(InactiveState.UNKNOWN);
                throw t;
            }
            stateRef.set(InactiveState.COMMITTED);
        }

        int getStatus() {
            return Status.STATUS_ACTIVE;
        }

        void setRollbackOnly() throws IllegalStateException, SystemException {
            final AtomicReference<State> stateRef = RemoteTransaction.this.stateRef;
            synchronized (stateRef) {
                // there's no "marking rollback" state
                final RollbackOnly newState = new RollbackOnly();
                if (! stateRef.compareAndSet(this, newState)) {
                    stateRef.get().setRollbackOnly();
                }
                control.setRollbackOnly();
            }
        }
    }

    final class RollbackOnly extends Unresolved {
        RollbackOnly() {}

        void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
            rollback();
            throw Log.log.rollbackOnlyRollback();
        }

        void setRollbackOnly() {
            // no operation
        }

        int getStatus() {
            return Status.STATUS_MARKED_ROLLBACK;
        }

        void registerSynchronization(final Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
            throw Log.log.markedRollbackOnly();
        }
    }

    static final class InactiveState extends State {
        private final int status;

        static final InactiveState ROLLED_BACK = new InactiveState(Status.STATUS_ROLLEDBACK);
        static final InactiveState COMMITTED = new InactiveState(Status.STATUS_COMMITTED);
        static final InactiveState UNKNOWN = new InactiveState(Status.STATUS_UNKNOWN);

        static final InactiveState COMMITTING = new InactiveState(Status.STATUS_COMMITTING);
        static final InactiveState ROLLING_BACK = new InactiveState(Status.STATUS_ROLLING_BACK);

        private InactiveState(final int status) {
            this.status = status;
        }

        void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
            throw Log.log.notActive();
        }

        void rollback() throws IllegalStateException, SystemException {
            throw Log.log.notActive();
        }

        void setRollbackOnly() throws IllegalStateException, SystemException {
            if (status != Status.STATUS_ROLLING_BACK) {
                throw Log.log.notActive();
            }
        }

        int getStatus() {
            return status;
        }

        void registerSynchronization(final Synchronization sync) throws RollbackException, IllegalStateException, SystemException {
            throw Log.log.notActive();
        }

        void registerInterposedSynchronization(final Synchronization sync) throws IllegalStateException {
            throw Log.log.notActive();
        }
    }

    class Key {
        Key() {
        }

        public int hashCode() {
            return location.hashCode();
        }

        public boolean equals(final Object obj) {
            return obj instanceof Key && equals((Key) obj);
        }

        private boolean equals(final Key key) {
            return location.equals(key.getLocation());
        }

        private URI getLocation() {
            return location;
        }
    }
}

