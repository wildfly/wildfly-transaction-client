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

package org.wildfly.transaction.client.provider.remoting;

import static java.security.AccessController.doPrivileged;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.ServiceNotFoundException;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;
import org.xnio.IoFuture;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class RemotingRemoteTransactionPeer implements RemoteTransactionPeer {
    private static final Attachments.Key<RemotingOperations> key = new Attachments.Key<>(RemotingOperations.class);
    private final URI location;
    private final Endpoint endpoint;
    private final RemotingFallbackPeerProvider fallbackProvider;
    private final Set<Xid> rollbackOnlyXids = new ConcurrentHashMap<Xid, Boolean>().keySet(Boolean.TRUE);

    RemotingRemoteTransactionPeer(final URI location, final Endpoint endpoint, final RemotingFallbackPeerProvider fallbackProvider) {
        this.location = location;
        this.endpoint = endpoint;
        this.fallbackProvider = fallbackProvider;
    }

    @NotNull
    RemotingOperations getOperations() throws IOException {
        final Connection connection = doPrivileged((PrivilegedAction<IoFuture<Connection>>) () -> endpoint.getConnection(location, "jta", "jboss")).get();
        final Attachments attachments = connection.getAttachments();
        RemotingOperations operations = attachments.getAttachment(key);
        if (operations != null) {
            return operations;
        }
        try {
            operations = TransactionClientChannel.forConnection(connection);
            final RemotingOperations appearing = attachments.attachIfAbsent(key, operations);
            if (appearing != null) {
                return appearing;
            }
        } catch (ServiceNotFoundException e) {
            // try alternatives
            RemotingFallbackPeerProvider fallbackProvider = this.fallbackProvider;
            if (fallbackProvider == null) {
                throw e;
            }
            try {
                operations = fallbackProvider.getOperations(connection);
            } catch (ServiceNotFoundException e1) {
                e1.addSuppressed(e);
                throw e1;
            }
            //noinspection ConstantConditions
            if (operations == null) {
                throw e;
            }
            final RemotingOperations appearing = attachments.attachIfAbsent(key, operations);
            if (appearing != null) {
                return appearing;
            }
        }
        return operations;
    }

    RemotingOperations getOperationsXA() throws XAException {
        try {
            return getOperations();
        } catch (IOException e) {
            throw Log.log.failedToAcquireConnectionXA(e, XAException.XAER_RMERR);
        }
    }

    @NotNull
    public SubordinateTransactionControl lookupXid(final Xid xid) throws XAException {
        return new SubordinateTransactionControl() {
            public void rollback() throws XAException {
                try {
                    getOperationsXA().rollback(xid);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void end(final int flags) throws XAException {
                if (flags == XAResource.TMFAIL && rollbackOnlyXids.add(xid)) try {
                    getOperationsXA().setRollbackOnly(xid);
                } catch (Throwable t) {
                    rollbackOnlyXids.remove(xid);
                    throw t;
                }
            }

            public void beforeCompletion() throws XAException {
                getOperationsXA().beforeCompletion(xid);
            }

            public int prepare() throws XAException {
                try {
                    return getOperationsXA().prepare(xid);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void forget() throws XAException {
                try {
                    getOperationsXA().forget(xid);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void commit(final boolean onePhase) throws XAException {
                try {
                    getOperationsXA().commit(xid, onePhase);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }
        };
    }

    @NotNull
    public Xid[] recover(final int flag, final String parentName) throws XAException {
        return getOperationsXA().recover(flag, parentName);
    }

    @NotNull
    public SimpleTransactionControl begin(final int timeout) throws SystemException {
        // this one is bound to the connection
        try {
            return getOperations().begin();
        } catch (IOException e) {
            throw Log.log.failedToAcquireConnection(e);
        }
    }
}
