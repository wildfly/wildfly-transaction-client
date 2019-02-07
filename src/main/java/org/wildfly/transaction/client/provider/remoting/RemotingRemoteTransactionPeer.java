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
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLContext;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.ConnectionPeerIdentity;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.ServiceOpenException;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.security.auth.client.AuthenticationConfiguration;
import org.wildfly.security.auth.client.AuthenticationContext;
import org.wildfly.security.auth.client.AuthenticationContextConfigurationClient;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class RemotingRemoteTransactionPeer implements RemoteTransactionPeer {
    private static final Attachments.Key<RemotingOperations> key = new Attachments.Key<>(RemotingOperations.class);
    private static final AuthenticationContextConfigurationClient CLIENT = doPrivileged(AuthenticationContextConfigurationClient.ACTION);
    private final URI location;
    private final SSLContext sslContext;
    private final AuthenticationConfiguration authenticationConfiguration;
    private final AuthenticationContext authenticationContext;
    private final Endpoint endpoint;
    private final RemotingFallbackPeerProvider fallbackProvider;
    private final Set<Xid> rollbackOnlyXids = new ConcurrentHashMap<Xid, Boolean>().keySet(Boolean.TRUE);

    RemotingRemoteTransactionPeer(final URI location, final SSLContext sslContext, final AuthenticationConfiguration authenticationConfiguration, final Endpoint endpoint, final RemotingFallbackPeerProvider fallbackProvider) {
        this.location = location;
        this.sslContext = sslContext;
        this.authenticationConfiguration = authenticationConfiguration;
        this.authenticationContext = AuthenticationContext.captureCurrent();
        this.endpoint = endpoint;
        this.fallbackProvider = fallbackProvider;
    }

    ConnectionPeerIdentity getPeerIdentity() throws IOException {
        SSLContext finalSslContext;
        if (sslContext == null) {
            try {
                finalSslContext = CLIENT.getSSLContext(location, authenticationContext, "jta", "jboss");
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        } else {
            finalSslContext = sslContext;
        }
        AuthenticationConfiguration finalAuthenticationConfiguration;
        if (authenticationConfiguration == null) {
            finalAuthenticationConfiguration = CLIENT.getAuthenticationConfiguration(location, authenticationContext, -1, "jta", "jboss");
        } else {
            finalAuthenticationConfiguration = authenticationConfiguration;
        }
        return endpoint.getConnectedIdentity(location, finalSslContext, finalAuthenticationConfiguration).get();
    }

    ConnectionPeerIdentity getPeerIdentityXA() throws XAException {
        try {
            return getPeerIdentity();
        } catch (IOException e) {
            throw Log.log.failedToAcquireConnectionXA(e, XAException.XAER_RMFAIL);
        }
    }

    @NotNull
    RemotingOperations getOperations(Connection connection) throws IOException {
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
        } catch (ServiceOpenException e) {
            // try alternatives
            RemotingFallbackPeerProvider fallbackProvider = this.fallbackProvider;
            if (fallbackProvider == null) {
                throw e;
            }
            try {
                operations = fallbackProvider.getOperations(connection);
            } catch (ServiceOpenException e1) {
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

    RemotingOperations getOperationsXA(Connection connection) throws XAException {
        try {
            return getOperations(connection);
        } catch (IOException e) {
            throw Log.log.failedToAcquireConnectionXA(e, XAException.XAER_RMERR);
        }
    }

    @NotNull
    public SubordinateTransactionControl lookupXid(final Xid xid) throws XAException {
        return new SubordinateTransactionControl() {
            public void rollback() throws XAException {
                try {
                    final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                    getOperationsXA(peerIdentity.getConnection()).rollback(xid, peerIdentity);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void end(final int flags) throws XAException {
                if (flags == XAResource.TMFAIL && rollbackOnlyXids.add(xid)) try {
                    final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                    getOperationsXA(peerIdentity.getConnection()).setRollbackOnly(xid, peerIdentity);
                } catch (Throwable t) {
                    rollbackOnlyXids.remove(xid);
                    throw t;
                }
            }

            public void beforeCompletion() throws XAException {
                final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                getOperationsXA(peerIdentity.getConnection()).beforeCompletion(xid, peerIdentity);
            }

            public int prepare() throws XAException {
                try {
                    final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                    return getOperationsXA(peerIdentity.getConnection()).prepare(xid, peerIdentity);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void forget() throws XAException {
                try {
                    final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                    getOperationsXA(peerIdentity.getConnection()).forget(xid, peerIdentity);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }

            public void commit(final boolean onePhase) throws XAException {
                try {
                    final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
                    getOperationsXA(peerIdentity.getConnection()).commit(xid, onePhase, peerIdentity);
                } finally {
                    rollbackOnlyXids.remove(xid);
                }
            }
        };
    }

    @NotNull
    public Xid[] recover(final int flag, final String parentName) throws XAException {
        final ConnectionPeerIdentity peerIdentity = getPeerIdentityXA();
        return getOperationsXA(peerIdentity.getConnection()).recover(flag, parentName, peerIdentity);
    }

    @NotNull
    public SimpleTransactionControl begin(final int timeout) throws SystemException {
        // this one is bound to the connection
        try {
            final ConnectionPeerIdentity peerIdentity = getPeerIdentity();
            return getOperations(peerIdentity.getConnection()).begin(peerIdentity);
        } catch (IOException e) {
            throw Log.log.failedToAcquireConnection(e);
        }
    }
}
