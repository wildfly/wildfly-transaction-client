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

import java.net.URI;

import javax.net.ssl.SSLContext;
import jakarta.transaction.SystemException;
import javax.transaction.xa.XAException;

import org.wildfly.security.auth.client.AuthenticationConfiguration;

/**
 * A remote transaction transport provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface RemoteTransactionProvider extends TransactionProvider {
    /**
     * Get a handle for a specific peer.
     *
     * @param location the peer location
     * @param sslContext the sticky SSL context to use (may be {@code null})
     * @param authenticationConfiguration the sticky authentication configuration to use (may be {@code null})
     * @return the handle, or {@code null} if this provider does not support this location
     * @throws SystemException if handle acquisition has failed
     */
    RemoteTransactionPeer getPeerHandle(URI location, SSLContext sslContext, AuthenticationConfiguration authenticationConfiguration) throws SystemException;

    /**
     * Get a handle for a specific peer for an XA operation.  Identical to {@link #getPeerHandle(URI, SSLContext, AuthenticationConfiguration)} except
     * that an {@code XAException} is thrown in case of error instead of {@code SystemException}.
     *
     * @param location the peer location (not {@code null})
     * @param sslContext the sticky SSL context to use (may be {@code null})
     * @param authenticationConfiguration the sticky authentication configuration to use (may be {@code null})
     * @return the handle, or {@code null} if this provider does not support this location
     */
    default RemoteTransactionPeer getPeerHandleForXa(URI location, SSLContext sslContext, AuthenticationConfiguration authenticationConfiguration) throws XAException {
        try {
            return getPeerHandle(location, sslContext, authenticationConfiguration);
        } catch (SystemException e) {
            final XAException xaException = new XAException(e.getMessage());
            xaException.errorCode = XAException.XAER_RMFAIL;
            xaException.setStackTrace(e.getStackTrace());
            throw xaException;
        }
    }

    /**
     * Determine whether the provider supports the given URI scheme.
     *
     * @param scheme the URI scheme (not {@code null})
     * @return {@code true} if the scheme is supported, {@code false} otherwise
     */
    boolean supportsScheme(String scheme);
}
