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

import java.net.URI;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import javax.transaction.SystemException;

import org.jboss.remoting3.Endpoint;
import org.kohsuke.MetaInfServices;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;

/**
 * A JBoss Remoting based transaction provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@MetaInfServices
public final class RemotingRemoteTransactionProvider implements RemoteTransactionProvider {
    private final RemotingFallbackPeerProvider fallbackProvider;

    /**
     * Construct a new instance.
     */
    public RemotingRemoteTransactionProvider() {
        ServiceLoader<RemotingFallbackPeerProvider> loader = ServiceLoader.load(RemotingFallbackPeerProvider.class, RemotingRemoteTransactionProvider.class.getClassLoader());
        final Iterator<RemotingFallbackPeerProvider> iterator = loader.iterator();
        RemotingFallbackPeerProvider fallbackProvider = null;
        for (;;) try {
            if (! iterator.hasNext()) {
                break;
            }
            fallbackProvider = iterator.next();
            break;
        } catch (ServiceConfigurationError e) {}
        this.fallbackProvider = fallbackProvider;
    }

    public boolean supportsScheme(final String scheme) {
        return Endpoint.getCurrent().isValidUriScheme(scheme);
    }

    public RemoteTransactionPeer getPeerHandle(final URI location) throws SystemException {
        return new RemotingRemoteTransactionPeer(location, Endpoint.getCurrent(), fallbackProvider);
    }
}
