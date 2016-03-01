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

import java.io.IOException;
import java.net.URI;

import javax.transaction.SystemException;
import org.jboss.remoting3.Endpoint;
import org.kohsuke.MetaInfServices;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;

/**
 * A JBoss Remoting based transaction provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@MetaInfServices
public final class RemotingTransactionProvider implements RemoteTransactionProvider {
    private final Endpoint endpoint;

    RemotingTransactionProvider() {
        this.endpoint = Endpoint.getCurrent();
    }

    public boolean supportsScheme(final String scheme) {
        return endpoint.isValidUriScheme(scheme);
    }

    public RemoteTransactionPeer getPeerHandle(final URI location) throws SystemException {
        try {
            return getPeerHandleDirect(location);
        } catch (IOException e) {
            throw Log.log.connectionFailed(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.connectionInterrupted();
        }
    }

    RemoteTransactionPeer getPeerHandleDirect(final URI location) throws IOException, InterruptedException {
        return TransactionClientChannel.forConnection(endpoint.getConnection(location).getInterruptibly());
    }
}
