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

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.Registration;
import org.jboss.remoting3.ServiceRegistrationException;
import org.wildfly.transaction.client.LocalTransactionContext;
import org.wildfly.transaction.client._private.Log;
import org.xnio.OptionMap;

/**
 * The remoting transaction service.  This covers the server side of transaction inflow for Remoting servers.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemotingTransactionService {
    private final Endpoint endpoint;
    private final LocalTransactionContext transactionContext;
    private static final Attachments.Key<RemotingTransactionServer> KEY = new Attachments.Key<>(RemotingTransactionServer.class);

    RemotingTransactionService(final Endpoint endpoint, final LocalTransactionContext transactionContext) {
        this.endpoint = endpoint;
        this.transactionContext = transactionContext;
    }

    Registration register() throws ServiceRegistrationException {
        return endpoint.registerService("txn", new OpenListener() {
            public void channelOpened(final Channel channel) {
                getServerForConnection(channel.getConnection()).openChannel(channel);
            }

            public void registrationTerminated() {
                // no operation
            }
        }, OptionMap.EMPTY);
    }

    public RemotingTransactionServer getServerForConnection(Connection connection) {
        if (connection.getEndpoint() != endpoint) {
            throw Log.log.invalidConnectionEndpoint();
        }
        final Attachments attachments = connection.getAttachments();
        RemotingTransactionServer server = attachments.getAttachment(KEY);
        if (server == null) {
            server = new RemotingTransactionServer(this, connection);
            RemotingTransactionServer appearing = attachments.attachIfAbsent(KEY, server);
            if (appearing != null) {
                server = appearing;
            }
        }
        return server;
    }

    /**
     * Get the local transaction context being used by this service.
     *
     * @return the local transaction context (not {@code null})
     */
    public LocalTransactionContext getTransactionContext() {
        return transactionContext;
    }

    Endpoint getEndpoint() {
        return endpoint;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Endpoint endpoint;
        private LocalTransactionContext transactionContext;

        Builder() {
        }

        public Builder setEndpoint(final Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder setTransactionContext(final LocalTransactionContext transactionContext) {
            this.transactionContext = transactionContext;
            return this;
        }

        public RemotingTransactionService build() {
            Endpoint endpoint = this.endpoint;
            if (endpoint == null) endpoint = Endpoint.getCurrent();
            LocalTransactionContext transactionContext = this.transactionContext;
            if (transactionContext == null) transactionContext = LocalTransactionContext.getCurrent();
            return new RemotingTransactionService(endpoint, transactionContext);
        }
    }
}
