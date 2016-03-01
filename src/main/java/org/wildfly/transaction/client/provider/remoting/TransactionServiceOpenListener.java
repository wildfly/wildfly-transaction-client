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

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.OpenListener;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class TransactionServiceOpenListener implements OpenListener {
    private final Endpoint endpoint;
    private final LocalTransactionProvider provider;

    TransactionServiceOpenListener(final Endpoint endpoint, final LocalTransactionProvider provider) {
        this.endpoint = endpoint;
        this.provider = provider;
    }

    public void channelOpened(final Channel channel) {
        final TransactionServerChannel serverChannel = new TransactionServerChannel(channel, provider, transactionManager);
        serverChannel.start();
    }

    public void registrationTerminated() {
        // no operation
    }

}
