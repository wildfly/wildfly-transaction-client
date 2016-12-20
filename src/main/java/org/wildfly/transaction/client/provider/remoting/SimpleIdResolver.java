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

import javax.transaction.InvalidTransactionException;

import org.jboss.remoting3.Connection;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.RemoteTransaction;

/**
 * The provider interface for {@link RemoteTransaction} instances which are located at a Remoting peer.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface SimpleIdResolver {
    /**
     * Get the transaction ID for the transaction from which this instance was taken.  Check against the given
     * connection to ensure that the transaction matches the connection, throwing an exception otherwise.
     *
     * @param connection the connection (must not be {@code null})
     * @return the transaction ID
     * @throws InvalidTransactionException if the transaction does not match the connection
     */
    int getTransactionId(@NotNull Connection connection) throws InvalidTransactionException;
}
