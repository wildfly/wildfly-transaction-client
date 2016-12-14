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

import org.jboss.remoting3.Connection;
import org.jboss.remoting3.ServiceNotFoundException;
import org.wildfly.common.annotation.NotNull;

/**
 * A fallback peer provider for compatibility with the old EJB-based transactions protocol.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface RemotingFallbackPeerProvider {
    /**
     * Get an alternative operations handler for the given connection.
     *
     * @param connection the connection (not {@code null})
     * @return the operations handler (must not be {@code null})
     * @throws ServiceNotFoundException if the fallback service wasn't located
     */
    @NotNull
    RemotingOperations getOperations(@NotNull Connection connection) throws IOException;
}
