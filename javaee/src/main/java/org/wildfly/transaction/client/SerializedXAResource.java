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

import static org.wildfly.transaction.client.OutflowHandleManager.FL_COMMITTED;
import static org.wildfly.transaction.client.OutflowHandleManager.FL_CONFIRMED;

import java.io.Serializable;
import java.net.URI;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class SerializedXAResource implements Serializable {
    private static final long serialVersionUID = - 575803514182093617L;

    private final URI location;
    private final String parentName;

    SerializedXAResource(final URI location, final String parentName) {
        this.location = location;
        this.parentName = parentName;
    }

    Object readResolve() {
        return new SubordinateXAResource(location, FL_COMMITTED | FL_CONFIRMED, parentName);
    }
}
