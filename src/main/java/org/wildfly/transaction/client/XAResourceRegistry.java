/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018 Red Hat, Inc., and individual contributors
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


import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.net.URI;

import static org.wildfly.transaction.client.OutflowHandleManager.FL_COMMITTED;
import static org.wildfly.transaction.client.OutflowHandleManager.FL_CONFIRMED;

/**
 * Registry that keeps track of outflowed resources info for a specific transaction.
 *
 * Used for recovery of those resources when they enter in doubt state.
 *
 * @author Flavia Rainone
 */
public abstract class XAResourceRegistry {

    /**
     * Adds a XA resource to this registry.
     *
     * @param resource the resource
     * @param uri the resource URI location
     * @throws SystemException if there is a problem recording this resource
     */
    protected abstract void addResource(XAResource resource, Xid xid, URI uri) throws SystemException;

    /**
     * Removes a XA resource from this registry.
     *
     * @param resource the resource
     * @throws XAException if there is a problem deleting this resource
     */
    protected abstract void removeResource(XAResource resource) throws XAException;

    /**
     * Flags the previously added resource as in doubt, meaning that it failed to complete prepare, rollback or commit.
     * It can be invoked more than once for the same resource if that resource repeatedly fails to perform those
     * operations, such as a resource that first fails to prepare, and then fails to rollback.
     *
     * @param resource the resource
     */
    protected abstract void resourceInDoubt(XAResource resource);

    /**
     * Reloads an in doubt resource, recreating a previously lost remote XA resource object. This method
     * must be invoked to recreate in doubt resources after a server shutdown or crash.
     *
     * @param uri      the URI where the outflowed resource is located
     * @param nodeName the node name of the resource
     * @return         a newly-created resource representing a previously lost XA resource that is in doubt
     */
    protected XAResource reloadInDoubtResource(URI uri, String nodeName, Xid xid) {
        return new SubordinateXAResource(uri, nodeName, xid, FL_COMMITTED | FL_CONFIRMED, this);
    }
}
