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

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.wildfly.transaction.client._private.Log;

final class XAOutflowedResources {

    private final LocalTransaction transaction;
    private final ConcurrentMap<URI, SubordinateXAResource> enlistments = new ConcurrentHashMap<>();

    XAOutflowedResources(final LocalTransaction transaction) {
        this.transaction = transaction;
    }

    SubordinateXAResource getOrEnlist(final URI location) throws SystemException, RollbackException {
        SubordinateXAResource xaResource = enlistments.get(location);
        if (xaResource != null) {
            return xaResource;
        }
        synchronized (this) {
            xaResource = enlistments.get(location);
            if (xaResource != null) {
                return xaResource;
            }
            xaResource = new SubordinateXAResource(location);
            if (! transaction.enlistResource(xaResource)) {
                throw Log.log.couldNotEnlist();
            }
            final SubordinateXAResource finalXaResource = xaResource;
            transaction.registerSynchronization(new Synchronization() {
                public void beforeCompletion() {
                    try {
                        if (finalXaResource.commit()) {
                            finalXaResource.beforeCompletion(finalXaResource.getXid());
                        } else {
                            // try and delist, so the TM can maybe perform a 1PC; if it fails that's OK
                            try {
                                transaction.delistResource(finalXaResource, XAResource.TMSUCCESS);
                            } catch (SystemException ignored) {
                                // optimization failed!
                            }
                        }
                    } catch (XAException e) {
                        throw new SynchronizationException(e);
                    }
                }

                public void afterCompletion(final int status) {
                    // ignored
                }
            });
            enlistments.put(location, xaResource);
            return xaResource;
        }
    }

    LocalTransaction getTransaction() {
        return transaction;
    }
}
