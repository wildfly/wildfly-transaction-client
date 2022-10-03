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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;

import org.wildfly.transaction.client._private.Log;

final class XAOutflowedResources {

    private final LocalTransaction transaction;
    private final ConcurrentMap<Key, SubordinateXAResource> enlistments = new ConcurrentHashMap<>();

    // protected by {@code this}
    private int enlistedSubordinates = 0;

    XAOutflowedResources(final LocalTransaction transaction) {
        this.transaction = transaction;
    }

    SubordinateXAResource getOrEnlist(final URI location, final String parentName) throws SystemException, RollbackException {
        final Key key = new Key(location, parentName);
        SubordinateXAResource xaResource = enlistments.get(key);
        if (xaResource != null) {
            return xaResource;
        }
        synchronized (this) {
            xaResource = enlistments.get(key);
            if (xaResource != null) {
                return xaResource;
            }
            final XAResourceRegistry resourceRegistry = transaction.getProvider().getXAResourceRegistry(transaction);
            xaResource = new SubordinateXAResource(location, parentName, resourceRegistry);
            if (! transaction.enlistResource(xaResource)) {
                throw Log.log.couldNotEnlist();
            }
            enlistedSubordinates ++;
            final SubordinateXAResource finalXaResource = xaResource;
            int status = transaction.getStatus();
            if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) try {
                transaction.registerSynchronization(new Synchronization() {
                    public void beforeCompletion() {
                        try {
                            if (finalXaResource.commitToEnlistment()) {
                                finalXaResource.beforeCompletion(finalXaResource.getXid());
                            }
                        } catch (XAException e) {
                            throw new SynchronizationException(e);
                        }
                    }

                    public void afterCompletion(final int status) {
                        // ignored
                    }
                });
            } catch (IllegalStateException e) {
                status = transaction.getStatus();
                if (status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK) {
                    throw e;
                }
                // else we don't care
            }
            enlistments.put(key, xaResource);
            return xaResource;
        }
    }

    int getEnlistedSubordinates() {
        synchronized (this) {
            return enlistedSubordinates;
        }
    }

    static final class Key {
        private final URI location;
        private final String parentName;

        Key(final URI location, final String parentName) {
            this.location = location;
            this.parentName = parentName;
        }

        public boolean equals(final Object obj) {
            return obj instanceof Key && equals((Key) obj);
        }

        private boolean equals(final Key key) {
            return Objects.equals(location, key.location) && Objects.equals(parentName, key.parentName);
        }

        public int hashCode() {
            return Objects.hashCode(location) * 31 + Objects.hashCode(parentName);
        }
    }
}
