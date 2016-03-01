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

import java.io.Serializable;
import java.net.URI;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class SubordinateXAResource implements XAResource, XARecoverable, Serializable {
    private static final long serialVersionUID = 444691792601946632L;

    private final URI location;
    private volatile int timeout;

    SubordinateXAResource(final URI location) {
        this.location = location;
    }

    public void start(final Xid xid, final int flags) throws XAException {
        // no operation
    }

    public void end(final Xid xid, final int flags) throws XAException {
        // no operation
    }

    public void beforeCompletion(final Xid xid) throws XAException {
        SubordinateTransactionControl control = lookup(xid);
        control.beforeCompletion();
    }

    public int prepare(final Xid xid) throws XAException {
        SubordinateTransactionControl control = lookup(xid);
        return control.prepare();
    }

    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        SubordinateTransactionControl control = lookup(xid);
        control.commit(onePhase);
    }

    public void rollback(final Xid xid) throws XAException {
        SubordinateTransactionControl control = lookup(xid);
        control.rollback();
    }

    public void forget(final Xid xid) throws XAException {
        SubordinateTransactionControl control = lookup(xid);
        control.forget();
    }

    private SubordinateTransactionControl lookup(final Xid xid) throws XAException {
        final RemoteTransactionProvider provider = getProvider();
        return provider.getPeerHandleForXa(location).lookupXid(xid);
    }

    private RemoteTransactionProvider getProvider() {
        return RemoteTransactionContext.getContextManager().get().getProvider(location);
    }

    public Xid[] recover(final int flag) throws XAException {
        final RemoteTransactionProvider provider = getProvider();
        return provider.getPeerHandleForXa(location).recover(flag);
    }

    public boolean isSameRM(final XAResource xaRes) throws XAException {
        return xaRes instanceof SubordinateXAResource && location.equals(((SubordinateXAResource) xaRes).location);
    }

    public int getTransactionTimeout() {
        return timeout;
    }

    public boolean setTransactionTimeout(final int seconds) throws XAException {
        if (seconds < 0) {
            throw Log.log.negativeTxnTimeoutXa(XAException.XAER_INVAL);
        }
        timeout = seconds;
        return true;
    }

    Object writeReplace() {
        return new SerializedXAResource(location);
    }

    public String toString() {
        return Log.log.subordinateXaResource(location);
    }
}
