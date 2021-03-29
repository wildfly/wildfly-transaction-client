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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.Serializable;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.wildfly.common.Assert;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.security.auth.client.AuthenticationConfiguration;
import org.wildfly.security.auth.client.AuthenticationContext;
import org.wildfly.security.auth.client.AuthenticationContextConfigurationClient;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class SubordinateXAResource implements XAResource, XARecoverable, Serializable {
    private static final long serialVersionUID = 444691792601946632L;

    private final URI location;
    private final String parentName;
    private final XAResourceRegistry resourceRegistry;
    private final AuthenticationContext authenticationContext;
    private volatile int timeout = LocalTransactionContext.DEFAULT_TXN_TIMEOUT;
    private long startTime = 0L;
    private volatile Xid xid;
    private int capturedTimeout;

    private final AtomicInteger stateRef = new AtomicInteger(0);

    SubordinateXAResource(final URI location, final String parentName, XAResourceRegistry recoveryRegistry) {
        this.location = location;
        this.parentName = parentName;
        this.resourceRegistry = recoveryRegistry;
        this.authenticationContext = AuthenticationContext.captureCurrent();
    }

    SubordinateXAResource(final URI location, final String parentName, Xid xid, final int flags, XAResourceRegistry recoveryRegistry) {
        this(location, parentName, recoveryRegistry);
        this.xid = xid;
        stateRef.set(flags);
    }

    SubordinateXAResource(final URI location, final int flags, final String parentName) {
        this.location = location;
        this.parentName = parentName;
        stateRef.set(flags);
        this.resourceRegistry = null;
        this.authenticationContext = AuthenticationContext.captureCurrent();
    }

    Xid getXid() {
        return xid;
    }

    XAOutflowHandle addHandle(Xid xid) {
        if (! OutflowHandleManager.open(stateRef)) {
            throw Log.log.invalidTxnState();
        }
        return new XAOutflowHandle() {
            private final AtomicBoolean done = new AtomicBoolean();
            @NotNull
            public Xid getXid() {
                return xid;
            }

            public int getRemainingTime() {
                return SubordinateXAResource.this.getRemainingTime();
            }

            public void forgetEnlistment() {
                if (done.compareAndSet(false, true)) {
                    OutflowHandleManager.forgetOne(stateRef);
                } else {
                    throw Log.log.alreadyForgotten();
                }
            }

            public void nonMasterEnlistment() {
                if (done.compareAndSet(false, true)) {
                    OutflowHandleManager.nonMasterOne(stateRef);
                } else {
                    throw Log.log.alreadyForgotten();
                }
            }

            public void verifyEnlistment() throws RollbackException, SystemException {
                if (done.compareAndSet(false, true)) {
                    OutflowHandleManager.verifyOne(stateRef);
                } else {
                    throw Log.log.alreadyEnlisted();
                }
            }
        };
    }

    boolean commitToEnlistment() {
        return OutflowHandleManager.commit(stateRef);
    }

    public void start(final Xid xid, final int flags) throws XAException {
        if (flags == TMJOIN) {
            // should be impossible
            throw Assert.unreachableCode();
        }
        // ensure that the timeout is registered
        startTime = System.nanoTime();
        capturedTimeout = timeout;
        lookup(xid);
        this.xid = xid;
        if (resourceRegistry != null) {
            try {
                resourceRegistry.addResource(this, xid, location);
            } catch (SystemException se) {
                XAException xaException = new XAException(Log.log.failedToAddXAResourceToRegistry(this, xid, location));
                xaException.errorCode = XAException.XAER_RMERR;
                xaException.addSuppressed(se);
                throw xaException;
            }
        }
    }

    public void end(final Xid xid, final int flags) throws XAException {
        if (flags == TMSUCCESS || flags == TMFAIL) {
            lookup(xid).end(flags);
        }
    }

    public void beforeCompletion(final Xid xid) throws XAException {
        if (commitToEnlistment()) lookup(xid).beforeCompletion();
    }

    public int prepare(final Xid xid) throws XAException {
        final int result;
        try {
            result = commitToEnlistment() ? lookup(xid).prepare() : XA_RDONLY;
        } catch (XAException | RuntimeException exception) {
            if (resourceRegistry != null)
                resourceRegistry.resourceInDoubt(this);
            throw exception;
        }
        if (resourceRegistry != null && result == XA_RDONLY) {
            resourceRegistry.removeResource(this);
        }
        return result;
    }

    public void commit(final Xid xid, final boolean onePhase) throws XAException {
        try {
            if (commitToEnlistment()) lookup(xid).commit(onePhase);
        } catch (XAException | RuntimeException exception) {
            if (resourceRegistry != null)
                resourceRegistry.resourceInDoubt(this);
            throw exception;
        }
        if (resourceRegistry != null) {
            resourceRegistry.removeResource(this);
        } else {
            // deserialized SubordinateXAResources created by Narayana recovery from the SerializedXAResource
            // as the registry was not assigned trying to find matching resource in registry
            // for being able to clean the Xid from registry immediately now after the commit
            for (XAResource xares : XAResourceRegistryProviderHolder.getInstance().getInDoubtXAResources()) {
                if (xares instanceof SubordinateXAResource) {
                    SubordinateXAResource subordinateXares = (SubordinateXAResource) xares;
                    if (SimpleXid.of(xid).equals(SimpleXid.of(subordinateXares.xid))) {
                        subordinateXares.resourceRegistry.removeResource(subordinateXares);
                    }
                }
            }
        }
    }

    public void rollback(final Xid xid) throws XAException {
        try {
            if (commitToEnlistment()) lookup(xid).rollback();
        } catch (XAException | RuntimeException e) {
            if (resourceRegistry != null)
                resourceRegistry.resourceInDoubt(this);
            throw e;
        }
        if (resourceRegistry != null)
            resourceRegistry.removeResource(this);
    }

    public void forget(final Xid xid) throws XAException {
        if (commitToEnlistment()) lookup(xid).forget();
    }

    private SubordinateTransactionControl lookup(final Xid xid) throws XAException {
        return getRemoteTransactionPeer().lookupXid(xid);
    }

    private RemoteTransactionProvider getProvider() {
        RemoteTransactionProvider provider = RemoteTransactionContext.getInstancePrivate().getProvider(location);
        if (provider == null) {
            throw Log.log.noProviderForUri(location);
        }
        return provider;
    }

    public Xid[] recover(final int flag) throws XAException {
        return recover(flag, parentName);
    }

    public Xid[] recover(final int flag, final String parentName) throws XAException {
        Xid[] recoveredXids = getRemoteTransactionPeer().recover(flag, parentName);
        if ((flag & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN && recoveredXids.length == 0 && resourceRegistry != null)
            resourceRegistry.removeResource(this);
        return recoveredXids;
    }

    private RemoteTransactionPeer getRemoteTransactionPeer() throws XAException {
        try {
            final AuthenticationContextConfigurationClient client = new AuthenticationContextConfigurationClient();
            final AuthenticationConfiguration configuration = client.getAuthenticationConfiguration(location, this.authenticationContext);
            SSLContext context = null;
            try {
                context = client.getSSLContext(location, this.authenticationContext);
            } catch (GeneralSecurityException ex) {
                // no-op, no changes to context
            }

            if (context == null) {
                try {
                    context = SSLContext.getDefault();
                } catch (GeneralSecurityException ex) {
                    // no-op, no changes to context
                }
            }

            final RemoteTransactionPeer provider = getProvider().getPeerHandleForXa(location, context, configuration);
            return authenticationContext.run((PrivilegedExceptionAction<RemoteTransactionPeer>) () -> provider);
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof XAException) {
                throw (XAException) cause;
            }

            throw new RuntimeException(e);
        }
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
        timeout = seconds == 0 ? LocalTransactionContext.DEFAULT_TXN_TIMEOUT : seconds;
        return true;
    }

    Object writeReplace() {
        return new SerializedXAResource(location, parentName);
    }

    public String toString() {
        return Log.log.subordinateXaResource(location);
    }

    int getRemainingTime() {
        long elapsed = max(0L, System.nanoTime() - startTime);
        final int capturedTimeout = this.capturedTimeout;
        return capturedTimeout - (int) min(capturedTimeout, elapsed / 1_000_000_000L);
    }
}
