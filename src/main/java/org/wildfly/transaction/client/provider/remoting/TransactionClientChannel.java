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

import static org.xnio.IoUtils.safeClose;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.ClientServiceHandle;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexMap;
import org.jboss.remoting3.util.BlockingInvocation;
import org.jboss.remoting3.util.InvocationTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.common.Assert;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionPeer;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;
import org.xnio.FinishedIoFuture;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TransactionClientChannel implements RemoteTransactionPeer {
    private final URI location;
    private final Channel channel;
    private final InvocationTracker invocationTracker;
    private final IntIndexMap<RemotingRemoteTransaction> peerTransactionMap = new IntIndexHashMap<RemotingRemoteTransaction>(RemotingRemoteTransaction.INDEXER);
    private final Channel.Receiver receiver = new ReceiverImpl();
    private final ConcurrentMap<SimpleXid, FutureRemoteSubordinateTransactionControl> subordinates = new ConcurrentHashMap<>();

    private static final ClientServiceHandle<TransactionClientChannel> CLIENT_SERVICE_HANDLE = new ClientServiceHandle<>("txn", TransactionClientChannel::construct);

    TransactionClientChannel(final URI location, final Channel channel) {
        this.location = location;
        this.channel = channel;
        invocationTracker = new InvocationTracker(channel);
    }

    private static IoFuture<TransactionClientChannel> construct(final Channel channel) {
        // future protocol versions might have to negotiate a version or capabilities before proceeding
        final TransactionClientChannel clientChannel = new TransactionClientChannel(channel.getConnection().getPeerURI(), channel);
        channel.receiveMessage(clientChannel.getReceiver());
        return new FinishedIoFuture<>(clientChannel);
    }

    URI getLocation() {
        return location;
    }

    public SubordinateTransactionControl lookupXid(final Xid xid) throws XAException {
        return getSubordinateTransaction(xid, 0);
    }

    public SimpleTransactionControl begin(final int timeout) throws SystemException {
        int id;
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final IntIndexMap<RemotingRemoteTransaction> map = this.peerTransactionMap;
        RemotingRemoteTransactionHandle handle;
        do {
            id = random.nextInt();
        } while (map.containsKey(id) || map.putIfAbsent(handle = new RemotingRemoteTransactionHandle(id, this)) != null);
        handle.begin(timeout);
        return handle;
    }

    RemotingSubordinateTransactionControl getSubordinateTransaction(Xid xid, int remainingTimeout) throws XAException {
        final SimpleXid globalXid = SimpleXid.of(xid).withoutBranch();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final IntIndexMap<RemotingRemoteTransaction> map = this.peerTransactionMap;
        int id;
        FutureRemoteSubordinateTransactionControl existing;
        // optimistic result: the transaction control is already there
        existing = subordinates.get(globalXid);
        if (existing != null) {
            return existing.get();
        }
        RemotingSubordinateTransactionControl handle;
        do {
            id = random.nextInt();
        } while (map.containsKey(id) || map.putIfAbsent(handle = new RemotingSubordinateTransactionControl(id, this, globalXid, remainingTimeout)) != null);
        final UnfinishedFutureRemoteSubordinateTransactionControl future = new UnfinishedFutureRemoteSubordinateTransactionControl();
        synchronized (future) {
            FutureRemoteSubordinateTransactionControl appearing = subordinates.putIfAbsent(globalXid, future);
            if (appearing != null) {
                map.remove(handle);
                return appearing.get();
            }
            // otherwise, publish this transaction to the remote system
            // this does not actually enlist the transaction though; that's up to the user to decide
            boolean ok = false;
            try {
                handle.begin();
                future.complete(handle);
                subordinates.replace(globalXid, future, handle);
                ok = true;
            } catch (final Throwable e) {
                future.fail(e);
                throw e;
            } finally {
                if (! ok) {
                    subordinates.remove(globalXid, future);
                    map.remove(handle);
                }
            }
            return handle;
        }
    }

    public Xid[] recover(final int flag) throws XAException {
        if (flag != XAResource.TMSTARTRSCAN) {
            return SimpleXid.NO_XIDS;
        }
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_RECOVER);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        final ArrayList<Xid> recoveryList = new ArrayList<>();
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_RECOVER) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                for (;;) {
                    if (id == Protocol.P_XA_ERROR) {
                        error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                    } else if (id == Protocol.P_SEC_EXC) {
                        sec = true;
                    } else if (id == Protocol.P_XID) {
                        if (error != 0 && ! sec) {
                            recoveryList.add(Protocol.readXid(is, StreamUtils.readPackedUnsignedInt32(is)));
                        }
                    } else if (id == -1) {
                        break;
                    } else {
                        error = XAException.XAER_RMERR;
                    }
                    id = is.read();
                }
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            }
            return recoveryList.toArray(SimpleXid.NO_XIDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
        }
    }

    InvocationTracker getInvocationTracker() {
        return invocationTracker;
    }

    static TransactionClientChannel forConnection(final Connection connection) throws IOException {
        return CLIENT_SERVICE_HANDLE.getClientService(connection, OptionMap.EMPTY).get();
    }

    static TransactionClientChannel forUri(final URI uri) throws IOException {
        final Endpoint endpoint = Endpoint.getCurrent();
        final IoFuture<Connection> future = endpoint.getConnection(uri);
        final Connection connection = future.get();
        return TransactionClientChannel.forConnection(connection);
    }

    Channel.Receiver getReceiver() {
        return receiver;
    }

    Connection getConnection() {
        return channel.getConnection();
    }

    static class UnfinishedFutureRemoteSubordinateTransactionControl extends FutureRemoteSubordinateTransactionControl {
        private volatile Object result;

        RemotingSubordinateTransactionControl get() throws XAException {
            Object result = this.result;
            if (result == null) {
                synchronized (this) {
                    result = this.result;
                }
                Assert.assertNotNull(result);
            }
            if (result instanceof RemotingSubordinateTransactionControl) {
                return (RemotingSubordinateTransactionControl) result;
            } else if (result instanceof Throwable) {
                try {
                    throw (Throwable) result;
                } catch (XAException | RuntimeException | Error e) {
                    throw e;
                } catch (Throwable throwable) {
                    throw new UndeclaredThrowableException(throwable);
                }
            } else {
                throw Assert.unreachableCode();
            }
        }

        void complete(final RemotingSubordinateTransactionControl handle) {
            Assert.assertHoldsLock(this);
            result = handle;
        }

        void fail(final Throwable throwable) {
            Assert.assertHoldsLock(this);
            result = throwable;
        }
    }

    class ReceiverImpl implements Channel.Receiver {
        public void handleError(final Channel channel, final IOException error) {
            handleEnd(channel);
        }

        public void handleEnd(final Channel channel) {
            for (RemotingRemoteTransaction transaction : peerTransactionMap) {
                transaction.disconnect();
            }
        }

        public void handleMessage(final Channel channel, final MessageInputStream message) {
            try {
                channel.receiveMessage(this);
                final int invId;
                try {
                    invId = message.readUnsignedShort();
                } catch (IOException e) {
                    // can't do much about this, but it's really unlikely anyway
                    Log.log.inboundException(e);
                    return;
                }
                invocationTracker.signalResponse(invId, 0, message, true);
            } finally {
                safeClose(message);
            }
        }
    }
}
