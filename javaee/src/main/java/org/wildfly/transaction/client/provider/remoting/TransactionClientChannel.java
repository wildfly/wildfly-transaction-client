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
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.ClientServiceHandle;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.ConnectionPeerIdentity;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexMap;
import org.jboss.remoting3.util.BlockingInvocation;
import org.jboss.remoting3.util.InvocationTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.common.rpc.RemoteExceptionCause;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;
import org.xnio.FinishedIoFuture;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TransactionClientChannel implements RemotingOperations {
    private final Channel channel;
    private final InvocationTracker invocationTracker;
    private final IntIndexMap<RemotingRemoteTransactionHandle> peerTransactionMap = new IntIndexHashMap<RemotingRemoteTransactionHandle>(RemotingRemoteTransactionHandle::getId);
    private final Channel.Receiver receiver = new ReceiverImpl();

    private static final ClientServiceHandle<TransactionClientChannel> CLIENT_SERVICE_HANDLE = new ClientServiceHandle<>("txn", TransactionClientChannel::construct);

    TransactionClientChannel(final Channel channel) {
        this.channel = channel;
        invocationTracker = new InvocationTracker(channel);
    }

    private static IoFuture<TransactionClientChannel> construct(final Channel channel) {
        // future protocol versions might have to negotiate a version or capabilities before proceeding
        final TransactionClientChannel clientChannel = new TransactionClientChannel(channel);
        channel.receiveMessage(clientChannel.getReceiver());
        return new FinishedIoFuture<>(clientChannel);
    }

    @NotNull
    public SimpleTransactionControl begin(final ConnectionPeerIdentity peerIdentity) throws SystemException {
        int id;
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final IntIndexMap<RemotingRemoteTransactionHandle> map = this.peerTransactionMap;
        RemotingRemoteTransactionHandle handle;
        do {
            id = random.nextInt();
        } while (map.containsKey(id) || map.putIfAbsent(handle = new RemotingRemoteTransactionHandle(id, this)) != null);
        return handle;
    }

    public void rollback(final Xid xid, final ConnectionPeerIdentity peerIdentity) throws XAException {
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_ROLLBACK);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_ROLLBACK) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(Log.log.peerXaException(error));
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(Log.log.peerSecurityException());
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public void setRollbackOnly(final Xid xid, final ConnectionPeerIdentity peerIdentity) throws XAException {
        // write rollback-only request
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_RB_ONLY);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_RB_ONLY) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(xa);
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(sx);
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public void beforeCompletion(final Xid xid, final ConnectionPeerIdentity peerIdentity) throws XAException {
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_BEFORE);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_BEFORE) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(xa);
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(sx);
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public int prepare(final Xid xid, final ConnectionPeerIdentity peerIdentity) throws XAException {
        boolean readOnly = false;
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_PREPARE);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_PREPARE) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(xa);
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(sx);
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id == Protocol.P_XA_RDONLY) {
                    readOnly = true;
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
        return readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK;
    }

    public void forget(final Xid xid, final ConnectionPeerIdentity peerIdentity) throws XAException {
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_FORGET);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_FORGET) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(xa);
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(sx);
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public void commit(final Xid xid, final boolean onePhase, final ConnectionPeerIdentity peerIdentity) throws XAException {
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_COMMIT);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
            if (onePhase) Protocol.writeParam(Protocol.P_ONE_PHASE, os);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_COMMIT) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    int error = is.readInt();
                    final XAException xa = Log.log.peerXaException(error);
                    xa.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(xa);
                        throw ex;
                    } else {
                        throw xa;
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    int len = StreamUtils.readPackedSignedInt32(is);
                    final SecurityException sx = Log.log.peerSecurityException();
                    sx.initCause(RemoteExceptionCause.readFromStream(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(sx);
                        throw ex;
                    } else {
                        throw sx;
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    @NotNull
    public Xid[] recover(final int flag, final String parentName, final ConnectionPeerIdentity peerIdentity) throws XAException {
        if ((flag & XAResource.TMSTARTRSCAN) != XAResource.TMSTARTRSCAN) {
            return SimpleXid.NO_XIDS;
        }
        final InvocationTracker invocationTracker = getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_RECOVER);
            final int peerIdentityId = peerIdentity.getId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
            Protocol.writeParam(Protocol.P_PARENT_NAME, os, parentName);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMFAIL);
        }
        final ArrayList<Xid> recoveryList = new ArrayList<>();
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_RECOVER) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                for (;;) {
                    if (id == Protocol.P_XID) {
                        recoveryList.add(Protocol.readXid(is, StreamUtils.readPackedUnsignedInt32(is)));
                    } else if (id == Protocol.P_XA_ERROR) {
                        int len = StreamUtils.readPackedSignedInt32(is);
                        int error = is.readInt();
                        final XAException xa = Log.log.peerXaException(error);
                        xa.initCause(RemoteExceptionCause.readFromStream(is));
                        if ((id = is.read()) != -1) {
                            XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                            ex.addSuppressed(xa);
                            throw ex;
                        } else {
                            throw xa;
                        }
                    } else if (id == Protocol.P_SEC_EXC) {
                        int len = StreamUtils.readPackedSignedInt32(is);
                        final SecurityException sx = Log.log.peerSecurityException();
                        sx.initCause(RemoteExceptionCause.readFromStream(is));
                        if ((id = is.read()) != -1) {
                            XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                            ex.addSuppressed(sx);
                            throw ex;
                        } else {
                            throw sx;
                        }
                    } else if (id != -1) {
                        throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                    } else {
                        break;
                    }
                    id = is.read();
                }
            }
            return recoveryList.toArray(SimpleXid.NO_XIDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            throw Log.log.responseFailedXa(e, XAException.XAER_RMFAIL);
        }
    }

    InvocationTracker getInvocationTracker() {
        return invocationTracker;
    }

    static TransactionClientChannel forConnection(final Connection connection) throws IOException {
        return CLIENT_SERVICE_HANDLE.getClientService(connection, OptionMap.EMPTY).get();
    }

    Channel.Receiver getReceiver() {
        return receiver;
    }

    Connection getConnection() {
        return channel.getConnection();
    }

    void notifyTransactionEnd(final int id){
        peerTransactionMap.removeKey(id);
    }

    class ReceiverImpl implements Channel.Receiver {
        public void handleError(final Channel channel, final IOException error) {
            handleEnd(channel);
        }

        public void handleEnd(final Channel channel) {
            for (RemotingRemoteTransactionHandle transaction : peerTransactionMap) {
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
