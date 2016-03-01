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

import static java.lang.Math.max;

import java.io.IOException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3.util.BlockingInvocation;
import org.jboss.remoting3.util.InvocationTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class RemotingSubordinateTransactionControl extends FutureRemoteSubordinateTransactionControl implements RemotingRemoteTransaction, SubordinateTransactionControl {
    private final TransactionClientChannel channel;
    private final SimpleXid xid;
    private final long startTimeStamp;
    private final int initialTimeout;
    private final int id;

    RemotingSubordinateTransactionControl(final int id, final TransactionClientChannel channel, final SimpleXid xid, final int initialTimeout) {
        this.channel = channel;
        this.xid = xid;
        this.initialTimeout = initialTimeout;
        startTimeStamp = System.nanoTime();
        this.id = id;
    }

    RemotingSubordinateTransactionControl get() {
        return this;
    }

    public int getId() {
        return id;
    }

    public void disconnect() {
        // nothing we can do
    }

    public void rollback() throws XAException {
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_ROLLBACK);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_ROLLBACK) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                if (id == Protocol.P_XA_ERROR) {
                    int error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(Log.log.peerXaException(error));
                        throw ex;
                    } else {
                        throw Log.log.protocolErrorXA(error);
                    }
                } else if (id == Protocol.P_SEC_EXC) {
                    if ((id = is.read()) != -1) {
                        XAException ex = Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                        ex.addSuppressed(Log.log.peerSecurityException());
                        throw ex;
                    } else {
                        throw Log.log.peerSecurityException();
                    }
                } else if (id != -1) {
                    throw Log.log.unrecognizedParameter(XAException.XAER_RMFAIL, id);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public void end(final int flags) {
        // no operation
    }

    public void beforeCompletion() throws XAException {
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_BEFORE);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_BEFORE) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                if (id == Protocol.P_XA_ERROR) {
                    error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                } else if (id == Protocol.P_SEC_EXC) {
                    sec = true;
                }
                if (id != -1) do {
                    // skip content
                    Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
                } while (is.read() != -1);
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public int prepare() throws XAException {
        boolean readOnly = false;
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_PREPARE);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_PREPARE) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                if (id == Protocol.P_XA_ERROR) {
                    error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                } else if (id == Protocol.P_SEC_EXC) {
                    sec = true;
                } else if (id == Protocol.P_XA_RDONLY) {
                    readOnly = true;
                }
                if (id != -1) do {
                    // skip content
                    Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
                } while (is.read() != -1);
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
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

    public void forget() throws XAException {
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_FORGET);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_FORGET) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                if (id == Protocol.P_XA_ERROR) {
                    error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                } else if (id == Protocol.P_SEC_EXC) {
                    sec = true;
                }
                if (id != -1) do {
                    // skip content
                    Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
                } while (is.read() != -1);
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public void commit(final boolean onePhase) throws XAException {
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_COMMIT);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
            if (onePhase) Protocol.writeParam(Protocol.P_ONE_PHASE, os);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_COMMIT) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                if (id == Protocol.P_XA_ERROR) {
                    error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                } else if (id == Protocol.P_SEC_EXC) {
                    sec = true;
                }
                if (id != -1) do {
                    // skip content
                    Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
                } while (is.read() != -1);
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    void begin() throws XAException {
        final InvocationTracker invocationTracker = channel.getInvocationTracker();
        final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
        // write request
        try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
            os.writeShort(invocation.getIndex());
            os.writeByte(Protocol.M_XA_BEGIN);
            Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
            final int peerIdentityId = channel.getConnection().getPeerIdentityId();
            if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
            Protocol.writeParam(Protocol.P_XID, os, xid);
            final int initialTimeout = this.initialTimeout;
            if (initialTimeout != 0) Protocol.writeParam(Protocol.P_TXN_TIMEOUT, os, initialTimeout, Protocol.UNSIGNED);
        } catch (IOException e) {
            throw Log.log.failedToSendXA(e, XAException.XAER_RMERR);
        }
        try (BlockingInvocation.Response response = invocation.getResponse()) {
            try (MessageInputStream is = response.getInputStream()) {
                if (is.readUnsignedByte() != Protocol.M_RESP_XA_BEGIN) {
                    throw Log.log.unknownResponseXa(XAException.XAER_RMERR);
                }
                int id = is.read();
                int error = 0;
                boolean sec = false;
                if (id == Protocol.P_XA_ERROR) {
                    error = Protocol.readIntParam(is, StreamUtils.readPackedSignedInt32(is));
                } else if (id == Protocol.P_SEC_EXC) {
                    sec = true;
                }
                if (id != -1) do {
                    // skip content
                    Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
                } while (is.read() != -1);
                if (sec) {
                    throw Log.log.peerSecurityException();
                }
                if (error != 0) {
                    throw Log.log.peerXaException(error);
                }
            } catch (IOException e) {
                throw Log.log.responseFailedXa(e, XAException.XAER_RMERR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Log.log.interruptedXA(XAException.XAER_RMERR);
        } catch (IOException e) {
            // failed to close the response, but we don't care too much
            Log.log.inboundException(e);
        }
    }

    public int getTransactionTimeout() throws XAException {
        return max(0, initialTimeout - (int) (max(0L, System.nanoTime() - startTimeStamp) / 1_000_000_000L));
    }
}
