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
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.jboss.remoting3.Connection;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3.util.BlockingInvocation;
import org.jboss.remoting3.util.InvocationTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.common.Assert;
import org.wildfly.common.rpc.RemoteExceptionCause;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.SimpleTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class RemotingRemoteTransactionHandle implements SimpleTransactionControl {

    private final TransactionClientChannel channel;
    private final AtomicInteger statusRef = new AtomicInteger(Status.STATUS_ACTIVE);
    private final int id;
    private final SimpleIdResolver resolver = connection -> {
        Assert.checkNotNullParam("connection", connection);
        if (getConnection() != connection) {
            throw Log.log.invalidTransactionConnection();
        }
        return getId();
    };

    RemotingRemoteTransactionHandle(final int id, final TransactionClientChannel channel) {
        this.id = id;
        this.channel = channel;
    }

    public int getId() {
        return id;
    }

    public void disconnect() {
        final AtomicInteger statusRef = this.statusRef;
        synchronized (statusRef) {
            final int oldVal = statusRef.get();
            if (oldVal == Status.STATUS_ACTIVE || oldVal == Status.STATUS_MARKED_ROLLBACK) {
                statusRef.set(Status.STATUS_ROLLEDBACK);
                channel.notifyTransactionEnd(id);
            }
        }
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
            throw Log.log.invalidTxnState();
        }
        synchronized (statusRef) {
            oldVal = statusRef.get();
            if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
                rollback();
                throw Log.log.rollbackOnlyRollback();
            }
            if (oldVal != Status.STATUS_ACTIVE) {
                throw Log.log.invalidTxnState();
            }
            statusRef.set(Status.STATUS_COMMITTING);
            try {
                final InvocationTracker invocationTracker = channel.getInvocationTracker();
                final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
                // write request
                try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
                    os.writeShort(invocation.getIndex());
                    os.writeByte(Protocol.M_UT_COMMIT);
                    Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
                    final int peerIdentityId = channel.getConnection().getPeerIdentityId();
                    if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
                } catch (IOException e) {
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw Log.log.failedToSend(e);
                }
                try (BlockingInvocation.Response response = invocation.getResponse()) {
                    try (MessageInputStream is = response.getInputStream()) {
                        if (is.readUnsignedByte() != Protocol.M_RESP_UT_COMMIT) {
                            throw Log.log.unknownResponse();
                        }
                        int messageId = is.read();
                        if (messageId == -1) {
                            statusRef.set(Status.STATUS_COMMITTED);
                            channel.notifyTransactionEnd(id);
                        } else {
                            int len = StreamUtils.readPackedUnsignedInt32(is);
                            if (messageId == Protocol.P_UT_HME_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final HeuristicMixedException e = Log.log.peerHeuristicMixedException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_UT_HRE_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final HeuristicRollbackException e = Log.log.peerHeuristicRollbackException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_UT_IS_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final IllegalStateException e = Log.log.peerIllegalStateException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_UT_RB_EXC) {
                                statusRef.set(Status.STATUS_ROLLEDBACK);
                                final RollbackException e = Log.log.transactionRolledBackByPeer();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_UT_SYS_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final SystemException e = Log.log.peerSystemException();
                                e.errorCode = is.readInt();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_SEC_EXC) {
                                statusRef.set(oldVal);
                                final SecurityException e = Log.log.peerSecurityException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                throw Log.log.unknownResponse();
                            }
                        }
                    } catch (IOException e) {
                        statusRef.set(Status.STATUS_UNKNOWN);
                        throw Log.log.responseFailed(e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw Log.log.operationInterrupted();
                } catch (IOException e) {
                    // failed to close the response, but we don't care too much
                    Log.log.inboundException(e);
                }
            } finally {
                statusRef.compareAndSet(Status.STATUS_COMMITTING, Status.STATUS_UNKNOWN);
            }
        }
    }

    public void rollback() throws SecurityException, SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
            throw Log.log.invalidTxnState();
        }
        synchronized (statusRef) {
            oldVal = statusRef.get();
            if (oldVal != Status.STATUS_ACTIVE && oldVal != Status.STATUS_MARKED_ROLLBACK) {
                throw Log.log.invalidTxnState();
            }
            statusRef.set(Status.STATUS_ROLLING_BACK);
            try {
                final InvocationTracker invocationTracker = channel.getInvocationTracker();
                final BlockingInvocation invocation = invocationTracker.addInvocation(BlockingInvocation::new);
                // write request
                try (MessageOutputStream os = invocationTracker.allocateMessage(invocation)) {
                    os.writeShort(invocation.getIndex());
                    os.writeByte(Protocol.M_UT_ROLLBACK);
                    Protocol.writeParam(Protocol.P_TXN_CONTEXT, os, id, Protocol.UNSIGNED);
                    final int peerIdentityId = channel.getConnection().getPeerIdentityId();
                    if (peerIdentityId != 0) Protocol.writeParam(Protocol.P_SEC_CONTEXT, os, peerIdentityId, Protocol.UNSIGNED);
                } catch (IOException e) {
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw Log.log.failedToSend(e);
                }
                try (BlockingInvocation.Response response = invocation.getResponse()) {
                    try (MessageInputStream is = response.getInputStream()) {
                        if (is.readUnsignedByte() != Protocol.M_RESP_UT_ROLLBACK) {
                            throw Log.log.unknownResponse();
                        }
                        int messageId = is.read();
                        if (messageId == -1) {
                            statusRef.set(Status.STATUS_ROLLEDBACK);
                            channel.notifyTransactionEnd(id);
                        } else {
                            int len = StreamUtils.readPackedUnsignedInt32(is);
                            if (messageId == Protocol.P_UT_IS_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final IllegalStateException e = Log.log.peerIllegalStateException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_UT_SYS_EXC) {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                final SystemException e = Log.log.peerSystemException();
                                e.errorCode = is.readInt();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else if (messageId == Protocol.P_SEC_EXC) {
                                statusRef.set(oldVal);
                                final SecurityException e = Log.log.peerSecurityException();
                                e.initCause(RemoteExceptionCause.readFromStream(is));
                                throw e;
                            } else {
                                statusRef.set(Status.STATUS_UNKNOWN);
                                throw Log.log.unknownResponse();
                            }
                        }
                    } catch (IOException e) {
                        statusRef.set(Status.STATUS_UNKNOWN);
                        throw Log.log.responseFailed(e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    statusRef.set(Status.STATUS_UNKNOWN);
                    throw Log.log.operationInterrupted();
                } catch (IOException e) {
                    // failed to close the response, but we don't care too much
                    Log.log.inboundException(e);
                }
            } finally {
                statusRef.compareAndSet(Status.STATUS_ROLLING_BACK, Status.STATUS_UNKNOWN);
            }
        }
    }

    public void setRollbackOnly() throws SystemException {
        final AtomicInteger statusRef = this.statusRef;
        int oldVal = statusRef.get();
        if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
            return;
        } else if (oldVal != Status.STATUS_ACTIVE) {
            throw Log.log.invalidTxnState();
        }
        synchronized (statusRef) {
            // re-check under lock
            oldVal = statusRef.get();
            if (oldVal == Status.STATUS_MARKED_ROLLBACK) {
                return;
            } else if (oldVal != Status.STATUS_ACTIVE) {
                throw Log.log.invalidTxnState();
            }
            statusRef.set(Status.STATUS_MARKED_ROLLBACK);
        }
    }

    public <T> T getProviderInterface(final Class<T> type) {
        return type.isAssignableFrom(SimpleIdResolver.class) ? type.cast(resolver) : null;
    }

    Connection getConnection() {
        return channel.getConnection();
    }
}
