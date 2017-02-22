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

import static org.jboss.remoting3.util.StreamUtils.writeInt8;
import static org.jboss.remoting3.util.StreamUtils.writePackedUnsignedInt31;
import static org.wildfly.transaction.client._private.Log.log;
import static org.wildfly.transaction.client.provider.remoting.Protocol.*;
import static org.wildfly.transaction.client.provider.remoting.RemotingTransactionServer.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3.RemotingOptions;
import org.jboss.remoting3.util.MessageTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.common.rpc.RemoteExceptionCause;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.transaction.client.ImportResult;
import org.wildfly.transaction.client.LocalTransaction;
import org.wildfly.transaction.client.LocalTransactionContext;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XARecoverable;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TransactionServerChannel {
    private final RemotingTransactionServer server;
    private final MessageTracker messageTracker;
    private final Channel channel;
    private final Channel.Receiver receiver = new ReceiverImpl();
    private final LocalTransactionContext localTransactionContext;

    private static final Attachments.Key<TransactionServerChannel> KEY = new Attachments.Key<>(TransactionServerChannel.class);

    TransactionServerChannel(final RemotingTransactionServer server, final Channel channel, final LocalTransactionContext localTransactionContext) {
        this.server = server;
        this.channel = channel;
        this.localTransactionContext = localTransactionContext;
        messageTracker = new MessageTracker(channel, channel.getOption(RemotingOptions.MAX_OUTBOUND_MESSAGES).intValue());
        channel.getConnection().getAttachments().attach(KEY, this);
    }

    void start() {
        channel.receiveMessage(receiver);
    }

    class ReceiverImpl implements Channel.Receiver {

        ReceiverImpl() {
        }

        public void handleMessage(final Channel channel, final MessageInputStream messageOriginal) {
            channel.receiveMessage(this);
            try (MessageInputStream message = messageOriginal) {
                final int invId = message.readUnsignedShort();
                try {
                    final int id = message.readUnsignedByte();
                    switch (id) {
                        case M_CAPABILITY: {
                            handleCapabilityMessage(message, invId);
                            break;
                        }

                        case M_UT_ROLLBACK: {
                            handleUserTxnRollback(message, invId);
                            break;
                        }
                        case M_UT_COMMIT: {
                            handleUserTxnCommit(message, invId);
                            break;
                        }

                        case M_XA_ROLLBACK: {
                            handleXaTxnRollback(message, invId);
                            break;
                        }
                        case M_XA_BEFORE: {
                            handleXaTxnBefore(message, invId);
                            break;
                        }
                        case M_XA_PREPARE: {
                            handleXaTxnPrepare(message, invId);
                            break;
                        }
                        case M_XA_FORGET: {
                            handleXaTxnForget(message, invId);
                            break;
                        }
                        case M_XA_COMMIT: {
                            handleXaTxnCommit(message, invId);
                            break;
                        }
                        case M_XA_RECOVER: {
                            handleXaTxnRecover(message, invId);
                            break;
                        }
                        case M_XA_RB_ONLY: {
                            handleXaTxnRollbackOnly(message, invId);
                            break;
                        }

                        default: {
                            try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                                outputStream.writeShort(invId);
                                outputStream.writeByte(M_RESP_ERROR);
                            } catch (IOException e) {
                                log.outboundException(e);
                            }
                            break;
                        }
                    }
                } catch (Throwable t) {
                    try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                        outputStream.writeShort(invId);
                        outputStream.writeByte(M_RESP_ERROR);
                    } catch (IOException e) {
                        log.outboundException(e);
                    }
                    throw t;
                }
            } catch (IOException e) {
                log.inboundException(e);
            }
        }

        public void handleError(final Channel channel, final IOException error) {
        }

        public void handleEnd(final Channel channel) {
        }
    }

    void handleCapabilityMessage(final MessageInputStream message, final int invId) throws IOException {
        while (message.read() != -1) {
            // ignore parameters
            readIntParam(message, StreamUtils.readPackedUnsignedInt32(message));
        }
        // acknowledge no capabilities
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(M_RESP_CAPABILITY);
        }
        return;
    }

    void handleUserTxnRollback(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != -1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = readIntParam(message, len);
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (! hasContext) {
            writeParamError(invId);
            return;
        }
        final LocalTxn txn = server.getTxnMap().removeKey(context);
        if (txn == null) {
            // nothing to roll back!
            writeSimpleResponse(M_RESP_UT_ROLLBACK, invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            final LocalTransaction transaction = txn.getTransaction();
            if (transaction != null) try {
                transaction.performAction(transaction::rollback);
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId);
                return;
            } catch (SystemException e) {
                writeExceptionResponse(M_RESP_UT_ROLLBACK, invId, e);
                return;
            } catch (Exception e) {
                writeExceptionResponse(M_RESP_UT_ROLLBACK, invId, log.unexpectedException(e));
                return;
            } else {
                writeParamError(invId);
                return;
            }
        });
    }

    void handleUserTxnCommit(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != -1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = readIntParam(message, len);
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (! hasContext) {
            writeParamError(invId);
            return;
        }
        final LocalTxn txn = server.getTxnMap().removeKey(context);
        if (txn == null) {
            // nothing to commit!
            writeSimpleResponse(M_RESP_UT_COMMIT, invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            final LocalTransaction transaction = txn.getTransaction();
            if (transaction != null) try {
                transaction.performAction(transaction::commit);
                writeSimpleResponse(M_RESP_UT_COMMIT, invId);
                return;
            } catch (HeuristicRollbackException e) {
                writeExceptionResponse(M_RESP_UT_COMMIT, invId, P_UT_HRE_EXC, e);
                return;
            } catch (RollbackException e) {
                writeExceptionResponse(M_RESP_UT_COMMIT, invId, P_UT_RB_EXC, e);
                return;
            } catch (HeuristicMixedException e) {
                writeExceptionResponse(M_RESP_UT_COMMIT, invId, P_UT_HME_EXC, e);
                return;
            } catch (SystemException e) {
                writeExceptionResponse(M_RESP_UT_COMMIT, invId, e);
                return;
            } catch (Exception e) {
                writeExceptionResponse(M_RESP_UT_COMMIT, invId, log.unexpectedException(e));
                return;
            } else {
                writeParamError(invId);
            }
        });
    }

    /////////////////////////

    void handleXaTxnRollback(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsObjIntConsumer((x, i) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                importResult.getTransaction().performConsumer(SubordinateTransactionControl::rollback, importResult.getControl());
                writeSimpleResponse(M_RESP_XA_ROLLBACK, i);
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, e);
                return;
            }
        }, xid.withoutBranch(), invId);
    }

    void handleXaTxnRollbackOnly(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsObjIntConsumer((x, i) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                importResult.getTransaction().performConsumer(SubordinateTransactionControl::end, importResult.getControl(), XAResource.TMFAIL);
                writeSimpleResponse(M_RESP_XA_RB_ONLY, i);
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, e);
                return;
            }
        }, xid.withoutBranch(), invId);
    }

    void handleXaTxnBefore(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsObjIntConsumer((x, i) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                importResult.getTransaction().performConsumer(SubordinateTransactionControl::beforeCompletion, importResult.getControl());
                writeSimpleResponse(M_RESP_XA_BEFORE, i);
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, e);
                return;
            }
        }, xid.withoutBranch(), invId);
    }

    void handleXaTxnPrepare(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsObjIntConsumer((x, i) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                int result = importResult.getTransaction().performToIntFunction(SubordinateTransactionControl::prepare, importResult.getControl());
                if (result == XAResource.XA_RDONLY) {
                    writeSimpleResponse(M_RESP_XA_PREPARE, i, P_XA_RDONLY);
                } else {
                    // XA_OK
                    writeSimpleResponse(M_RESP_XA_PREPARE, i);
                }
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, e);
                return;
            } catch (Exception e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            }
        }, xid.withoutBranch(), invId);
    }

    void handleXaTxnForget(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsObjIntConsumer((x, i) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                importResult.getTransaction().performConsumer(SubordinateTransactionControl::forget, importResult.getControl());
                writeSimpleResponse(M_RESP_XA_FORGET, i);
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, e);
                return;
            } catch (Exception e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, i, xae);
                return;
            }
        }, xid, invId);
    }

    void handleXaTxnCommit(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        SimpleXid xid = null;
        int secContext = 0;
        boolean hasSecContext = false;
        boolean onePhase = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_XID: {
                    xid = readXid(message, len);
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                case P_ONE_PHASE: {
                    onePhase = true;
                    readIntParam(message, len);
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        if (xid == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAsConsumer((o, x) -> {
            try {
                final ImportResult<LocalTransaction> importResult = localTransactionContext.findOrImportTransaction(x, 0);
                // run operation while associated
                importResult.getTransaction().performConsumer((c, flag) -> c.commit(flag.booleanValue()), importResult.getControl(), o);
                writeSimpleResponse(M_RESP_XA_COMMIT, invId);
            } catch (SystemException e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, xae);
                return;
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e);
                return;
            } catch (Exception e) {
                final XAException xae = new XAException(XAException.XAER_RMERR);
                xae.initCause(e);
                writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, xae);
                return;
            }
        }, Boolean.valueOf(onePhase), xid.withoutBranch());
    }

    void handleXaTxnRecover(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int secContext = 0;
        String parentName = null;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_SEC_CONTEXT: {
                    secContext = readIntParam(message, len);
                    hasSecContext = true;
                    break;
                }
                case P_PARENT_NAME: {
                    parentName = readStringParam(message, len);
                    break;
                }
                default: {
                    // ignore bad parameter
                    readIntParam(message, len);
                }
            }
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        final String finalParentName = parentName;
        securityIdentity.runAs(() -> {
            final XARecoverable recoverable = localTransactionContext.getRecoveryInterface();
            Xid[] xids;
            try {
                // get the first batch
                xids = recoverable.recover(XAResource.TMSTARTRSCAN, finalParentName);
            } catch (XAException e) {
                writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e);
                return;
            }
            try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                outputStream.writeShort(invId);
                outputStream.writeByte(M_RESP_XA_RECOVER);
                // maintain a "seen" set as some transaction managers don't treat recovery scanning as a cursor...
                // once the "seen" set hasn't been modified by a scan request, the scan is done
                Set<Xid> seen = new HashSet<Xid>();
                boolean added;
                do {
                    added = false;
                    for (final Xid xid : xids) {
                        SimpleXid simpleXid = SimpleXid.of(xid).withoutBranch();
                        if (seen.add(simpleXid)) {
                            added = true;
                            writeParam(P_XID, outputStream, simpleXid);
                        }
                    }
                    if (added) try {
                        // get the next batch
                        xids = recoverable.recover(XAResource.TMNOFLAGS, finalParentName);
                    } catch (XAException e) {
                        try {
                            recoverable.recover(XAResource.TMENDRSCAN, finalParentName);
                            writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e);
                        } catch (XAException e1) {
                            e1.addSuppressed(e);
                            writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e1);
                        }
                        return;
                    }
                } while (xids.length > 0 && added);
                try {
                    xids = recoverable.recover(XAResource.TMENDRSCAN, finalParentName);
                } catch (XAException e) {
                    try {
                        recoverable.recover(XAResource.TMENDRSCAN, finalParentName);
                        writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e);
                    } catch (XAException e1) {
                        e1.addSuppressed(e);
                        writeExceptionResponse(M_RESP_XA_ROLLBACK, invId, e1);
                    }
                    return;
                }
                for (final Xid xid : xids) {
                    SimpleXid simpleXid = SimpleXid.of(xid).withoutBranch();
                    if (seen.add(simpleXid)) {
                        writeParam(P_XID, outputStream, xid);
                    }
                }
            } catch (IOException e) {
                log.outboundException(e);
                try {
                    recoverable.recover(XAResource.TMENDRSCAN);
                } catch (XAException e1) {
                    // ignored
                    log.recoverySuppressedException(e1);
                }
            }
        });
    }

    ///////////////////////////////////////////////////////////////

    void writeSimpleResponse(final int msgId, final int invId, final int param1) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(msgId);
            writeParam(param1, outputStream);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    private void writeExceptionResponse(final int msgId, final int invId, final int exceptionKind, final Exception e) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(msgId);
            writeInt8(outputStream, exceptionKind);
            final RemoteExceptionCause remoteExceptionCause = RemoteExceptionCause.of(e);
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(os);
            remoteExceptionCause.writeToStream(dos);
            dos.flush();
            writePackedUnsignedInt31(outputStream, os.size());
            os.writeTo(outputStream);
        } catch (IOException ioe) {
            log.outboundException(ioe);
        }
    }

    private void writeExceptionResponse(final int msgId, final int invId, final int exceptionKind, final Exception e, int errorCode) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(msgId);
            writeInt8(outputStream, exceptionKind);
            final RemoteExceptionCause remoteExceptionCause = RemoteExceptionCause.of(e);
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final DataOutputStream dos = new DataOutputStream(os);
            dos.writeInt(errorCode);
            remoteExceptionCause.writeToStream(dos);
            dos.flush();
            writePackedUnsignedInt31(outputStream, os.size());
            os.writeTo(outputStream);
        } catch (IOException ioe) {
            log.outboundException(ioe);
        }
    }

    private void writeExceptionResponse(final int msgId, final int invId, final SystemException e) {
        writeExceptionResponse(msgId, invId, P_UT_SYS_EXC, e, e.errorCode);
    }

    private void writeExceptionResponse(final int msgId, final int invId, final XAException e) {
        writeExceptionResponse(msgId, invId, P_XA_ERROR, e, e.errorCode);
    }

    void writeSimpleResponse(final int msgId, final int invId) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(msgId);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    void writeParamError(final int invId) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeShort(invId);
            outputStream.writeByte(M_RESP_PARAM_ERROR);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }
}
