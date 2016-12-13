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
import static java.lang.Math.min;
import static org.wildfly.transaction.client._private.Log.log;
import static org.wildfly.transaction.client.provider.remoting.Protocol.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3.RemotingOptions;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexMap;
import org.jboss.remoting3.util.MessageTracker;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.common.function.ExceptionSupplier;
import org.wildfly.security.ParametricPrivilegedAction;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XAImporter;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class TransactionServerChannel {
    private final MessageTracker messageTracker;
    private final Channel channel;
    private final Channel.Receiver receiver = new ReceiverImpl();
    private final XAImporter importer;
    private final TransactionManager transactionManager;
    private final IntIndexMap<Txn> txns = new IntIndexHashMap<Txn>(Txn::getId);

    TransactionServerChannel(final Channel channel, final XAImporter importer, final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        messageTracker = new MessageTracker(channel, channel.getOption(RemotingOptions.MAX_OUTBOUND_MESSAGES).intValue());
        this.importer = importer;
        this.channel = channel;
    }

    void handleClosed() {
        for (Txn txn : txns) {
            safeRollback(txn.getTransaction());
        }
    }

    void start() {
        channel.receiveMessage(receiver);
    }

    class ReceiverImpl implements Channel.Receiver {

        ReceiverImpl() {
        }

        public void handleMessage(final Channel channel, final MessageInputStream messageOriginal) {
            try (MessageInputStream message = messageOriginal) {
                final int id = message.readUnsignedByte();
                final int invId = message.readUnsignedShort();
                switch (id) {
                    case M_CAPABILITY: {
                        handleCapabilityMessage(message, invId);
                        break;
                    }

                    case M_UT_BEGIN: {
                        handleUserTxnBegin(message, invId);
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

                    case M_XA_BEGIN: {
                        handleXaTxnBegin(message, invId);
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

                    default: {
                        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                            outputStream.writeByte(M_RESP_ERROR);
                            outputStream.writeShort(invId);
                        } catch (IOException e) {
                            log.outboundException(e);
                        }
                        break;
                    }
                }
            } catch (IOException e) {
                log.inboundException(e);
            }
            channel.receiveMessage(this);
        }

        public void handleError(final Channel channel, final IOException error) {
            handleClosed();
        }

        public void handleEnd(final Channel channel) {
            handleClosed();
        }
    }

    void handleCapabilityMessage(final MessageInputStream message, final int invId) throws IOException {
        while (message.read() != -1) {
            // ignore parameters
            readIntParam(message, StreamUtils.readPackedUnsignedInt32(message));
        }
        // acknowledge no capabilities
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeByte(M_RESP_CAPABILITY);
            outputStream.writeShort(invId);
        }
        return;
    }

    void handleUserTxnBegin(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int timeout = 0;
        int context = 0;
        int secContext = 0;
        boolean hasTimeout = false;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != -1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_TIMEOUT: {
                    timeout = readIntParam(message, len);
                    hasTimeout = true;
                    break;
                }
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
        if (hasTimeout && timeout < 1) {
            writeSimpleResponse(M_RESP_UT_BEGIN, invId, P_UT_SYS_EXC);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        doUserTxBegin(securityIdentity, invId, timeout, context);
    }

    private void doUserTxBegin(final SecurityIdentity securityIdentity, final int invId, final int timeout, final int context) {
        securityIdentity.runAs(() -> {
            try {
                final TransactionManager transactionManager = this.transactionManager;
                transactionManager.setTransactionTimeout(timeout);
                try {
                    transactionManager.begin();
                } catch (NotSupportedException e) {
                    // unlikely
                    writeSimpleResponse(M_RESP_UT_BEGIN, invId, P_UT_SYS_EXC);
                    return;
                }
                final Transaction transaction = transactionManager.suspend();
                if (txns.putIfAbsent(new LocalTxn(context, transaction)) != null) {
                    log.debugf("Duplicate transaction ID %d", context);
                    writeSimpleResponse(M_RESP_UT_BEGIN, invId, P_UT_SYS_EXC);
                    return;
                }
                try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                    outputStream.writeByte(M_RESP_UT_BEGIN);
                    outputStream.writeShort(invId);
                } catch (IOException e) {
                    log.outboundException(e);
                }
            } catch (SystemException e) {
                writeSimpleResponse(M_RESP_UT_BEGIN, invId, P_UT_SYS_EXC);
                return;
            }
        });
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
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.removeKey(context);
        if (txn == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            final Transaction transaction = txn.getTransaction();
            if (transaction != null) try {
                transaction.rollback();
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId);
            } catch (SystemException e) {
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId, P_UT_SYS_EXC);
                return;
            } else {
                writeParamError(invId);
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
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.removeKey(context);
        if (txn == null) {
            writeParamError(invId);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            final Transaction transaction = txn.getTransaction();
            if (transaction != null) try {
                transaction.commit();
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId);
            } catch (SystemException e) {
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId, P_UT_SYS_EXC);
                return;
            } catch (HeuristicRollbackException e) {
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId, P_UT_HRE_EXC);
                return;
            } catch (RollbackException e) {
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId, P_UT_RB_EXC);
                return;
            } catch (HeuristicMixedException e) {
                writeSimpleResponse(M_RESP_UT_ROLLBACK, invId, P_UT_HME_EXC);
                return;
            } else {
                writeParamError(invId);
            }
        });
    }

    /////////////////////////

    void handleXaTxnBegin(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int context = 0;
        int timeout = 0;
        SimpleXid xid = null;
        boolean hasContext = false;
        final ParameterIterator iterator = new ParameterIterator(message);
        while ((param = iterator.getId()) != -1) {
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    // ignored for now
                    iterator.readAsIntegerIo();
                    break;
                }
                case P_XID: {
                    xid = iterator.readAsXidIo();
                    break;
                }
                case P_TXN_TIMEOUT: {
                    timeout = max(0, iterator.readAsIntegerIo());
                    break;
                }
                default: {
                    // ignore bad parameter
                    iterator.readAsIntegerIo();
                    break;
                }
            }
        }
        if (! hasContext || xid == null) {
            writeParamError(invId);
            return;
        }

        ImportedTxn importedTxn = new ImportedTxn(context, xid, timeout);
        if (txns.putIfAbsent(importedTxn) != null) {
            log.debugf("Duplicate transaction ID %d", context);
            writeSimpleResponse(M_RESP_XA_BEGIN, invId, P_UT_SYS_EXC);
            return;
        }
        writeSimpleResponse(M_RESP_XA_BEGIN, invId);
    }

    void handleXaTxnRollback(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.get(context);
        if (txn == null) {
            writeXaExceptionResponse(M_RESP_XA_ROLLBACK, invId, XAException.XAER_NOTA);
            return;
        }
        if (! (txn instanceof ImportedTxn)) {
            writeXaExceptionResponse(M_RESP_XA_ROLLBACK, invId, XAException.XAER_NOTA);
            return;
        }
        txns.remove(txn);
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            try {
                importer.rollback(((ImportedTxn) txn).getXid());
                writeSimpleResponse(M_RESP_XA_ROLLBACK, invId);
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_ROLLBACK, invId, e.errorCode);
                return;
            }
        });
    }

    void handleXaTxnBefore(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.get(context);
        if (txn == null) {
            writeXaExceptionResponse(M_RESP_XA_BEFORE, invId, XAException.XAER_NOTA);
            return;
        }
        if (! (txn instanceof ImportedTxn)) {
            writeXaExceptionResponse(M_RESP_XA_BEFORE, invId, XAException.XAER_NOTA);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            try {
                importer.beforeComplete(((ImportedTxn) txn).getXid());
                writeSimpleResponse(M_RESP_XA_BEFORE, invId);
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_BEFORE, invId, e.errorCode);
                return;
            }
        });
    }

    void handleXaTxnPrepare(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.get(context);
        if (txn == null) {
            writeXaExceptionResponse(M_RESP_XA_PREPARE, invId, XAException.XAER_NOTA);
            return;
        }
        if (! (txn instanceof ImportedTxn)) {
            writeXaExceptionResponse(M_RESP_XA_PREPARE, invId, XAException.XAER_NOTA);
            return;
        }
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            try {
                int result = importer.prepare(((ImportedTxn) txn).getXid());
                if (result == XAResource.XA_RDONLY) {
                    writeSimpleResponse(M_RESP_XA_BEFORE, invId, P_XA_RDONLY);
                } else {
                    // XA_OK
                    writeSimpleResponse(M_RESP_XA_BEFORE, invId);
                }
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_BEFORE, invId, e.errorCode);
                return;
            }
        });
    }

    void handleXaTxnForget(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        final Txn txn = txns.get(context);
        if (txn == null) {
            writeXaExceptionResponse(M_RESP_XA_FORGET, invId, XAException.XAER_NOTA);
            return;
        }
        if (! (txn instanceof ImportedTxn)) {
            writeXaExceptionResponse(M_RESP_XA_FORGET, invId, XAException.XAER_NOTA);
            return;
        }
        txns.remove(txn);
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(() -> {
            try {
                importer.forget(((ImportedTxn) txn).getXid());
                writeSimpleResponse(M_RESP_XA_FORGET, invId);
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_FORGET, invId, e.errorCode);
                return;
            }
        });
    }

    void handleXaTxnCommit(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int context = 0;
        int secContext = 0;
        boolean hasContext = false;
        boolean hasSecContext = false;
        boolean onePhase = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_TXN_CONTEXT: {
                    context = message.readInt();
                    hasContext = true;
                    break;
                }
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
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
        if (! hasContext) {
            writeParamError(invId);
            return;
        }
        final Txn txn = txns.get(context);
        if (txn == null) {
            writeXaExceptionResponse(M_RESP_XA_COMMIT, invId, XAException.XAER_NOTA);
            return;
        }
        if (! (txn instanceof ImportedTxn)) {
            writeXaExceptionResponse(M_RESP_XA_COMMIT, invId, XAException.XAER_NOTA);
            return;
        }
        txns.remove(txn);
        SecurityIdentity securityIdentity;
        if (hasSecContext) {
            securityIdentity = channel.getConnection().getLocalIdentity(secContext);
        } else {
            securityIdentity = channel.getConnection().getLocalIdentity();
        }
        securityIdentity.runAs(Boolean.valueOf(onePhase), (ParametricPrivilegedAction<Void, Boolean>) onePhaseRef -> {
            try {
                importer.commit(((ImportedTxn) txn).getXid(), onePhaseRef.booleanValue());
                writeSimpleResponse(M_RESP_XA_COMMIT, invId);
                return null;
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_COMMIT, invId, e.errorCode);
                return null;
            }
        });
    }

    void handleXaTxnRecover(final MessageInputStream message, final int invId) throws IOException {
        int param;
        int len;
        int secContext = 0;
        boolean hasSecContext = false;
        while ((param = message.read()) != - 1) {
            len = StreamUtils.readPackedUnsignedInt32(message);
            switch (param) {
                case P_SEC_CONTEXT: {
                    secContext = message.readInt();
                    hasSecContext = true;
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
        securityIdentity.runAs(() -> {
            Xid[] xids;
            try {
                // get the first batch
                xids = importer.recover(XAResource.TMSTARTRSCAN);
            } catch (XAException e) {
                writeXaExceptionResponse(M_RESP_XA_RECOVER, invId, e.errorCode);
                return;
            }
            try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
                outputStream.writeByte(M_RESP_XA_RECOVER);
                outputStream.writeShort(invId);
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
                        xids = importer.recover(XAResource.TMNOFLAGS);
                    } catch (XAException e) {
                        writeParam(P_XA_ERROR, outputStream, e.errorCode, SIGNED);
                        try {
                            importer.recover(XAResource.TMENDRSCAN);
                        } catch (XAException e1) {
                            // ignored
                            log.recoverySuppressedException(e1);
                        }
                        return;
                    }
                } while (xids.length > 0 && added);
                try {
                    xids = importer.recover(XAResource.TMENDRSCAN);
                } catch (XAException e) {
                    writeParam(P_XA_ERROR, outputStream, e.errorCode, SIGNED);
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
                    importer.recover(XAResource.TMENDRSCAN);
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
            outputStream.writeByte(msgId);
            outputStream.writeShort(invId);
            writeParam(param1, outputStream);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    void writeXaExceptionResponse(final int msgId, final int invId, final int errorCode) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeByte(msgId);
            outputStream.writeShort(invId);
            writeParam(P_XA_ERROR, outputStream, errorCode, SIGNED);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    void writeSimpleResponse(final int msgId, final int invId) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeByte(msgId);
            outputStream.writeShort(invId);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    void writeParamError(final int invId) {
        try (final MessageOutputStream outputStream = messageTracker.openMessageUninterruptibly()) {
            outputStream.writeByte(M_RESP_PARAM_ERROR);
            outputStream.writeShort(invId);
        } catch (IOException e) {
            log.outboundException(e);
        }
    }

    private static void safeRollback(final Transaction transaction) {
        if (transaction != null) try {
            transaction.rollback();
        } catch (SystemException e) {
            log.trace("Got exception during rollback-on-disconnect", e);
        }
    }

    abstract static class Txn {
        private final int id;

        Txn(final int id) {
            this.id = id;
        }

        int getId() {
            return id;
        }

        abstract Transaction getTransaction();
    }

    static final class LocalTxn extends Txn {
        private final Transaction transaction;

        LocalTxn(final int id, final Transaction transaction) {
            super(id);
            this.transaction = transaction;
        }

        Transaction getTransaction() {
            return transaction;
        }
    }

    final class ImportedTxn extends Txn implements ExceptionSupplier<Transaction, XAException> {
        private final Xid xid;
        private final long startTime;
        private final int timeout;
        private volatile Transaction transaction;

        ImportedTxn(final int id, final Xid xid, final int timeout) {
            super(id);
            this.xid = xid;
            startTime = System.nanoTime();
            this.timeout = timeout;
        }

        Xid getXid() {
            return xid;
        }

        Transaction getTransaction() {
            return transaction;
        }

        public Transaction get() throws XAException {
            Transaction transaction = this.transaction;
            if (transaction != null) {
                return transaction;
            }
            synchronized (this) {
                transaction = this.transaction;
                if (transaction != null) {
                    return transaction;
                }
                long elapsed = max(0L, System.nanoTime() - startTime) / 1_000_000_000L;
                if (elapsed >= timeout) {
                    // don't even import it
                    throw log.transactionTimedOut(XAException.XA_RBTIMEOUT);
                }
                // don't set if there's an exception on import
                final XAImporter.ImportResult result = importer.findOrImportTransaction(xid, max(1, timeout - (int) min((long) Integer.MAX_VALUE, elapsed)));
                // TODO
                return this.transaction = result.getTransaction();
            }
        }
    }
}
