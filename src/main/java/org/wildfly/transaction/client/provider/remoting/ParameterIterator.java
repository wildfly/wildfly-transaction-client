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

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;

import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.util.StreamUtils;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client._private.Log;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class ParameterIterator {
    private final MessageInputStream is;
    private int id;

    ParameterIterator(final MessageInputStream is) throws IOException {
        this.is = is;
        id = is.read();
    }

    public void requireParameter(int id) throws SystemException {
        requireParameter(id, null);
    }

    public void requireParameter(int id, Throwable suppressed) throws SystemException {
        if (this.id != id) {
            SystemException ex = Log.log.expectedParameter(id, this.id);
            if (suppressed != null) ex.addSuppressed(suppressed);
            throw ex;
        }
    }

    public void requireParameterXa(int id) throws XAException {
        requireParameterXa(id, null);
    }

    public void requireParameterXa(int id, Throwable suppressed) throws XAException {
        if (this.id != id) {
            XAException ex = Log.log.expectedParameterXa(XAException.XAER_RMFAIL, id, this.id);
            if (suppressed != null) ex.addSuppressed(suppressed);
            throw ex;
        }
    }

    public int getId() {
        return id;
    }

    public int readAsIntegerIo() throws IOException {
        final int i = Protocol.readIntParam(is, StreamUtils.readPackedUnsignedInt32(is));
        id = is.read();
        return i;
    }

    public int readAsInteger() throws SystemException {
        try {
            return readAsIntegerIo();
        } catch (IOException e) {
            throw Log.log.failedToReceive(e);
        }
    }

    public int readAsIntegerXa() throws XAException {
        try {
            return readAsIntegerIo();
        } catch (IOException e) {
            throw Log.log.failedToReceiveXA(e, XAException.XAER_RMFAIL);
        }
    }

    public SimpleXid readAsXidIo() throws IOException {
        SimpleXid xid = Protocol.readXid(is, StreamUtils.readPackedUnsignedInt32(is));
        id = is.read();
        return xid;
    }

    public SimpleXid readAsXid() throws XAException {
        try {
            return readAsXidIo();
        } catch (IOException e) {
            throw Log.log.failedToReceiveXA(e, XAException.XAER_RMFAIL);
        }
    }
}
