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

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.wildfly.transaction.client.spi.SimpleTransactionControl;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface RemotingOperations {
    void commit(Xid xid, boolean onePhase) throws XAException;

    void forget(Xid xid) throws XAException;

    int prepare(Xid xid) throws XAException;

    void rollback(Xid xid) throws XAException;

    void setRollbackOnly(Xid xid) throws XAException;

    void beforeCompletion(Xid xid) throws XAException;

    Xid[] recover(int flag) throws XAException;

    SimpleTransactionControl begin(int timeout) throws SystemException;
}
