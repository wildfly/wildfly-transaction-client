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

import javax.resource.spi.XATerminator;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * An interface which specifies the common recovery portion of the {@link XAResource} and {@link XATerminator} APIs.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface XARecoverable {
    Xid[] recover(int flag) throws XAException;

    void commit(Xid xid, boolean onePhase) throws XAException;

    void forget(Xid xid) throws XAException;

    static XARecoverable from(XATerminator xaTerminator) {
        return xaTerminator instanceof XARecoverable ? (XARecoverable) xaTerminator : new XARecoverable() {
            public Xid[] recover(final int flag) throws XAException {
                return xaTerminator.recover(flag);
            }

            public void commit(final Xid xid, final boolean onePhase) throws XAException {
                xaTerminator.commit(xid, onePhase);
            }

            public void forget(final Xid xid) throws XAException {
                xaTerminator.forget(xid);
            }
        };
    }

    static XARecoverable from(XAResource xaResource) {
        return xaResource instanceof XARecoverable ? (XARecoverable) xaResource : new XARecoverable() {
            public Xid[] recover(final int flag) throws XAException {
                return xaResource.recover(flag);
            }

            public void commit(final Xid xid, final boolean onePhase) throws XAException {
                xaResource.commit(xid, onePhase);
            }

            public void forget(final Xid xid) throws XAException {
                xaResource.forget(xid);
            }
        };
    }
}
