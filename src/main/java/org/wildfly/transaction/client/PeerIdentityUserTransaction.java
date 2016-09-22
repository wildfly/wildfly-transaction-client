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

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.wildfly.security.ParametricPrivilegedExceptionAction;
import org.wildfly.security.auth.client.PeerIdentity;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class PeerIdentityUserTransaction implements UserTransaction {
    private final UserTransaction delegate;
    private final PeerIdentity peerIdentity;

    PeerIdentityUserTransaction(final UserTransaction delegate, final PeerIdentity peerIdentity) {
        this.delegate = delegate;
        this.peerIdentity = peerIdentity;
    }

    public void begin() throws NotSupportedException, SystemException {
        try {
            peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Void, UserTransaction>) d -> {
                d.begin();
                return null;
            });
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (NotSupportedException | SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        try {
            peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Void, UserTransaction>) d -> {
                d.commit();
                return null;
            });
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException  | SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        try {
            peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Void, UserTransaction>) d -> {
                d.rollback();
                return null;
            });
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException {
        try {
            peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Void, UserTransaction>) d -> {
                d.setRollbackOnly();
                return null;
            });
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }

    public int getStatus() throws SystemException {
        try {
            return peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Integer, UserTransaction>) d -> Integer.valueOf(d.getStatus())).intValue();
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }

    public void setTransactionTimeout(final int seconds) throws SystemException {
        try {
            peerIdentity.runAs(delegate, (ParametricPrivilegedExceptionAction<Integer, UserTransaction>) d -> {
                d.setTransactionTimeout(seconds);
                return null;
            });
        } catch (PrivilegedActionException e) {
            try {
                throw e.getCause();
            } catch (SystemException | RuntimeException | Error e1) {
                throw e1;
            } catch (Throwable throwable) {
                throw new UndeclaredThrowableException(throwable);
            }
        }
    }
}
