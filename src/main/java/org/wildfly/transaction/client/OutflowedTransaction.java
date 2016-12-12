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

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client._private.Log;

final class OutflowedTransaction {

    private final SimpleXid transactionXid;
    private final Transaction transaction;
    private final ConcurrentMap<URI, Enlistment> enlistments = new ConcurrentHashMap<>();

    OutflowedTransaction(final SimpleXid transactionXid, final Transaction transaction) {
        this.transactionXid = transactionXid;
        this.transaction = transaction;
    }

    Enlistment getEnlistment(final URI location) {
        return enlistments.computeIfAbsent(location, Enlistment::new);
    }

    Transaction getTransaction() {
        return transaction;
    }

    static final int ST_ACTIVE_CNT_MASK     = 0x000f_ffff;
    static final int ST_STATE_MASK          = 0x0030_0000;

    static final int ST_STATE_NOT_REGISTERED= 0x0000_0000; // state 0 (lock on all exiting transitions)
    static final int ST_STATE_NOT_ENLISTED  = 0x0010_0000; // state 1 (lock on all exiting transitions iff CNT > 0 (1A))
    static final int ST_STATE_ENLISTED      = 0x0020_0000; // state 2 (lock on all exiting transitions iff CNT = 0 (2B))
    static final int ST_STATE_DONE          = 0x0030_0000; // state 3

    static final int ST_HANDLE_STATE_INITIAL    = 0; // locked state
    static final int ST_HANDLE_STATE_ENLISTED   = 1;
    static final int ST_HANDLE_STATE_FORGOTTEN  = 2;

    class Enlistment {
        private final SubordinateXAResource xaResource;
        private final AtomicInteger enlistmentState = new AtomicInteger();

        Enlistment(final URI uri) {
            xaResource = new SubordinateXAResource(uri);
        }

        DelayedEnlistmentHandle createHandle() throws RollbackException, SystemException {
            final AtomicInteger enlistmentState = this.enlistmentState;
            int oldVal, newVal;
            do {
                oldVal = enlistmentState.get();
                switch (oldVal & ST_STATE_MASK) {
                    case ST_STATE_NOT_REGISTERED: {
                        if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                            // can't leave this state without the lock; recurse
                            return createHandle();
                        }
                        // can't have active delayed enlistment yet
                        assert (oldVal & ST_ACTIVE_CNT_MASK) == 0;
                        newVal = ST_STATE_NOT_ENLISTED | 1; // not enlisted, one outstanding (state 1A)
                        // perform the synch registration
                        transaction.registerSynchronization(createSynchronization());
                        // we hold the lock; this is guaranteed to succeed so don't bother to CAS
                        enlistmentState.set(newVal);
                        return new Handle();
                    }
                    case ST_STATE_NOT_ENLISTED: {
                        // two substates: no outstanding (1B), or >0 outstanding (1A)
                        int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                        if (outstanding > 0) {
                            if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                // can't leave this sub-state without the lock; recurse
                                return createHandle();
                            }
                            // add active outstanding and set outright (we hold the lock)
                            newVal = oldVal + 1;
                            enlistmentState.set(newVal);
                            return new Handle();
                        } else {
                            // add active outstanding and CAS
                            newVal = oldVal + 1;
                        }
                        break;
                    }
                    case ST_STATE_ENLISTED: {
                        // two substates: no outstanding (2B), or >0 outstanding (2A)
                        int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                        if (outstanding == 0) {
                            if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                // can't leave this sub-state without the lock; recurse
                                return createHandle();
                            }
                            // add active outstanding and set outright (we hold the lock)
                            newVal = oldVal + 1;
                            enlistmentState.set(newVal);
                            return new Handle();
                        } else {
                            // add active outstanding and CAS
                            newVal = oldVal + 1;
                        }
                        break;
                    }
                    case ST_STATE_DONE: {
                        throw Log.log.noTransaction();
                    }
                    default: {
                        throw Assert.impossibleSwitchCase((oldVal & ST_STATE_MASK) >> Integer.numberOfTrailingZeros(ST_STATE_MASK));
                    }
                }
            } while (! enlistmentState.compareAndSet(oldVal, newVal));
            return new Handle();
        }

        private Synchronization createSynchronization() {
            return new Synchronization() {
                public void beforeCompletion() {
                    final AtomicInteger enlistmentState = Enlistment.this.enlistmentState;
                    int oldVal, newVal;
                    do {
                        oldVal = enlistmentState.get();
                        switch (oldVal & ST_STATE_MASK) {
                            case ST_STATE_NOT_ENLISTED: {
                                // two substates: no outstanding (1B), or >0 outstanding (1A)
                                int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                                if (outstanding > 0) {
                                    if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                        // can't leave this sub-state without the lock; recurse
                                        beforeCompletion();
                                        return;
                                    }
                                    // outstanding remain; it's an error
                                    throw Log.log.delayedEnlistmentFailed(null);
                                } else {
                                    newVal = ST_STATE_DONE;
                                }
                                break;
                            }
                            case ST_STATE_ENLISTED: {
                                // two substates: no outstanding (2B), or >0 outstanding (2A)
                                int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                                if (outstanding > 0) {
                                    // outstanding remain; it's an error
                                    throw Log.log.delayedEnlistmentFailed(null);
                                } else {
                                    if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                        // can't leave this sub-state without the lock; recurse
                                        beforeCompletion();
                                        return;
                                    }
                                    // propagate beforeCompletion notification
                                    try {
                                        xaResource.beforeCompletion(transactionXid);
                                    } catch (XAException e) {
                                        throw Log.log.beforeCompletionFailed(e, xaResource);
                                    }
                                    newVal = ST_STATE_DONE;
                                    // we hold the lock; this is guaranteed to succeed so don't bother to CAS
                                    enlistmentState.set(newVal);
                                    return;
                                }
                            }
                            default: {
                                throw Assert.impossibleSwitchCase((oldVal & ST_STATE_MASK) >> Integer.numberOfTrailingZeros(ST_STATE_MASK));
                            }
                        }
                    } while (! enlistmentState.compareAndSet(oldVal, newVal));
                }

                public void afterCompletion(final int status) {
                    // no operation
                }
            };
        }

        class Handle implements DelayedEnlistmentHandle {
            private final AtomicInteger stateRef = new AtomicInteger();

            public void forgetEnlistment() {
                final AtomicInteger stateRef = this.stateRef;
                int oldVal = stateRef.get();
                switch (oldVal) {
                    case ST_HANDLE_STATE_INITIAL: {
                        if (! Thread.holdsLock(stateRef)) synchronized (stateRef) {
                            forgetEnlistment();
                            return;
                        }
                        // transition main state
                        doForgetEnlistment();
                        stateRef.set(ST_HANDLE_STATE_FORGOTTEN);
                    }
                    case ST_HANDLE_STATE_FORGOTTEN: {
                        // idempotent
                        return;
                    }
                    case ST_HANDLE_STATE_ENLISTED: {
                        throw Log.log.alreadyEnlisted();
                    }
                    default: {
                        throw Assert.impossibleSwitchCase(oldVal);
                    }
                }
            }

            private void doForgetEnlistment() {
                final AtomicInteger enlistmentState = Enlistment.this.enlistmentState;
                int oldVal, newVal;
                do {
                    oldVal = enlistmentState.get();
                    switch (oldVal & ST_STATE_MASK) {
                        case ST_STATE_NOT_ENLISTED: {
                            // two substates: no outstanding (1B), or >0 outstanding (1A)
                            // however, we know 1B is impossible because *we* are an outstanding handle!
                            int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                            assert outstanding > 0;
                            if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                doForgetEnlistment();
                                return;
                            }
                            newVal = ST_STATE_NOT_ENLISTED | outstanding - 1;
                            // holding lock, just set and get out of here
                            enlistmentState.set(newVal);
                            return;
                        }
                        case ST_STATE_ENLISTED: {
                            // two substates: no outstanding (2B), or >0 outstanding (2A)
                            // however, we know 2B is impossible because *we* are an outstanding handle!
                            int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                            assert outstanding > 0;
                            newVal = ST_STATE_ENLISTED | outstanding - 1;
                            // CAS
                            break;
                        }
                        default: {
                            throw Assert.impossibleSwitchCase((oldVal & ST_STATE_MASK) >> Integer.numberOfTrailingZeros(ST_STATE_MASK));
                        }
                    }
                } while (! enlistmentState.compareAndSet(oldVal, newVal));
            }

            public void verifyEnlistment() throws RollbackException, SystemException {
                final AtomicInteger stateRef = this.stateRef;
                int oldVal = stateRef.get();
                switch (oldVal) {
                    case ST_HANDLE_STATE_INITIAL: {
                        if (! Thread.holdsLock(stateRef)) synchronized (stateRef) {
                            verifyEnlistment();
                            return;
                        }
                        // transition main state
                        doVerifyEnlistment();
                        stateRef.set(ST_HANDLE_STATE_ENLISTED);
                    }
                    case ST_HANDLE_STATE_FORGOTTEN: {
                        throw Log.log.alreadyForgotten();
                    }
                    case ST_HANDLE_STATE_ENLISTED: {
                        // idempotent
                        return;
                    }
                    default: {
                        throw Assert.impossibleSwitchCase(oldVal);
                    }
                }
            }

            private void doVerifyEnlistment() throws SystemException, RollbackException {
                final AtomicInteger enlistmentState = Enlistment.this.enlistmentState;
                int oldVal, newVal;
                do {
                    oldVal = enlistmentState.get();
                    switch (oldVal & ST_STATE_MASK) {
                        case ST_STATE_NOT_ENLISTED: {
                            // two substates: no outstanding (1B), or >0 outstanding (1A)
                            // however, we know 1B is impossible because *we* are an outstanding handle!
                            int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                            assert outstanding > 0;
                            if (! Thread.holdsLock(enlistmentState)) synchronized (enlistmentState) {
                                doVerifyEnlistment();
                                return;
                            }
                            if (! transaction.enlistResource(xaResource)) {
                                throw Log.log.delayedEnlistmentFailed(null);
                            }
                            newVal = ST_STATE_ENLISTED | outstanding - 1;
                            // holding lock, just set and get out of here
                            enlistmentState.set(newVal);
                            return;
                        }
                        case ST_STATE_ENLISTED: {
                            // two substates: no outstanding (2B), or >0 outstanding (2A)
                            // however, we know 2B is impossible because *we* are an outstanding handle!
                            int outstanding = oldVal & ST_ACTIVE_CNT_MASK;
                            assert outstanding > 0;
                            newVal = ST_STATE_ENLISTED | outstanding - 1;
                            // CAS
                            break;
                        }
                        default: {
                            throw Assert.impossibleSwitchCase((oldVal & ST_STATE_MASK) >> Integer.numberOfTrailingZeros(ST_STATE_MASK));
                        }
                    }
                } while (! enlistmentState.compareAndSet(oldVal, newVal));
            }
        }
    }
}
