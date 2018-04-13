/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018 Red Hat, Inc., and individual contributors
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

package org.wildfly.transaction.client.provider;

import java.util.ArrayList;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client.SynchronizationException;
import org.wildfly.transaction.client._private.Log;

/**
 * A synchronization list that implements all required behaviors of transaction synchronization completion.  This includes
 * supporting addition of synchronizations while the list is already running.
 */
public final class TransactionSynchronizationList implements Synchronization {
    private static final int ST_INITIAL = 0;
    private static final int ST_BC_RUNNING = 1;
    private static final int ST_BC_DONE = 2;
    private static final int ST_AC_RUNNING = 3;
    private static final int ST_AC_DONE = 4;

    private final ArrayList<Synchronization> synchList = new ArrayList<>();
    private int state = ST_INITIAL;

    /**
     * Construct a new, empty instance.
     */
    public TransactionSynchronizationList() {
    }

    /**
     * Add a synchronization to the list.  If {@code beforeCompletion} processing has already completed, only the
     * {@code afterCompletion} portion of the given synchronization will be run.  If {@code afterCompletion} processing
     * has already completed, an exception is thrown.
     *
     * @param synchronization the synchronization to add (must not be {@code null})
     * @throws IllegalArgumentException if the given synchronization is {@code null}
     * @throws IllegalStateException if the {@code afterCompletion} stage has already run
     */
    public void registerSynchronization(Synchronization synchronization) throws IllegalArgumentException, IllegalStateException {
        Assert.checkNotNullParam("synchronization", synchronization);
        final ArrayList<Synchronization> synchList = this.synchList;
        synchronized (synchList) {
            if (state >= ST_BC_DONE) throw Log.log.cannotRegisterSynchronization();
            synchList.add(synchronization);
        }
    }

    /**
     * Run the {@code beforeCompletion} stage of all the included synchronizations.
     *
     * @throws SynchronizationException if {@code beforeCompletion} processing fails
     * @throws IllegalStateException if {@code beforeCompletion} processing was already initiated
     */
    public void beforeCompletion() throws SynchronizationException, IllegalStateException {
        beforeCompletion(null);
    }

    /**
     * Run the {@code beforeCompletion} stage of all the included synchronizations during error recovery.
     *
     * @param thrown the thrown exception to add problems to, or {@code null} to throw a new exception
     * @throws SynchronizationException if {@code beforeCompletion} processing fails and a {@code thrown} is not set
     * @throws IllegalStateException if {@code beforeCompletion} processing was already initiated
     */
    public void beforeCompletion(Throwable thrown) throws SynchronizationException, IllegalStateException {
        final ArrayList<Synchronization> synchList = this.synchList;
        Synchronization sync;
        synchronized (synchList) {
            if (state >= ST_BC_RUNNING) throw Log.log.beforeCompletionAlreadyRun();
            if (synchList.size() == 0) {
                state = ST_BC_DONE;
                return;
            }
            state = ST_BC_RUNNING;
            sync = synchList.get(0);
        }
        SynchronizationException toThrow = null;
        try {
            sync.beforeCompletion();
        } catch (Throwable t) {
            if (thrown == null) {
                toThrow = Log.log.beforeCompletionFailed(t, null);
            } else {
                thrown.addSuppressed(t);
            }
        }
        int index = 1;
        for (;;) {
            synchronized (synchList) {
                if (synchList.size() >= index) {
                    // done
                    state = ST_BC_DONE;
                    if (toThrow != null) throw toThrow;
                    return;
                }
                sync = synchList.get(index++);
            }
            try {
                sync.beforeCompletion();
            } catch (Throwable t) {
                if (thrown == null) {
                    if (toThrow == null) {
                        toThrow = t instanceof SynchronizationException ?  (SynchronizationException) t : Log.log.beforeCompletionFailed(t, null);
                    } else {
                        toThrow.addSuppressed(t);
                    }
                } else {
                    thrown.addSuppressed(t);
                }
            }
        }
    }

    /**
     * Run the {@code afterCompletion} stage of all the included synchronizations.
     *
     * @param status the final transaction status value from {@link Status}
     * @throws IllegalStateException if {@code afterCompletion} processing was already initiated
     */
    public void afterCompletion(final int status) throws IllegalStateException {
        final ArrayList<Synchronization> synchList = this.synchList;
        Synchronization sync;
        synchronized (synchList) {
            if (state >= ST_AC_RUNNING) throw Log.log.afterCompletionAlreadyRun();
            if (synchList.isEmpty()) {
                state = ST_AC_DONE;
                return;
            }
            state = ST_AC_RUNNING;
            sync = synchList.remove(synchList.size() - 1);
        }
        try {
            sync.afterCompletion(status);
        } catch (Throwable t) {
            Log.log.afterCompletionException(t);
        }
        for (;;) {
            synchronized (synchList) {
                if (synchList.isEmpty()) {
                    // done
                    state = ST_AC_DONE;
                    return;
                }
                sync = synchList.remove(synchList.size() - 1);
            }
            try {
                sync.afterCompletion(status);
            } catch (Throwable t) {
                Log.log.afterCompletionException(t);
            }
        }
    }

}
