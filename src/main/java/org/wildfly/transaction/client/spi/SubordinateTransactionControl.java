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

package org.wildfly.transaction.client.spi;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * The control interface for subordinate transactions.  This interface is used in the following cases:
 * <ul>
 *     <li>When a subordinate transaction is locally inflowed, an instance of this interface is used to control the
 *     lifecycle of the local transaction (the transaction's lifecycle methods are blocked in this case).</li>
 *     <li>The local handle for a subordinate transaction implements this interface.</li>
 * </ul>
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface SubordinateTransactionControl {

    /**
     * Roll back the subordinate.  In the event of error, the following error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_NOTA}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     *     <li>{@link XAException#XA_HEURHAZ}</li>
     *     <li>{@link XAException#XA_HEURCOM}</li>
     *     <li>{@link XAException#XA_HEURRB}</li>
     *     <li>{@link XAException#XA_HEURMIX}</li>
     * </ul>
     *
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    void rollback() throws XAException;

    /**
     * End work on behalf of a transaction branch, disassociating the subordinate from the transaction branch.  The
     * {@code flags} parameter may equal one of the following:
     * <ul>
     *     <li>{@link XAResource#TMFAIL} indicating that the transaction work has failed, and that the subordinate may
     *     mark the transaction as rollback-only</li>
     *     <li>{@link XAResource#TMSUCCESS} indicating that the work has completed successfully and that a subsequent
     *     {@link #prepare()} is likely</li>
     * </ul>
     * In the event of error, the following error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     *     <li>{@link XAException#XA_RBCOMMFAIL}</li>
     *     <li>{@link XAException#XA_RBDEADLOCK}</li>
     *     <li>{@link XAException#XA_RBINTEGRITY}</li>
     *     <li>{@link XAException#XA_RBOTHER}</li>
     *     <li>{@link XAException#XA_RBPROTO}</li>
     *     <li>{@link XAException#XA_RBROLLBACK}</li>
     *     <li>{@link XAException#XA_RBTIMEOUT}</li>
     *     <li>{@link XAException#XA_RBTRANSIENT}</li>
     * </ul>
     *
     * @param flags one of the valid flag values: {@code TMSUSPEND}, {@code TMFAIL}, or {@code TMSUCCESS}
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    void end(int flags) throws XAException;

    /**
     * Perform before-commit operations, including running all transaction synchronizations.  In the event of an error,
     * the following error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     *     <li>{@link XAException#XA_RBCOMMFAIL}</li>
     *     <li>{@link XAException#XA_RBDEADLOCK}</li>
     *     <li>{@link XAException#XA_RBINTEGRITY}</li>
     *     <li>{@link XAException#XA_RBOTHER}</li>
     *     <li>{@link XAException#XA_RBPROTO}</li>
     *     <li>{@link XAException#XA_RBROLLBACK}</li>
     *     <li>{@link XAException#XA_RBTIMEOUT}</li>
     *     <li>{@link XAException#XA_RBTRANSIENT}</li>
     * </ul>
     *
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    void beforeCompletion() throws XAException;

    /**
     * Prepare the transaction.  If before-commit processing was not yet run, it is run.  In the event of an error,
     * the following error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     *     <li>{@link XAException#XA_RBCOMMFAIL}</li>
     *     <li>{@link XAException#XA_RBDEADLOCK}</li>
     *     <li>{@link XAException#XA_RBINTEGRITY}</li>
     *     <li>{@link XAException#XA_RBOTHER}</li>
     *     <li>{@link XAException#XA_RBPROTO}</li>
     *     <li>{@link XAException#XA_RBROLLBACK}</li>
     *     <li>{@link XAException#XA_RBTIMEOUT}</li>
     *     <li>{@link XAException#XA_RBTRANSIENT}</li>
     * </ul>
     *
     * @return {@link XAResource#XA_OK} or {@link XAResource#XA_RDONLY}
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    int prepare() throws XAException;

    /**
     * Forget the (previously prepared) transaction.  In the event of error, the following error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     * </ul>
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    void forget() throws XAException;

    /**
     * Commit a transaction, either in a single phase or after a previous prepare.  In the event of error, the following
     * error codes are possible:
     * <ul>
     *     <li>{@link XAException#XAER_RMERR}</li>
     *     <li>{@link XAException#XAER_RMFAIL}</li>
     *     <li>{@link XAException#XAER_INVAL}</li>
     *     <li>{@link XAException#XAER_PROTO}</li>
     *     <li>{@link XAException#XA_HEURHAZ}</li>
     *     <li>{@link XAException#XA_HEURCOM}</li>
     *     <li>{@link XAException#XA_HEURRB}</li>
     *     <li>{@link XAException#XA_HEURMIX}</li>
     * </ul>
     * If the {@code onePhase} flag is {@code true}, one of the following error codes is also possible:
     * <ul>
     *     <li>{@link XAException#XA_RBCOMMFAIL}</li>
     *     <li>{@link XAException#XA_RBDEADLOCK}</li>
     *     <li>{@link XAException#XA_RBINTEGRITY}</li>
     *     <li>{@link XAException#XA_RBOTHER}</li>
     *     <li>{@link XAException#XA_RBPROTO}</li>
     *     <li>{@link XAException#XA_RBROLLBACK}</li>
     *     <li>{@link XAException#XA_RBTIMEOUT}</li>
     *     <li>{@link XAException#XA_RBTRANSIENT}</li>
     * </ul>
     * @param onePhase {@code true} to commit in a single phase, {@code false} to commit after prepare
     * @throws XAException (with one of the above error codes) if an error occurs
     */
    void commit(boolean onePhase) throws XAException;

    /**
     * Get the estimated remaining timeout of the current branch.
     *
     * @return the estimated remaining timeout of the current branch
     */
    int getTransactionTimeout() throws XAException;
}
