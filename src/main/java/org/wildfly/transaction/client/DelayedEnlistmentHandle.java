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

import javax.transaction.RollbackException;
import javax.transaction.SystemException;

/**
 * A handle for a delayed enlistment of a transactional resource.  If the handle is not forgotten
 * by the time transaction processing completes, then the resource is enlisted at that time if it has not
 * already been.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public interface DelayedEnlistmentHandle {
    /**
     * Signal that this enlistment should be forgotten as it was not used.
     */
    void forgetEnlistment();

    /**
     * Signal that this enlistment should be activated effective immediately.
     *
     * @throws RollbackException if the transaction was rolled back before the enlistment could be activated
     * @throws SystemException if the enlistment failed for an unexpected reason
     */
    void verifyEnlistment() throws RollbackException, SystemException;
}
