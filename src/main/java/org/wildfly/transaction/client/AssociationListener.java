/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
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

import org.wildfly.transaction.client.AbstractTransaction;

/**
 * A transaction-to-thread association listener, which may be called when the association with a thread is changed.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@FunctionalInterface
public interface AssociationListener {
    /**
     * The association of the transaction to the thread has changed.
     *
     * @param transaction the transaction that was associated or disassociated (not {@code null})
     * @param associated {@code true} if the transaction is now associated with the thread, {@code false} if the transaction
     * is no longer associated with the thread
     */
    void associationChanged(AbstractTransaction transaction, boolean associated);
}
