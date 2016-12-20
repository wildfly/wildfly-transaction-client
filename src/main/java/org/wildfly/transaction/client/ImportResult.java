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

import org.wildfly.common.Assert;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

/**
 * Class representing the result of a transaction import.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ImportResult {
    private final LocalTransaction transaction;
    private final SubordinateTransactionControl control;
    private final boolean isNew;

    /**
     * Construct a new instance.
     *
     * @param transaction the new transaction (must not be {@code null})
     * @param control the controller for the subordinate transaction (must not be {@code null})
     * @param isNew {@code true} if the transaction was just now imported, {@code false} if the transaction already existed
     */
    public ImportResult(final LocalTransaction transaction, final SubordinateTransactionControl control, final boolean isNew) {
        this.transaction = Assert.checkNotNullParam("transaction", transaction);
        this.control = Assert.checkNotNullParam("control", control);
        this.isNew = isNew;
    }

    /**
     * Get the transaction.
     *
     * @return the transaction (not {@code null})
     */
    public LocalTransaction getTransaction() {
        return transaction;
    }

    /**
     * Get the subordinate controller.
     *
     * @return the subordinate controller (not {@code null})
     */
    public SubordinateTransactionControl getControl() {
        return control;
    }

    /**
     * Determine whether this import resulted in a new transaction.
     *
     * @return {@code true} if the transaction was new, {@code false} otherwise
     */
    public boolean isNew() {
        return isNew;
    }
}
