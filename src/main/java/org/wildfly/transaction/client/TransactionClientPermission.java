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

import java.security.BasicPermission;

/**
 * A simple permission type for transaction client permission checks.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class TransactionClientPermission extends BasicPermission {

    private static final long serialVersionUID = 8308174511411571515L;

    public static final TransactionClientPermission GET_TRANSACTION_SYSTEM = new TransactionClientPermission("getTransactionSystem");

    /**
     * Construct a new instance.
     *
     * @param name the permission name (must not be {@code null})
     */
    public TransactionClientPermission(final String name) {
        super(name);
    }

    /**
     * Construct a new instance.
     *
     * @param name the permission name (must not be {@code null})
     * @param actions ignored
     */
    public TransactionClientPermission(final String name, final String actions) {
        super(name, actions);
    }
}
