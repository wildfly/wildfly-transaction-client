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

import javax.transaction.Transaction;

import org.wildfly.common.Assert;
import org.wildfly.transaction.client.ContextTransactionManager;

/**
 */
public final class ProviderUtil {
    private ProviderUtil() {}

    /**
     * Roll back a transaction, appending any rollback exception to the given {@code Throwable} as a suppressed
     * throwable.
     *
     * @param transaction the transaction (must not be {@code null})
     * @param throwable the throwable (must not be {@code null})
     */
    public static void safeRollback(Transaction transaction, Throwable throwable) {
        Assert.checkNotNullParam("transaction", transaction);
        Assert.checkNotNullParam("throwable", throwable);
        try {
            transaction.rollback();
        } catch (Throwable t) {
            throwable.addSuppressed(t);
        }
    }

    /**
     * Suspend the current transaction (if any), appending any thrown exception to the given {@code Throwable} as a
     * suppressed throwable.
     *
     * @param throwable the transaction (must not be {@code null})
     */
    public static void safeSuspend(Throwable throwable) {
        Assert.checkNotNullParam("throwable", throwable);
        try {
            ContextTransactionManager.getInstance().suspend();
        } catch (Throwable t) {
            throwable.addSuppressed(t);
        }
    }
}
