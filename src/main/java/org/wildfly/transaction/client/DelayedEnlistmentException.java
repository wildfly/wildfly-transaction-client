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

/**
 * An exception thrown during transaction completion when a delayed enlistment has failed.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public class DelayedEnlistmentException extends RuntimeException {
    private static final long serialVersionUID = - 2258479941930247259L;

    /**
     * Constructs a new {@code DelayedEnlistmentException} instance.  The message is left blank ({@code null}), and no
     * cause is specified.
     */
    public DelayedEnlistmentException() {
    }

    /**
     * Constructs a new {@code DelayedEnlistmentException} instance with an initial message.  No
     * cause is specified.
     *
     * @param msg the message
     */
    public DelayedEnlistmentException(final String msg) {
        super(msg);
    }

    /**
     * Constructs a new {@code DelayedEnlistmentException} instance with an initial cause.  If
     * a non-{@code null} cause is specified, its message is used to initialize the message of this
     * {@code DelayedEnlistmentException}; otherwise the message is left blank ({@code null}).
     *
     * @param cause the cause
     */
    public DelayedEnlistmentException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code DelayedEnlistmentException} instance with an initial message and cause.
     *
     * @param msg the message
     * @param cause the cause
     */
    public DelayedEnlistmentException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
