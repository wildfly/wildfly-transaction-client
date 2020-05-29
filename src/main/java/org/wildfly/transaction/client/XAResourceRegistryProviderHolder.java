/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2020 Red Hat, Inc., and individual contributors
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
 * A single point to take a currently configured provider that is capable to list all the in-doubt
 * {@link javax.transaction.xa.XAResource}s from the all records maintained as {@link XAResourceRegistry} instances.
 */
public final class XAResourceRegistryProviderHolder {
    private static XAResourceRegistryProvider INSTANCE;

    /**
     * Returning instance of the currently configured {@link XAResourceRegistryProvider}.
     *
     * @return xaresource registry provider instance which was {@link #register(XAResourceRegistryProvider)}ed
     *         beforehand manually in the code
     */
    public static XAResourceRegistryProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Registering the {@link XAResourceRegistryProvider}. This give a way how
     * to approach the in-doubt {@link javax.transaction.xa.XAResource}s when the {@link SubordinateXAResource}
     * is not linked with the {@link XAResourceRegistry}.
     *
     * @param xaResourceRegistryProvider
     */
    public static void register(XAResourceRegistryProvider xaResourceRegistryProvider) {
        INSTANCE = xaResourceRegistryProvider;
    }
}