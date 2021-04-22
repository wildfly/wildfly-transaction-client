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

import javax.transaction.xa.XAResource;

/**
 * <p>
 * Interface which provides ability to list in-doubt {@link XAResource}s at the time.
 * These XAResources are taken cross all instances of the {@link XAResourceRegistry}.
 * </p>
 * <p>
 * The provider is used to compare currently existing XAResource registry records
 * to running commit commands if the XAResource (or rather {@link SubordinateXAResource} is not connected to
 * an existing registry instance.
 * </p>
 */
public interface XAResourceRegistryProvider {

    /**
     * <p>
     * List all in-doubt XAResources over all instances of the {@link XAResourceRegistry} known to provider.
     * </p>
     * <p>
     * Let's say we have a provider which is based on the file system. Then there are several files where each file
     * represents one {@link XAResourceRegistry} record. This method returns all data from all the records.
     * </p>
     *
     * @return list of in-doubt {@link XAResource}s previously saved via handling of {@link XAResourceRegistry}.
     */
    XAResource[] getInDoubtXAResources();
}
