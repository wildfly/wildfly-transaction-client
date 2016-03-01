/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015 Red Hat, Inc., and individual contributors
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

package org.wildfly.transaction.client.naming.txn;

import javax.naming.Binding;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class ReadOnlyBinding extends Binding {

    private static final long serialVersionUID = 613145972402574684L;

    public ReadOnlyBinding(final String name, final Object obj) {
        super(name, obj);
    }

    public ReadOnlyBinding(final String name, final Object obj, final boolean isRelative) {
        super(name, obj, isRelative);
    }

    public ReadOnlyBinding(final String name, final String className, final Object obj) {
        super(name, className, obj);
    }

    public ReadOnlyBinding(final String name, final String className, final Object obj, final boolean isRelative) {
        super(name, className, obj, isRelative);
    }

    public void setObject(final Object obj) {
        // ignored
    }

    public void setName(final String name) {
        // ignored
    }

    public void setClassName(final String name) {
        // ignored
    }

    public void setNameInNamespace(final String fullName) {
        // ignored
    }
}
