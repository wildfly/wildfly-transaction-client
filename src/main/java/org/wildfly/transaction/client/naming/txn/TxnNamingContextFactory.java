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

import java.util.Collection;

import javax.naming.Context;
import javax.naming.NamingException;

import org.kohsuke.MetaInfServices;
import org.wildfly.naming.client.NamingContextFactory;
import org.wildfly.naming.client.NamingProvider;
import org.wildfly.naming.client.util.FastHashtable;

/**
 * The naming context factory for transaction service objects.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@MetaInfServices
public final class TxnNamingContextFactory implements NamingContextFactory {
    public boolean supportsUriScheme(final Collection<NamingProvider> namingProviders, final String nameScheme) {
        return nameScheme != null && nameScheme.equals("txn");
    }

    public Context createRootContext(final Collection<NamingProvider> namingProviders, final String nameScheme, final FastHashtable<String, Object> env) throws NamingException {
        return new TxnNamingContext(namingProviders == null ? null : namingProviders.toArray(new NamingProvider[namingProviders.size()]));
    }
}
