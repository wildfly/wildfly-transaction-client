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

import java.net.URI;
import java.util.Collections;

import javax.naming.Binding;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NamingException;
import javax.transaction.UserTransaction;

import org.wildfly.naming.client.AbstractContext;
import org.wildfly.naming.client.CloseableNamingEnumeration;
import org.wildfly.transaction.client.RemoteTransactionContext;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
class TxnNamingContext extends AbstractContext {

    private final URI location;

    TxnNamingContext(final URI location) {
        this.location = location;
    }

    private UserTransaction getUserTransaction() {
        return RemoteTransactionContext.getInstance().getUserTransaction(location);
    }

    protected Object lookupNative(final Name name) throws NamingException {
        final String str = name.get(0);
        switch (str) {
            case "UserTransaction": {
                return getUserTransaction();
            }
            default: {
                throw nameNotFound(name);
            }
        }
    }

    protected Object lookupLinkNative(final Name name) throws NamingException {
        return lookupNative(name);
    }

    protected CloseableNamingEnumeration<NameClassPair> listNative(final Name name) throws NamingException {
        if (!name.isEmpty()) {
            throw nameNotFound(name);
        }
        return CloseableNamingEnumeration.fromIterable(Collections.singleton(getUserTransactionNameClassPair()));
    }

    protected CloseableNamingEnumeration<Binding> listBindingsNative(final Name name) throws NamingException {
        if (!name.isEmpty()) {
            throw nameNotFound(name);
        }
        return CloseableNamingEnumeration.fromIterable(Collections.singleton(getUserTransactionBinding()));
    }

    private Binding getUserTransactionBinding() {
        return new Binding("UserTransaction", getUserTransaction());
    }

    private NameClassPair getUserTransactionNameClassPair() {
        return new NameClassPair("UserTransaction", UserTransaction.class.getName());
    }

    @Override
    public void close() throws NamingException {
    }

    @Override
    public String getNameInNamespace() throws NamingException {
        return "";
    }
}
