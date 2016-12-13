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

import static java.security.AccessController.doPrivileged;

import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;
import javax.transaction.xa.Xid;

import org.wildfly.common.Assert;
import org.wildfly.common.context.ContextManager;
import org.wildfly.common.context.Contextual;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.RemoteTransactionProvider;

/**
 * The remote transaction context.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class RemoteTransactionContext implements Contextual<RemoteTransactionContext> {

    private static final RemoteTransactionProvider[] NO_PROVIDERS = new RemoteTransactionProvider[0];

    private final ConcurrentMap<URI, LocatedUserTransaction> userTransactions = new ConcurrentHashMap<>();
    private final List<RemoteTransactionProvider> providers;
    private final ConcurrentMap<SimpleXid, OutflowedTransaction> outflowedTransactions = new ConcurrentHashMap<>();

    /**
     * Construct a new instance.  The given class loader is scanned for transaction providers.
     *
     * @param classLoader the class loader to scan for transaction providers ({@code null} indicates the application or bootstrap class loader)
     */
    public RemoteTransactionContext(final ClassLoader classLoader) {
        this(doPrivileged((PrivilegedAction<List<RemoteTransactionProvider>>) () -> {
            final ServiceLoader<RemoteTransactionProvider> loader = ServiceLoader.load(RemoteTransactionProvider.class, classLoader);
            final ArrayList<RemoteTransactionProvider> providers = new ArrayList<RemoteTransactionProvider>();
            final Iterator<RemoteTransactionProvider> iterator = loader.iterator();
            for (;;) try {
                if (! iterator.hasNext()) break;
                providers.add(iterator.next());
            } catch (ServiceConfigurationError e) {
                Log.log.serviceConfigurationFailed(e);
            }
            providers.trimToSize();
            return providers;
        }), false);
    }

    /**
     * Construct a new instance.  The given non-empty list of providers is used.
     *
     * @param providers the list of providers to use (must not be {@code null} or empty)
     */
    public RemoteTransactionContext(final List<RemoteTransactionProvider> providers) {
        this(providers, true);
    }

    RemoteTransactionContext(List<RemoteTransactionProvider> providers, boolean clone) {
        Assert.checkNotNullParam("providers", providers);
        if (clone) {
            providers = Arrays.asList(providers.toArray(NO_PROVIDERS));
        }
        Assert.checkNotEmptyParam("providers", providers);
        final int size = providers.size();
        for (int i = 0; i < size; i++) {
            Assert.checkNotNullArrayParam("providers", i, providers.get(i));
        }
        this.providers = providers;
    }

    private static final ContextManager<RemoteTransactionContext> CONTEXT_MANAGER = new ContextManager<RemoteTransactionContext>(RemoteTransactionContext.class, "wildfly-transaction-client.context");
    private static final Supplier<RemoteTransactionContext> PRIVILEGED_SUPPLIER = doPrivileged((PrivilegedAction<Supplier<RemoteTransactionContext>>) CONTEXT_MANAGER::getPrivilegedSupplier);

    /**
     * Get the remote transaction context manager.
     *
     * @return the context manager
     */
    public static ContextManager<RemoteTransactionContext> getContextManager() {
        return CONTEXT_MANAGER;
    }

    /** {@inheritDoc} */
    public ContextManager<RemoteTransactionContext> getInstanceContextManager() {
        return getContextManager();
    }

    /**
     * Get the active remote transaction context instance.
     *
     * @return the remote transaction context instance (not {@code null})
     */
    public static RemoteTransactionContext getInstance() {
        return getContextManager().get();
    }

    static RemoteTransactionContext getInstancePrivate() {
        return PRIVILEGED_SUPPLIER.get();
    }

    public <T> T getProviderInterface(URI location, Class<T> clazz) {
        Assert.checkNotNullParam("location", location);
        Assert.checkNotNullParam("clazz", clazz);
        return getProvider(location, p -> p.getProviderInterface(clazz));
    }

    /**
     * Get a {@code UserTransaction} that controls a remote transactions state at the given {@code location}.  The transaction
     * context may cache these instances by location.
     *
     * @param location the location (must not be {@code null})
     * @return the {@code UserTransaction} (not {@code null})
     */
    public UserTransaction getUserTransaction(final URI location) {
        Assert.checkNotNullParam("location", location);
        return userTransactions.computeIfAbsent(location, LocatedUserTransaction::new);
    }

    /**
     * Outflow the current {@code TransactionManager} {@link Transaction} to the given location.  The returned handle
     * must be used to confirm or forget the enlistment attempt either immediately or at some point in the future;
     * failure to do so may cause the transaction to be rolled back with an error.
     *
     * @param location the location to outflow to (must not be {@code null})
     * @param transaction the transaction (must not be {@code null})
     * @param xid the transaction XID (must not be {@code null}, the branch qualifier is ignored)
     * @return the enlistment handle (not {@code null})
     * @throws SystemException if the transaction manager fails for some reason
     * @throws RollbackException if the transaction has been rolled back in the meantime
     * @throws IllegalStateException if no transaction is active
     * @throws UnsupportedOperationException if the provider for the location does not support outflow
     */
    public DelayedEnlistmentHandle outflowTransaction(final URI location, final Transaction transaction, Xid xid) throws SystemException, IllegalStateException, UnsupportedOperationException, RollbackException {
        Assert.checkNotNullParam("location", location);
        Assert.checkNotNullParam("transaction", transaction);
        Assert.checkNotNullParam("xid", xid);
        final SimpleXid globalXid = SimpleXid.of(xid).withoutBranch();
        final ConcurrentMap<SimpleXid, OutflowedTransaction> outflowedTransactions = this.outflowedTransactions;
        OutflowedTransaction outflowedTransaction = outflowedTransactions.get(globalXid);
        if (outflowedTransaction == null) {
            OutflowedTransaction appearing = outflowedTransactions.putIfAbsent(globalXid, outflowedTransaction = new OutflowedTransaction(globalXid, transaction));
            if (appearing == null) {
                final OutflowedTransaction value = outflowedTransaction;
                // register this to clean up after the transaction is done
                transaction.registerSynchronization(new Synchronization() {
                    public void beforeCompletion() {
                        // no operation
                    }

                    public void afterCompletion(final int status) {
                        outflowedTransactions.remove(globalXid, value);
                    }
                });
            } else {
                outflowedTransaction = appearing;
            }
        }
        if (outflowedTransaction.getTransaction() != transaction) {
            throw Log.log.outflowAcrossTransactionManagers();
        }
        final OutflowedTransaction.Enlistment enlistment = outflowedTransaction.getEnlistment(location);
        return enlistment.createHandle();
    }

    <R> R getProvider(final URI location, Function<RemoteTransactionProvider, R> function) {
        for (RemoteTransactionProvider provider : providers) {
            if (provider.supportsScheme(location.getScheme())) {
                return function.apply(provider);
            }
        }
        return null;
    }
}
