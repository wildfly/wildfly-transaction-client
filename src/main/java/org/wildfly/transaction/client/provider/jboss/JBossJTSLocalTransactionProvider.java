/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
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

package org.wildfly.transaction.client.provider.jboss;

import static java.security.AccessController.doPrivileged;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.Xid;

import com.arjuna.ats.internal.jta.resources.jts.orbspecific.JTAInterposedSynchronizationImple;
import com.arjuna.ats.internal.jta.resources.jts.orbspecific.SynchronizationImple;
import com.arjuna.ats.internal.jta.transaction.jts.TransactionImple;
import com.arjuna.ats.internal.jta.transaction.jts.TransactionManagerImple;
import org.jboss.tm.ExtendedJBossXATerminator;
import org.jboss.tm.TransactionTimeoutConfiguration;
import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client._private.Log;

final class JBossJTSLocalTransactionProvider extends JBossLocalTransactionProvider {

    private final Object resourceLock = new Object();

    JBossJTSLocalTransactionProvider(final int staleTransactionTime, final ExtendedJBossXATerminator ext, final TransactionManager tm) {
        super(ext, staleTransactionTime, tm);
    }

    int getTransactionManagerTimeout() throws SystemException {
        final TransactionManager tm = getTransactionManager();
        if (tm instanceof TransactionTimeoutConfiguration) {
            return ((TransactionTimeoutConfiguration) tm).getTransactionTimeout();
        } else if (tm instanceof TransactionManagerImple) {
            return ((TransactionManagerImple) tm).getTimeout();
        } else {
            return 0;
        }
    }

    private static final MethodHandle registerSynchronizationImple;

    static {
        registerSynchronizationImple = doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            try {
                final Method declaredMethod = TransactionImple.class.getDeclaredMethod("registerSynchronizationImple", SynchronizationImple.class);
                declaredMethod.setAccessible(true);
                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                return lookup.unreflect(declaredMethod);
            } catch (Throwable t) {
                throw Log.log.unexpectedFailure(t);
            }
        });
    }

    public void registerInterposedSynchronization(@NotNull final Transaction transaction, @NotNull final Synchronization sync) throws IllegalArgumentException {
        // this is silly but for some reason they've locked this API up tight
        try {
            registerSynchronizationImple.invoke((TransactionImple) transaction, new JTAInterposedSynchronizationImple (sync));
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw Log.log.unexpectedFailure(t);
        }
    }

    public Object getResource(@NotNull final Transaction transaction, @NotNull final Object key) {
        return ((TransactionImple) transaction).getTxLocalResource(key);
    }

    public void putResource(@NotNull final Transaction transaction, @NotNull final Object key, final Object value) throws IllegalArgumentException {
        ((TransactionImple) transaction).putTxLocalResource(key, value);
    }

    public Object putResourceIfAbsent(@NotNull final Transaction transaction, @NotNull final Object key, final Object value) throws IllegalArgumentException {
        synchronized (resourceLock) {
            Object existing = getResource(transaction, key);
            if (existing != null) {
                return existing;
            }
            putResource(transaction, key, value);
            return null;
        }
    }

    public boolean getRollbackOnly(@NotNull final Transaction transaction) throws IllegalArgumentException {
        try {
            return transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK;
        } catch (SystemException e) {
            throw Log.log.unexpectedFailure(e);
        }
    }

    @NotNull
    public Object getKey(@NotNull final Transaction transaction) throws IllegalArgumentException {
        return ((TransactionImple) transaction).get_uid();
    }

    public void commitLocal(@NotNull final Transaction transaction) throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).commitLocal();
    }

    public void rollbackLocal(@NotNull final Transaction transaction) throws IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).rollbackLocal();
    }

    public int getTimeout(@NotNull final Transaction transaction) {
        return ((TransactionImple) transaction).getTimeout();
    }

    @NotNull
    public Xid getXid(@NotNull final Transaction transaction) {
        if (transaction instanceof TransactionImple) {
            return ((TransactionImple) transaction).getTxId();
        } else {
            throw Log.log.unknownTransactionType(TransactionImple.class, transaction.getClass());
        }
    }

    public String toString() {
        return "JBoss JTS transaction provider";
    }
}
