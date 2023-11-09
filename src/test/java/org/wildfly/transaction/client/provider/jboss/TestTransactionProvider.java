/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2022 Red Hat, Inc., and individual contributors
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

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import org.jboss.tm.XAResourceRecoveryRegistry;
import org.wildfly.common.Assert;
import org.wildfly.transaction.client.SimpleXid;

import javax.transaction.xa.Xid;
import java.nio.file.Path;

/**
* {@link org.wildfly.transaction.client.spi.LocalTransactionProvider} is a spi that allows to integrate transaction client
 * with underlying transaction manager implementation (Narayana). In tests we mock both the provider and the implementation.
*/
public class TestTransactionProvider extends JBossLocalTransactionProvider {

    public static boolean newTransactionCreated = false;

    public TestTransactionProvider(final int staleTransactionTime,
                                   final Path xaRecoveryPath) {
        super(new TestExtendedJBossXATerminator(), staleTransactionTime, new TestTransactionManager(), new TestXAResourceRecoveryRegistry(), xaRecoveryPath);
    }

    public TestTransactionProvider(final int staleTransactionTime,
                                   final Path xaRecoveryPath,
                                   final XAResourceRecoveryRegistry xaResourceRecoveryRegistry) {
        super(new TestExtendedJBossXATerminator(), staleTransactionTime, new TestTransactionManager(), xaResourceRecoveryRegistry, xaRecoveryPath);
    }

    @Override
    public Transaction createNewTransaction(int timeout) throws SystemException, SecurityException {
        newTransactionCreated = true;
        return super.createNewTransaction(timeout);
    }

    @Override
    int getTransactionManagerTimeout() {
        return 5000;
    }

    @Override
    public void registerInterposedSynchronization(Transaction transaction, Synchronization sync) throws IllegalArgumentException {

    }

    @Override
    public Object getKey(Transaction transaction) throws IllegalArgumentException {
        throw Assert.unsupported();
    }

    @Override
    public int getTimeout(Transaction transaction) {
        return 5000;
    }

    @Override
    public Xid getXid(Transaction transaction) {
        return ((TestTransaction)transaction).getXid();
    }

    @Override
    public Object getResource(Transaction transaction, Object key) {
        return ((TestTransaction) transaction).getResource(key);
    }

    @Override
    public void putResource(Transaction transaction, Object key, Object value) throws IllegalArgumentException {
        ((TestTransaction) transaction).putResource(key, value);
    }

    @Override
    public Object putResourceIfAbsent(Transaction transaction, Object key, Object value) throws IllegalArgumentException {
        ((TestTransaction) transaction).putResource(key, value);
        return value;
    }

    @Override
    public boolean getRollbackOnly(Transaction transaction) throws IllegalArgumentException {
        return false;
    }

    @Override
    public void commitLocal(Transaction transaction) throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).commitLocal();
    }

    @Override
    public void rollbackLocal(Transaction transaction) throws IllegalStateException, SystemException {
        getEntryFor(transaction, SimpleXid.of(getXid(transaction)).withoutBranch()).rollbackLocal();
    }

    public static void reset(){
        newTransactionCreated = false;
    }
}
