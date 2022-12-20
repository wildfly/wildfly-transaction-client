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
package org.wildfly.transaction.client;

import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.transaction.client.provider.jboss.TestImportedTransaction;
import org.wildfly.transaction.client.provider.jboss.TestTransaction;
import org.wildfly.transaction.client.provider.jboss.TestTransactionManager;
import org.wildfly.transaction.client.provider.jboss.TestTransactionProvider;
import org.wildfly.transaction.client.provider.jboss.TestXAResource;
import org.wildfly.transaction.client.provider.jboss.TestXid;

import javax.transaction.xa.XAResource;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

public class LocalUserTransactionTestCase {

    @BeforeClass
    public static void init() {
        final LocalTransactionContext transactionContext = new LocalTransactionContext(new TestTransactionProvider(500, Path.of("./target")));
        LocalTransactionContext.getContextManager().setGlobalDefault(transactionContext);
    }

    @Before
    public void reset() {
        TestTransactionProvider.reset();
        TestTransactionManager.reset();
    }

    @Test
    public void simpleCommitTest() throws Exception {
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        tm.begin();

        Assert.assertTrue(TestTransactionProvider.newTransactionCreated);
        Assert.assertNotNull(tm.stateRef.get().transaction);

        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof LocalTransaction);

        tm.commit();
        Assert.assertNull(tm.stateRef.get().transaction);
        Assert.assertTrue(TestTransactionManager.committed);
        Assert.assertFalse(TestTransactionManager.rolledback);
    }

    @Test
    public void simpleRollbackTest() throws Exception {
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        tm.begin();

        Assert.assertTrue(TestTransactionProvider.newTransactionCreated);
        Assert.assertNotNull(tm.stateRef.get().transaction);

        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof LocalTransaction);

        tm.rollback();
        Assert.assertNull(tm.stateRef.get().transaction);
        Assert.assertFalse(TestTransactionManager.committed);
        Assert.assertTrue(TestTransactionManager.rolledback);
    }

    @Test
    public void importedTransactionTest() throws Exception {
        LocalTransactionContext ltc = LocalTransactionContext.getCurrent();
        LocalTransaction lt = ltc.findOrImportTransaction(new TestXid(), 5000, false).getTransaction();

        boolean exception = false;
        try {
            lt.commit();
        } catch (SystemException expected) {
            exception = true;
        }
        Assert.assertTrue(exception);


        TestImportedTransaction it = TestImportedTransaction.latest;
        Assert.assertEquals(it.getState(), TestTransaction.State.ACTIVE);

        it.doPrepare();
        Assert.assertEquals(it.getState(), TestTransaction.State.PREPARED);

        it.doCommit();
        Assert.assertEquals(it.getState(), TestTransaction.State.COMMITTED);


    }

    @Test
    public void xaTest() throws Exception {
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        tm.begin();
        Assert.assertTrue(TestTransactionProvider.newTransactionCreated);
        Assert.assertNotNull(tm.stateRef.get().transaction);

        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof LocalTransaction);
        LocalTransaction lt = (LocalTransaction) t;

        TestXAResource xaResource1 = new TestXAResource();
        lt.enlistResource(xaResource1);
        TestXAResource xaResource2 = new TestXAResource();
        lt.enlistResource(xaResource2);

        tm.commit();
        Assert.assertNull(tm.stateRef.get().transaction);
        Assert.assertTrue(TestTransactionManager.committed);
        Assert.assertFalse(TestTransactionManager.rolledback);

        TestTransaction testTransaction = TestTransaction.latest;

        List<XAResource> enlistedResources = testTransaction.getEnlistedResources();

        Assert.assertEquals(enlistedResources.size(), 2);
        Assert.assertTrue(enlistedResources.contains(xaResource1));
        Assert.assertTrue(enlistedResources.contains(xaResource2));
    }

    @Test
    public void outflowedTransactionTest() throws Exception {
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        tm.begin();
        Assert.assertTrue(TestTransactionProvider.newTransactionCreated);

        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof LocalTransaction);
        LocalTransaction lt = (LocalTransaction) t;

        URI location = new URI("remote://localhost:12345");

        RemoteTransactionContext.getInstance().outflowTransaction(location, lt);

        tm.commit();

        TestTransaction testTransaction = TestTransaction.latest;

        List<XAResource> enlistedResources = testTransaction.getEnlistedResources();

        Assert.assertEquals(enlistedResources.size(), 1);
        XAResource resource = enlistedResources.get(0);
        Assert.assertTrue(resource instanceof SubordinateXAResource);

        Assert.assertNull(tm.stateRef.get().transaction);
        Assert.assertTrue(TestTransactionManager.committed);
        Assert.assertFalse(TestTransactionManager.rolledback);
    }

}
