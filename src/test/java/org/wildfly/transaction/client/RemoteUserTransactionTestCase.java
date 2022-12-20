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

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import jakarta.transaction.Transaction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wildfly.transaction.client.provider.jboss.TestTransactionProvider;
import org.wildfly.transaction.client.provider.remoting.RemotingTransactionServer;

import java.net.URI;
import java.nio.file.Path;

@RunWith(BMUnitRunner.class)
@BMScript(dir="target/test-classes")
public class RemoteUserTransactionTestCase {

    public static volatile RemotingTransactionServer remotingTransactionServer;

    public static volatile int transactionId;

    private DummyServer dummyServer = null;

    @BeforeClass
    public static void init() {
        final RemoteTransactionContext context = new RemoteTransactionContext(RemoteUserTransaction.class.getClassLoader());
        RemoteTransactionContext.getContextManager().setGlobalDefault(context);

        final LocalTransactionContext transactionContext = new LocalTransactionContext(new TestTransactionProvider(5000, Path.of("./target")));
        LocalTransactionContext.getContextManager().setGlobalDefault(transactionContext);
    }

    @Before
    public void prepare() throws Exception {
        remotingTransactionServer = null;
        transactionId = 0;

        dummyServer = new DummyServer("localhost",12345,"TX");
        dummyServer.start();
    }

    @After
    public void clean() throws Exception {
        dummyServer.stop();
    }

    @Test
    public void testCommit() throws Exception {

        RemoteUserTransaction rut = RemoteTransactionContext.getInstance().getUserTransaction();
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        rut.begin();
        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof RemoteTransaction);
        RemoteTransaction rt = (RemoteTransaction) t;

        Assert.assertEquals(rt.getStatus(), Status.STATUS_ACTIVE);

        //transaction have to be resolved
        rt.setLocation(new URI("remote://localhost:12345"));

        //wait for Byteman to intercept data
        try {
            Thread.sleep(100);
        } catch(Exception ignored){}

        mockEJBInvocation();

        rut.commit();

        Assert.assertNull(tm.stateRef.get().transaction);

        Assert.assertTrue(rt.getStatus() == Status.STATUS_COMMITTED);
    }

    @Test
    public void testRollback() throws Exception {

        RemoteUserTransaction rut = RemoteTransactionContext.getInstance().getUserTransaction();
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        rut.begin();
        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof RemoteTransaction);
        RemoteTransaction rt = (RemoteTransaction) t;

        Assert.assertEquals(rt.getStatus(), Status.STATUS_ACTIVE);

        //transaction have to be resolved
        rt.setLocation(new URI("remote://localhost:12345"));

        //wait for Byteman to intercept data
        try {
            Thread.sleep(100);
        } catch(Exception ignored){}

        mockEJBInvocation();

        rut.rollback();

        Assert.assertNull(tm.stateRef.get().transaction);

        Assert.assertTrue(rt.getStatus() == Status.STATUS_ROLLEDBACK);
    }

    @Test
    public void testRollbackOnly() throws Exception {

        RemoteUserTransaction rut = RemoteTransactionContext.getInstance().getUserTransaction();
        ContextTransactionManager tm = ContextTransactionManager.getInstance();
        Assert.assertNull(tm.stateRef.get().transaction);
        rut.begin();
        Transaction t = tm.stateRef.get().transaction;
        Assert.assertNotNull(t);
        Assert.assertTrue(t instanceof RemoteTransaction);
        RemoteTransaction rt = (RemoteTransaction) t;

        Assert.assertEquals(rt.getStatus(), Status.STATUS_ACTIVE);

        //transaction have to be resolved
        rt.setLocation(new URI("remote://localhost:12345"));

        //lets wait for Byteman to intercept data
        try {
            Thread.sleep(100);
        } catch(Exception ignored){}

        mockEJBInvocation();

        rut.setRollbackOnly();

        Assert.assertTrue(rt.getStatus() == Status.STATUS_MARKED_ROLLBACK);

        boolean rollbackException = false;
        try {
            rut.commit();
        } catch(RollbackException expected){
            rollbackException = true;
        }
        Assert.assertTrue(rollbackException);

        Assert.assertNull(tm.stateRef.get().transaction);

        Assert.assertTrue(rt.getStatus() == Status.STATUS_ROLLEDBACK);
    }

    private void mockEJBInvocation() throws SystemException {
        remotingTransactionServer.getOrBeginTransaction(transactionId, 5000);
    }
}
