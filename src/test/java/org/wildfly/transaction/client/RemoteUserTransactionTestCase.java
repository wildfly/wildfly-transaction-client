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

import java.net.URI;
import java.nio.file.Path;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wildfly.transaction.client.provider.jboss.TestTransactionProvider;
import org.wildfly.transaction.client.provider.remoting.RemotingTransactionServer;

/**
 * Checks the functionality of the RemoteUserTransaction, that is the transaction created by the client that runs outside the application server.
 */
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
        // clean up any lingering transaction
        try {
            RemoteUserTransaction rut = RemoteTransactionContext.getInstance().getUserTransaction();
            rut.rollback();
        } catch (Exception ignore) {
        }

        dummyServer.stop();
    }

    /**
     * Sanity check of RemoteUserTransaction commit scenario. Check whether the remote transaction has been correctly started and
     * associated with the current context. Later, sets the location of the underlying transaction and makes sure that commit
     * operation is correctly propagated to the transaction instance.
     *
     * @throws Exception
     */
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
        waitForByteman();

        mockEJBInvocation();

        rut.commit();

        Assert.assertNull(tm.stateRef.get().transaction);

        Assert.assertTrue(rt.getStatus() == Status.STATUS_COMMITTED);
    }

    /**
     * Sanity check of RemoteUserTransaction rollback scenario. Check whether the remote transaction has been correctly started and
     * associated with the current context. Later, sets the location of the underlying transaction and makes sure that rollback
     * operation is correctly propagated to the transaction instance.
     *
     * @throws Exception
     */
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
        waitForByteman();

        mockEJBInvocation();

        rut.rollback();

        Assert.assertNull(tm.stateRef.get().transaction);

        Assert.assertTrue(rt.getStatus() == Status.STATUS_ROLLEDBACK);
    }

    /**
     * Sanity check of RemoteUserTransaction setRollbackOnly scenario. Check whether the remote transaction has been correctly started and
     * associated with the current context. Later, sets the location of the underlying transaction and makes sure that setRollbackOnly
     * operation is correctly propagated to the transaction instance and that the commit operation cannot be performed on that transaction.
     *
     * @throws Exception
     */
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

        //wait for Byteman to intercept data
        waitForByteman();

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


    /**
     * If no invocations are performed, then client optimizes by not performing any transaction related invocations on the server.
     * This method is needed to circumvent that for the test purposes.
     *
     * @throws SystemException on errors
     */
    private void mockEJBInvocation() throws SystemException {
        remotingTransactionServer.getOrBeginTransaction(transactionId, 5000);
    }

    private static void waitForByteman() {
        try {
            for (int i = 0; i < 10 && remotingTransactionServer == null; i++) {
                Thread.sleep(200);
            }
        } catch (Exception ignored) {
        }

        // We've seen cases where remotingTransactionServer has not been injected
        // to this test class by byteman, e.g., when running on Windows with jdk 17.
        // So proceed only when remotingTransactionServer has been set successfully.
        System.out.printf("remotingTransactionServer set by byteman: %s%n", remotingTransactionServer);
        Assume.assumeNotNull(remotingTransactionServer);
    }
}
