/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2026 Red Hat, Inc., and individual contributors
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

import jakarta.transaction.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.transaction.client.provider.jboss.JBossLocalTransactionProvider;
import org.wildfly.transaction.client.provider.jboss.TestTransactionManager;
import org.wildfly.transaction.client.provider.jboss.TestTransactionProvider;
import org.wildfly.transaction.client.spi.SubordinateTransactionControl;

import javax.transaction.xa.Xid;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies that imported transactions are properly cleaned up from the internal tracking map
 * after commit and rollback, preventing memory leaks (OOM) under sustained load.
 */
public class ImportedTransactionMemoryLeakTestCase {

    private static TestTransactionProvider provider;

    @BeforeClass
    public static void init() {
        provider = new TestTransactionProvider(500, Path.of("./target"));
        final LocalTransactionContext transactionContext = new LocalTransactionContext(provider);
        LocalTransactionContext.getContextManager().setGlobalDefault(transactionContext);
    }

    @Before
    public void reset() {
        TestTransactionProvider.reset();
        TestTransactionManager.reset();
    }

    /**
     * Simulates the OOM reproducer scenario: imports many transactions, commits them via
     * two-phase commit (prepare + commit), and verifies that the internal {@code known} map
     * is cleaned up after each transaction completes.
     */
    @Test
    public void importedTransactionTwoPhaseCommitCleansUpKnownMap() throws Exception {
        final int transactionCount = 500;
        XAImporter xaImporter = provider.getXAImporter();
        ConcurrentMap<SimpleXid, ?> knownMap = getKnownMap(provider);

        for (int i = 0; i < transactionCount; i++) {
            Xid xid = createUniqueXid(i);
            ImportResult<Transaction> result = (ImportResult<Transaction>) xaImporter.findOrImportTransaction(xid, 5000, false);
            Assert.assertNotNull("Transaction should be imported", result);
            Assert.assertTrue("Transaction should be newly imported", result.isNew());

            SubordinateTransactionControl control = result.getControl();
            control.prepare();
            control.commit(false);
        }

        Assert.assertEquals("known map should be empty after all transactions are committed", 0, knownMap.size());
    }

    /**
     * Verifies that one-phase commit also properly cleans up the known map.
     */
    @Test
    public void importedTransactionOnePhaseCommitCleansUpKnownMap() throws Exception {
        final int transactionCount = 500;
        XAImporter xaImporter = provider.getXAImporter();
        ConcurrentMap<SimpleXid, ?> knownMap = getKnownMap(provider);

        for (int i = 0; i < transactionCount; i++) {
            Xid xid = createUniqueXid(i);
            ImportResult<Transaction> result = (ImportResult<Transaction>) xaImporter.findOrImportTransaction(xid, 5000, false);
            Assert.assertNotNull("Transaction should be imported", result);

            SubordinateTransactionControl control = result.getControl();
            control.commit(true);
        }

        Assert.assertEquals("known map should be empty after all one-phase commits", 0, knownMap.size());
    }

    /**
     * Verifies that rollback also properly cleans up the known map, preventing leaks
     * when transactions are rolled back instead of committed.
     */
    @Test
    public void importedTransactionRollbackCleansUpKnownMap() throws Exception {
        final int transactionCount = 500;
        XAImporter xaImporter = provider.getXAImporter();
        ConcurrentMap<SimpleXid, ?> knownMap = getKnownMap(provider);

        for (int i = 0; i < transactionCount; i++) {
            Xid xid = createUniqueXid(i);
            ImportResult<Transaction> result = (ImportResult<Transaction>) xaImporter.findOrImportTransaction(xid, 5000, false);
            Assert.assertNotNull("Transaction should be imported", result);

            SubordinateTransactionControl control = result.getControl();
            control.rollback();
        }

        Assert.assertEquals("known map should be empty after all transactions are rolled back", 0, knownMap.size());
    }

    /**
     * Simulates the high-concurrency scenario from the reproducer (40 concurrent connections,
     * 50,000 requests) at a smaller scale to verify thread-safety of cleanup under contention.
     */
    @Test
    public void concurrentImportAndCommitCleansUpKnownMap() throws Exception {
        final int threadCount = 40;
        final int transactionsPerThread = 200;
        final XAImporter xaImporter = provider.getXAImporter();
        final ConcurrentMap<SimpleXid, ?> knownMap = getKnownMap(provider);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger failures = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < transactionsPerThread; i++) {
                        Xid xid = createUniqueXid(threadId * transactionsPerThread + i);
                        ImportResult<Transaction> result = (ImportResult<Transaction>) xaImporter.findOrImportTransaction(xid, 5000, false);
                        SubordinateTransactionControl control = result.getControl();
                        control.commit(true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failures.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Release all threads simultaneously
        startLatch.countDown();
        Assert.assertTrue("All threads should complete within 60 seconds",
                doneLatch.await(60, TimeUnit.SECONDS));
        executor.shutdown();

        Assert.assertEquals("No thread should have failed", 0, failures.get());
        Assert.assertEquals("known map should be empty after all concurrent transactions complete",
                0, knownMap.size());
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<SimpleXid, ?> getKnownMap(JBossLocalTransactionProvider provider) throws Exception {
        Field knownField = JBossLocalTransactionProvider.class.getDeclaredField("known");
        knownField.setAccessible(true);
        return (ConcurrentMap<SimpleXid, ?>) knownField.get(provider);
    }

    private static Xid createUniqueXid(int id) {
        return new Xid() {
            private final byte[] gtid = ByteBuffer.allocate(4).putInt(id).array();

            @Override
            public int getFormatId() {
                return 1;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return gtid;
            }

            @Override
            public byte[] getBranchQualifier() {
                return new byte[0];
            }
        };
    }
}
