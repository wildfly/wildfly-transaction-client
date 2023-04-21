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

import jakarta.transaction.Transaction;
import org.jboss.tm.ExtendedJBossXATerminator;
import org.jboss.tm.ImportedTransaction;
import org.jboss.tm.TransactionImportResult;
import org.wildfly.common.Assert;

import javax.transaction.xa.Xid;
/**
 * {@link ExtendedJBossXATerminator} is a part of transaction provider spi. In unit tests we use it to verify transaction
 * import.
 */
public class TestExtendedJBossXATerminator implements ExtendedJBossXATerminator {

    public TestExtendedJBossXATerminator() {
    }

    @Override
    public TransactionImportResult importTransaction(Xid xid, int i) {
        return new TransactionImportResult(new TestImportedTransaction(xid), true);
    }

    @Override
    public Transaction getTransaction(Xid xid) {
        throw Assert.unsupported();
    }

    @Override
    public Transaction getTransactionById(Object o) {
        throw Assert.unsupported();
    }

    @Override
    public Object getCurrentTransactionId() {
        throw Assert.unsupported();
    }

    @Override
    public ImportedTransaction getImportedTransaction(Xid xid) {
        throw Assert.unsupported();
    }

    @Override
    public Xid[] getXidsToRecoverForParentNode(boolean b, String s, int i) {
        throw Assert.unsupported();
    }

    @Override
    public Xid[] doRecover(Xid xid, String s) {
        throw Assert.unsupported();
    }

    @Override
    public boolean isRecoveryByNodeOrXidSupported() {
        return false;
    }

    @Override
    public void removeImportedTransaction(Xid xid) {
        throw Assert.unsupported();
    }
}
