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

import org.jboss.tm.ImportedTransaction;
import org.wildfly.common.Assert;

import javax.transaction.xa.Xid;
import java.util.Collections;
import java.util.List;

/**
 * The {@link ImportedTransaction} implementation is provided by transaction manager (Narayana).
 * In the test we are mocking it.
 */
public class TestImportedTransaction extends TestTransaction implements ImportedTransaction {

    public static TestImportedTransaction latest = null;

    public TestImportedTransaction(Xid xid){
        super(xid);
        latest = this;
    }

    @Override
    public int doPrepare() {
        Assert.assertTrue(state.equals(State.ACTIVE));
        state = State.PREPARED;
        return 1;
    }

    @Override
    public boolean doCommit() throws IllegalStateException {
        Assert.assertTrue(state.equals(State.PREPARED));
        state = State.COMMITTED;
        return true;
    }

    @Override
    public void doRollback() throws IllegalStateException {
        Assert.assertTrue(state.equals(State.PREPARED));
        state = State.ROLLED_BACK;
    }

    @Override
    public void doOnePhaseCommit() throws IllegalStateException {
        Assert.assertTrue(state.equals(State.ACTIVE));
        state = State.COMMITTED;
    }

    @Override
    public void doForget() throws IllegalStateException {

    }

    @Override
    public boolean doBeforeCompletion() {
        return true;
    }

    @Override
    public boolean activated() {
        return true;
    }

    @Override
    public void recover() {

    }

    @Override
    public Xid baseXid() {
        return xid;
    }

    @Override
    public Object getId() {
        throw Assert.unsupported();
    }

    @Override
    public List<Throwable> getDeferredThrowables() {
        return Collections.emptyList();
    }

    @Override
    public boolean supportsDeferredThrowables() {
        return false;
    }
}
