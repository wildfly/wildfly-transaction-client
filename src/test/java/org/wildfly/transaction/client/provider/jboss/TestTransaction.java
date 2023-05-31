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

import jakarta.transaction.Synchronization;
import jakarta.transaction.Transaction;
import org.wildfly.common.Assert;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link javax.transaction.Transaction} implementation is provided by transaction manager (Narayana).
 * In the test we are mocking it.
 */
public class TestTransaction implements Transaction {

    public static TestTransaction latest;

    public Map<Object, Object> resources = new HashMap<>();

    private List<XAResource> enlistedResources = new ArrayList<>();

    public enum State {
        ACTIVE, SUSPENDED, PREPARED, COMMITTED, ROLLED_BACK
    }

    State state;

    Xid xid;

    public TestTransaction(Xid xid) {
        state = State.ACTIVE;
        this.xid = xid;
        latest = this;
    }

    public TestTransaction() {
        this(new TestXid());
    }

    @Override
    public void commit() throws SecurityException, IllegalStateException {
        state = State.COMMITTED;
    }

    @Override
    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException {
        if (enlistedResources.contains(xaRes)) {
            enlistedResources.remove(xaRes);
            return true;
        }
        return false;
    }

    @Override
    public boolean enlistResource(XAResource xaRes) throws IllegalStateException {
        enlistedResources.add(xaRes);
        return true;
    }

    @Override
    public int getStatus() {
        switch (state) {
            case ACTIVE:
                return 0;
            case PREPARED:
                return 2;
            case COMMITTED:
                return 3;
            case ROLLED_BACK:
                return 4;
        }
        throw new IllegalStateException();
    }

    @Override
    public void registerSynchronization(Synchronization sync) throws IllegalStateException {

    }

    @Override
    public void rollback() throws IllegalStateException {
        state = State.ROLLED_BACK;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException {

    }

    void suspend() {
        state = State.SUSPENDED;
    }

    void resume() {
        state = State.ACTIVE;
    }

    Xid getXid() {
        return xid;
    }

    void putResource(Object key, Object value) {
        resources.put(key, value);
    }

    Object getResource(Object key) {
        return resources.get(key);
    }

    public State getState() {
        return state;
    }

    public List<XAResource> getEnlistedResources() {
        return enlistedResources;
    }
}
