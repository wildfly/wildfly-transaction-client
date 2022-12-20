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
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

public class TestTransactionManager implements TransactionManager {

    Transaction current = null;

    static public boolean committed = false;
    static public boolean rolledback = false;

    public TestTransactionManager(){
    }

    @Override
    public void begin() {
        current = new TestTransaction();
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        if (current != null) {
            current.commit();
            committed = true;
            current = null;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getStatus() {
        return 0;
    }

    @Override
    public Transaction getTransaction() {
        return current;
    }

    @Override
    public void resume(Transaction suspended) throws IllegalStateException {
        if(current == null){
            current = suspended;
        }
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        if (current != null) {
            current.rollback();
            rolledback = true;
            current = null;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException {

    }

    @Override
    public void setTransactionTimeout(int seconds) {

    }

    @Override
    public Transaction suspend() {
        Transaction suspended = current;
        current = null;
        return suspended;
    }

    public static void reset(){
        committed = false;
        rolledback = false;
    }
}
