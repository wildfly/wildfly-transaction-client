package org.wildfly.transaction.client.provider.jboss;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class TestXAResource implements XAResource {

    @Override
    public void commit(Xid xid, boolean b) {

    }

    @Override
    public void end(Xid xid, int i) {

    }

    @Override
    public void forget(Xid xid) {

    }

    @Override
    public int getTransactionTimeout() {
        return 0;
    }

    @Override
    public boolean isSameRM(XAResource xaResource) {
        return false;
    }

    @Override
    public int prepare(Xid xid) {
        return 0;
    }

    @Override
    public Xid[] recover(int i) {
        return new Xid[0];
    }

    @Override
    public void rollback(Xid xid) {

    }

    @Override
    public boolean setTransactionTimeout(int i) {
        return false;
    }

    @Override
    public void start(Xid xid, int i) {

    }
}
