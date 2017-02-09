/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2016 Red Hat, Inc., and individual contributors
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

import java.util.concurrent.atomic.AtomicInteger;

final class OutflowHandleManager {
    private OutflowHandleManager() {
    }

    /**
     * If this flag is set, then we're committed and cannot do any more outflow.
     */
    static final int FL_COMMITTED = 1 << 31;
    /**
     * If this flag is set, then we locked in an answer: if NON_MASTER is set, do not contact; if it is not, definitely contact.
     * As long as this flag is clear, the count is valid for the number of outstanding requests.
     */
    static final int FL_CONFIRMED = 1 << 30;
    /**
     * If this flag is set, then at least one handle reported back that the resource is not the master of the subordinate.
     */
    static final int FL_NON_MASTER = 1 << 29;

    static final int FLAGS = FL_COMMITTED | FL_CONFIRMED | FL_NON_MASTER;

    static int count(int val) {
        return val & ~FLAGS;
    }

    static int flags(int val) {
        return val & FLAGS;
    }

    static boolean isSet(int val, int flags) {
        return (val & flags) == flags;
    }

    static boolean open(AtomicInteger stateRef) {
        // open a new handle
        int oldVal, newVal;
        do {
            oldVal = stateRef.get();
            if (isSet(oldVal, FL_COMMITTED)) {
                return false;
            }
            if (isSet(oldVal, FL_CONFIRMED)) {
                // no count anymore, otherwise we're fine
                return true;
            }
            newVal = oldVal + 1;
        } while (! stateRef.compareAndSet(oldVal, newVal));
        return true;
    }

    static boolean commit(AtomicInteger stateRef) {
        int oldVal, newVal;
        do {
            oldVal = stateRef.get();
            if (isSet(oldVal, FL_COMMITTED)) {
                break;
            } else if (isSet(oldVal, FL_CONFIRMED)) {
                // already confirmed, just go with the current flags
                newVal = FL_COMMITTED | flags(oldVal);
            } else if (count(oldVal) > 0) {
                // outstanding invocations; just confirm, period (may be wrong if we're really non-master, but that's OK)
                newVal = FL_COMMITTED | FL_CONFIRMED;
            } else {
                // all enlistments were definitely forgotten, or else we're non-master: either way, commit with no confirm
                newVal = FL_COMMITTED;
            }
        } while (! stateRef.compareAndSet(oldVal, newVal));
        return isSet(oldVal, FL_CONFIRMED) && ! isSet(oldVal, FL_NON_MASTER);
    }

    static void verifyOne(AtomicInteger stateRef) {
        int oldVal, newVal;
        do {
            oldVal = stateRef.get();
            if (isSet(oldVal, FL_COMMITTED)) {
                // do nothing; it's committed
                return;
            }
            if (isSet(oldVal, FL_CONFIRMED)) {
                // do nothing; it's confirmed
                return;
            }
            // just clear the count
            newVal = FL_CONFIRMED;
        } while (! stateRef.compareAndSet(oldVal, newVal));
    }

    static void forgetOne(AtomicInteger stateRef) {
        int oldVal, newVal;
        do {
            oldVal = stateRef.get();
            if (isSet(oldVal, FL_COMMITTED)) {
                // do nothing; it's committed
                return;
            }
            if (isSet(oldVal, FL_CONFIRMED)) {
                // do nothing; it's confirmed
                return;
            }
            if (isSet(oldVal, FL_NON_MASTER) && count(oldVal) == 1) {
                // special case: we're confirming non-master
                newVal = FL_CONFIRMED | FL_NON_MASTER;
            } else {
                newVal = oldVal - 1;
            }
        } while (! stateRef.compareAndSet(oldVal, newVal));
    }

    static void nonMasterOne(final AtomicInteger stateRef) {
        int oldVal, newVal;
        do {
            oldVal = stateRef.get();
            if (isSet(oldVal, FL_COMMITTED)) {
                // do nothing; it's committed
                return;
            }
            if (isSet(oldVal, FL_CONFIRMED)) {
                // do nothing; it's confirmed
                return;
            }
            if (count(oldVal) == 1) {
                // special case: we're confirming non-master
                newVal = FL_CONFIRMED | FL_NON_MASTER;
            } else {
                // set the non-master flag and release the handle
                newVal = oldVal - 1 | FL_NON_MASTER;
            }
        } while (! stateRef.compareAndSet(oldVal, newVal));
    }
}
