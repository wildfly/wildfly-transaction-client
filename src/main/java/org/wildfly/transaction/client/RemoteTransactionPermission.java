
package org.wildfly.transaction.client;

import org.wildfly.security.permission.AbstractBooleanPermission;

/**
 * Represents permission to invoke transaction operations remotely
 *
 * @author Stuart Douglas
 */
public class RemoteTransactionPermission extends AbstractBooleanPermission<RemoteTransactionPermission> {

    /**
     * Construct a new instance.
     */
    public RemoteTransactionPermission() {
    }

    /**
     * Construct a new instance.
     *
     * @param name ignored
     */
    public RemoteTransactionPermission(@SuppressWarnings("unused") final String name) {
    }

    /**
     * Construct a new instance.
     *
     * @param name ignored
     * @param actions ignored
     */
    public RemoteTransactionPermission(@SuppressWarnings("unused") final String name, @SuppressWarnings("unused") final String actions) {
    }

    private static final RemoteTransactionPermission INSTANCE = new RemoteTransactionPermission();

    /**
     * Get the instance of this class.
     *
     * @return the instance of this class
     */
    public static RemoteTransactionPermission getInstance() {
        return INSTANCE;
    }
}
