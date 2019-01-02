/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018 Red Hat, Inc., and individual contributors
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

import static java.security.AccessController.doPrivileged;

import org.wildfly.common.annotation.NotNull;
import org.wildfly.transaction.client.LocalTransaction;
import org.wildfly.transaction.client.SimpleXid;
import org.wildfly.transaction.client.XAResourceRegistry;
import org.wildfly.transaction.client._private.Log;
import org.wildfly.transaction.client.spi.LocalTransactionProvider;

import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.io.FilePermission;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A registry persisted in a series of log files, containing all outflowed resources info for unfinished transactions.
 * This registry is created whenever a subordinate resource is outflowed to a remote location,
 * and deleted only when all outflowed resources participating in that transaction are successfully prepared,
 * committed or rolled back (the two latter in the case of a one-phase commit).
 *
 * Used for {@link #getInDoubtXAResources()}  recovery of in doubt resources}.
 *
 * @author Flavia Rainone
 */
final class FileSystemXAResourceRegistry {

    private static final FilePermission FILE_PERMISSION = new FilePermission("<<ALL FILES>>", "read,write");
    /**
     * Name of recovery dir. Location of this dir can be defined at constructor.
     */
    private static final String RECOVERY_DIR = "ejb-xa-recovery";

    /**
     * Empty utility array.
     */
    private static final XAResource[] EMPTY_IN_DOUBT_RESOURCES = new XAResource[0];

    /**
     * Key for keeeping the xa resource registry associated with a local transaction
     */
    private static final Object XA_RESOURCE_REGISTRY_KEY = new Object();

    /**
     * The local transaction provider associated with this file system XAResource registry
     */
    private final LocalTransactionProvider provider;

    /**
     * The xa recovery path, i.e., the path containing {@link #RECOVERY_DIR}.
     */
    private final Path xaRecoveryPath;

    /**
     * A set containing the list of all registry files that are currently open. Used as a support to
     * identify in doubt registries. See {@link #recoverInDoubtRegistries}.
     */
    private final Set<String> openFilePaths = Collections.synchronizedSet(new HashSet<>());

    /**
     * A set of in doubt resources, i.e., outflowed resources whose prepare/rollback/commit operation was not
     * completed normally, or resources that have been recovered from in doubt registries. See
     * {@link XAResourceRegistryFile#resourceInDoubt} and {@link XAResourceRegistryFile#loadInDoubtResources}.
     */
    private final Set<XAResource> inDoubtResources = Collections.synchronizedSet(new HashSet<>()); // it is a set because we could have an in doubt resource reincide in failure to complete

    /**
     * Creates a FileSystemXAResourceRegistry.
     *
     * @param relativePath the path recovery dir is relative to
     */
    FileSystemXAResourceRegistry (LocalTransactionProvider provider, Path relativePath) {
        this.provider = provider;
        if (relativePath == null)
             this.xaRecoveryPath = FileSystems.getDefault().getPath(RECOVERY_DIR);
        else
            this.xaRecoveryPath = relativePath.resolve(RECOVERY_DIR);
    }

    /**
     * Returns the XAResourceRegistry file for {@code transaction}.
     *
     * @param transaction the transaction
     * @return the XAResourceRegistry for {@code transaction}. If there is no such registry file, a new one is created.
     * @throws SystemException if an unexpected failure occurs when creating the registry file
     */
    XAResourceRegistry getXAResourceRegistryFile(LocalTransaction transaction) throws SystemException {
        XAResourceRegistry registry = (XAResourceRegistry) transaction.getResource(XA_RESOURCE_REGISTRY_KEY);
        if (registry != null)
            return registry;
        registry = new XAResourceRegistryFile(transaction.getXid());
        transaction.putResource(XA_RESOURCE_REGISTRY_KEY, registry);
        return registry;
    }

    /**
     * Returns a list containing all in doubt xa resources. A XAResource is considered in doubt if:
     * <ul>
     *     <li>it failed to prepare on a two-phase commit by throwing an exception</li>
     *     <li>it failed to commit or rollback in a one-phase commit by throwing an exception</li>
     * </ul>
     * An in doubt resource is no longer considered in doubt if it succeeded to rollback without an exception.
     *
     * Notice that in doubt xa resources are kept after the server shuts down, guaranteeing that they can eventually be
     * recovered, even if in a different server JVM instance than the one that outflowed the resource. This mechanism
     * assures proper recovery and abortion of the original in-doubt outflowed resource, that belongs to an external
     * remote server.
     *
     * @return a list of the in doubt xa resources
     */
    XAResource[] getInDoubtXAResources() {
        try {
            recoverInDoubtRegistries();
        } catch (IOException e) {
            throw Log.log.unexpectedExceptionOnXAResourceRecovery(e);
        }
        return inDoubtResources.isEmpty() ? EMPTY_IN_DOUBT_RESOURCES : inDoubtResources.toArray(
                    new XAResource[inDoubtResources.size()]);
    }

    /**
     * Recovers closed registries files from file system. All those registries are considered in doubt.
     *
     * @throws IOException if there is an I/O error when reading the recovered registry files
     */
    private void recoverInDoubtRegistries() throws IOException {
        final File recoveryDir = xaRecoveryPath.toFile();
        if (!recoveryDir.exists()) {
            return;
        }
        final String[] xaRecoveryFileNames = recoveryDir.list();
        if (xaRecoveryFileNames == null) {
            Log.log.listXAResourceRecoveryFilesNull(recoveryDir);
            return;
        }
        for (String xaRecoveryFileName : xaRecoveryFileNames) {
            // check if file is not open already
            if (!openFilePaths.contains(xaRecoveryFileName))
                new XAResourceRegistryFile(xaRecoveryFileName, provider);
        }
    }


    /**
     * Represents a single file in the file system that records all outflowed resources per a specific local transaction.
     */
    private final class XAResourceRegistryFile extends XAResourceRegistry {

        /**
         * Path to the registry file.
         */
        @NotNull
        private final Path filePath;

        /**
         * The file channel, if non-null, it indicates that this registry represents a current, on-going transaction,
         * if null, then this registry represents an registry file recovered from file system.
         */
        private final FileChannel fileChannel;

        /**
         * Keeps track of the XA outflowed resources stored in this registry, see {@link #addResource} and
         * {@link #removeResource}.
         */
        private final Set<XAResource> resources = Collections.synchronizedSet(new HashSet<>());


        /**
         * Creates a XA  recovery registry for a transaction. This method assumes that there is no file already
         * existing for this transaction, and, furthermore, it is not thread safe  (the creation of this object is
         * already thread protected at the caller).
         *
         * @param xid the transaction xid
         * @throws SystemException if the there was a problem when creating the recovery file in file system
         */
        XAResourceRegistryFile(Xid xid) throws SystemException {
            
            final String xidString = SimpleXid.of(xid).toHexString('_');
            this.filePath = xaRecoveryPath.resolve(xidString);
            
            try {
                fileChannel = doPrivileged((PrivilegedExceptionAction<FileChannel>) () -> {
                    final SecurityManager sm = System.getSecurityManager();
                    if(sm != null) {
                        sm.checkPermission(FILE_PERMISSION);
                    }
                    xaRecoveryPath.toFile().mkdir(); // create dir if non existent
                    return FileChannel.open(filePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE_NEW);
                });
                openFilePaths.add(xidString);
                fileChannel.lock();
                Log.log.xaResourceRecoveryFileCreated(filePath);
            } catch (PrivilegedActionException e) {
                throw Log.log.createXAResourceRecoveryFileFailed(filePath, (IOException)e.getCause());
            } catch (IOException e) {
                throw Log.log.createXAResourceRecoveryFileFailed(filePath, e);
            }
        }

        /**
         * Reload a registry that is in doubt, i.e., the registry is not associated yet with a current
         * transaction in this server, but with a transaction of a previous jvm instance that is now
         * being recovered.
         * This will happen only if the jvm crashes before a transaction with XA outflowed resources is
         * fully prepared. In this case, any lines in the registry can correspond to in doubt outflowed
         * resources. The goal is to reload those resources so they can be recovered.
         *
         * @param inDoubtFilePath the file path of the in doubt registry
         * @throws IOException if there is an I/O error when realoding the registry file
         */
        private XAResourceRegistryFile(String inDoubtFilePath, LocalTransactionProvider provider) throws IOException {
            this.filePath = xaRecoveryPath.resolve(inDoubtFilePath);
            this.fileChannel = null; // no need to open file channel here
            openFilePaths.add(inDoubtFilePath);
            loadInDoubtResources(provider.getNodeName());
            Log.log.xaResourceRecoveryRegistryReloaded(filePath);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void addResource(XAResource resource, URI uri) throws SystemException {
            assert fileChannel != null;
            try {
                assert fileChannel.isOpen();
                fileChannel.write(ByteBuffer.wrap((uri.toString() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)));
                fileChannel.force(true);
            } catch (IOException e) {
                throw Log.log.appendXAResourceRecoveryFileFailed(uri, filePath, e);
            }
            this.resources.add(resource);
            Log.log.xaResourceAddedToRecoveryRegistry(uri, filePath);
        }

        /**
         * {@inheritDoc}
         * The registry file is closed and deleted if there are no more resources left.
         *
         * @throws XAException if there is a problem deleting the registry file
         */
        @Override
        protected void removeResource(XAResource resource) throws XAException {
            if (resources.remove(resource)) {
                if (resources.isEmpty()) {
                    // delete file
                    try {
                        if (fileChannel != null) {
                            fileChannel.close();
                        }
                        Files.delete(filePath);
                        openFilePaths.remove(filePath.toString());
                    } catch (IOException e) {
                        throw Log.log.deleteXAResourceRecoveryFileFailed(XAException.XAER_RMERR, filePath, resource, e);
                    }
                    Log.log.xaResourceRecoveryFileDeleted(filePath);
                }
                // remove resource from in doubt list, in case the resource was in doubt
                inDoubtResources.remove(resource);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void resourceInDoubt(XAResource resource) {
            inDoubtResources.add(resource);
        }

        /**
         * Loads in doubt resources from recovered registry file.
         *
         * @throws IOException if an I/O error occurs when reloading the resources from the file
         */
        private void loadInDoubtResources(String nodeName) throws IOException {
            assert fileChannel == null;
            final List<String> uris;
            try {
                uris = Files.readAllLines(filePath);
            } catch (IOException e) {
                throw Log.log.readXAResourceRecoveryFileFailed(filePath, e);
            }
            for (String uriString : uris) {
                // adding a line separator at the end of each uri entry results in an extra empty line
                if (uriString.isEmpty())
                    continue;
                final URI uri;
                try {
                    uri = new URI(uriString);
                } catch (URISyntaxException e) {
                    throw Log.log.readURIFromXAResourceRecoveryFileFailed(uriString, filePath, e);
                }
                final XAResource xaresource = reloadInDoubtResource(uri, nodeName);
                inDoubtResources.add(xaresource);
                Log.log.xaResourceRecoveredFromRecoveryRegistry(uri, filePath);
            }
        }
    }
}
