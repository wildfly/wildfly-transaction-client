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

import org.jboss.logging.Logger;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.EndpointBuilder;
import org.jboss.remoting3.spi.NetworkServerProvider;
import org.wildfly.security.auth.realm.SimpleMapBackedSecurityRealm;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.SaslAuthenticationFactory;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.password.interfaces.ClearPassword;
import org.wildfly.security.permission.PermissionVerifier;
import org.wildfly.security.sasl.util.SaslFactories;
import org.wildfly.transaction.client.provider.remoting.RemotingTransactionService;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Sequence;
import org.xnio.Xnio;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DummyServer implements AutoCloseable {

    private static final Logger logger = Logger.getLogger(DummyServer.class);
    /*
     * Reject unmarshalling an instance of IAE, as a kind of 'blocklist'.
     * In normal tests this type would never be sent, which is analogous to
     * how blocklisted classes are normally not sent. And then we can
     * deliberately send an IAE in tests to confirm it is rejected.
     */
    private static final Function<String, Boolean> DEFAULT_CLASS_FILTER = cName -> !cName.equals(IllegalArgumentException.class.getName());

    private Endpoint endpoint;
    private final int port;
    private final String host;
    private final String endpointName;
    private AcceptingChannel<org.xnio.StreamConnection> server;


    final Set<Channel> currentConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public DummyServer(final String host, final int port, final String endpointName)  {
        this.host = host;
        this.port = port;
        this.endpointName = endpointName;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    public String getEndpointName() {
        return endpointName;
    }

    public void start() throws Exception {
        logger.info("Starting " + this);

        // create a Remoting endpoint
        final OptionMap options = OptionMap.EMPTY;
        EndpointBuilder endpointBuilder = Endpoint.builder();
        endpointBuilder.setEndpointName(this.endpointName);
        endpointBuilder.buildXnioWorker(Xnio.getInstance()).populateFromOptions(options);
        endpoint = endpointBuilder.build();

        final RemotingTransactionService remotingTransactionService = RemotingTransactionService.builder().setEndpoint(endpoint)
                .setTransactionContext(LocalTransactionContext.getCurrent()).build();

        remotingTransactionService.register();

        // add a connection provider factory for the URI scheme "remote"
        // endpoint.addConnectionProvider("remote", new RemoteConnectionProviderFactory(), OptionMap.create(Options.SSL_ENABLED, Boolean.FALSE));
        final NetworkServerProvider serverProvider = endpoint.getConnectionProviderInterface("remote", NetworkServerProvider.class);

        // set up a security realm called default with a user called test
        final SimpleMapBackedSecurityRealm realm = new SimpleMapBackedSecurityRealm();
        realm.setPasswordMap("test", ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, "test".toCharArray()));

        // set up a security domain which has realm "default"
        final SecurityDomain.Builder domainBuilder = SecurityDomain.builder();
        domainBuilder.addRealm("default", realm).build();                                  // add the security realm called "default" to the security domain
        domainBuilder.setDefaultRealmName("default");
        domainBuilder.setPermissionMapper((permissionMappable, roles) -> PermissionVerifier.ALL);
        SecurityDomain testDomain = domainBuilder.build();

        // set up a SaslAuthenticationFactory (i.e. a SaslServerFactory)
        SaslAuthenticationFactory saslAuthenticationFactory = SaslAuthenticationFactory.builder()
                .setSecurityDomain(testDomain)
                .setMechanismConfigurationSelector(mechanismInformation -> {
                    switch (mechanismInformation.getMechanismName()) {
                        case "ANONYMOUS":
                        case "PLAIN": {
                            return MechanismConfiguration.EMPTY;
                        }
                        default: return null;
                    }
                })
                .setFactory(SaslFactories.getElytronSaslServerFactory())
                .build();

        final OptionMap serverOptions = OptionMap.create(Options.SASL_MECHANISMS, Sequence.of("ANONYMOUS"), Options.SASL_POLICY_NOANONYMOUS, Boolean.FALSE);
        final SocketAddress bindAddress = new InetSocketAddress(InetAddress.getByName(host), port);
        this.server = serverProvider.createServer(bindAddress, serverOptions, saslAuthenticationFactory, null);
    }

    public void stop() throws Exception {
        if (server !=  null) {
            this.server.close();
            this.server = null;
            IoUtils.safeClose(this.endpoint);
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public void hardKill() throws IOException {
        for (Channel i : currentConnections) {
            try {
                i.close();
            } catch (IOException e) {
                logger.error("failed to close", e);
            }
        }
        server.close();
        server = null;
        endpoint.close();
    }
}