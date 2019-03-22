package com.asymmetrik.nifi.services.cache.distributed;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.services.cache.SetCacheService;
import com.asymmetrik.nifi.services.cache.distributed.comm.CommsSession;
import com.asymmetrik.nifi.services.cache.distributed.comm.SSLCommsSession;
import com.asymmetrik.nifi.services.cache.distributed.comm.StandardCommsSession;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.protocol.exception.HandshakeException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.stream.io.DataOutputStream;

/**
 * Modeled after org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService
 */
@Tags({"asymmetrik", "duplicate", "dedupe", "distributed", "cache"})
@CapabilityDescription("Used to identify duplicate flowfiles across nodes in a NiFi cluster. Must be used in conjunction with a DistributedCacheServer service.")
@SeeAlso(classNames = {
        "com.asymmetrik.nifi.standard.services.cache.server.DistributedCacheServer",
        "com.asymmetrik.nifi.standard.services.cache.local.LocalCache",
        "com.asymmetrik.nifi.standard.processors.IdentifyDuplicate"
})
public class DistributedCacheClient extends AbstractControllerService implements SetCacheService<String> {

    private static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Server Hostname")
            .description("The name of the server that is running the DistributedCacheServer service")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Server Port")
            .description("The port on the remote server that is to be used when communicating with the DistributedCacheServer service")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("4557")
            .build();
    private static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("If specified, indicates the SSL Context Service that is used to communicate with the "
                    + "remote server. If not specified, communications will not be encrypted")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    private static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("Specifies how long to wait when communicating with the remote server before determining that "
                    + "there is a communications failure if data cannot be sent or received")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    private final BlockingQueue<CommsSession> queue = new LinkedBlockingQueue<>();
    private volatile ConfigurationContext configContext;
    private volatile boolean closed = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(COMMUNICATIONS_TIMEOUT);
        return descriptors;
    }

    @OnEnabled
    public void initialize(final ConfigurationContext context) {
        this.closed = false;
        this.configContext = context;
    }

    @OnDisabled
    public void shutdown() {
        this.closed = true;

        CommsSession commsSession;
        while ((commsSession = queue.poll()) != null) {
            try (final DataOutputStream dos = new DataOutputStream(commsSession.getOutputStream())) {
                dos.writeUTF("close");
                dos.flush();
                commsSession.close();
            } catch (final IOException e) { /* empty */ }
        }
        if (getLogger().isDebugEnabled() && getIdentifier() != null) {
            getLogger().debug("Closed {}", new Object[]{getIdentifier()});
        }
    }

    @Override
    public boolean addIfAbsent(String key) {
        return invokeRemoteMethod("addIfAbsent", key);
    }

    @Override
    public List<Boolean> addIfAbsent(List<String> keys) {
        return withCommsSession(new CommsAction<List<Boolean>>() {
            @Override
            public List<Boolean> execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("addListIfAbsent");

                dos.writeInt(keys.size());
                for (String key : keys) {
                    serialize(key, dos);
                }
                dos.flush();

                final DataInputStream dis = new DataInputStream(session.getInputStream());
                List<Boolean> returnValues = new ArrayList<>();

                try {
                    for (int i = 0; i < keys.size(); i++) {
                        returnValues.add(dis.readBoolean());
                    }
                } catch (EOFException eof) {
                    getLogger().warn("EOFException caught: read {} returnvalues, expected {}", new Object[]{returnValues.size(), keys.size()});
                }
                return returnValues;
            }
        });
    }

    @Override
    public boolean contains(String key) {
        return invokeRemoteMethod("contains", key);
    }

    @Override
    public void remove(String key) {
        boolean result = invokeRemoteMethod("remove", key);
        if (!result) {
            throw new IllegalStateException("Expected to receive confirmation of 'remove' request but received unexpected response");
        }
    }

    @Override
    public void removeAll() {
        boolean result = invokeRemoteMethod("removeAll", null);
        if (!result) {
            throw new IllegalStateException("Expected to receive confirmation of 'removeAll' request but received unexpected response");
        }
    }

    private CommsSession createCommsSession(final ConfigurationContext context) throws IOException {
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final long timeoutMillis = context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        final CommsSession commsSession;
        if (sslContextService == null) {
            commsSession = new StandardCommsSession(hostname, port);
        } else {
            commsSession = new SSLCommsSession(sslContextService.createSSLContext(ClientAuth.REQUIRED), hostname, port);
        }

        commsSession.setTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        return commsSession;
    }

    private CommsSession leaseCommsSession() throws RuntimeException {
        CommsSession session = queue.poll();
        if (session != null && !session.isClosed()) {
            return session;
        }

        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(1);
        try {
            session = createCommsSession(configContext);
            ProtocolHandshake.initiateHandshake(session.getInputStream(), session.getOutputStream(), versionNegotiator);
            return session;
        } catch (final IOException | HandshakeException e) {
            throw new RuntimeException(e);
        }
    }

    private void serialize(final String value, final DataOutputStream dos) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(data.length);
        dos.write(data);
    }

    private boolean invokeRemoteMethod(final String methodName, final String key) throws RuntimeException {
        return withCommsSession(new CommsAction<Boolean>() {
            @Override
            public Boolean execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF(methodName);

                if (key != null) {
                    serialize(key, dos);
                }

                dos.flush();

                final DataInputStream dis = new DataInputStream(session.getInputStream());
                return dis.readBoolean();
            }
        });
    }

    private <T> T withCommsSession(final CommsAction<T> action) throws RuntimeException {
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }
        boolean tryToRequeue = true;
        final CommsSession session = leaseCommsSession();
        try {
            return action.execute(session);
        } catch (final IOException ioe) {
            tryToRequeue = false;
            throw new RuntimeException(ioe);
        } finally {
            if (tryToRequeue == true && this.closed == false) {
                queue.offer(session);
            } else {
                IOUtils.closeQuietly(session);
            }
        }
    }

    private static interface CommsAction<T> {
        T execute(CommsSession commsSession) throws IOException;
    }

}
