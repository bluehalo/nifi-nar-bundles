package com.asymmetrik.nifi.services.cache.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import com.asymmetrik.nifi.services.cache.SetCache;
import com.asymmetrik.nifi.services.cache.SetCacheService;
import com.asymmetrik.nifi.services.cache.impl.SetCacheImpl;

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
import org.apache.nifi.remote.io.socket.SocketChannelInputStream;
import org.apache.nifi.remote.io.socket.SocketChannelOutputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelInputStream;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelOutputStream;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modeled after org.apache.nifi.distributed.cache.server.DistributedCacheServer
 */
@Tags({"asymmetrik", "duplicate", "dedupe", "distributed", "cache", "cluster", "server"})
@CapabilityDescription("Provides a SetCache that can be accessed over a socket. This service is intended "
        + " to be used in clusters, and executed on the manager node only. Interaction with this service " +
        " is typically accomplished via a DistributedCacheClient service.")
@SeeAlso(classNames = {"com.asymmetrik.nifi.standard.services.cache.distributed.DistributedCacheClient"})
public class DistributedCacheServer extends AbstractControllerService {
    private static final Logger logger = LoggerFactory.getLogger(DistributedCacheServer.class);

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The port to listen on for incoming connections")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("4557")
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("If specified, this service will be used to create an SSL Context that will be used "
                    + "to secure communications; if not specified, communications will not be secure")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected volatile boolean stopped = false;
    private final Set<Thread> processInputThreads = new CopyOnWriteArraySet<>();
    private volatile ServerSocketChannel serverSocketChannel;

    private SetCache<ByteBuffer> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PORT);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(SetCacheService.MAX_SIZE);
        properties.add(SetCacheService.AGE_OFF_DURATION);
        properties.add(SetCacheService.CONCURRENCY_LEVEL);
        return properties;
    }

    @OnEnabled
    public void start(final ConfigurationContext context) throws IOException {
        createCache(context);

        startServer(context);
    }

    @OnDisabled
    public void stop() throws IOException {
        stopServer();

        cache.removeAll();
        cache = null;
    }

    private boolean listen(final InputStream in, final OutputStream out, final int version) throws IOException {
        final DataInputStream dis = new DataInputStream(in);
        final DataOutputStream dos = new DataOutputStream(out);
        final String action = dis.readUTF();
        try {
            switch (action) {
                case "addIfAbsent": {
                    final ByteBuffer key = readBytes(dis);
                    final boolean added = cache.addIfAbsent(key);
                    dos.writeBoolean(added);
                    break;
                }
                case "addListIfAbsent": {
                    final int numKeys = dis.readInt();
                    for (int i = 0; i < numKeys; i++) {
                        final boolean added = cache.addIfAbsent(readBytes(dis));
                        dos.writeBoolean(added);
                    }
                    break;
                }
                case "contains": {
                    final ByteBuffer key = readBytes(dis);
                    final boolean contains = cache.contains(key);
                    dos.writeBoolean(contains);
                    break;
                }
                case "remove": {
                    final ByteBuffer key = readBytes(dis);
                    cache.remove(key);
                    dos.writeBoolean(true);
                    break;
                }
                case "removeAll": {
                    cache.removeAll();
                    dos.writeBoolean(true);
                    break;
                }
                default: {
                    throw new IOException("Illegal Request");
                }
            }
        } finally {
            dos.flush();
        }

        return true;
    }

    private ByteBuffer readBytes(final DataInputStream dis) throws IOException {
        final int numBytes = dis.readInt();
        final byte[] buffer = new byte[numBytes];
        dis.readFully(buffer);
        return ByteBuffer.wrap(buffer);
    }

    private void createCache(final ConfigurationContext context) {
        final int maxSize = context.getProperty(SetCacheService.MAX_SIZE).asInteger();
        final Long expireDuration = context.getProperty(SetCacheService.AGE_OFF_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
        final int concurrencyLevel = context.getProperty(SetCacheService.CONCURRENCY_LEVEL).isSet() ?
                context.getProperty(SetCacheService.CONCURRENCY_LEVEL).asInteger() : -1;
        this.cache = new SetCacheImpl<>(maxSize, expireDuration, TimeUnit.MILLISECONDS, concurrencyLevel);
    }

    private void startServer(ConfigurationContext context) throws IOException {
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext;
        if (sslContextService == null) {
            sslContext = null;
        } else {
            sslContext = sslContextService.createSSLContext(ClientAuth.REQUIRED);
        }

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(true);
        serverSocketChannel.bind(new InetSocketAddress(port));

        final Runnable runnable = new Runnable() {

            @Override
            public void run() {
                while (true) {
                    final SocketChannel socketChannel;
                    try {
                        socketChannel = serverSocketChannel.accept();
                        logger.debug("Connected to {}", new Object[]{socketChannel});
                    } catch (final IOException e) {
                        if (!stopped) {
                            logger.error("{} unable to accept connection from remote peer due to {}", this, e.toString());
                            if (logger.isDebugEnabled()) {
                                logger.error("", e);
                            }
                        }
                        return;
                    }

                    final Runnable processInputRunnable = new Runnable() {
                        @Override
                        public void run() {
                            final InputStream rawInputStream;
                            final OutputStream rawOutputStream;
                            final String peer = socketChannel.socket().getInetAddress().getHostName();

                            try {
                                if (sslContext == null) {
                                    rawInputStream = new SocketChannelInputStream(socketChannel);
                                    rawOutputStream = new SocketChannelOutputStream(socketChannel);
                                } else {
                                    final SSLSocketChannel sslSocketChannel = new SSLSocketChannel(sslContext, socketChannel, false);
                                    sslSocketChannel.connect();
                                    rawInputStream = new SSLSocketChannelInputStream(sslSocketChannel);
                                    rawOutputStream = new SSLSocketChannelOutputStream(sslSocketChannel);
                                }
                            } catch (IOException e) {
                                logger.error("Cannot create input and/or output streams for {}", new Object[]{getIdentifier()}, e);
                                if (logger.isDebugEnabled()) {
                                    logger.error("", e);
                                }
                                try {
                                    socketChannel.close();
                                } catch (IOException swallow) { /* empty */ }

                                return;
                            }
                            try (final InputStream in = new BufferedInputStream(rawInputStream);
                                 final OutputStream out = new BufferedOutputStream(rawOutputStream)) {

                                final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(1);

                                ProtocolHandshake.receiveHandshake(in, out, versionNegotiator);

                                boolean continueComms = true;
                                while (continueComms) {
                                    continueComms = listen(in, out, versionNegotiator.getVersion());
                                }
                                // client has issued 'close'
                                logger.debug("Client issued close on {}", new Object[]{socketChannel});
                            } catch (final SocketTimeoutException e) {
                                logger.debug("30 sec timeout reached", e);
                            } catch (final IOException | HandshakeException e) {
                                if (!stopped) {
                                    logger.error("{} unable to communicate with remote peer {} due to {}", new Object[]{this, peer, e.toString()});
                                    if (logger.isDebugEnabled()) {
                                        logger.error("", e);
                                    }
                                }
                            } finally {
                                processInputThreads.remove(Thread.currentThread());
                            }
                        }
                    };

                    final Thread processInputThread = new Thread(processInputRunnable);
                    processInputThread.setName("Distributed Cache Server Communications Thread: " + getIdentifier());
                    processInputThread.setDaemon(true);
                    processInputThread.start();
                    processInputThreads.add(processInputThread);
                }
            }
        };

        final Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("Distributed Cache Server: " + getIdentifier());
        thread.start();

        stopped = false;
    }

    private void stopServer() throws IOException {
        stopped = true;

        logger.info("Stopping CacheServer {}", new Object[]{getIdentifier()});

        if (serverSocketChannel != null && serverSocketChannel.isOpen()) {
            serverSocketChannel.close();
        }
        // need to close out the created SocketChannels...this is done by interrupting
        // the created threads that loop on listen().
        for (Thread processInputThread : processInputThreads) {
            processInputThread.interrupt();
            int i = 0;
            while (!processInputThread.isInterrupted() && i++ < 5) {
                try {
                    Thread.sleep(50); // allow thread to gracefully terminate
                } catch (InterruptedException e) { /* empty */ }
            }
        }
        processInputThreads.clear();
    }
}
