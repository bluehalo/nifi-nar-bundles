package com.asymmetrik.nifi.services.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoIterable;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;

@Tags({"asymmetrik", "mongo", "database", "connection"})
@CapabilityDescription("Provides Mongo Database Client Service.")
public class StandardMongoClientService extends AbstractControllerService implements MongoClientService {

    private static final int DEFAULT_PORT = 27017;
    private MongoClient mongoClient;

    public static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("MONGO_HOSTS")
            .displayName("Mongo Hosts")
            .description("A comma-separated list of mongo hosts, eg. e01sv01:27017, e01sv02:27017, e01sv03:27017")
            .defaultValue("localhost:27017")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

    public static final PropertyDescriptor MAX_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Pool Size")
            .description("The maximum number of connections in the connection pool. The default value is 100")
            .defaultValue("100")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MIN_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum Pool Size")
            .description("The minimum number of connections in the connection pool. The default value is 0")
            .defaultValue("0")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username used while attempting to authenticate.")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the associated with the username.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor AUTH_DATABASE = new PropertyDescriptor.Builder()
            .name("Authentication Database")
            .description("The name of the authenticating database to connect to.")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties = ImmutableList.of(
            HOSTS, SSL_CONTEXT_SERVICE, CLIENT_AUTH, AUTH_DATABASE, USERNAME, PASSWORD, MIN_POOL_SIZE, MAX_POOL_SIZE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        List<ServerAddress> addresses = parseServerAddresses(context.getProperty(HOSTS).evaluateAttributeExpressions().getValue());

        MongoClientOptions clientOptions = getClientOptions(context);

        PropertyValue username = context.getProperty(USERNAME);
        PropertyValue password = context.getProperty(PASSWORD);
        PropertyValue database = context.getProperty(AUTH_DATABASE);
        if (username.isSet() && password.isSet() && database.isSet()) {
            MongoCredential credential = MongoCredential.createCredential(username.getValue(), database.getValue(), password.getValue().toCharArray());
            mongoClient = new MongoClient(addresses, Arrays.asList(credential), clientOptions);
        } else {
            mongoClient = new MongoClient(addresses, clientOptions);
        }

        MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
        boolean hasNext = false;
        try {
            hasNext = dbNames.iterator().hasNext();
        } catch (Exception ex) {
            throw new InitializationException("Unable to find Mongo DBs", ex);
        }
        if (!hasNext) {
            throw new InitializationException("Unable to find Mongo DBs");
        }
    }

    @OnDisabled
    public void onDisabled() {
        try {
            mongoClient.close();
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        PropertyValue usernameProperty = context.getProperty(USERNAME);
        PropertyValue passwordProperty = context.getProperty(PASSWORD);
        PropertyValue authDatabaseProperty = context.getProperty(AUTH_DATABASE);

        boolean valid = true;
        if (usernameProperty.isSet() || passwordProperty.isSet() || authDatabaseProperty.isSet()) {
            valid = usernameProperty.isSet() && passwordProperty.isSet() && authDatabaseProperty.isSet();
        }

        results.add(new ValidationResult.Builder()
                .explanation("Using authentication requires Username, Password, and the Authentication Database")
                .valid(valid)
                .subject("Mongo Authentication")
                .build());

        return results;
    }

    List<ServerAddress> parseServerAddresses(String hostlist) {
        List<ServerAddress> addresses = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(hostlist, ",; ");
        while (tokenizer.hasMoreTokens()) {
            String host = tokenizer.nextToken();
            String[] split = host.split(":");
            int port = DEFAULT_PORT;
            if (split.length == 2) {
                port = Integer.parseInt(split[1]);
            }
            addresses.add(new ServerAddress(split[0], port));
        }
        return addresses;
    }

    /**
     * Build and return the MongoClientOptions based on the input configuration
     * @param context - configuration context
     * @return options to use to create a Mongo client
     */
    protected MongoClientOptions getClientOptions(final ConfigurationContext context) {
        int minConnectionsPerHost = context.getProperty(MIN_POOL_SIZE).isSet() ? context.getProperty(MIN_POOL_SIZE).asInteger() : 0;
        int maxConnectionsPerHost = context.getProperty(MAX_POOL_SIZE).isSet() ? context.getProperty(MAX_POOL_SIZE).asInteger() : 100;

        Builder builder = MongoClientOptions.builder()
                .minConnectionsPerHost(minConnectionsPerHost)
                .connectionsPerHost(maxConnectionsPerHost);

        // Only add / set the SSL context if it was provided. Otherwise, use the default socket factory
        SSLContext sslContext = getSslContext(context);
        if(sslContext != null) {
            /*
             * Need to set socket factory after setting SSL Enabled.
             * Setting or resetting sslEnabled will set the SocketFactory
             * to SSLSocketFactory.getDefault() or SocketFactory.getDefault(), respectively
             */
            builder.sslEnabled(true)
                .socketFactory(sslContext.getSocketFactory());
        }

        return builder.build();
    }

    /**
     * Based on the input context for this service, returns either a valid SSL Context or null
     * @param context - configuration context
     * @return the instance of the SSL context to use for this Mongo client, or null if none was configured
     */
    protected SSLContext getSslContext(final ConfigurationContext context) {
        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();
        final SSLContext sslContext;

        if (sslService != null) {
            final SSLContextService.ClientAuth clientAuth;
            if (StringUtils.isBlank(rawClientAuth)) {
                clientAuth = SSLContextService.ClientAuth.REQUIRED;
            } else {
                try {
                    clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
                } catch (final IllegalArgumentException iae) {
                    throw new ProviderCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                            rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
                }
            }
            sslContext = sslService.createSSLContext(clientAuth);
        } else {
            sslContext = null;
        }

        return sslContext;
    }
}
