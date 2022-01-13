package com.asymmetrik.nifi.processors.aws;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;


@PrimaryNodeOnly
@TriggerSerially
@Tags({"asymmetrik", "s3", "list", "aws"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Lists specified files from s3 gradually, unlike the ListS3 processor. Specification consists of a bucket, and " + 
    "optional prefix, start key and end key. A job snapshot is stored in the processor state, and needs to be cleared to restart listing."
)
@Stateful(description = "Stores the key of the last file listed by the processor, the completion state and the latest configuration.", scopes = {Scope.CLUSTER})
// @formatter:off
@WritesAttributes({
    @WritesAttribute(attribute = "s3.bucket",       description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "filename",        description = "The name of the file"),
    @WritesAttribute(attribute = "s3.etag",         description = "The ETag that can be used to see if the file has changed"),
    @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
    @WritesAttribute(attribute = "s3.size",         description = "The size of the object in bytes"),
    @WritesAttribute(attribute = "s3.storeClass",   description = "The storage class of the object")})
// @formatter:on
public class ListS3Batch extends AbstractProcessor {
    
    @VisibleForTesting
    static final String FILENAME_ATTRIBUTE = CoreAttributes.FILENAME.key();

    @VisibleForTesting
    static final String S3_BUCKET_ATTRIBUTE = "s3.bucket";

    @VisibleForTesting
    static final String S3_ETAG_ATTRIBUTE = "s3.etag";

    @VisibleForTesting
    static final String LAST_MODIFIED_ATTRIBUTE = "s3.lastModified";

    @VisibleForTesting
    static final String S3_SIZE_ATTRIBUTE = "s3.size";

    @VisibleForTesting
    static final String S3_STORECLASS_ATTRIBUTE = "s3.storeClass";

    @VisibleForTesting
    static final String LAST_ELEMENT_KEY = "lastElement";
    
    @VisibleForTesting
    static final String STARTED_AT = "startedAt";
    
    @VisibleForTesting
    static final String COMPLETED_AT = "completedAt";
    
    @VisibleForTesting
    static final String LAST_CONFIG = "lastConfig";
    
    @VisibleForTesting
    static final String OBJECTS_PROCESSED = "objectsProcessed";
    
    public static final PropertyDescriptor S3_BUCKET = new PropertyDescriptor.Builder()
            .name("S3 Bucket")
            .description("S3 bucket to list objects from.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor S3_PREFIX = new PropertyDescriptor.Builder()
            .name("File S3 Prefix")
            .description("S3 prefix to list files from. Defaults to the root of the bucket.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();
    
    public static final PropertyDescriptor S3_START_PREFIX = new PropertyDescriptor.Builder()
            .name("S3 Start Prefix")
            .description("Subprefix under File S3 Prefix marking the inclusive start point for listing files. "
                    + "Note that because of the way S3 keys work this will be exclusive of the specified object "
                    + "if you use a full object key instead of a prefix.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();
    
    public static final PropertyDescriptor S3_END_PREFIX = new PropertyDescriptor.Builder()
            .name("S3 End Prefix")
            .description("Subprefix under File S3 Prefix marking the exclusive end point for listing files.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max S3 Keys Per Batch")
            .description("Maximum number of keys to fetch per execution of ListS3. Highest allowable value is 1000.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 1000, true))
            .defaultValue("100")
            .build();

    static final PropertyDescriptor FETCH_OWNER = new PropertyDescriptor.Builder()
            .name("Fetch S3 Object's Owner")
            .description("Determines whether or not to return the owner of each object when fetching from s3.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor AWS_REGION = new PropertyDescriptor.Builder()
            .name("S3 Bucket AWS Region")
            .description("Determines which region that the S3 bucket is in. Default is us-east-1.")
            .defaultValue("us-east-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .description("The Controller Service that is used to obtain aws credentials provider")
            .required(true)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    public static final PropertyDescriptor OVERRIDE_ENDPOINT = new PropertyDescriptor.Builder()
            .name("AWS Endpoint Override")
            .description("Endpoint to use with the AWS client, as an override to the default endpoint.")
            .defaultValue(null)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .description("All successfully read S3 objects get transferred to this relationship.")
            .name("success")
            .build();

    private AmazonS3 client;
    
    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            AWS_REGION,
            S3_BUCKET,
            S3_PREFIX,
            S3_START_PREFIX,
            S3_END_PREFIX,
            BATCH_SIZE,
            FETCH_OWNER,
            OVERRIDE_ENDPOINT,
            SSL_CONTEXT_SERVICE
        );
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.client = getClient(context);
    }

    @OnStopped
    public void onStopped() {
        if (this.client != null) {
            this.client.shutdown();
            this.client = null;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String region = context.getProperty(AWS_REGION).getValue();
        final boolean fetchOwner = context.getProperty(FETCH_OWNER).asBoolean();
        final String bucketName = context.getProperty(S3_BUCKET)
                .evaluateAttributeExpressions()
                .getValue();
        final String prefix = context.getProperty(S3_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final String startKey = context.getProperty(S3_START_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final String endKey = context.getProperty(S3_END_PREFIX)
                .evaluateAttributeExpressions()
                .getValue();
        final int batchSize = context.getProperty(BATCH_SIZE)
                .evaluateAttributeExpressions()
                .asInteger();

        final String lastConfig = getItemFromContext(context, LAST_CONFIG);
        final String config = region + "," + bucketName + "," + prefix + "," + startKey + "," + endKey + "," + fetchOwner;
        final boolean propsHaveNotChanged = config.equals(lastConfig);

        // If the configuration properties have not changed, Yield if the current listing is complete
        if (propsHaveNotChanged && isComplete(context)) {
            context.yield();
            return;
        }
        
        // If the configuration properties have not changed, find where we left off in any prior iteration
        // Otherwise set the starting list point by the configured prefix and starting key
        final String startAfter = propsHaveNotChanged
                ? getItemFromContext(context, LAST_ELEMENT_KEY)
                : prefix + startKey;
        
        // Create the List S3 request
        final ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix)
                .withStartAfter(startAfter)
                .withMaxKeys(batchSize)
                .withFetchOwner(fetchOwner);

        // Execute the listS3 request
        final ListObjectsV2Result listResponse = client.listObjectsV2(request);
        List<S3ObjectSummary> s3obj = listResponse.getObjectSummaries();
        
        if (CollectionUtils.isEmpty(s3obj)) {
            persistState(context, null, config, true, 0);
            return;
        }
        
        boolean isDone = !listResponse.isTruncated();
        final String lastObjectKey = getLastObjectKey(s3obj);
        
        if (!endKey.isEmpty()) {
            final String fullEndKey = prefix + endKey;
            if (fullEndKey.compareTo(lastObjectKey) <= 0) {
                s3obj = s3obj.stream()
                        .filter(x -> x.getKey().compareTo(fullEndKey) < 0)
                        .collect(Collectors.toList());
                isDone = true;
            }
        }
        
        if (s3obj.isEmpty()) {
            persistState(context, null, config, true, 0);
            return;
        }
        
        // For each response value, create a FlowFile with the received metadata, and gather them into a list
        final List<FlowFile> flowFiles = s3obj.stream()
                .map(obj -> s3ObjectToFlowFile(obj, session, bucketName, fetchOwner))
                .collect(Collectors.toList());

        // Send all the FlowFiles to the success relationship
        session.transfer(flowFiles, SUCCESS);

        // Persist the processor state by saving the last object key, whether to continue listing files for the current prefix, and the control file contents.
        persistState(context, lastObjectKey, config, isDone, s3obj.size());
    }

    /**
     * Function to store processor state.
     * If isComplete is true, then the processor also persists the current time.
     * @param context The processor context
     * @param objectKey The key to store in the processor's state.
     * @param isComplete Whether the processor is done listing files. If true, it adds a flag to stop listing additional files.
     */
    private void persistState(final ProcessContext context, String lastKey, String config, boolean isComplete, int objProcessed) {
        try {
            final Map<String, String> state = new HashMap<>();

            final String lastConfig = getItemFromContext(context, LAST_CONFIG);
            final String alreadyProcessed = getItemFromContext(context, OBJECTS_PROCESSED);
            final int objectsProcessed = alreadyProcessed != null 
                    ? Integer.parseInt(alreadyProcessed) + objProcessed
                    : objProcessed;

            final String startedAt = config.equals(lastConfig)
                    ? getItemFromContext(context,STARTED_AT)
                    : Instant.now().toString();

            if (isComplete) {
                state.put(COMPLETED_AT, Instant.now().toString());
            }

            state.put(STARTED_AT, startedAt);
            state.put(LAST_ELEMENT_KEY, lastKey);
            state.put(LAST_CONFIG, config);
            state.put(OBJECTS_PROCESSED, String.valueOf(objectsProcessed));

            context.getStateManager().setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save local state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }
    

    /**
     * Function to convert an S3Object to a FlowFile.
     * @param obj The S3Object to convert to a FlowFile
     * @param session The processor's ProcessSession
     * @param bucketName The bucket that the file metadata was fetched from
     * @param fetchOwner Whether or not the owner was fetched by the listS3 request
     * @return FlowFile with attributes containing the s3 object's metadata, as defined in the processor's header.
     */
    private FlowFile s3ObjectToFlowFile(S3ObjectSummary obj, ProcessSession session, String bucketName, boolean fetchOwner) {

        final Map<String, String> attributes = new HashMap<>();
        
        attributes.put(FILENAME_ATTRIBUTE, obj.getKey());
        attributes.put(S3_BUCKET_ATTRIBUTE, bucketName);
        attributes.put(S3_ETAG_ATTRIBUTE, obj.getETag());
        attributes.put(LAST_MODIFIED_ATTRIBUTE, String.valueOf(obj.getLastModified().toInstant()));
        attributes.put(S3_SIZE_ATTRIBUTE, String.valueOf(obj.getSize()));
        attributes.put(S3_STORECLASS_ATTRIBUTE, obj.getStorageClass());

        if (fetchOwner) {
            attributes.put("s3.owner", obj.getOwner().getId());
        }

        return session.putAllAttributes(session.create(), attributes);
    }
    
    /**
     * Gets the key of the last object from a list of S3 objects.
     */
    private String getLastObjectKey(List<S3ObjectSummary> objects) {
        return objects.get(objects.size() - 1).getKey();
    }
    
    private String getItemFromContext(ProcessContext context, String item) throws ProcessException {
        try {
            StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            return stateMap.get(item);
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    private boolean isComplete(ProcessContext context) throws ProcessException {
        try {
            StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            return stateMap.get(COMPLETED_AT) != null;
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    private AWSCredentialsProvider getCredentials(final ProcessContext context) {
        final AWSCredentialsProviderService credsService = context
                .getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AWSCredentialsProviderService.class);

        return credsService.getCredentialsProvider();
    }

    private ClientConfiguration getClientConfiguration(ProcessContext context) {
        ClientConfiguration config = new ClientConfiguration();

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(ClientAuth.NONE);
            SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, new DefaultHostnameVerifier());
            config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
        }

        return config;
    }
    
    protected AmazonS3 getClient(ProcessContext context) {
        String region = context.getProperty(AWS_REGION)
                .getValue();
        String overrideEndpoint = context.getProperty(OVERRIDE_ENDPOINT)
                .evaluateAttributeExpressions()
                .getValue();

        AmazonS3 c = AmazonS3Client.builder()
                .withRegion(region)
                .withCredentials(getCredentials(context))
                .withClientConfiguration(getClientConfiguration(context))
                .build();

        if (overrideEndpoint != null) {
            client.setEndpoint(overrideEndpoint);
        }

        return c;
    }
}
