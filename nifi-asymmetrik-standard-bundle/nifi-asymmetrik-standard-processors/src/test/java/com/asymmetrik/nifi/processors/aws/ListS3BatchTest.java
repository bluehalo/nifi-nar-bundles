package com.asymmetrik.nifi.processors.aws;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static com.asymmetrik.nifi.processors.aws.ListS3Batch.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class ListS3BatchTest {

    private TestRunner runner;

    private AmazonS3 mockClient = Mockito.mock(AmazonS3.class);

    private static final String BUCKET_NAME = "test-bucket";

    private static final S3ObjectSummary OBJECT1 = new S3ObjectSummary();
    static {
        OBJECT1.setSize(1L);
        OBJECT1.setKey("test1");
        OBJECT1.setETag("tag1");
        OBJECT1.setStorageClass("STANDARD");
        OBJECT1.setLastModified(Date.from(Instant.EPOCH));
    }

    private static final S3ObjectSummary OBJECT2 = new S3ObjectSummary();
    static {
        OBJECT2.setSize(2L);
        OBJECT2.setKey("test2");
        OBJECT2.setETag("tag2");
        OBJECT2.setStorageClass("REDUCED_REDUNDANCY");
        OBJECT2.setLastModified(Date.from(Instant.ofEpochMilli(1637182274751L)));
    }

    private static final ListObjectsV2Result RESPONSE1 = new ListObjectsV2Result();
    static {
        RESPONSE1.getObjectSummaries().addAll(ImmutableList.of(OBJECT1));
        RESPONSE1.setTruncated(true);
    }

    private static final ListObjectsV2Result RESPONSE2 = new ListObjectsV2Result();
    static {
        RESPONSE2.getObjectSummaries().addAll(ImmutableList.of(OBJECT2));
        RESPONSE2.setTruncated(false);
    }

    private class MockAWSCredentialsProviderService extends AbstractControllerService implements AWSCredentialsProviderService {

        @Override
        public AWSCredentialsProvider getCredentialsProvider() throws ProcessException {
            return null;
        }

    }

    @Before
    public void setUp() throws Exception {
        
        runner = TestRunners.newTestRunner(new ListS3Batch() {
            protected AmazonS3 getClient(ProcessContext context) {
                return mockClient;
            }
        });

        MockAWSCredentialsProviderService mockCredsService = new MockAWSCredentialsProviderService();
        runner.addControllerService("mockCredsService", mockCredsService);
        runner.enableControllerService(mockCredsService);

        runner.setValidateExpressionUsage(false);

        runner.setProperty(BATCH_SIZE, "1");
        runner.setProperty(S3_BUCKET, BUCKET_NAME);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "mockCredsService");
    }

    @Test
    public void testLimitFlow() {

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(RESPONSE1)
                .thenReturn(RESPONSE2);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 1);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "1", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test1", Scope.CLUSTER);
        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }

    @Test
    public void testCorrectAttributesAdded() {

        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(RESPONSE1)
                .thenReturn(RESPONSE2);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 1);
        final MockFlowFile flowFile1 = runner.getFlowFilesForRelationship(SUCCESS).get(0);

        flowFile1.assertAttributeEquals(S3_BUCKET_ATTRIBUTE, BUCKET_NAME);
        flowFile1.assertAttributeEquals(FILENAME_ATTRIBUTE, "test1");
        flowFile1.assertAttributeEquals(S3_ETAG_ATTRIBUTE, "tag1");
        flowFile1.assertAttributeEquals(LAST_MODIFIED_ATTRIBUTE, Instant.EPOCH.toString());
        flowFile1.assertAttributeEquals(S3_SIZE_ATTRIBUTE, String.valueOf(1L));
        flowFile1.assertAttributeEquals(S3_STORECLASS_ATTRIBUTE, "STANDARD");

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "1", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test1", Scope.CLUSTER);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);
        final MockFlowFile flowFile2 = runner.getFlowFilesForRelationship(SUCCESS).get(1);

        flowFile2.assertAttributeEquals(S3_BUCKET_ATTRIBUTE, BUCKET_NAME);
        flowFile2.assertAttributeEquals(FILENAME_ATTRIBUTE, "test2");
        flowFile2.assertAttributeEquals(S3_ETAG_ATTRIBUTE, "tag2");
        flowFile2.assertAttributeEquals(LAST_MODIFIED_ATTRIBUTE, Instant.ofEpochMilli(1637182274751L).toString());
        flowFile2.assertAttributeEquals(S3_SIZE_ATTRIBUTE, String.valueOf(2L));
        flowFile2.assertAttributeEquals(S3_STORECLASS_ATTRIBUTE, "REDUCED_REDUNDANCY");

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);

        runner.run(1, false);
        runner.assertTransferCount(SUCCESS, 2);

        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "2", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, "test2", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    
    @Test
    public void testEndKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_END_PREFIX, "03/A");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        

        final List<S3ObjectSummary> respS3Obj = makeS3ObjectSummarys(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        final List<ListObjectsV2Result> responses = makeResponses(respS3Obj, batchSize);
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(responses.get(0), responses.subList(1,responses.size()).toArray(new ListObjectsV2Result[0]));

        for (int i = 0; i < responses.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        final List<S3ObjectSummary> expS3Obj = makeS3ObjectSummarys(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/"), ImmutableList.of("A","B"));
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.getKey()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "4", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    @Test
    public void testStartKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_START_PREFIX, "02/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        
        final List<S3ObjectSummary> allS3Obj = makeS3ObjectSummarys(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        setupMockClient(allS3Obj, batchSize);
        for (int i = 0; i < allS3Obj.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        List<S3ObjectSummary> expS3Obj = allS3Obj.stream().filter((x) -> "one/abc/02/B".compareTo(x.getKey()) < 0).collect(Collectors.toList());
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.getKey()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "4", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }
    
    @Test
    public void testStartAndEndKey() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_START_PREFIX, "01/B");
        runner.setProperty(S3_END_PREFIX, "03/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        
        final List<S3ObjectSummary> allS3Obj = makeS3ObjectSummarys(ImmutableList.of("one/"), ImmutableList.of("abc/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        setupMockClient(allS3Obj, batchSize);
        for (int i = 0; i < allS3Obj.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream().map((x) -> x.getAttributes().get("filename")).collect(Collectors.toList());
        
        List<S3ObjectSummary> expS3Obj = allS3Obj.stream().filter((x) -> "one/abc/01/B".compareTo(x.getKey()) < 0 & x.getKey().compareTo("one/abc/03/B") < 0).collect(Collectors.toList());
        final List<String> expKeys = expS3Obj.stream().map((x) -> x.getKey()).collect(Collectors.toList());
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "3", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }

    @Test
    public void testStartAndEndKeyNonAscii() {
        
        final int batchSize = 3;
        
        runner.setProperty(S3_PREFIX, "one/ßƒ˚˜©®/");
        runner.setProperty(S3_START_PREFIX, "01/B");
        runner.setProperty(S3_END_PREFIX, "03/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        
        final List<S3ObjectSummary> allS3Obj = makeS3ObjectSummarys(ImmutableList.of("one/"), ImmutableList.of("ßƒ˚˜©®/"), ImmutableList.of("01/","02/","03/","04/"), ImmutableList.of("A","B"));
        setupMockClient(allS3Obj, batchSize);
        for (int i = 0; i < allS3Obj.size(); ++i) {
            runner.run();
        }
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(SUCCESS);
        final List<String> actKeys = flowFiles.stream()
                .map((x) -> x.getAttributes().get("filename"))
                .collect(Collectors.toList());
        
        List<S3ObjectSummary> expS3Obj = allS3Obj.stream()
                .filter((x) -> "one/ßƒ˚˜©®/01/B".compareTo(x.getKey()) < 0 & x.getKey()
                .compareTo("one/ßƒ˚˜©®/03/B") < 0)
                .collect(Collectors.toList());
        final List<String> expKeys = expS3Obj.stream()
                .map((x) -> x.getKey())
                .collect(Collectors.toList());
                
        assertEquals(expKeys, actKeys);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "3", Scope.CLUSTER);
        runner.getStateManager().assertStateSet(COMPLETED_AT, Scope.CLUSTER);
    }

    @Test
    public void testNoResults() {
        final int batchSize = 3;

        runner.setProperty(S3_PREFIX, "one/abc/");
        runner.setProperty(S3_START_PREFIX, "01/B");
        runner.setProperty(S3_END_PREFIX, "03/B");
        runner.setProperty(BATCH_SIZE, String.valueOf(batchSize));
        setupMockClient(ImmutableList.of(), batchSize);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(SUCCESS, 0);
        runner.getStateManager().assertStateEquals(LAST_CONFIG, "us-east-1,test-bucket,one/abc/,01/B,03/B,false", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(OBJECTS_PROCESSED, "0", Scope.CLUSTER);
        runner.getStateManager().assertStateEquals(LAST_ELEMENT_KEY, null, Scope.CLUSTER);
    }
    
    private void setupMockClient(List<S3ObjectSummary> allS3Obj, int batchSize) {
        when(mockClient.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(
                invocation -> {
                    ListObjectsV2Request request = invocation.getArgument(0);
                    if (request.getStartAfter() != null) {
                        List<S3ObjectSummary> afterS3Obj = allS3Obj.stream().filter((x) -> request.getStartAfter().compareTo(x.getKey()) < 0).collect(Collectors.toList());
                        if (afterS3Obj.isEmpty()) {
                            ListObjectsV2Result toReturn = new ListObjectsV2Result();
                                toReturn.getObjectSummaries().addAll(new ArrayList<S3ObjectSummary>());
                                toReturn.setTruncated(false);
                            return toReturn;
                        }
                        else {
                            return makeResponses(afterS3Obj,batchSize).get(0);
                        }
                    }
                    else {
                        return makeResponses(allS3Obj,batchSize).get(0);
                    }
                }
            );
    }
    
    private List<S3ObjectSummary> makeS3ObjectSummarys(List<String> as, List<String> bs, List<String> cs, List<String> ds) {
        List<S3ObjectSummary> out = new ArrayList<>();
        for (final String a : as) {
            for (final String b : bs) {
                for (final String c : cs) {
                    for (final String d : ds) {
                        final String key = a + b + c + d;
                        final S3ObjectSummary o = new S3ObjectSummary();
                        o.setSize(1L);
                        o.setKey(key);
                        o.setETag("tag1");
                        o.setStorageClass("STANDARD");
                        o.setLastModified(Date.from(Instant.EPOCH));
                        out.add(o);
                    }
                }
            }
        }
        return out;
    }
    
    private List<ListObjectsV2Result> makeResponses(List<S3ObjectSummary> s3obj, int batchSize) {
        final List<ListObjectsV2Result> out = new ArrayList<>();
        for(int i=0; i < s3obj.size(); i+=batchSize) {
            final List<S3ObjectSummary> sub = s3obj.subList(i,Math.min(i+batchSize,s3obj.size()));
            final ListObjectsV2Result response = new ListObjectsV2Result();
            response.getObjectSummaries().addAll(sub);
            response.setTruncated(true);
            out.add(response);
        }
        return out;
    }
    
}
