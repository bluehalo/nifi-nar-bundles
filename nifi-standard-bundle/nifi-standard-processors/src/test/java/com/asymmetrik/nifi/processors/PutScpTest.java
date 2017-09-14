package com.asymmetrik.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static com.asymmetrik.nifi.processors.PutScp.*;
import static org.junit.Assert.assertTrue;

@Ignore("Test requires password-less ssh to localhost")
public class PutScpTest {
    private static File TMP_DIR;
    private static Map<String, String> ATTRIBUTES;

    private static String TEST_FILE_NAME = "TestPutScp";
    private static String HOST = "localhost";
    private static String USER = System.getenv("USER");
    private static String KEY = System.getenv("HOME") + "/.ssh/id_rsa";

    private TestRunner runner;
    private byte[] input, input2;

    @BeforeClass
    public static void setupClass() throws IOException {
        TMP_DIR = Files.createTempDirectory("TestPutScp").toFile();
        ATTRIBUTES = ImmutableMap.of(CoreAttributes.FILENAME.key(), TEST_FILE_NAME);
    }

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutScp.class);
        runner.setValidateExpressionUsage(false);

        runner.setProperty(AbstractScpProcessor.HOSTNAME, HOST);
        runner.setProperty(AbstractScpProcessor.USERNAME, USER);
        runner.setProperty(AbstractScpProcessor.PRIVATE_KEY_PATH, KEY);
        runner.setProperty(AbstractScpProcessor.REMOTE_PATH, TMP_DIR.getAbsolutePath());

        try (InputStream in = getClass().getResourceAsStream("/sample.json")) {
            input = IOUtils.toByteArray(in);
        }
        try (InputStream in = getClass().getResourceAsStream("/sample2.json")) {
            input2 = IOUtils.toByteArray(in);
        }
    }

    @After
    public void teardown() throws Exception {
        if (TMP_DIR.exists()) {
            for (final File file : TMP_DIR.listFiles()) {
                if (file.isFile()) {
                    assertTrue(file.delete());
                }
            }
        }
    }

    private void assertFileExists(File parent, String name) {
        File file = new File(parent, name);
        assertTrue(file.exists());
    }

    @Test
    public void testValid() {
        runner.assertValid();
    }

    @Test
    public void testUnauthenticated() {
        runner.removeProperty(AbstractScpProcessor.PRIVATE_KEY_PATH);
        runner.enqueue(input);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }

    @Test
    public void testAbsoluteDest() {
        runner.enqueue(input, ATTRIBUTES);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, TEST_FILE_NAME);

    }

    @Test
    public void testRejectZeroLengthFile() {
        runner.setProperty(AbstractScpProcessor.REJECT_ZERO_BYTE, "true");
        runner.enqueue(new byte[0]);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_REJECT, 1);
    }

    @Test
    public void testAllowZeroLengthFile() {
        runner.setProperty(AbstractScpProcessor.REJECT_ZERO_BYTE, "false");
        runner.enqueue(new byte[0], ATTRIBUTES);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, TEST_FILE_NAME);
    }

    @Test
    public void testUseCompression() {
        runner.setProperty(AbstractScpProcessor.USE_COMPRESSION, "true");
        runner.enqueue(input, ATTRIBUTES);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, TEST_FILE_NAME);
    }

    @Test
    public void testPermissions() {
        runner.setProperty(AbstractScpProcessor.PERMISSIONS, "0700");
        runner.enqueue(input, ATTRIBUTES);

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, TEST_FILE_NAME);
    }

    @Test
    public void testBatch() {
        runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testBatch-1.json"));
        runner.enqueue(input2, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testBatch-2.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        assertFileExists(TMP_DIR, "testBatch-1.json");
        assertFileExists(TMP_DIR, "testBatch-2.json");
    }

    @Test
    public void testLargerBatch() {
        final int count = 100;

        for (int i = 0; i < count; i++) {
            runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testLargeBatch-" + i + ".json"));
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, count);
        for (int i = 0; i < count; i++) {
            assertFileExists(TMP_DIR, "testLargeBatch-" + i + ".json");
        }
    }

    @Test
    public void testSuccessiveRuns() {
        runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testSuccessiveRuns-1.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, "testSuccessiveRuns-1.json");

        runner.clearTransferState();
        runner.enqueue(input2, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testSuccessiveRuns-2.json"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertFileExists(TMP_DIR, "testSuccessiveRuns-2.json");
    }

    @Test
    public void testOneRejectRemainingSuccess() {
        runner.setProperty(AbstractScpProcessor.REJECT_ZERO_BYTE, "true");

        runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testBatch-1.json"));
        runner.enqueue(new byte[0], ImmutableMap.of(CoreAttributes.FILENAME.key(), "testRejectZeroLengthFile.json"));
        runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testBatch-2.json"));
        runner.enqueue(input, ImmutableMap.of(CoreAttributes.FILENAME.key(), "testBatch-3.json"));

        runner.run();
        runner.assertTransferCount(REL_REJECT, 1);
        runner.assertTransferCount(REL_SUCCESS, 3);
        assertFileExists(TMP_DIR, "testBatch-1.json");
        assertFileExists(TMP_DIR, "testBatch-2.json");
        assertFileExists(TMP_DIR, "testBatch-3.json");
    }
}
