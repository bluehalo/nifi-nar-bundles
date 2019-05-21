package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class DecryptZipContentTest {

    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = newTestRunner(DecryptZipContent.class);
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void emptyBytesTest() {
        runner.setProperty(DecryptZipContent.PROP_ENCRYPTION_PASSWORD, "password");
        runner.assertValid();
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptZipContent.REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @Test
    public void passwordlessTest() throws IOException {
        byte[] bytes = readBytes("passwordless.zip");
        runner.enqueue(bytes);
        runner.assertValid();
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptZipContent.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        String compressedContents = new String(flowFiles.get(0).toByteArray(), StandardCharsets.UTF_8);
        assertTrue(compressedContents.startsWith("PK"));
        assertTrue(compressedContents.contains("one.txt"));
        assertTrue(compressedContents.contains("two.txt"));
        assertTrue(compressedContents.contains("three.txt"));
    }

    @Test
    public void simpleRegularEncryptionTest() throws Exception {
        runner.setProperty(DecryptZipContent.PROP_ENCRYPTION_PASSWORD, "password");
        byte[] bytes = readBytes("encrypted.zip");
        runner.enqueue(bytes);
        runner.assertValid();
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptZipContent.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        String compressedContents = new String(flowFiles.get(0).toByteArray(), StandardCharsets.UTF_8);
        assertTrue(compressedContents.startsWith("PK") && compressedContents.contains("test1.txt"));
    }


    @Test
    public void simpleAES256EncryptionTest() throws IOException {
        runner.setProperty(DecryptZipContent.PROP_ENCRYPTION_PASSWORD, "testing");
        runner.setProperty(DecryptZipContent.PROP_ENCRYPTION_TYPE, DecryptZipContent.AES256);
        runner.enqueue(readBytes("AES256Test.zip"));
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptZipContent.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        String compressedContents = new String(flowFiles.get(0).toByteArray(), StandardCharsets.UTF_8);
        assertTrue(compressedContents.startsWith("PK"));
    }

    @Test
    public void invalidEncryptionTest() throws Exception {
        runner.setProperty(DecryptZipContent.PROP_ENCRYPTION_PASSWORD, "password");
        byte[] bytes = readBytes("invalid.zip");
        runner.enqueue(bytes);
        runner.assertValid();
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptZipContent.REL_FAILURE);
        assertEquals(1, flowFiles.size());
    }

    private byte[] readBytes(String filename) throws IOException {
        String dir = "src/test/resources/DecryptZipContent";
        return Files.readAllBytes(Paths.get(dir, filename));
    }
}
