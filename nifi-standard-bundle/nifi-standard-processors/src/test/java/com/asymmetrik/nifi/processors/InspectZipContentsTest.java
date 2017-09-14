package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class InspectZipContentsTest {

    @Test
    public void attributeIsNotEmpty() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new InspectZipContents());

        byte[] bytes = readBytes("sample.zip");
        runner.enqueue(bytes);
        runner.run();
        MockFlowFile ffOut = runner.getFlowFilesForRelationship(InspectZipContents.REL_SUCCESS).get(0);
        String entry = ffOut.getAttribute(InspectZipContents.ATTR);
        Assert.assertEquals("test.txt", entry);
    }

    private byte[] readBytes(String filename) throws IOException {
        String dir = "src/test/resources";
        return Files.readAllBytes(Paths.get(dir, filename));
    }

}