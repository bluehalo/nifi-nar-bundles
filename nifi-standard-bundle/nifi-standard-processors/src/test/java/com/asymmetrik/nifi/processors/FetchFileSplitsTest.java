package com.asymmetrik.nifi.processors;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FetchFileSplitsTest {

    @Test
    public void testValidWithoutSplits() {
        TestRunner runner = TestRunners.newTestRunner(FetchFileSplits.class);
        runner.setProperty(FetchFileSplits.LINES_PER_SPLIT, "10");
        runner.setProperty(FetchFileSplits.INCLUDE_HEADER, "true");
        runner.setProperty(FetchFileSplits.LINES_TO_SKIP, "1");
        runner.assertValid();

        runner.enqueue(new byte[0], ImmutableMap.of("filename", "src/test/resources/test.csv"));

        runner.run();

        runner.assertTransferCount(FetchFileSplits.REL_ORIGINAL, 1);
        runner.assertTransferCount(FetchFileSplits.REL_NOT_FOUND, 0);
        runner.assertTransferCount(FetchFileSplits.REL_PERMISSION_DENIED, 0);
        runner.assertTransferCount(FetchFileSplits.REL_FAILURE, 0);
        runner.assertTransferCount(FetchFileSplits.REL_SPLITS, 1);

        MockFlowFile mff = runner.getFlowFilesForRelationship(FetchFileSplits.REL_SPLITS).get(0);
        String[] lines = new String[]{
            "HEADER LINE",
            "1,plane",
            "2,train",
            "3,automobile",
            "4,helicopter",
            "5,motorcycle"
        };
        String expectedContent = StringUtils.join(lines, System.lineSeparator());
        mff.assertContentEquals(expectedContent);

    }

    @Test
    public void testValidWithSplits() {
        TestRunner runner = TestRunners.newTestRunner(FetchFileSplits.class);
        runner.setProperty(FetchFileSplits.LINES_PER_SPLIT, "2");
        runner.setProperty(FetchFileSplits.INCLUDE_HEADER, "true");
        runner.setProperty(FetchFileSplits.LINES_TO_SKIP, "1");
        runner.assertValid();

        runner.enqueue(new byte[0], ImmutableMap.of("filename", "src/test/resources/test.csv"));

        runner.run();

        runner.assertTransferCount(FetchFileSplits.REL_ORIGINAL, 1);
        runner.assertTransferCount(FetchFileSplits.REL_NOT_FOUND, 0);
        runner.assertTransferCount(FetchFileSplits.REL_PERMISSION_DENIED, 0);
        runner.assertTransferCount(FetchFileSplits.REL_FAILURE, 0);
        runner.assertTransferCount(FetchFileSplits.REL_SPLITS, 3);

        List<MockFlowFile> mffs = runner.getFlowFilesForRelationship(FetchFileSplits.REL_SPLITS);

        String opt1 = StringUtils.join(new String[] {
                "HEADER LINE",
                "1,plane",
                "2,train",
        }, System.lineSeparator());

        String opt2 = StringUtils.join(new String[] {
                "HEADER LINE",
                "3,automobile",
                "4,helicopter"
        }, System.lineSeparator());

        String opt3 = StringUtils.join(new String[] {
                "HEADER LINE",
                "5,motorcycle"
        }, System.lineSeparator());

        String[] options = new String[] {opt1, opt2, opt3};
        for(String option : options) {
            boolean oneMatch = false;
            // Assert that both splits are accounted for
            for(MockFlowFile mff : mffs) {
                if(mff.isContentEqual(option)) {
                    oneMatch = true;
                }
            }
            assertTrue(oneMatch);
        }

    }

}
