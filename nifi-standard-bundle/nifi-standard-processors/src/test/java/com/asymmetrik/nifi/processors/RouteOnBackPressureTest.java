package com.asymmetrik.nifi.processors;


import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class RouteOnBackPressureTest {

    @Test
    public void testBackPressured() {
        final TestRunner runner = TestRunners.newTestRunner(RouteOnBackPressure.class);
        runner.assertValid();

        for (int i = 0; i < 100; ++i) {
            runner.enqueue(new byte[0]);
        }

        runner.run(100);

        runner.assertTransferCount(RouteOnBackPressure.PASS_THROUGH, 100);
        runner.assertTransferCount(RouteOnBackPressure.BACK_PRESSURED, 0);

        runner.setRelationshipUnavailable(RouteOnBackPressure.PASS_THROUGH);

        for (int i = 0; i < 100; ++i) {
            runner.enqueue(new byte[0]);
        }

        runner.run(100);

        runner.assertTransferCount(RouteOnBackPressure.PASS_THROUGH, 100);
        runner.assertTransferCount(RouteOnBackPressure.BACK_PRESSURED, 100);
    }
}
