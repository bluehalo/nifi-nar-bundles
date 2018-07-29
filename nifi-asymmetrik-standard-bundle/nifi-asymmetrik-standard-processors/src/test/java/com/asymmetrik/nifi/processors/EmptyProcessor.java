package com.asymmetrik.nifi.processors;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * No-op processor thats entire purpose is to provide a process context for NiFi unit tests
 */
public class EmptyProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext arg0, ProcessSession arg1)
            throws ProcessException {
        // Do nothing
    }

}
