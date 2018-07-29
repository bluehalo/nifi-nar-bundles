package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "scp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to a remote server via scp")
public class PutScp extends AbstractScpProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the remote system; failure is usually looped back to this processor")
            .build();
    public static final Relationship REL_REJECT = new Relationship.Builder()
            .name("reject")
            .description("FlowFiles that were rejected by the destination system")
            .build();

    private final List<PropertyDescriptor> properties;
    private final Set<Relationship> relationships;

    private ComponentLog logger;
    private int batchSize;

    public PutScp() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(PORT);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(PRIVATE_KEY_PATH);
        properties.add(PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_PATH);
        properties.add(BATCH_SIZE);
        properties.add(CONNECTION_TIMEOUT);
        properties.add(DATA_TIMEOUT);
        properties.add(REJECT_ZERO_BYTE);
        properties.add(HOST_KEY_FILE);
        properties.add(STRICT_HOST_KEY_CHECKING);
        properties.add(USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(USE_COMPRESSION);
        properties.add(PERMISSIONS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_REJECT);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void initialize(final ProcessContext context) {
        this.batchSize = context.getProperty(BATCH_SIZE).asInteger();
        this.logger = getLogger();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession processSession) {
        List<FlowFile> flowFiles = processSession.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        Session jschSession = null;
        Channel channel = null;
        try {
            jschSession = openSession(context);
            final String remotePath = context.getProperty(REMOTE_PATH).evaluateAttributeExpressions().getValue();
            channel = openExecChannel(context, jschSession, "scp -r -d -t " + remotePath);

            InputStream channelIn = channel.getInputStream();
            OutputStream channelOut = channel.getOutputStream();

            channel.connect();
            waitForAck(channelIn);

            ListIterator<FlowFile> fileIt = flowFiles.listIterator();
            while (fileIt.hasNext()) {
                final FlowFile flowFile = fileIt.next();

                // conditionally reject files that are zero bytes or less
                if (context.getProperty(REJECT_ZERO_BYTE).asBoolean() && flowFile.getSize() == 0) {
                    logger.warn("Rejecting {} because it is zero bytes", new Object[]{flowFile});
                    processSession.transfer(processSession.penalize(flowFile), REL_REJECT);
                    fileIt.remove();
                    continue;
                }

                final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                final String permissions = context.getProperty(PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();

                // destination path + filename
                // final String fullPath = buildFullPath(context, flowFile, filename);

                processSession.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream flowFileIn) throws IOException {
                        // send "C0644 filesize filename", where filename should not include '/'
                        StringBuilder command = new StringBuilder("C").append(permissions).append(' ');
                        command.append(flowFile.getSize()).append(' ');
                        command.append(filename).append('\n');

                        channelOut.write(command.toString().getBytes(StandardCharsets.UTF_8));
                        channelOut.flush();
                        waitForAck(channelIn);

                        IOUtils.copy(flowFileIn, channelOut);
                        channelOut.flush();
                        sendAck(channelOut);
                        waitForAck(channelIn);
                    }
                });

                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.getProvenanceReporter().send(flowFile, remotePath);
                fileIt.remove();
                if (logger.isDebugEnabled()) {
                    logger.debug("Sent {} to remote host", new Object[]{flowFile});
                }
            }

        } catch (JSchException | IOException ex) {
            context.yield();
            logger.error("Unable to create session to remote host due to {}", new Object[]{ex}, ex);
            processSession.transfer(flowFiles, REL_FAILURE);

        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (jschSession != null) {
                jschSession.disconnect();
            }
        }
    }
}
