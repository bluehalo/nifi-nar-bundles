package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.LoggerFactory;

public abstract class AbstractScpProcessor extends AbstractProcessor {
    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The fully qualified hostname or IP address of the remote system")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The port that the remote system is listening on for file transfers")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .required(true)
            .defaultValue("22")
            .build();
    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password for the user account")
            .addValidator(Validator.VALID)
            .required(false)
            .sensitive(true)
            .build();
    static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
            .name("Private Key Path")
            .description("The fully qualified path to the Private Key file")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("Private Key Passphrase")
            .description("Password for the private key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
            .name("Remote Path")
            .description("The absolute path on the remote system from which to pull or push files")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to send in a single connection")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();
    static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Data Timeout")
            .description("When transferring a file between the local and remote system, this value specifies how long is allowed to elapse without any data being transferred between systems")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .build();
    static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Amount of time to wait before timing out while creating a connection")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .build();
    static final PropertyDescriptor REJECT_ZERO_BYTE = new PropertyDescriptor.Builder()
            .name("Reject Zero-Byte Files")
            .description("Determines whether or not Zero-byte files should be rejected without attempting to transfer")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor HOST_KEY_FILE = new PropertyDescriptor.Builder()
            .name("Host Key File")
            .description("If supplied, the given file will be used as the Host Key; otherwise, no use host key file will be used")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor STRICT_HOST_KEY_CHECKING = new PropertyDescriptor.Builder()
            .name("Strict Host Key Checking")
            .description("Indicates whether or not strict enforcement of hosts keys should be applied")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final PropertyDescriptor USE_KEEPALIVE_ON_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Send Keep Alive On Timeout")
            .description("Indicates whether or not to send a single Keep Alive message when SSH socket times out")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    static final PropertyDescriptor USE_COMPRESSION = new PropertyDescriptor.Builder()
            .name("Use Compression")
            .description("Indicates whether or not ZLIB compression should be used when transferring files")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
    static final PropertyDescriptor PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("The permissions to assign to the file after transferring it. Format must be "
                    + "an octal number (e.g. 0644).")
            .required(true)
            .defaultValue("0644")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static {
        JSch.setLogger(new com.jcraft.jsch.Logger() {
            @Override
            public boolean isEnabled(int level) {
                return true;
            }

            @Override
            public void log(int level, String message) {
                LoggerFactory.getLogger(AbstractScpProcessor.class).debug("SFTP Log: {}", message);
            }
        });
    }

    protected Session openSession(final ProcessContext context) throws JSchException {
        final JSch jsch = new JSch();
        final String hostKeyVal = context.getProperty(HOST_KEY_FILE).getValue();
        if (hostKeyVal != null) {
            jsch.setKnownHosts(hostKeyVal);
        }

        final Session session = jsch.getSession(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(PORT).evaluateAttributeExpressions().asInteger().intValue());

        final Properties properties = new Properties();
        properties.setProperty("StrictHostKeyChecking", context.getProperty(STRICT_HOST_KEY_CHECKING).asBoolean() ? "yes" : "no");
        properties.setProperty("PreferredAuthentications", "publickey,password,keyboard-interactive");

        final PropertyValue compressionValue = context.getProperty(USE_COMPRESSION);
        if (compressionValue != null && "true".equalsIgnoreCase(compressionValue.getValue())) {
            properties.setProperty("compression.s2c", "zlib@openssh.com,zlib,none");
            properties.setProperty("compression.c2s", "zlib@openssh.com,zlib,none");
        } else {
            properties.setProperty("compression.s2c", "none");
            properties.setProperty("compression.c2s", "none");
        }

        session.setConfig(properties);

        final String privateKeyFile = context.getProperty(PRIVATE_KEY_PATH).evaluateAttributeExpressions().getValue();
        if (privateKeyFile != null) {
            jsch.addIdentity(privateKeyFile, context.getProperty(PRIVATE_KEY_PASSPHRASE).evaluateAttributeExpressions().getValue());
        }

        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        if (password != null) {
            session.setPassword(password);
        }

        session.setTimeout(context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        session.connect();
        return session;
    }

    protected Channel openExecChannel(final ProcessContext context, final Session session, final String command) throws JSchException {
        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        channel.setCommand(command);

        session.setTimeout(context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        if (!context.getProperty(USE_KEEPALIVE_ON_TIMEOUT).asBoolean()) {
            session.setServerAliveCountMax(0); // do not send keepalive message on SocketTimeoutException
        }

        return channel;
    }

    /**
     * Send an ack.
     *
     * @param out the output stream to use
     * @throws IOException on error
     */
    protected void sendAck(final OutputStream out) throws IOException {
        final byte[] buf = new byte[1];
        buf[0] = 0;
        out.write(buf);
        out.flush();
    }

    /**
     * Reads the response, throws a BuildException if the response
     * indicates an error.
     *
     * @param in the input stream to use
     * @throws IOException on I/O error
     */
    protected void waitForAck(final InputStream in)
            throws IOException {
        final int b = in.read();

        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,

        if (b == -1) {
            // didn't receive any response
            throw new IOException("No response from server");
        } else if (b != 0) {
            final StringBuilder sb = new StringBuilder();

            int c = in.read();
            while (c > 0 && c != '\n') {
                sb.append((char) c);
                c = in.read();
            }

            if (b == 1) {
                throw new IOException("server indicated an error: " + sb.toString());
            } else if (b == 2) {
                throw new IOException("server indicated a fatal error: " + sb.toString());
            } else {
                throw new IOException("unknown response, code " + b + " message: " + sb.toString());
            }
        }
    }
}
