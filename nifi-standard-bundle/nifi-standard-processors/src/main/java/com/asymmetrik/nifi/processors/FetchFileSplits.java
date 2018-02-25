package com.asymmetrik.nifi.processors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * Note: this is adapted from Apache NiFi's FetchFile processor, which gets credit for handling
 * all file processing and completion actions. This processor is primarily concerned with the
 * adaptation of reading file splits directly as a means of handling large files more efficiently
 * than reading them into NiFi and splitting within NiFi in a separate process.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"asymmetrik", "get", "file", "split"})
@CapabilityDescription("Reads in a file from the local filesystem and splits it while reading it in.")
public class FetchFileSplits extends AbstractProcessor {

    static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Moves the file to the directory specified by the <Move Destination Directory> property");
    static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the file system");

    static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File", "The newly ingested file should replace the existing file in the Destination Directory");
    static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing", "The existing file should in the Destination Directory should stay intact and the newly "
            + "ingested file should be deleted");
    static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail", "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
    static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename", "The existing destination file should remain intact. The newly ingested file should be moved to the "
            + "destination directory but be renamed to a random filename");


    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Upon successful completion of the file fetch and split, the " +
                "original FlowFile is routed to 'original'")
        .build();
    static final Relationship REL_SPLITS = new Relationship.Builder()
        .name("splits")
        .description("The splits of the input file are routed to 'splits'")
        .build();
    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
        .name("not.found")
        .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
        .build();
    static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder()
        .name("permission.denied")
        .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If any errors occur during reading or splitting, the " +
                "original flow file is routed to 'failure'")
        .build();

    static final PropertyDescriptor LINES_TO_SKIP = new PropertyDescriptor.Builder()
        .name("Lines to Skip")
        .description("The number of lines to skip before reading lines for the splits.")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("0")
        .build();
    static final PropertyDescriptor INCLUDE_HEADER = new PropertyDescriptor.Builder()
        .name("Include Header Line")
        .description("Whether or not to read and include a header line that will be " +
                "repeated for each split FlowFile")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
        .defaultValue("false")
        .build();
    static final PropertyDescriptor LINES_PER_SPLIT = new PropertyDescriptor.Builder()
        .name("Lines per Split")
        .description("The number of lines to be included in each split, not including the " +
                "header line (if applicable).")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("1")
        .build();
    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
        .name("Filename")
        .description("Name of the file that will be retrieved from the file system")
        .required(true)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .defaultValue("${filename}")
        .build();
    static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
        .name("Completion Strategy")
        .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
        .expressionLanguageSupported(false)
        .allowableValues(COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE)
        .defaultValue(COMPLETION_NONE.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor MOVE_DESTINATION_DIR = new PropertyDescriptor.Builder()
        .name("Move Destination Directory")
        .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". "
                + "If the directory does not exist, it will be created.")
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();
    static final PropertyDescriptor CONFLICT_STRATEGY = new PropertyDescriptor.Builder()
        .name("Move Conflict Strategy")
        .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies "
                + "how that naming conflict should be resolved")
        .allowableValues(CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL)
        .defaultValue(CONFLICT_RENAME.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor FILE_NOT_FOUND_LOG_LEVEL = new PropertyDescriptor.Builder()
        .name("Log level when file not found")
        .description("Log level to use in case the file does not exist when the processor is triggered")
        .allowableValues(LogLevel.values())
        .defaultValue(LogLevel.ERROR.toString())
        .required(true)
        .build();
    static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = new PropertyDescriptor.Builder()
        .name("Log level when permission denied")
        .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
        .allowableValues(LogLevel.values())
        .defaultValue(LogLevel.ERROR.toString())
        .required(true)
        .build();

    private final List<PropertyDescriptor> properties =
            ImmutableList.of(LINES_TO_SKIP, INCLUDE_HEADER, LINES_PER_SPLIT,
                    FILENAME, COMPLETION_STRATEGY, MOVE_DESTINATION_DIR,
                    FILE_NOT_FOUND_LOG_LEVEL, PERM_DENIED_LOG_LEVEL);

    private final Set<Relationship> relationships =
            ImmutableSet.of(REL_ORIGINAL, REL_SPLITS, REL_NOT_FOUND,
                    REL_PERMISSION_DENIED, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        if (COMPLETION_MOVE.getValue().equalsIgnoreCase(validationContext.getProperty(COMPLETION_STRATEGY).getValue())) {
            if (!validationContext.getProperty(MOVE_DESTINATION_DIR).isSet()) {
                results.add(new ValidationResult.Builder().subject(MOVE_DESTINATION_DIR.getName())
                        .input(null).valid(false)
                        .explanation(MOVE_DESTINATION_DIR.getName() + " must be specified if " +
                            COMPLETION_STRATEGY.getName() + " is set to " +
                            COMPLETION_MOVE.getDisplayName()).build());
            }
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        final LogLevel levelFileNotFound = LogLevel.valueOf(context.getProperty(FILE_NOT_FOUND_LOG_LEVEL).getValue());
        final LogLevel levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());
        final File file = new File(filename);

        // Verify that file system is reachable and file exists
        Path filePath = file.toPath();
        if (!Files.exists(filePath) && !Files.notExists(filePath)){ // see https://docs.oracle.com/javase/tutorial/essential/io/check.html for more details
            getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure",
                    new Object[] {file, flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } else if (!Files.exists(filePath)) {
            getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", new Object[] {file, flowFile});
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
            return;
        }

        // Verify read permission on file
        final String user = System.getProperty("user.name");
        if (!isReadable(file)) {
            getLogger().log(levelPermDenied, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied",
                    new Object[] {file, flowFile, user});
            session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
            session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
            return;
        }

        // If configured to move the file and fail if unable to do so, check that the existing file does not exist and that we have write permissions for the parent file.
        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        final String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
        if (targetDirectoryName != null) {
            final File targetDir = new File(targetDirectoryName);
            if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
                    getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                    + "but that is not a directory or user {} does not have permissions to write to that directory",
                            new Object[] {file, flowFile, targetDir, user});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }

                final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();

                if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
                    final File targetFile = new File(targetDir, file.getName());
                    if (targetFile.exists()) {
                        getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                        + "but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure",
                                new Object[] {file, flowFile, targetDir, file.getName()});
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }
            }
        }

        // import content from file system
        int linesToSkip = context.getProperty(LINES_TO_SKIP).evaluateAttributeExpressions(flowFile).asInteger();
        boolean includeHeader = context.getProperty(INCLUDE_HEADER).evaluateAttributeExpressions(flowFile).asBoolean();
        int linesPerSplit = context.getProperty(LINES_PER_SPLIT).evaluateAttributeExpressions(flowFile).asInteger();

        try (final FileInputStream fis = new FileInputStream(file)) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));

            for(int i=0; i < linesToSkip; i++) {
                bufferedReader.readLine();
            }

            final String headerLine = includeHeader ? bufferedReader.readLine() : null;

            FlowFile flowFileSplit = createWithHeader(session, flowFile, headerLine);

            int writtenLines = 0;
            StringBuffer sb = new StringBuffer();
            String nextLine;
            while( (nextLine = bufferedReader.readLine()) != null ) {

                getLogger().info("Read line: " + nextLine);
                sb.append(System.lineSeparator()).append(nextLine);
                writtenLines += 1;

                if(writtenLines >= linesPerSplit) {
                    // Write the current split to a FF
                    writeAndTransfer(session, flowFileSplit, sb);

                    // Prep for the next split
                    writtenLines = 0;
                    sb = new StringBuffer();
                    flowFileSplit = createWithHeader(session, flowFile, headerLine);
                }
            }

            // If any lines were written before we reached the next split threshold,
            if(writtenLines > 0) {
                getLogger().info("Some lines remained afterwards!");
                writeAndTransfer(session, flowFileSplit, sb);
            }
            else {
                session.remove(flowFileSplit);
            }

        } catch (final IOException ioe) {
            getLogger().error("Could not fetch file {} from file system for {} due to {}; routing to failure", new Object[] {file, flowFile, ioe.toString()}, ioe);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_ORIGINAL);

        // It is critical that we commit the session before we perform the Completion Strategy. Otherwise, we could have a case where we
        // ingest the file, delete/move the file, and then NiFi is restarted before the session is committed. That would result in data loss.
        // As long as we commit the session right here, before we perform the Completion Strategy, we are safe.
        session.commit();

        // Attempt to perform the Completion Strategy action
        Exception completionFailureException = null;
        if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
            // convert to path and use Files.delete instead of file.delete so that if we fail, we know why
            try {
                delete(file);
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
            final File targetDirectory = new File(targetDirectoryName);
            final File targetFile = new File(targetDirectory, file.getName());
            try {
                if (targetFile.exists()) {
                    final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                    if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // don't move, just delete the original
                        Files.delete(file.toPath());
                    } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // rename to add a random UUID but keep the file extension if it has one.
                        final String simpleFilename = targetFile.getName();
                        final String newName;
                        if (simpleFilename.contains(".")) {
                            newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID().toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
                        } else {
                            newName = simpleFilename + "-" + UUID.randomUUID().toString();
                        }

                        move(file, new File(targetDirectory, newName), false);
                    } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
                        move(file, targetFile, true);
                    }
                } else {
                    move(file, targetFile, false);
                }
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        }

        // Handle completion failures
        if (completionFailureException != null) {
            getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success",
                    new Object[] {file, flowFile, completionFailureException}, completionFailureException);
        }
    }

    private FlowFile createWithHeader(final ProcessSession session, final FlowFile flowFile, final String headerLine) {
        FlowFile flowFileSplit = session.clone(flowFile, 0, 0);
        if(headerLine != null) {
            final ComponentLog logger = getLogger();
            flowFileSplit = session.write(flowFileSplit, os -> {
                logger.info("Writing new header line: " + headerLine);
                os.write(headerLine.getBytes(StandardCharsets.UTF_8));
            });
        }
        return flowFileSplit;
    }

    private void writeAndTransfer(final ProcessSession session, FlowFile flowFileSplit, final StringBuffer sb) throws IOException {
        final String splitContent = sb.toString();
        getLogger().info("Writing content: " + splitContent);
        flowFileSplit = session.append(flowFileSplit, os ->
                os.write(splitContent.getBytes(StandardCharsets.UTF_8)));

        InputStream read = session.read(flowFileSplit);
        List<String> lines = IOUtils.readLines(read);
        read.close();

        getLogger().info("Transferring FF with contents: " + StringUtils.join(lines, "\n"));

        session.transfer(flowFileSplit, REL_SPLITS);
    }

    protected void move(final File source, final File target, final boolean overwrite) throws IOException {
        final File targetDirectory = target.getParentFile();

        // convert to path and use Files.move instead of file.renameTo so that if we fail, we know why
        final Path targetPath = target.toPath();
        if (!targetDirectory.exists()) {
            Files.createDirectories(targetDirectory.toPath());
        }

        final CopyOption[] copyOptions = overwrite ? new CopyOption[] {StandardCopyOption.REPLACE_EXISTING} : new CopyOption[] {};
        Files.move(source.toPath(), targetPath, copyOptions);
    }

    protected void delete(final File file) throws IOException {
        Files.delete(file.toPath());
    }

    protected boolean isReadable(final File file) {
        return file.canRead();
    }

    protected boolean isWritable(final File file) {
        return file.canWrite();
    }

    protected boolean isDirectory(final File file) {
        return file.isDirectory();
    }
}
