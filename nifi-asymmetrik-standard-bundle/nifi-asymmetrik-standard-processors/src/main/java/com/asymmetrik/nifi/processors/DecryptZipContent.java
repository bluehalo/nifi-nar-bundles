package com.asymmetrik.nifi.processors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import com.asymmetrik.nifi.processors.util.ZipDecryptInputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"unzip", "zip", "decompress", "password", "decrypt"})
@CapabilityDescription(
        "Unzips incoming content, assuming it is the contents of a password-protected zip file. This processor "
                + "can decrypt Regular password-protected zip files as well as AES-256 encrypted zip files. In which case the "
                + "incoming payload is required to be flushed to disk prior to decryption and intermediate files will "
                + "be deleted automatically. The output of this processor will be the "
                + "unencrypted zip file with the same zip entries.")
public class DecryptZipContent extends AbstractProcessor {

    static final String AES256 = "AES256";
    static final String REGULAR = "Regular";

    static final PropertyDescriptor PROP_ENCRYPTION_TYPE = new PropertyDescriptor.Builder()
            .name("encryption")
            .displayName("Encryption Type")
            .description("The type of encryption used.")
            .allowableValues(AES256, REGULAR)
            .defaultValue(REGULAR)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_ENCRYPTION_PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Encryption Password")
            .description("The password used to decrypt the contents of the zip file.")
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Unencrypted zip file entries will be sent to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Zip files that cannot be processed will be routed to this relationship")
            .build();

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (null == flowFile) {
            return;
        }

        // final List<FlowFile> flowFiles = new ArrayList<>();
        PropertyValue passwordProp = context.getProperty(PROP_ENCRYPTION_PASSWORD);
        final String password =
                passwordProp.isSet() ? passwordProp.evaluateAttributeExpressions(flowFile).getValue() : null;

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        try {
            String type = context.getProperty(PROP_ENCRYPTION_TYPE).getValue();
            final byte[] unencryptedByteArray = type.equals(REGULAR) ?
                    convertNormalEncryption(password, buffer) :
                    convertAes256Encryption(password, buffer);
            flowFile = session.write(flowFile, outStream -> outStream.write(unencryptedByteArray));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private byte[] convertNormalEncryption(String password, byte[] buffer) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ZipEntry entry;

        InputStream byteStream = new ByteArrayInputStream(buffer);
        InputStream zipStream = (null == password) ? byteStream : new ZipDecryptInputStream(byteStream, password);
        ByteArrayOutputStream baos;

        try (ZipInputStream zipInputStream = new ZipInputStream(zipStream);
                ZipArchiveOutputStream zipOutputStream = new ZipArchiveOutputStream(output)) {
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }

                // Read this entry into a byte array output stream
                baos = new ByteArrayOutputStream();
                IOUtils.copy(zipInputStream, baos);
                byte[] bytes = baos.toByteArray();
                String text = new String(bytes, StandardCharsets.UTF_8);

                byte[] transformed = text.getBytes(StandardCharsets.UTF_8);
                ZipArchiveEntry transformedEntry = new ZipArchiveEntry(entry.getName());
                transformedEntry.setSize(transformed.length);
                zipOutputStream.putArchiveEntry(transformedEntry);
                zipOutputStream.write(transformed);
                zipOutputStream.closeArchiveEntry();
            }
            zipOutputStream.finish();
            zipOutputStream.flush();
        }
        return output.toByteArray();
    }

    private byte[] convertAes256Encryption(String password, byte[] buffer) throws IOException, ZipException {

        File tempFile = null;
        File zipParentDir = null;

        // Write contents to disk
        tempFile = File.createTempFile(this.getClass().getSimpleName() + "-", ".zip");
        FileUtils.writeByteArrayToFile(tempFile, buffer, true);

        ZipFile zipFile = new ZipFile(tempFile);
        if (zipFile.isEncrypted()) {
            zipFile.setPassword(password);
        }

        String zipFilePath = tempFile.getAbsolutePath();
        zipParentDir = new File(zipFilePath.substring(0, zipFilePath.lastIndexOf(".")));
        FileUtils.forceMkdir(zipParentDir);

        // Get the list of files from the zip
        List fileHeaderList = zipFile.getFileHeaders();
        for (Object headerList : fileHeaderList) {

            // Get File headers
            FileHeader fileHeader = (FileHeader) headerList;

            // Skip Directories within the Zip File
            if (fileHeader.isDirectory()) {
                continue;
            }

            String filename = fileHeader.getFileName();

            // extract file into directory with same name as temporary zip file, minus the .zip extension
            zipFile.extractFile(fileHeader, zipParentDir.getAbsolutePath(), null,
                    filename.substring(filename.lastIndexOf("/") + 1));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (File file : zipParentDir.listFiles()) {
                ZipEntry entry = new ZipEntry(file.getName());
                entry.setSize(file.length());
                zos.putNextEntry(entry);
                zos.write(FileUtils.readFileToByteArray(file));
                zos.closeEntry();
            }
            tempFile.delete();
            FileUtils.deleteDirectory(zipParentDir);
        }
        return baos.toByteArray();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROP_ENCRYPTION_TYPE, PROP_ENCRYPTION_PASSWORD);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS, REL_FAILURE);
    }
}
