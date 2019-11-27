/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.execution.file;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.siddhi.extension.util.Constant.TAR_FILE_EXTENSION;
import static io.siddhi.extension.util.Constant.ZIP_FILE_EXTENSION;
import static io.siddhi.query.api.definition.Attribute.Type.BOOL;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to archive files.
 */
@Extension(
        name = "archive",
        namespace = "file",
        description = "Archives files and folders as a zip or in tar format that are available in the given file " +
                "uri.\n",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file or the directory",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute directory path of the the archived file.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "archive.type",
                        description = "Archive type can be zip or tar",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "zip"
                ),
                @Parameter(
                        name = "regexp",
                        description = "pattern to be checked before zipping the file.\n" +
                                "Note: Add an empty string to match all files",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "exclude.subdirectories",
                        description = "This flag is used to exclude the subdirectories and its files without " +
                                "archiving.",
                        type = DataType.BOOL,
                        defaultValue = "false",
                        optional = true
                ),
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type", "regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type", "regexp",
                                "exclude.subdirectories"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "isSuccess",
                        description = "Status of the file archive operation (true if success)",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file.zip')",
                        description = "Archives to_be_archived folder in zip format and stores archive_destination " +
                                "folder as file.zip."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', 'tar')",
                        description = "Archives to_be_archived folder in tar format and stores in " +
                                "archive_destination folder as file.tar."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', 'tar', '.*test3.txt$')",
                        description = "Archives files which adheres to '.*test3.txt$' regex in to_be_archived " +
                                "folder in tar format and stores in archive_destination folder as file.tar."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', '', '', 'false')",
                        description = "Archives to_be_archived folder excluding the sub-folders in zip format and " +
                                "stores in archive_destination folder as file.tar."
                )
        }
)
public class FileArchiveExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileArchiveExtension.class);
    private Pattern pattern;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String uri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String destinationDirUri = (String) attributeExpressionExecutors[1].execute(streamEvent);
            boolean excludeSubdirectories = false;
            String regex = "";
            String archiveType = ZIP_FILE_EXTENSION;
            if (attributeExpressionExecutors.length >= 3) {
                archiveType = (String) attributeExpressionExecutors[2].execute(streamEvent);
            }
            if (attributeExpressionExecutors.length >= 4) {
                regex = (String) attributeExpressionExecutors[3].execute(streamEvent);
            }
            if (attributeExpressionExecutors.length == 5) {
                excludeSubdirectories = (Boolean) attributeExpressionExecutors[4].execute(streamEvent);
            }
            pattern = Pattern.compile(regex);
            if (archiveType.compareToIgnoreCase(ZIP_FILE_EXTENSION) == 0) {
                File sourceFile = new File(uri);
                List<String> fileList = new ArrayList<>();
                generateFileList(uri, sourceFile, fileList, excludeSubdirectories);
                try {
                    zip(uri, destinationDirUri, fileList);
                } catch (IOException e) {
                    throw new SiddhiAppRuntimeException("IOException occurred when archiving  " + uri, e);
                }
            } else {
                try {
                    if (archiveType.compareToIgnoreCase(TAR_FILE_EXTENSION) == 0) {
                        addToTarArchiveCompression(
                                getTarArchiveOutputStream(destinationDirUri), new File(uri), uri);
                    } else {
                        throw new SiddhiAppRuntimeException("Unsupported archive type: " + archiveType);
                    }
                } catch (IOException e) {
                    throw new SiddhiAppRuntimeException("Exception occurred when archiving " + uri, e);
                }
            }
        }
    }

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length > 1 && attributeExpressionExecutors.length < 6) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to uri (first argument) " +
                        "file:archive() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for uri " +
                        "(first argument) of file:archive() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors[1] == null) {
                throw new SiddhiAppValidationException("Invalid input given to destination.dir.uri (first argument) " +
                        "file:archive() function. Argument cannot be null");
            }
            Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
            if (!(secondAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for destination.dir.uri " +
                        "(second argument) of file:archive() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors.length == 3) {
                if (attributeExpressionExecutors[2] == null) {
                    throw new SiddhiAppValidationException("Invalid input given to exclude.subdirectories " +
                            "(forth argument) file:archive() function. Argument cannot be null");
                }
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[2].getReturnType();
                if (!(thirdAttributeType == STRING)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for exclude.subdirectories " +
                            "(forth argument) of file:archive() function, required " + Attribute.Type.STRING +
                            " but found " + firstAttributeType.toString());
                }
            }
            if (attributeExpressionExecutors.length == 4) {
                if (attributeExpressionExecutors[3] == null) {
                    throw new SiddhiAppValidationException("Invalid input given to exclude.subdirectories " +
                            "(forth argument) file:archive() function. Argument cannot be null");
                }
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[3].getReturnType();
                if (!(thirdAttributeType == STRING)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for exclude.subdirectories " +
                            "(forth argument) of file:archive() function, required " + Attribute.Type.STRING +
                            " but found " + firstAttributeType.toString());
                }
            }
            if (attributeExpressionExecutors.length == 5) {
                if (attributeExpressionExecutors[4] == null) {
                    throw new SiddhiAppValidationException("Invalid input given to regex " +
                            "(third argument) file:archive() function. Argument cannot be null");
                }
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[4].getReturnType();
                if (!(thirdAttributeType == BOOL)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for regex " +
                            "(third argument) of file:archive() function, required " + Attribute.Type.STRING +
                            " but found " + firstAttributeType.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:archive() function, "
                    + "required 2 or 3, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("isSuccess", BOOL));
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    /**
     * Zip it
     *
     * @param zipFile output ZIP file location
     */
    private void zip(String sourceFileUri, String zipFile, List<String> fileList) throws IOException {
        byte[] buffer = new byte[1024];
        FileInputStream in = null;
        String filePath = null;
        ZipOutputStream zos = null;
        try {
            if (!zipFile.endsWith(TAR_FILE_EXTENSION)) {
                zipFile = zipFile.concat("." + ZIP_FILE_EXTENSION);
            }
            FileOutputStream fos = new FileOutputStream(zipFile);
            zos = new ZipOutputStream(fos);
            if (log.isDebugEnabled()) {
                log.debug("Output to Zip : " + zipFile + " started for folder/ file: " + sourceFileUri);
            }
            for (String file : fileList) {
                if (log.isDebugEnabled()) {
                    log.debug("File Adding : " + file + " to " + zipFile + ".");
                }
                ZipEntry ze = new ZipEntry(file);
                zos.putNextEntry(ze);
                in = new FileInputStream(sourceFileUri + File.separator + file);
                int len;
                while ((len = in.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                in.close();
            }
            zos.closeEntry();
            if (log.isDebugEnabled()) {
                log.debug("Output to Zip : " + zipFile + " is complete for folder/ file: " + sourceFileUri);
            }
        }  finally {
            if (in != null) {
                in.close();
            }
            if (zos != null) {
                zos.close();
            }
        }
    }

    private void addToTarArchiveCompression(TarArchiveOutputStream out, File file, String sourceFileUri)
            throws IOException {
        if (file.isFile()) {
            String entryName = generateZipEntry(file.getAbsoluteFile().toString(), sourceFileUri);
            TarArchiveEntry entry = new TarArchiveEntry(file, entryName);
            out.putArchiveEntry(entry);
            FileInputStream fin = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fin);
            IOUtils.copy(bis, out);
            out.closeArchiveEntry();
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addToTarArchiveCompression(out, child, sourceFileUri);
                }
            }
        } else {
            log.error(file.getName() + " is not supported for archiving. Archiving process continues..");
        }
    }

    private static TarArchiveOutputStream getTarArchiveOutputStream(String name) throws IOException {
        if (!name.endsWith(TAR_FILE_EXTENSION)) {
            name = name.concat("." + TAR_FILE_EXTENSION);
        }
        TarArchiveOutputStream taos = new TarArchiveOutputStream(new FileOutputStream(name));
        // TAR has an 8 gig file limit by default, this gets around that
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
        // TAR originally didn't support long file names, so enable the support for it
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        taos.setAddPaxHeadersForNonAsciiNames(true);
        return taos;
    }

    /**
     * Traverse a directory and get all files,
     * and add the file into fileList
     *
     * @param node file or directory
     */
    private void generateFileList(String sourceFileUri, File node, List<String> fileList,
                                  boolean excludeSubdirectories) {
        //add file only
        if (node.isFile() && pattern.matcher(node.getName()).lookingAt()) {
            fileList.add(generateZipEntry(node.getAbsoluteFile().toString(), sourceFileUri));
        }
        if (node.isDirectory() && !excludeSubdirectories) {
            String[] subNote = node.list();
            if (subNote != null) {
                for (String filename : subNote) {
                    generateFileList(sourceFileUri, new File(node, filename), fileList, false);
                }
            }
        }
    }

    /**
     * Format the file path for zip
     *
     * @param file file path
     * @return Formatted file path
     */
    private String generateZipEntry(String file, String sourceFileUri) {
        return file.substring(sourceFileUri.length());
    }
}
