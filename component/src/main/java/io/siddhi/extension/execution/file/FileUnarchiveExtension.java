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
import io.siddhi.extension.util.Constant;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.siddhi.extension.io.file.util.Constants.BUFFER_SIZE;
import static io.siddhi.query.api.definition.Attribute.Type.BOOL;

/**
 * This extension can be used to unarchive a file.
 */
@Extension(
        name = "unarchive",
        namespace = "file",
        description = "This function decompresses a given file",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file to be decompressed in the format of zip or tar.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute path of the destination directory.\n" +
                                "Note: If the folder structure does not exist, it will be created.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "exclude.root.folder",
                        description = "This flag excludes parent folder when extracting the content.",
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "exclude.root.folder"}
                )
        },
        examples = {
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.zip', '/User/wso2/destination')",
                        description = "Unarchive a zip file in a given path to a given destination."
                ),
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination')",
                        description = "Unarchive a tar file in a given path to a given destination."
                ),
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination', true)",
                        description = "Unarchive a tar file in a given path to a given destination excluding " +
                                "the root folder."
                )
        }
)
public class FileUnarchiveExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileUnarchiveExtension.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String filePathUri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String destinationDirUri = (String) attributeExpressionExecutors[1].execute(streamEvent);
            boolean excludeRootFolder = false;
            if (attributeExpressionExecutors.length == 3) {
                excludeRootFolder = (Boolean) attributeExpressionExecutors[2].execute(streamEvent);
            }
            FileObject sourceFileObject = Utils.getFileObject(filePathUri);
            String sourceFileExtension = sourceFileObject.getName().getExtension();
            if (!excludeRootFolder) {
                if (destinationDirUri.endsWith(File.separator)) {
                    destinationDirUri = destinationDirUri.substring(0, destinationDirUri.length() - 1);
                }
                String fileName = sourceFileObject.getName().getBaseName();
                destinationDirUri =
                        destinationDirUri.concat(File.separator +
                                fileName.substring(0, fileName.lastIndexOf(sourceFileExtension) - 1));
            }
            File destinationDirFile = new File(destinationDirUri);
            // create output directory if it doesn't exist
            if (!destinationDirFile.exists()) {
                if (!destinationDirFile.mkdirs()) {
                    throw new SiddhiAppRuntimeException("Directory cannot be created to unzip the file: " +
                            filePathUri);
                }
            }
            FileInputStream fis = null;
            //buffer for read and write data to file
            byte[] buffer = new byte[BUFFER_SIZE];
            ZipInputStream zis = null;
            String filePath = null;
            FileOutputStream fos = null;
            try {
                if (sourceFileExtension.compareToIgnoreCase(Constant.ZIP_FILE_EXTENSION) == 0) {
                    fis = new FileInputStream(filePathUri);
                    zis = new ZipInputStream(fis);
                    ZipEntry ze = zis.getNextEntry();
                    while (ze != null) {
                        String fileName = ze.getName();
                        filePath = destinationDirUri + File.separator + fileName;
                        File newFile = new File(filePath);
                        if (log.isDebugEnabled()) {
                            log.debug("Decompressing: " + newFile.getAbsolutePath());
                        }
                        createParentDirectory(newFile, filePathUri);
                        fos = new FileOutputStream(newFile);
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                        fos.close();
                        //close this ZipEntry
                        zis.closeEntry();
                        ze = zis.getNextEntry();
                    }
                    //close last ZipEntry
                    zis.closeEntry();
                } else if (sourceFileExtension.compareToIgnoreCase(Constant.TAR_FILE_EXTENSION) == 0) {
                    try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(filePathUri))) {
                        TarArchiveEntry entry;
                        while ((entry = fin.getNextTarEntry()) != null) {
                            if (entry.isDirectory()) {
                                continue;
                            }
                            File curfile = new File(destinationDirFile, entry.getName());
                            createParentDirectory(curfile, filePathUri);
                            IOUtils.copy(fin, new FileOutputStream(curfile));
                        }
                    }
                } else {
                    throw new SiddhiAppRuntimeException("Unsupported extension found for file: " + filePathUri +
                            ". Function only supports zip and tar files.");
                }
            } catch (IOException e) {
                throw new SiddhiAppRuntimeException("Exception occurred when getting the decompressing file: " +
                        filePathUri, e);
            } finally {
                if (zis != null) {
                    try {
                        zis.close();
                    } catch (IOException e) {
                        log.error("IO exception occurred when closing zip input stream for file path: " + filePath);
                    }
                }
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (IOException e) {
                        log.error("IO exception occurred when closing file output stream for file path: " + filePath);
                    }
                }
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (IOException e) {
                        log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                    }
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
        if (attributeExpressionExecutors.length == 2 || attributeExpressionExecutors.length == 3) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to uri (first argument) " +
                        "file:unarchive() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for uri " +
                        "(first argument) of file:unarchive() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors[1] == null) {
                throw new SiddhiAppValidationException("Invalid input given to destination.dir.uri (first argument) " +
                        "file:unarchive() function. Argument cannot be null");
            }
            Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
            if (!(secondAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for destination.dir.uri " +
                        "(second argument) of file:unarchive() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors.length == 3) {
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[2].getReturnType();
                if (!(thirdAttributeType == Attribute.Type.BOOL)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for exclude.root.folder " +
                            "(third argument) of file:unarchive() function, required " + Attribute.Type.BOOL +
                            " but found " + firstAttributeType.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:unarchive() function, "
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

    private void createParentDirectory(File file, String filePathUri) {
        if (!file.getParentFile().exists()) {
            boolean suDirCreateResult = new File(file.getParent()).mkdirs();
            if (!suDirCreateResult) {
                throw new SiddhiAppRuntimeException("Subdirectories cannot be created to decompress the " +
                        "file: " + filePathUri);
            }
        }
    }
}
