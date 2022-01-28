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
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.util.Constant;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class provides implementation to list files in a compressed file.
 */
@Extension(
        name = "searchInArchive",
        namespace = "file",
        description = "This.",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path of the zip or tar file.",
                        type = {DataType.STRING},
                        dynamic = true
                ),
                @Parameter(
                        name = "include.by.regexp",
                        description = "Only the files matching the patterns will be searched.\n" +
                                "Note: Add an empty string to match all files",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. \n" +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'\n" +
                                "Note: when IDENTITY is used, use a RSA PRIVATE KEY",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "file.system.options"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "fileNameList",
                        description = "The list file names in the archived file.",
                        type = {DataType.OBJECT})},
        examples = {
                @Example(
                        syntax = "ListArchivedFileStream#file:listFilesInArchive(filePath)",
                        description = "Lists the files inside the compressed file in the given path."
                ),
                @Example(
                        syntax = "ListArchivedFileStream#file:listFilesInArchive(filePath, '.*test3.txt$')",
                        description = "Filters file names adheres to the given regex and lists the files inside the " +
                                "compressed file in the given path."
                )
        }
)
public class FileSearchInArchiveExtension extends StreamFunctionProcessor {
    private static final Logger log = LogManager.getLogger(FileSearchInArchiveExtension.class);
    private Pattern pattern = null;
    private int inputExecutorLength;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 4 &&
                attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue().toString();
        }
        return null;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("fileNameList", Attribute.Type.OBJECT));
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    protected Object[] process(Object[] data) {
        String zipFilePathUri = (String) data[0];
        String regex = "";
        List<String> fileList = new ArrayList<>();
        if (inputExecutorLength >= 2) {
            regex = (String) data[1];
        }
        if (pattern == null) {
            pattern = Pattern.compile(regex);
        }
        ZipInputStream zip = null;
        TarArchiveInputStream tarInput = null;
        try {
            FileObject fileObj = Utils.getFileObject(zipFilePathUri, fileSystemOptions);
            if (fileObj.isFile()) {
                if (zipFilePathUri.endsWith(Constant.ZIP_FILE_EXTENSION)) {
                    InputStream input = fileObj.getContent().getInputStream();
                    zip = new ZipInputStream(input);
                    ZipEntry zipEntry;
                    // iterates over entries in the zip file
                    while ((zipEntry = zip.getNextEntry()) != null) {
                        if (!zipEntry.isDirectory()) {
                            //add the entries
                            if (pattern.matcher(zipEntry.getName()).lookingAt()) {
                                fileList.add(zipEntry.getName());
                            }
                        }
                    }
                } else if (zipFilePathUri.endsWith(Constant.TAR_FILE_EXTENSION)) {
                    tarInput = new TarArchiveInputStream(new FileInputStream(zipFilePathUri));
                    TarArchiveEntry tarEntry;
                    while ((tarEntry = tarInput.getNextTarEntry()) != null) {
                        if (!tarEntry.isDirectory()) {
                            if (pattern.matcher(tarEntry.getName()).lookingAt()) {
                                fileList.add(tarEntry.getName());
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Error while processing file: " + zipFilePathUri + ". " +
                    e.getMessage(), e);
        } finally {
            if (zip != null) {
                try {
                    zip.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing zip input stream for file path:" +
                            zipFilePathUri, e);
                }
            }
            if (tarInput != null) {
                try {
                    tarInput.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing tar input stream for file path:" +
                            zipFilePathUri, e);
                }
            }
        }
        return new Object[]{fileList};
    }

    @Override
    protected Object[] process(Object data) {
        return process(new Object[]{data});
    }
}
