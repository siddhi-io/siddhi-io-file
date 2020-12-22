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
import io.siddhi.extension.io.file.metrics.FileCopyMetrics;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This extension can be used to copy files from a particular source to a destination.
 */
@Extension(
        name = "copy",
        namespace = "file",
        description = "This function performs copying file from one directory to another.\n",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the File or the directory.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute path of the destination directory.\n" +
                                "Note: Parent folder structure will be created if it does not exist.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "include.by.regexp",
                        description = "Only the files matching the patterns will be copied.\n" +
                                "Note: Add an empty string to match all files",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "exclude.root.dir",
                        description = "This flag is used to exclude parent folder when copying the content.",
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. " +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "include.by.regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "include.by.regexp", "exclude.root.dir"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "include.by.regexp", "exclude.root.dir",
                                "file.system.options"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "isSuccess",
                        description = "Status of the file copying operation (true if success)",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "InputStream#file:copy('/User/wso2/source/test.txt', 'User/wso2/destination/')",
                        description = "Copies 'test.txt' in 'source' folder to the 'destination' folder."
                ),
                @Example(
                        syntax = "InputStream#file:copy('/User/wso2/source/', 'User/wso2/destination/')",
                        description = "Copies 'source' folder to the 'destination' folder with all its content"
                ),
                @Example(
                        syntax = "InputStream#" +
                                "file:copy('/User/wso2/source/', 'User/wso2/destination/', '.*test3.txt$')",
                        description = "Copies 'source' folder to the 'destination' folder ignoring files doesnt " +
                                "adhere to the given regex."
                ),
                @Example(
                        syntax = "InputStream#" +
                                "file:copy('/User/wso2/source/', 'User/wso2/destination/', '', true)",
                        description = "Copies only the files resides in 'source' folder to 'destination' folder."
                )
        }
)
public class FileCopyExtension extends StreamFunctionProcessor {

    private static final Logger log = Logger.getLogger(FileCopyExtension.class);
    private Pattern pattern = null;
    private int inputExecutorLength;
    private FileCopyMetrics fileCopyMetrics;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (attributeExpressionExecutors.length >= 3 &&
                attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            pattern = Pattern.compile(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[2]).getValue().toString());
        }
        if (inputExecutorLength == 5 &&
                attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue().toString();
        }
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        Constants.PROMETHEUS_REPORTER_NAME)) {
                    String siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
                    fileCopyMetrics = new FileCopyMetrics(siddhiAppName);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence file metrics will not be initialized.");
            }
        }
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<>();
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    protected Object[] process(Object[] data) {
        String uri = (String) data[0];
        String destinationDirUri = (String) data[1];
        String regex = "";
        boolean excludeRootFolder = false;
        if (inputExecutorLength == 3) {
            regex = (String) data[2];
        }
        if (pattern == null) {
            pattern = Pattern.compile(regex);
        }
        FileObject rootFileObject = null;
        try {
            rootFileObject = Utils.getFileObject(uri, fileSystemOptions);
            if (rootFileObject.getType().hasContent() &&
                    pattern.matcher(rootFileObject.getName().getBaseName()).lookingAt()) {
                copyFileToDestination(rootFileObject, destinationDirUri, pattern, rootFileObject);
            } else if (rootFileObject.getType().hasChildren()) {
                if (inputExecutorLength >= 4) {
                    excludeRootFolder = (Boolean) data[3];
                }
                if (!excludeRootFolder) {
                    if (destinationDirUri.endsWith(File.separator)) {
                        destinationDirUri = destinationDirUri.substring(0, destinationDirUri.length() - 1);
                    }
                    destinationDirUri =
                            destinationDirUri.concat(File.separator + rootFileObject.getName().getBaseName());
                }
                List<FileObject> fileObjectList = new ArrayList<>();
                Utils.generateFileList(Utils.getFileObject(uri, fileSystemOptions), fileObjectList, false);
                for (FileObject sourceFileObject : fileObjectList) {
                    if (sourceFileObject.getType().hasContent() &&
                            pattern.matcher(sourceFileObject.getName().getBaseName()).lookingAt()) {
                        copyFileToDestination(sourceFileObject, destinationDirUri, pattern, rootFileObject);
                    }
                }
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the file type " +
                    uri, e);
        } finally {
            if (rootFileObject != null) {
                try {
                    rootFileObject.close();
                } catch (FileSystemException e) {
                    throw new SiddhiAppRuntimeException("Exception occurred when closing file object for " +
                            rootFileObject.getName().getPath(), e);
                }
            }
        }
        return new Object[0];
    }

    @Override
    protected Object[] process(Object data) {
        return new Object[0];
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    private void copyFileToDestination(FileObject sourceFileObject, String destinationDirUri, Pattern pattern,
                                       FileObject rootSourceFileObject) {
        FileObject destinationFileObject = null;
        if (fileCopyMetrics != null) {
            fileCopyMetrics.setSource(Utils.getShortFilePath(sourceFileObject.getName().getPath()));
            fileCopyMetrics.setDestination(Utils.getShortFilePath(destinationDirUri));
            fileCopyMetrics.setTime(System.currentTimeMillis());
        }
        try {
            String fileName = sourceFileObject.getName().getBaseName();
            String destinationPath;
            if (rootSourceFileObject.isFile()) {
                destinationPath = destinationDirUri + File.separator + sourceFileObject.getName().getBaseName();
            } else {
                destinationPath = destinationDirUri + File.separator + sourceFileObject.getName().getPath().
                        substring(rootSourceFileObject.getName().getPath().length());
            }
            destinationFileObject = Utils.getFileObject(destinationPath, fileSystemOptions);
            if (!destinationFileObject.exists()) {
                destinationFileObject.createFile();
            }
            if (pattern.matcher(fileName).lookingAt()) {
                destinationFileObject.copyFrom(sourceFileObject, Selectors.SELECT_ALL);
                destinationFileObject.close();
            }
            if (fileCopyMetrics !=  null) {
                fileCopyMetrics.getCopyMetric(1);
            }
        } catch (FileSystemException e) {
            if (fileCopyMetrics != null) {
                fileCopyMetrics.getCopyMetric(0);
            }
            throw new SiddhiAppRuntimeException("Exception occurred when doing file operations when copying for " +
                    "file: " + sourceFileObject.getName().getPath(), e);
        } finally {
            if (destinationFileObject != null) {
                try {
                    destinationFileObject.close();
                } catch (FileSystemException e) {
                    log.error("Exception occurred when closing file object for " +
                            destinationFileObject.getName().getPath(), e);
                }
            }
            if (sourceFileObject != null) {
                try {
                    sourceFileObject.close();
                } catch (FileSystemException e) {
                    log.error("Exception occurred when closing file object for " +
                            sourceFileObject.getName().getPath(), e);
                }
            }
        }
    }
}
