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
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.local.LocalFileName;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * This class provides implementation to list files in a given file path.
 */
@Extension(
        name = "search",
        namespace = "file",
        description = "Searches files in a given folder and lists.",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path of the directory.",
                        type = {DataType.STRING},
                        dynamic = true
                ),
                @Parameter(
                        name = "include.by.regexp",
                        description = "Only the files matching the patterns will be searched.\n" +
                                "Note: Add an empty string to match all files",
                        type = {DataType.STRING},
                        optional = true,
                        dynamic = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "exclude.subdirectories",
                        description = "This flag is used to exclude the files un subdirectories when listing.",
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
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "exclude.subdirectories"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "exclude.subdirectories", "file.system.options"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "fileNameList",
                        description = "The lit file name matches in the directory.",
                        type = {DataType.OBJECT})},
        examples = {
                @Example(
                        syntax = "ListFileStream#file:search(filePath)",
                        description = "This will list all the files (also in sub-folders) in a given path."
                ),
                @Example(
                        syntax = "ListFileStream#file:search(filePath, '.*test3.txt$')",
                        description = "This will list all the files (also in sub-folders) which adheres to a given " +
                                "regex file pattern in a given path."
                ),
                @Example(
                        syntax = "ListFileStream#file:search(filePath, '.*test3.txt$', true)",
                        description = "This will list all the files excluding the files in sub-folders which adheres " +
                                "to a given regex file pattern in a given path."
                )
        }
)
public class FileSearchExtension extends StreamFunctionProcessor {
    private static final Logger log = Logger.getLogger(FileSearchExtension.class);
    private Pattern pattern = null;
    private int inputExecutorLength;
    private boolean excludeSubdirectories = false;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength >= 2 &&
                attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            pattern = Pattern.compile(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[1]).getValue().toString());
        }
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
        List<String> fileList = new ArrayList<>();
        String sourceFileUri = (String) data[0];
        String regex = "";
        if (inputExecutorLength >= 2) {
            regex = (String) data[1];
        }
        if (pattern == null) {
            pattern = Pattern.compile(regex);
        }
        if (inputExecutorLength == 3) {
            excludeSubdirectories = (Boolean) data[2];
        }
        try {
            FileObject fileObj = Utils.getFileObject(sourceFileUri, fileSystemOptions);
            if (fileObj.exists()) {
                FileObject[] children = fileObj.getChildren();
                for (FileObject child : children) {
                    try {
                        if (child.getType() == FileType.FILE && (pattern.matcher(child.getName().
                                getBaseName()).lookingAt() || pattern.toString().isEmpty())) {
                            fileList.add(getFilePath(child.getName()));
                        } else if (child.getType() == FileType.FOLDER) {
                            searchSubFolders(child, fileList);
                        }
                    } catch (IOException e) {
                        throw new SiddhiAppRuntimeException("Unable to search a file with pattern" +
                                pattern.toString() + " in " + sourceFileUri, e);
                    } finally {
                        try {
                            if (child != null) {
                                child.close();
                            }
                        } catch (IOException e) {
                            log.error("Error while closing Directory: " + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the searching files in path " +
                    sourceFileUri, e);
        }
        return new Object[]{fileList};
    }

    @Override
    protected Object[] process(Object data) {
        return process(new Object[]{data});
    }

    /**
     * Get the file path of a file including the drive letter of windows files.
     *
     * @param fileName fileName
     * @return file path
     */
    private String getFilePath(FileName fileName) {
        if (fileName instanceof LocalFileName) {
            LocalFileName localFileName = (LocalFileName) fileName;
            return localFileName.getRootFile() + localFileName.getPath();
        } else {
            return fileName.getPath();
        }
    }

    /**
     * @param child            sub folder
     */
    private void searchSubFolders(FileObject child, List<String> fileList) {
        List<FileObject> fileObjectList = new ArrayList<FileObject>();
        getAllFiles(child, fileObjectList);
        try {
            for (FileObject file : fileObjectList) {
                if (file.getType() == FileType.FILE && (pattern.matcher(file.getName().
                        getBaseName().toLowerCase(Locale.ENGLISH)).lookingAt() || pattern.toString().isEmpty())) {
                    fileList.add(getFilePath(file.getName()));
                } else if (file.getType() == FileType.FOLDER && !excludeSubdirectories) {
                    searchSubFolders(file, fileList);
                }
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Unable to search a file with pattern" +
                    pattern.toString() + " in " + child.getName().getPath() + ". " + e.getMessage(), e);
        } finally {
            try {
                child.close();
            } catch (IOException e) {
                log.error("Error while closing Directory: " + e.getMessage(), e);
            }
        }
    }

    /**
     * @param dir      sub directory
     * @param fileList list of file inside directory
     */
    private void getAllFiles(FileObject dir, List<FileObject> fileList) {
        try {
            FileObject[] children = dir.getChildren();
            fileList.addAll(Arrays.asList(children));
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Unable to list all files when searching files with pattern. " +
                    e.getMessage(), e);
        } finally {
            try {
                if (dir != null) {
                    dir.close();
                }
            } catch (IOException e) {
                log.error("Error while closing Directory: " + e.getMessage(), e);
            }
        }
    }
}
