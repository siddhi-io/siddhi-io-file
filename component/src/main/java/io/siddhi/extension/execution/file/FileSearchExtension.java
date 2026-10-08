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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.local.LocalFileName;

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
                        name = "subdirectory.depth",
                        description = "The depth of subdirectories to include when searching. " +
                                "0 includes only the root directory, 1 includes the root and its immediate " +
                                "subdirectories, and so on. Use this parameter as an alternative to " +
                                "`exclude.subdirectories` for finer-grained control.",
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "-1"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. \n" +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:<Relative path from '<Product_Home>/wso2/server/' directory>," +
                                "IDENTITY_PASS_PHRASE:wso2carbon'\n" +
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
                        parameterNames = {"uri", "include.by.regexp", "exclude.subdirectories"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "exclude.subdirectories", "file.system.options"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "subdirectory.depth"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "include.by.regexp", "subdirectory.depth", "file.system.options"}
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
                ),
                @Example(
                        syntax = "ListFileStream#file:search(filePath, '.*test3.txt$', 1)",
                        description = "This will list all the files in the root directory and one level of " +
                                "subdirectories which adheres to a given regex file pattern in a given path."
                )
        }
)
public class FileSearchExtension extends StreamFunctionProcessor {
    private static final Logger log = LogManager.getLogger(FileSearchExtension.class);
    private Pattern pattern = null;
    private boolean excludeSubdirectories = false;
    private int subdirectoryDepth = -1;
    private boolean useDepthMode = false;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        int inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength < 2) {
            pattern = Pattern.compile("");
        } else if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            pattern = Pattern.compile(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[1]).getValue().toString());
        }
        if (inputExecutorLength >= 3 &&
                attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Object val = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            if (val instanceof Boolean) {
                excludeSubdirectories = (Boolean) val;
            } else if (val instanceof Integer) {
                subdirectoryDepth = (Integer) val;
                useDepthMode = true;
            }
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
        Pattern eventPattern = pattern != null ? pattern : Pattern.compile((String) data[1]);
        try {
            FileObject fileObj = Utils.getFileObject(sourceFileUri, fileSystemOptions);
            if (fileObj.exists()) {
                if (useDepthMode) {
                    searchWithDepth(fileObj, fileList, subdirectoryDepth, eventPattern);
                } else {
                    searchFiles(fileObj, fileList, eventPattern);
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
     * Search files in the root directory, optionally recursing into subdirectories
     * unless {@code excludeSubdirectories} is true.
     *
     * @param dir      root directory to search
     * @param fileList accumulator for matched file paths
     */
    private void searchFiles(FileObject dir, List<String> fileList, Pattern eventPattern) {
        try {
            FileObject[] children = dir.getChildren();
            for (FileObject child : children) {
                try {
                    if (child.getType() == FileType.FILE && (eventPattern.matcher(child.getName().
                            getBaseName()).lookingAt() || eventPattern.toString().isEmpty())) {
                        fileList.add(getFilePath(child.getName()));
                    } else if (child.getType() == FileType.FOLDER && !excludeSubdirectories) {
                        searchSubFolders(child, fileList, eventPattern);
                    }
                } catch (IOException e) {
                    throw new SiddhiAppRuntimeException("Unable to search a file with pattern" +
                            eventPattern.toString() + " in " + dir.getName().getPath(), e);
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
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the searching files in path " +
                    dir.getName().getPath(), e);
        }
    }

    /**
     * @param child            sub folder
     */
    private void searchSubFolders(FileObject child, List<String> fileList, Pattern eventPattern) {
        List<FileObject> fileObjectList = new ArrayList<FileObject>();
        getAllFiles(child, fileObjectList);
        try {
            for (FileObject file : fileObjectList) {
                if (file.getType() == FileType.FILE && (eventPattern.matcher(file.getName().
                        getBaseName().toLowerCase(Locale.ENGLISH)).lookingAt()
                        || eventPattern.toString().isEmpty())) {
                    fileList.add(getFilePath(file.getName()));
                } else if (file.getType() == FileType.FOLDER && !excludeSubdirectories) {
                    searchSubFolders(file, fileList, eventPattern);
                }
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Unable to search a file with pattern" +
                    eventPattern.toString() + " in " + child.getName().getPath() + ". " + e.getMessage(), e);
        } finally {
            try {
                child.close();
            } catch (IOException e) {
                log.error("Error while closing Directory: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Search files up to a given depth. remainingDepth=0 means only files in dir itself, not subdirectories.
     *
     * @param dir            directory to search
     * @param fileList       accumulator for matched file paths
     * @param remainingDepth how many more levels of subdirectories to descend
     */
    private void searchWithDepth(FileObject dir, List<String> fileList, int remainingDepth,
                                 Pattern eventPattern) {
        try {
            FileObject[] children = dir.getChildren();
            for (FileObject child : children) {
                try {
                    if (child.getType() == FileType.FILE &&
                            (eventPattern.matcher(child.getName().getBaseName()).lookingAt()
                                    || eventPattern.toString().isEmpty())) {
                        fileList.add(getFilePath(child.getName()));
                    } else if (child.getType() == FileType.FOLDER && remainingDepth > 0) {
                        searchWithDepth(child, fileList, remainingDepth - 1, eventPattern);
                    }
                } catch (IOException e) {
                    throw new SiddhiAppRuntimeException("Unable to search file with pattern " +
                            eventPattern + " in " + dir.getName().getPath(), e);
                } finally {
                    try {
                        child.close();
                    } catch (IOException e) {
                        log.error("Error closing file: " + e.getMessage(), e);
                    }
                }
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception searching files in path " +
                    dir.getName().getPath(), e);
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
