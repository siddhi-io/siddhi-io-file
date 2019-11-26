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
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
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
        name = "searchAndList",
        namespace = "file",
        description = "Searches files in a given folder and lists.",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path of the directory.",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(
                        name = "regexp",
                        description = "Pattern to be checked before listing the file name.",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(
                        name = "exclude.subdirectories",
                        description = "This flag is used to exclude the files un subdirectories when listing.",
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "fileName",
                        description = "The file name matches with the regex.",
                        type = {DataType.STRING})},
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
public class FileSearchExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileSearchExtension.class);
    private Pattern pattern;
    private boolean excludeSubdirectories;
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String sourceFileUri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String filePattern = (String) attributeExpressionExecutors[1].execute(streamEvent);
            pattern = Pattern.compile(filePattern);
            excludeSubdirectories = (Boolean) attributeExpressionExecutors[2].execute(streamEvent);
            FileSystemOptions opts = new FileSystemOptions();
            FileSystemManager fsManager;
            try {
                fsManager = VFS.getManager();
                FileObject fileObj = fsManager.resolveFile(sourceFileUri, opts);
                if (fileObj.exists()) {
                    FileObject[] children = fileObj.getChildren();
                    for (FileObject child : children) {
                        try {
                            if (child.getType() == FileType.FILE && (pattern.matcher(child.getName().
                                    getBaseName()).lookingAt() || pattern.toString().isEmpty())) {
                                Object[] data = {getFilePath(child.getName())};
                                sendEvents(streamEvent, data, streamEventChunk);
                            } else if (child.getType() == FileType.FOLDER) {
                                searchSubFolders(child, streamEvent, streamEventChunk);
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
        }
    }

    /**
     * The initialization method for {@link StreamProcessor}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     *
     * @param metaStreamEvent            the  stream event meta
     * @param abstractDefinition         the incoming stream definition
     * @param expressionExecutors        the executors for the function parameters
     * @param configReader               this hold the Stream Processor configuration reader.
     * @param streamEventClonerHolder    streamEventCloner Holder
     * @param outputExpectsExpiredEvents whether output can be expired events
     * @param findToBeExecuted           find will be executed
     * @param siddhiQueryContext         current siddhi query context
     */
    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length == 2 || attributeExpressionExecutors.length == 3) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to sourceFileUri (first argument) " +
                        "file:search() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for sourceFileUri " +
                        "(first argument) of file:search() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors[1] == null) {
                throw new SiddhiAppValidationException("Invalid input given to filePattern (second argument) of " +
                        "file:search() function. Input 'path' argument cannot be null");
            }
            Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
            if (secondAttributeType != Attribute.Type.STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found for filePattern " +
                        "(second argument) of file:search() function, required " + Attribute.Type.STRING +
                        ", but found " + secondAttributeType.toString());
            }
            if (attributeExpressionExecutors.length == 3) {
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[2].getReturnType();
                if (thirdAttributeType != Attribute.Type.BOOL) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for recursive " +
                            "(third argument) of file:search() function, required " + Attribute.Type.BOOL +
                            ", but found " + secondAttributeType.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:search() function, "
                    + "required 2 or 3, but found " + attributeExpressionExecutors.length);
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
        attributes.add(new Attribute("fileName", Attribute.Type.STRING));
        return attributes;
    }

    private void sendEvents(StreamEvent streamEvent, Object[] data, ComplexEventChunk<StreamEvent> streamEventChunk) {
        complexEventPopulater.populateComplexEvent(streamEvent, data);
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
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
     * @param streamEvent      the message context that is generated for processing the file
     * @param streamEventChunk OMFactory
     * @throws IOException
     */
    private void searchSubFolders(FileObject child, StreamEvent streamEvent,
                                  ComplexEventChunk<StreamEvent> streamEventChunk) {
        List<FileObject> fileList = new ArrayList<FileObject>();
        getAllFiles(child, fileList);
        try {
            for (FileObject file : fileList) {
                if (file.getType() == FileType.FILE && (pattern.matcher(file.getName().
                        getBaseName().toLowerCase(Locale.ENGLISH)).lookingAt() || pattern.toString().isEmpty())) {
                    Object[] data = {getFilePath(file.getName())};
                    sendEvents(streamEvent, data, streamEventChunk);
                } else if (file.getType() == FileType.FOLDER && !excludeSubdirectories) {
                    searchSubFolders(file, streamEvent, streamEventChunk);
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
