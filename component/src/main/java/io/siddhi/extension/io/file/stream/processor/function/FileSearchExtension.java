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

package io.siddhi.extension.io.file.stream.processor.function;

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
        description = "This function searches a given folder for files with a given regex expressions and returns " +
                "file names matches with the regex.",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The file path .",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(
                        name = "regex.pattern",
                        description = "The path of the set of elements that will be tokenized. " +
                                "Give empty string to list all files",
                        type = {DataType.STRING},
                        dynamic = true),
                @Parameter(
                        name = "archive.recursively",
                        description = "This parameter will allow recursive search for the sub folders.",
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
                        syntax = "define stream ListFileStream (filePath string);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from ListFileStream#file:search(filePath, '', 'true')\n" +
                                "select fileName\n" +
                                "insert into ResultStream;",
                        description = "This query will searched for all the files recursively for a given path. " +
                                "The file names will be returned to the 'RecordStream'."
                ),
                @Example(
                        syntax = "define stream ListFileStream (filePath string);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from ListFileStream#file:search(filePath, '.*test3.txt$', 'true')\n" +
                                "select fileName\n" +
                                "insert into ResultStream;",
                        description = "This query will searched for all the files with a given regex file pattern " +
                                "recursively for a given path. The file names will be returned to the 'RecordStream'."
                ),
                @Example(
                        syntax = "define stream ListFileStream (filePath string);\n\n" +
                                "@info(name = 'query1')\n" +
                                "from ListFileStream#file:search(filePath, '.*test3.txt$', 'false')\n" +
                                "select fileName\n" +
                                "insert into ResultStream;",
                        description = "This query will searched for all the files with a given regex file pattern " +
                                "only in the given path. The file names will be returned to the 'RecordStream'."
                )
        }
)
public class FileSearchExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileSearchExtension.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String sourceFileUri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String filePattern = (String) attributeExpressionExecutors[1].execute(streamEvent);
            Pattern pattern = Pattern.compile(filePattern);
            boolean recursiveSearch = (Boolean) attributeExpressionExecutors[2].execute(streamEvent);
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
                            } else if (child.getType() == FileType.FOLDER && recursiveSearch) {
                                searchSubFolders(child, pattern, streamEvent, streamEventChunk);
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
     * @param pattern          pattern of the file to be searched
     * @param streamEvent      the message context that is generated for processing the file
     * @param streamEventChunk OMFactory
     * @throws IOException
     */
    private void searchSubFolders(FileObject child, Pattern pattern, StreamEvent streamEvent,
                                  ComplexEventChunk<StreamEvent> streamEventChunk) {
        List<FileObject> fileList = new ArrayList<FileObject>();
        getAllFiles(child, fileList);
        try {
            for (FileObject file : fileList) {
                if (file.getType() == FileType.FILE && (pattern.matcher(file.getName().
                        getBaseName().toLowerCase(Locale.ENGLISH)).lookingAt() || pattern.toString().isEmpty())) {
                    Object[] data = {getFilePath(file.getName())};
                    sendEvents(streamEvent, data, streamEventChunk);
                } else if (file.getType() == FileType.FOLDER) {
                    searchSubFolders(file, pattern, streamEvent, streamEventChunk);
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
