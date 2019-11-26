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
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static io.siddhi.query.api.definition.Attribute.Type.BOOL;

/**
 * This extension can be used to copy files from a particular source to a destination.
 */
@Extension(
        name = "move",
        namespace = "file",
        description = "This function performs copying file from one directory to another.\n",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file or directory path.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute file path to the destination directory.\n" +
                                "Note: Parent folder structure will be created if it does not exist.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "regexp",
                        description = "Regex pattern to be matched with file base name to move the files.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "exclude.root.folder",
                        description = "Exclude parent folder when moving the content.",
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
                        parameterNames = {"uri", "destination.dir.uri", "regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "regexp", "exclude.root.folder"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "isSuccess",
                        description = "Status of the file moving operation (true if success)",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "InputStream#file:move('/User/wso2/source/test.txt', 'User/wso2/destination/')",
                        description = "Moves 'test.txt' in 'source' folder to the 'destination' folder."
                ),
                @Example(
                        syntax = "InputStream#file:move('/User/wso2/source/', 'User/wso2/destination/')",
                        description = "Moves 'source' folder to the 'destination' folder with all its content"
                ),
                @Example(
                        syntax = "InputStream#" +
                                "file:move('/User/wso2/source/', 'User/wso2/destination/', '.*test3.txt$')",
                        description = "Moves 'source' folder to the 'destination' folder excluding files doesnt " +
                                "adhere to the given regex."
                ),
                @Example(
                        syntax = "InputStream#" +
                                "file:move('/User/wso2/source/', 'User/wso2/destination/', '', true)",
                        description = "Moves only the files resides in 'source' folder to 'destination' folder."
                )
        }
)
public class FileMoveExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileCopyExtension.class);
    private FileSystemOptions opts = new FileSystemOptions();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String uri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String destinationDirUri = (String) attributeExpressionExecutors[1].execute(streamEvent);
            String regex = "";
            boolean excludeParentFolder = false;
            if (attributeExpressionExecutors.length == 3) {
                regex = (String) attributeExpressionExecutors[2].execute(streamEvent);
            }
            Pattern pattern = Pattern.compile(regex);
            try {
                FileObject rootFileObject = Utils.getFileObject(uri);
                if (rootFileObject.getType().hasContent() &&
                        pattern.matcher(rootFileObject.getName().getBaseName()).lookingAt()) {
                    moveFileToDestination(rootFileObject, destinationDirUri, pattern, rootFileObject);
                } else if (rootFileObject.getType().hasChildren()) {
                    if (attributeExpressionExecutors.length == 4) {
                        excludeParentFolder = (Boolean) attributeExpressionExecutors[3].execute(streamEvent);
                    }
                    if (!excludeParentFolder) {
                        destinationDirUri =
                                destinationDirUri.concat(File.separator + rootFileObject.getName().getBaseName());
                    }
                    List<String> fileList = new ArrayList<>();
                    Utils.generateFileList(new File(uri), fileList, false);
                    for (String s : fileList) {
                        FileObject sourceFileObject = Utils.getFileObject(s);
                        if (sourceFileObject.getType().hasContent() &&
                                pattern.matcher(sourceFileObject.getName().getBaseName()).lookingAt()) {
                            moveFileToDestination(sourceFileObject, destinationDirUri, pattern, rootFileObject);
                        }
                    }
                }
                Object[] data = {true};
                sendEvents(streamEvent, data, streamEventChunk);
            } catch (FileSystemException e) {
                throw new SiddhiAppRuntimeException("Exception occurred when getting the file type " +
                        uri, e);
            }
        }
    }

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length > 1 && attributeExpressionExecutors.length < 5) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to uri (first argument) " +
                        "file:move() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for uri " +
                        "(first argument) of file:move() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors[1] == null) {
                throw new SiddhiAppValidationException("Invalid input given to destination.dir.uri (second argument) " +
                        "file:move() function. Argument cannot be null");
            }
            Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
            if (!(secondAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for destination.dir.uri " +
                        "(second argument) of file:move() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors.length == 3) {
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[2].getReturnType();
                if (!(thirdAttributeType == Attribute.Type.STRING)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for regex " +
                            "(third argument) of file:move() function, required " + Attribute.Type.STRING +
                            " but found " + firstAttributeType.toString());
                }
            }
            if (attributeExpressionExecutors.length == 4) {
                Attribute.Type thirdAttributeType = attributeExpressionExecutors[3].getReturnType();
                if (!(thirdAttributeType == Attribute.Type.BOOL)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for exclude.root.folder " +
                            "(forth argument) of file:move() function, required " + Attribute.Type.BOOL +
                            " but found " + firstAttributeType.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:move() function, "
                    + "required 2, 3 or 4, but found " + attributeExpressionExecutors.length);
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

    private void moveFileToDestination(FileObject sourceFileObject, String destinationDirUri, Pattern pattern,
                                       FileObject rootSourceFileObject) {
        try {
            FileSystemManager fsManager = VFS.getManager();
            String fileName = sourceFileObject.getName().getBaseName();
            FileObject destinationPath;
            if (rootSourceFileObject.isFile()) {
                destinationPath = fsManager.resolveFile(
                        destinationDirUri + File.separator + sourceFileObject.getName().getBaseName(), this.opts);
            } else {
                destinationPath = fsManager.resolveFile(
                        destinationDirUri +
                                File.separator +
                                sourceFileObject.getName().getPath().substring(
                                        rootSourceFileObject.getName().getPath().length()), this.opts);
            }
            if (!destinationPath.exists()) {
                destinationPath.createFile();
            }
            if (pattern.matcher(fileName).lookingAt()) {
                sourceFileObject.moveTo(destinationPath);
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when doing file operations when moving for file: " +
                    sourceFileObject.getName().getPath(), e);
        }
    }

    private void sendEvents(StreamEvent streamEvent, Object[] data, ComplexEventChunk<StreamEvent> streamEventChunk) {
        complexEventPopulater.populateComplexEvent(streamEvent, data);
        nextProcessor.process(streamEventChunk);
    }
}
