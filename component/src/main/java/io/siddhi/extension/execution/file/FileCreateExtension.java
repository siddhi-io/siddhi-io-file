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
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.siddhi.extension.io.file.util.Constants.WAIT_TILL_DONE;
import static io.siddhi.extension.util.Constant.CREATE;
import static io.siddhi.extension.util.Constant.CREATE_FOLDER;

/**
 * This extension can be used to create a file or a folder.
 */
@Extension(
        name = "create",
        namespace = "file",
        description = "Create a file or a folder in the given location",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path which needs to be created.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "is.directory",
                        description = "This flag is used when creating file path is a directory",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "isSuccess",
                        description = "Status of the file or the directory creation.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "from CreateFileStream#file:create('/User/wso2/source/test.txt', false)",
                        description = "Creates a file in the given path with the name of 'test.txt'."
                ),
                @Example(
                        syntax = "from CreateFileStream#file:create('/User/wso2/source/', true)",
                        description = "Creates a folder in the given path with the name of 'source'."
                )
        }
)
public class FileCreateExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileCreateExtension.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String fileSourcePath = (String) attributeExpressionExecutors[0].execute(streamEvent);
            boolean isDirectory = (Boolean) attributeExpressionExecutors[1].execute(streamEvent);
            VFSClientConnector vfsClientConnector = new VFSClientConnector();
            VFSClientConnectorCallback vfsClientConnectorCallback = new VFSClientConnectorCallback();
            BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                    fileSourcePath.getBytes(StandardCharsets.UTF_8)), true);
            Map<String, String> properties = new HashMap<>();
            properties.put(Constants.URI, fileSourcePath);
            properties.put(Constants.ACTION, CREATE);
            if (isDirectory) {
                properties.put(CREATE_FOLDER, "true");
            } else {
                properties.put(CREATE_FOLDER, "false");
            }
            try {
                vfsClientConnector.send(carbonMessage, vfsClientConnectorCallback, properties);
                vfsClientConnectorCallback.waitTillDone(WAIT_TILL_DONE, fileSourcePath);
                Object[] data = {true};
                sendEvents(streamEvent, data, streamEventChunk);
            } catch (ClientConnectorException e) {
                throw new SiddhiAppRuntimeException("Failure occurred in vfs-client while creating the file " +
                        fileSourcePath, e);
            } catch (InterruptedException e) {
                throw new SiddhiAppRuntimeException("Failed to get callback from vfs-client  for file " +
                        fileSourcePath, e);
            }
        }

    }

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to uri (first argument) " +
                        "file:create() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for uri " +
                        "(first argument) of file:create() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors[1] == null) {
                throw new SiddhiAppValidationException("Invalid input given to is.directory (second argument) " +
                        "file:create() function. Argument cannot be null");
            }
            Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
            if (!(secondAttributeType == Attribute.Type.BOOL)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for is.directory " +
                        "(second argument) of file:create() function, required " + Attribute.Type.BOOL +
                        " but found " + firstAttributeType.toString());
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:archive() function, "
                    + "required 2, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("isSuccess", Attribute.Type.BOOL));
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

    private void sendEvents(StreamEvent streamEvent, Object[] data, ComplexEventChunk<StreamEvent> streamEventChunk) {
        complexEventPopulater.populateComplexEvent(streamEvent, data);
        nextProcessor.process(streamEventChunk);
    }
}
