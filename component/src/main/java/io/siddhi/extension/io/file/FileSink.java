/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.file;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the implementation of siddhi-io-file sink which provides the functionality of
 * publishing data to files through siddhi.
 */

@Extension(
        name = "file" ,
        namespace = "sink" ,
        description = "" +
                "This extension publishes (writes) event data that is processed within Siddhi to files. \n" +
                "Siddhi-io-file sink provides support to write both textual and binary data into files\n",
        parameters = {
                @Parameter(name = "file.uri",
                        description =
                                "The path to thee file in which the data needs to be published. ",
                        type = {DataType.STRING},
                        dynamic = true
                ),

                @Parameter(name = "append",
                        description = "" +
                                "This specifies whether the data should be appended to the file or not.\n" +
                                "If this parameter is set to 'true', data is written at the end of the file without" +
                                " changing the existing content.\n " +
                                "If the parameter is set to 'false', the existing content of the file is deleted and" +
                                " the content you are publishing is added to replace it.\n" +
                                "If the file does not exist, a new file is created and then the data is written in" +
                                " it. In such a scenario, the value specified for this parameter is not applicable\n",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),
                @Parameter(
                        name = "add.line.separator",
                        description = "If this parameter is set to 'true', events added to the file are " +
                                "separated by adding each event in a new line.\n",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true. (However, if the 'csv' mapper is used, it is false)"
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@sink(type='file', @map(type='json'), " +
                                "append='false', " +
                                "file.uri='/abc/{{symbol}}.txt') " +
                                "define stream BarStream (symbol string, price float, volume long); ",

                        description = "" +
                                "In the above configuration, each output event is published in the " +
                                "'/abc/{{symbol}}.txt' file in JSON format.The output looks as follows:\n" +
                                "{\n" +
                                "    \"event\":{\n" +
                                "        \"symbol\":\"WSO2\",\n" +
                                "        \"price\":55.6,\n" +
                                "        \"volume\":100\n" +
                                "    }\n" +
                                "}\n" +
                                "If the file does not exist at the time an output event is generated, the system " +
                                "creates the file and proceeds to publish the output event in it."
                )
        }
)
public class FileSink extends Sink {
    private static final Logger log = Logger.getLogger(FileSink.class);

    private VFSClientConnector vfsClientConnector = null;
    private Map<String, String> properties = null;
    private Option uriOption;
    private SiddhiAppContext siddhiAppContext;
    private boolean addEventSeparator;


    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, byte[].class, Object.class};
    }

    public String[] getSupportedDynamicOptions() {
        return new String[]{Constants.FILE_URI};
    }

    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        uriOption = optionHolder.validateAndGetOption(Constants.FILE_URI);
        String append = optionHolder.validateAndGetStaticValue(Constants.APPEND, Constants.TRUE);
        properties = new HashMap<>();
        properties.put(Constants.ACTION, Constants.WRITE);
        if (Constants.TRUE.equalsIgnoreCase(append)) {
            properties.put(Constants.APPEND, append);
        }
        String mapType = streamDefinition.getAnnotations().get(0).getAnnotations().get(0).getElements().get(0)
                .getValue();
        addEventSeparator = optionHolder.isOptionExists(Constants.ADD_EVENT_SEPARATOR) ?
                Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(Constants.ADD_EVENT_SEPARATOR)) :
                !mapType.equalsIgnoreCase("csv");
        return null;
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    public void connect() throws ConnectionUnavailableException {
        vfsClientConnector = new VFSClientConnector();
    }

    public void disconnect() {
    }

    public void destroy() {
    }

    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        byte[] byteArray = new byte[0];
        boolean canBeWritten = true;
        if (payload instanceof byte[]) {
            byteArray = (byte[]) payload;
        } else {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append(payload.toString());
                if (this.addEventSeparator) {
                    sb.append("\n");
                }
                byteArray = sb.toString().getBytes(Constants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                canBeWritten = false;
                log.error("Received payload does not support UTF-8 encoding. Hence dropping the event." , e);
            }
        }

        if (canBeWritten) {
            String uri = uriOption.getValue(dynamicOptions);
            BinaryCarbonMessage binaryCarbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(byteArray), true);
            properties.put(Constants.URI, uri);
            try {
                vfsClientConnector.send(binaryCarbonMessage, null, properties);
            } catch (ClientConnectorException e) {
                throw new ConnectionUnavailableException("Writing data into the file " + uri + " failed during the " +
                        "execution of '" + siddhiAppContext.getName() + "' SiddhiApp, due to " +
                        e.getMessage(), e);
            }
        }
    }
}
