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

package org.wso2.extension.siddhi.io.file;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.transport.file.connector.sender.VFSClientConnector;
import org.wso2.extension.siddhi.io.file.processors.FileSinkMessageProcessor;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by minudika on 18/5/17.
 */

@Extension(
        name = "file",
        namespace = "sink",
        description = "File Sink",
        parameters = {
                @Parameter(name = "uri",
                        description =
                                "Used to specify the file for data to be written. ",
                        type = {DataType.STRING},
                        dynamic = true
                ),
                @Parameter(name = "append",
                        description = "" +
                                "This parameter is used to specify whether the data should be " +
                                "append to the file or not." +
                                "If append = 'true', " +
                                "data will be write at the end of the file without " +
                                "changing the existing content." +
                                "If file does not exist, a new fill will be crated and then data will be written." +
                                "If append append = 'false', " +
                                "If given file exists, existing content will be deleted and then data will be " +
                                "written back to the file." +
                                "If given file does not exist, a new file will be created and " +
                                "then data will be written on it.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@sink(type='file', @map(type='json'), " +
                                "append='false', " +
                                "uri='/abc/{{symbol}}.txt') " +
                                "define stream BarStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, for each event, " +
                                "a file will be generated if there's no such a file," +
                                "and then data will be written to that file as json messages" +
                                "output will looks like below." +
                                "{\n" +
                                "    \"event\":{\n" +
                                "        \"symbol\":\"WSO2\",\n" +
                                "        \"price\":55.6,\n" +
                                "        \"volume\":100\n" +
                                "    }\n" +
                                "}\n")
        }
)
public class FileSink extends Sink {
    private static final Logger log = Logger.getLogger(FileSink.class);

    private String fileURI = null;
    private VFSClientConnector vfsClientConnector = null;
    private Map<String, String> properties = null;
    private Option uriOption;


    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[0];
    }

    public String[] getSupportedDynamicOptions() {
        return new String[]{"uri"};
    }

    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        uriOption = optionHolder.validateAndGetOption(Constants.URI);
        String append = optionHolder.validateAndGetStaticValue(Constants.APPEND, Constants.TRUE);
        properties = new HashMap();
        properties.put(Constants.ACTION, Constants.WRITE);
        if (Constants.TRUE.equalsIgnoreCase(append)) {
            properties.put(Constants.APPEND, append);
        }
    }

    public void connect() throws ConnectionUnavailableException {
        vfsClientConnector = new VFSClientConnector();
    }

    public void disconnect() {

    }

    public void destroy() {

    }

    public void publish(Object payload, DynamicOptions dynamicOptions) throws ConnectionUnavailableException {
        byte[] byteArray = payload.toString().getBytes();
        String uri = uriOption.getValue(dynamicOptions);
        properties.put(Constants.URI, uri);
        FileSinkMessageProcessor fileSinkMessageProcessor = new FileSinkMessageProcessor();
        BinaryCarbonMessage binaryCarbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(byteArray), true);
        vfsClientConnector.setMessageProcessor(fileSinkMessageProcessor);
        try {
            vfsClientConnector.send(binaryCarbonMessage, null, properties);
        } catch (ClientConnectorException e) {
            log.error("Writing data into the file " + fileURI + "failed.\n" + e.getMessage());
        }
    }

    public Map<String, Object> currentState() {
        return null;
    }

    public void restoreState(Map<String, Object> map) {

    }
}
