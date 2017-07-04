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

package org.wso2.extensions.siddhi.io.file;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.transport.file.connector.sender.VFSClientConnector;
import org.wso2.extensions.siddhi.io.file.utils.Constants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.output.sink.Sink;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.Option;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.BufferedWriter;
import java.io.File;
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
                @Parameter(name = "enclosing.element",
                        description =
                                "TBD",
                        type = {DataType.STRING}),
                @Parameter(name = "fail.on.missing.attribute",
                        description = "This can either have value true or false. By default it will be true. This "
                                + "attribute allows user to handle unknown attributes. By default if an json "
                                + "execution "
                                + "fails or returns null DAS will drop that message. However setting this property"
                                + " to "
                                + "false will prompt DAS to send and event with null value to Siddhi where user "
                                + "can handle"
                                + " it accordingly(ie. Assign a default value)",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description =  "Above configuration will do a default JSON input mapping. Expected "
                                + "input will look like below."
                                + "{\n"
                                + "    \"event\":{\n"
                                + "        \"symbol\":\"WSO2\",\n"
                                + "        \"price\":55.6,\n"
                                + "        \"volume\":100\n"
                                + "    }\n"
                                + "}\n"),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json', "
                                + "enclosing.element=\"$.portfolio\", "
                                + "@attributes(symbol = \"company.symbol\", price = \"price\", volume = \"volume\")))",
                        description =  "Above configuration will perform a custom JSON mapping. Expected input will "
                                + "look like below."
                                + "{"
                                + " \"portfolio\":{\n"
                                + "     \"stock\":{"
                                + "        \"volume\":100,\n"
                                + "        \"company\":{\n"
                                + "           \"symbol\":\"WSO2\"\n"
                                + "       },\n"
                                + "        \"price\":55.6\n"
                                + "    }\n"
                                + "}")
        }
)
public class FileSink extends Sink{
    private static final Logger log = Logger.getLogger(FileSink.class);

    private String fileURI = null;
    private String mode = null;
    private boolean isAppendEnabled = false;
    private String actionAfterProcess = null;
    private String moveAfterProcess = null;

    private File file;
    private BufferedWriter bufferedWriter = null;
    private VFSClientConnector vfsClientConnector = null;
    private Map<String,String> properties = null;
    private OptionHolder optionHolder = null;
    private Option uriOption;

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[0];
    }

    public String[] getSupportedDynamicOptions() {
        return new String[]{"uri"};
    }

    protected void init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.optionHolder = optionHolder;
        uriOption= optionHolder.validateAndGetOption(Constants.URI);
        String append = optionHolder.validateAndGetStaticValue(Constants.APPEND, Constants.TRUE);
        properties = new HashMap<>();
        properties.put(Constants.ACTION, Constants.WRITE);
        if(Constants.TRUE.equalsIgnoreCase(append)){
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
        BinaryCarbonMessage binaryCarbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(byteArray),true);
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
