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
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;
import org.wso2.extensions.siddhi.io.file.utils.Constants;
import org.wso2.extensions.siddhi.io.file.utils.FileSourceConfiguration;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by minudika on 18/5/17.
 */
@Extension(
        name = "file",
        namespace = "source",
        description = "File Source",
        parameters = {
                @Parameter(name = "enclosing.element",
                        description =
                                "Used to specify the enclosing element in case of sending multiple events in same "
                                        + "JSON message. WSO2 DAS will treat the child element of given enclosing "
                                        + "element as events"
                                        + " and execute json path expressions on child elements. If enclosing.element "
                                        + "is not provided "
                                        + "multiple event scenario is disregarded and json path will be evaluated "
                                        + "with respect to "
                                        + "root element.",
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
public class FileSource extends Source{
    private static final Logger log = Logger.getLogger(FileSource.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private final static String FILE_SYSTEM_SERVER_CONNECTOR_ID = "siddhi.io.file";
    private FileServerConnector fileServerConnector = null;
    private ServerConnector serverConnector;
    private FileSystemMessageProcessor fileSystemMessageProcessor = null;
    private final String POLLING_INTERVAL = "1000";
    private Map<String,Object> currentState;
    private long filePointer = 0;

    private String uri;
    private String mode;
    private String actionAfterProcess;
    private String moveAfterProcess;
    private String tailing;
    private String beginRegex;
    private String endRegex;

    private boolean isDirectory = false;

    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        fileSystemServerConnectorProvider = new FileSystemServerConnectorProvider();

        uri = optionHolder.validateAndGetStaticValue(Constants.URI, null);
        mode = optionHolder.validateAndGetStaticValue(Constants.MODE, null);
        actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS, Constants.MOVE);
        moveAfterProcess = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_PROCESS, generateDefaultFileMoveLocation());
        tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        beginRegex = optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null);
        endRegex = optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null);

        fileSourceConfiguration = createSourceConf();
    }

    public void connect() throws ConnectionUnavailableException {
        Map<String, String> properties = getFileSystemServerProperties();
        serverConnector = fileSystemServerConnectorProvider.createConnector(FILE_SYSTEM_SERVER_CONNECTOR_ID,
                properties);
        fileSystemMessageProcessor = new FileSystemMessageProcessor(sourceEventListener, fileSourceConfiguration);
        serverConnector.setMessageProcessor(fileSystemMessageProcessor);

        try{
            serverConnector.start();
            fileSystemMessageProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Error when establishing a connection with file-system-server for stream: "
                    + sourceEventListener.getStreamDefinition().getId() + " due to "+ e.getMessage());
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Error occurred while processing directory : "+ e.getMessage());
        }
    }

    public void disconnect() {

    }

    public void destroy() {

    }

    public void pause() {

    }

    public void resume() {

    }

    public Map<String, Object> currentState() {
        currentState.put(Constants.FILE_POINTER, fileSystemMessageProcessor);
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        filePointer = (long) map.get(Constants.FILE_POINTER);
        fileSourceConfiguration.setFilePointer(filePointer);
    }

    public void tmpFileServerRun(){
        Map<String, String> properties = new HashMap<>();
        properties.put(Constants.PATH, "/home/minudika/Projects/WSO2/siddhi-io-file/testDir/test2.txt");
        properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
        properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
        properties.put(Constants.ACTION, Constants.READ);
        properties.put(Constants.POLLING_INTERVAL, "1000");

        FileProcessor fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
        FileServerConnectorProvider fileServerConnectorProvider;
        fileServerConnectorProvider = new FileServerConnectorProvider();
        ServerConnector fileServerConnector = fileServerConnectorProvider.createConnector("siddhi-io-line",
                properties);
        fileServerConnector.setMessageProcessor(fileProcessor);
        try {
            fileServerConnector.start();
            fileProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private String generateDefaultFileMoveLocation(){
        StringBuilder sb = new StringBuilder();
        URI uri = null;
        URI parent = null;
        try {
            uri = new URI(this.uri);
            parent = uri.getPath().endsWith("/") ? uri.resolve("..") : uri.resolve(".");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return sb.append(parent.toString()).append("read").toString();
    }

    private FileSourceConfiguration createSourceConf(){
        FileSourceConfiguration conf = new FileSourceConfiguration();
        conf.setUri(uri);
        conf.setMoveAfterProcessUri(moveAfterProcess);
        conf.setBeginRegex(beginRegex);
        conf.setEndRegex(endRegex);
        conf.setMode(mode);
        conf.setActionAfterProcess(actionAfterProcess);
        conf.setTailingEnabled(Boolean.parseBoolean(tailing));
        return conf;
    }

    private HashMap<String,String> getFileSystemServerProperties(){
        HashMap<String,String> map = new HashMap<>();

        map.put(Constants.TRANSPORT_FILE_DIR_URI, uri);
        map.put(Constants.ACTION_AFTER_PROCESS_KEY, actionAfterProcess.toUpperCase());
        map.put(Constants.MOVE_AFTER_PROCESS_KEY, moveAfterProcess);
        map.put(Constants.POLLING_INTERVAL, POLLING_INTERVAL);
        map.put(Constants.FILE_SORT_ATTRIBUTE, Constants.NAME);
        map.put(Constants.FILE_SORT_ASCENDING, Constants.TRUE);
        map.put(Constants.CREATE_MOVE_DIR, Constants.TRUE);

        if(Constants.BINARY_FULL.equalsIgnoreCase(mode) ||
                Constants.TEXT_FULL.equalsIgnoreCase(mode)){
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
        } else{
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.FALSE);
        }

        return map;
    }
}
