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
    private FileSourceServiceProvider fileSourceServiceProvider;
    private FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private FileServerConnectorProvider fileServerConnectorProvider;
    private final static String FILE_SYSTEM_SERVER_CONNECTOR_ID = "siddhi.io.file";
    private FileServerConnector fileServerConnector = null;
    private ServerConnector fileSystemServerConnector;
    private FileSystemMessageProcessor fileSystemMessageProcessor = null;
    private final String POLLING_INTERVAL = "1000";
    private Map<String,Object> currentState;
    private String filePointer = "0";

    private String uri;
    private String mode;
    private String actionAfterProcess;
    private String moveAfterProcess;
    private String tailing;
    private String beginRegex;
    private String endRegex;

    private boolean isDirectory = false;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] strings,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.currentState = new HashMap<>();
        fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
        fileSystemServerConnectorProvider = fileSourceServiceProvider.getFileSystemServerConnectorProvider();

        uri = optionHolder.validateAndGetStaticValue(Constants.URI, null);
        actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS, null);
        mode = optionHolder.validateAndGetStaticValue(Constants.MODE, null);
        moveAfterProcess = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_PROCESS,
                generateDefaultFileMoveLocation());
        if(Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)){
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.FALSE);
        }else{
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        }
        beginRegex = optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null);
        endRegex = optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null);

        validateParameters();
        fileSourceConfiguration = createInitialSourceConf();

        siddhiAppContext.getSnapshotService().addSnapshotable("file-source",this);
        //filePointerMap = fileSourceServiceProvider.getFilePointerMap();
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        Map<String, String> properties = getFileSystemServerProperties();
        //fileSourceConfiguration.setFilePointer(filePointer);
        fileSystemServerConnector = fileSystemServerConnectorProvider.createConnector(fileSourceServiceProvider
                        .getServerConnectorID(),
                properties);
        fileSystemMessageProcessor = new FileSystemMessageProcessor(sourceEventListener, fileSourceConfiguration);
        fileSystemServerConnector.setMessageProcessor(fileSystemMessageProcessor);
        fileSourceConfiguration.setFileSystemServerConnector(fileSystemServerConnector);

        try{
            fileSystemServerConnector.start();
            //fileSystemMessageProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Error when establishing a connection with file-system-server " +
                    "for stream : '"
                    + sourceEventListener.getStreamDefinition().getId() + "' due to "+ e.getMessage());
        }
    }

    @Override
    public void disconnect() {
        /*fileServerConnectorProvider = fileSourceServiceProvider.getFileServerConnectorProvider();
        for(Thread thread : fileSourceServiceProvider.getServerConnectorList()){
            thread.stop();
        }
        try {
            for(ServerConnector serverConnector : fileSystemMessageProcessor.getFileServerConnectorList()){
                serverConnector.stop();
            }
            fileSystemServerConnector.stop();
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Error occurred when trying to stop fileSystemServerConnector : "+
                    e.getMessage());
        }*/
        try {
            fileSourceConfiguration.getFileServerConnector().stop();
            fileSystemServerConnector.stop();
        } catch (ServerConnectorException e) {
            e.printStackTrace();
        }
        fileSourceServiceProvider.reset();
    }


    public void destroy() {

    }

    public void pause() {

    }

    public void resume() {

    }

    public Map<String, Object> currentState() {
        currentState.put(Constants.FILE_POINTER, fileSourceConfiguration.getFilePointer());
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        //filePointerMap = (Map<String, Long>) map.get(Constants.FILE_POINTER_MAP);
        this.filePointer = map.get(Constants.FILE_POINTER).toString();
        fileSourceConfiguration.setFilePointer(filePointer);
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

    private FileSourceConfiguration createInitialSourceConf(){
        FileSourceConfiguration conf = new FileSourceConfiguration();
        conf.setDirURI(uri);
        conf.setMoveAfterProcessUri(moveAfterProcess);
        conf.setBeginRegex(beginRegex);
        conf.setEndRegex(endRegex);
        conf.setMode(mode);
        conf.setActionAfterProcess(actionAfterProcess);
        conf.setTailingEnabled(Boolean.parseBoolean(tailing));
        conf.setFilePointer("0");
        return conf;
    }

    private HashMap<String,String> getFileSystemServerProperties(){
        HashMap<String,String> map = new HashMap<>();

        map.put(Constants.TRANSPORT_FILE_DIR_URI, uri);
        if(actionAfterProcess != null) {
            map.put(Constants.ACTION_AFTER_PROCESS_KEY, actionAfterProcess.toUpperCase());
        }
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

    private void validateParameters(){
        if(uri == null){
            throw new SiddhiAppRuntimeException("Uri is a mandatory parameter and has not been provided." +
                    " Hence stopping the SiddhiApp.");
        }
        if(actionAfterProcess == null && !Constants.TRUE.equalsIgnoreCase(tailing)){
            throw new SiddhiAppRuntimeException("actionAfterProcess is a mandatory parameter and " +
                    "has not been provided. Hence stopping the SiddhiApp.");
        }
        if(Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)){
            if(Constants.TRUE.equalsIgnoreCase(tailing)){
                throw new SiddhiAppRuntimeException("Tailing can't be enabled in '"+mode+"' mode.");
            }
        }
        if(Constants.MOVE.equalsIgnoreCase(actionAfterProcess) && (moveAfterProcess == null)){
            throw new SiddhiAppRuntimeException("'moveAfterProcess' has not been provided where it is mandatory when" +
                    " 'actoinAfterProcess' is 'move'. Hence stopping the SiddhiApp. ");
        }
        if(Constants.REGEX.equalsIgnoreCase(mode)){
            if(beginRegex == null && endRegex == null){
                mode = Constants.LINE;
            }
        }
    }
}
