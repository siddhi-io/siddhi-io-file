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
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;
import org.wso2.extension.siddhi.io.file.processors.FileSystemMessageProcessor;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.extension.siddhi.io.file.util.FileSourceConfiguration;
import org.wso2.extension.siddhi.io.file.util.FileSourceServiceProvider;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Implementation of siddhi-io-file source.
 * */
@Extension(
        name = "file",
        namespace = "source",
        description = "File Source",
        parameters = {
                @Parameter(
                        name = "uri",
                        description =
                                "Used to specify the directory to be processed. " +
                                        " All the files inside this directory will be processed",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "mode",
                        description =
                                "This parameter is used to specify how files in given directory should",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "tailing",
                        description = "This can either have value true or false. By default it will be true. This "
                                + "attribute allows user to specify whether the file should be tailed or not. " +
                                "If tailing is enabled, the first file of the directory will be tailed.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),

                @Parameter(
                        name = "action.after.process",
                        description = "This parameter is used to specify the action which should be carried out " +
                                "after processing a file in the given directory. " +
                                "It can be either DELETE or MOVE. " +
                                "If the action.after.process is MOVE, user must specify the location to " +
                                "move consumed files.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "move.after.process",
                        description = "If action.after.process is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.process' parameter.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "begin.regex",
                        description = "This will define the regex to be matched at the beginning of the " +
                                "retrieved content. ",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = ".+"
                ),

                @Parameter(
                        name = "end.regex",
                        description = "This will define the regex to be matched at the end of the " +
                                "retrieved content. ",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "\n"
                ),
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@source(type='file',mode='text.full', tailing='false', " +
                                "uri='/abc/xyz'," +
                                "action.after.process='delete'," +
                                "@map(type='json'))" +
                                "define stream FooStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, all the files in directory will be picked and read " +
                                "one by one." +
                                "After reading is finished, the file will be deleted."
                ),

                @Example(
                        syntax = "" +
                                "@source(type='file',mode='text.full', tailing='true', " +
                                "uri='/abc/xyz'," +
                                "@map(type='json'))" +
                                "define stream FooStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, the first file in the directory will be " +
                                "picked and consumed. It will also be tailed. "

                )
        }
)
public class FileSource extends Source {
    private static final Logger log = Logger.getLogger(FileSource.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private ServerConnector fileSystemServerConnector;
    private FileSourceServiceProvider fileSourceServiceProvider;
    private Map<String, Object> currentState;
    private String filePointer = "0";
    private String[] requiredProperties;
    private ExecutorService executorService;
    private OptionHolder optionHolder;

    private String uri;
    private String mode;
    private String actionAfterProcess;
    private String moveAfterProcess;
    private String tailing;
    private String beginRegex;
    private String endRegex;
    private String tailedFileURI;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] requiredProperties,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.currentState = new HashMap();
        this.executorService = siddhiAppContext.getExecutorService();
        this.optionHolder = optionHolder;
        this.requiredProperties = requiredProperties;

        fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
        fileSystemServerConnectorProvider = fileSourceServiceProvider.getFileSystemServerConnectorProvider();

        uri = optionHolder.validateAndGetStaticValue(Constants.URI, null);
        actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS, null);
        mode = optionHolder.validateAndGetStaticValue(Constants.MODE, null);
        moveAfterProcess = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_PROCESS,
                null);
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.FALSE);
        } else {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        }
        beginRegex = optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null);
        endRegex = optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null);

        validateParameters();
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[0];
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        fileSourceConfiguration = createInitialSourceConf();
        fileSourceConfiguration.setRequiredProperties(getTrpList(requiredProperties, optionHolder));
        fileSourceConfiguration.setExecutorService(executorService);
        Map<String, String> properties = getFileSystemServerProperties();
        fileSystemServerConnector = fileSystemServerConnectorProvider.createConnector("fileSystemServerConnector",
                properties);
        FileSystemMessageProcessor fileSystemMessageProcessor = new FileSystemMessageProcessor(sourceEventListener,
                fileSourceConfiguration);
        fileSystemServerConnector.setMessageProcessor(fileSystemMessageProcessor);
        fileSourceConfiguration.setFileSystemServerConnector(fileSystemServerConnector);

        try {
            fileSystemServerConnector.start();
        } catch (ServerConnectorException e) {
            throw new ConnectionUnavailableException("Failed to connect to the file system server. Trying again.");
        }
    }

    @Override
    public void disconnect() {
        try {
            fileSystemServerConnector.stop();
            if (Constants.TRUE.equalsIgnoreCase(tailing)) {
                fileSourceConfiguration.getFileServerConnector().stop();
            }
        } catch (ServerConnectorException e) {
           throw new SiddhiAppRuntimeException("Failed to stop the file server : " + e.getMessage());
        }
    }


    public void destroy() {

    }

    public void pause() {

    }

    public void resume() {

    }

    public Map<String, Object> currentState() {
        currentState.put(Constants.FILE_POINTER, fileSourceConfiguration.getFilePointer());
        currentState.put(Constants.TAILED_FILE, fileSourceConfiguration.getTailedFileURI());
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        this.filePointer = map.get(Constants.FILE_POINTER).toString();
        this.tailedFileURI = map.get(Constants.TAILED_FILE).toString();
        fileSourceConfiguration.setFilePointer(filePointer);
    }

    private FileSourceConfiguration createInitialSourceConf() {
        FileSourceConfiguration conf = new FileSourceConfiguration();
        conf.setDirURI(uri);
        conf.setMoveAfterProcessUri(moveAfterProcess);
        conf.setBeginRegex(beginRegex);
        conf.setEndRegex(endRegex);
        conf.setMode(mode);
        conf.setActionAfterProcess(actionAfterProcess);
        conf.setTailingEnabled(Boolean.parseBoolean(tailing));
        conf.setFilePointer(filePointer);
        conf.setTailedFileURI(tailedFileURI);
        return conf;
    }

    private HashMap<String, String> getFileSystemServerProperties() {
        HashMap<String, String> map = new HashMap();

        map.put(Constants.TRANSPORT_FILE_DIR_URI, uri);
        if (actionAfterProcess != null) {
            map.put(Constants.ACTION_AFTER_PROCESS_KEY, actionAfterProcess.toUpperCase());
        }
        map.put(Constants.MOVE_AFTER_PROCESS_KEY, moveAfterProcess);
        map.put(Constants.POLLING_INTERVAL, "1000");
        map.put(Constants.FILE_SORT_ATTRIBUTE, Constants.NAME);
        map.put(Constants.FILE_SORT_ASCENDING, Constants.TRUE);
        map.put(Constants.CREATE_MOVE_DIR, Constants.TRUE);
        map.put(Constants.ACK_TIME_OUT, "5000");

        if (Constants.BINARY_FULL.equalsIgnoreCase(mode) ||
                Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
        } else {
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.FALSE);
        }
        return map;
    }


    private String[] getTrpList(String[] requiredProperties, OptionHolder optionHolder){
        String[] list = new String[requiredProperties.length];
        int i=0;
        for(String property : requiredProperties){
            String value = optionHolder.validateAndGetStaticValue(property, null);
            if(value != null){
                list[i++] = value;
            } else {
                throw new SiddhiAppCreationException("Required property '" + property + "' has not been provided in " +
                        "transport configuration.");
            }
        }
        return list;
    }

    private void validateParameters() {
        if (uri == null) {
            throw new SiddhiAppRuntimeException("Uri is a mandatory parameter and has not been provided." +
                    " Hence stopping the SiddhiApp.");
        }
        if (actionAfterProcess == null && !Constants.TRUE.equalsIgnoreCase(tailing)) {
            throw new SiddhiAppRuntimeException("actionAfterProcess is mandatory when tailing is not enabled but " +
                    "has not been provided. Hence stopping the SiddhiApp.");
        }
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            if (Constants.TRUE.equalsIgnoreCase(tailing)) {
                throw new SiddhiAppRuntimeException("Tailing can't be enabled in '" + mode + "' mode.");
            }
        }
        if (Constants.MOVE.equalsIgnoreCase(actionAfterProcess) && (moveAfterProcess == null)) {
            throw new SiddhiAppRuntimeException("'moveAfterProcess' has not been provided where it is mandatory when" +
                    " 'actionAfterProcess' is 'move'. Hence stopping the SiddhiApp. ");
        }
        if (Constants.REGEX.equalsIgnoreCase(mode)) {
            if (beginRegex == null && endRegex == null) {
                mode = Constants.LINE;
            }
        }
    }

}
