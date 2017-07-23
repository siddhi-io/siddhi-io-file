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
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.sender.VFSClientConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.exception.FileSystemServerConnectorException;
import org.wso2.extension.siddhi.io.file.processors.FileProcessor;
import org.wso2.extension.siddhi.io.file.processors.FileSystemMessageProcessor;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.extension.siddhi.io.file.util.FileSourceConfiguration;
import org.wso2.extension.siddhi.io.file.util.FileSourceServiceProvider;
import org.wso2.extension.siddhi.io.file.util.VFSClientConnectorCallback;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Implementation of siddhi-io-file source.
 * */
@Extension(
        name = "file",
        namespace = "source",
        description = "" +
                "File Source provides the functionality for user to feed data to siddhi from " +
                "files. Both text and binary files are supported by file source.",
        parameters = {
                @Parameter(
                        name = "dir.uri",
                        description =
                                "Used to specify a directory to be processed. " +
                                "All the files inside this directory will be processed. " +
                                "Only one of 'dir.uri' and 'file.uri' should be provided.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "file.uri",
                        description =
                                "Used to specify a file to be processed. " +
                                        " Only one of 'dir.uri' and 'file.uri' should be provided.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "mode", // TODO : state possible values here
                        description =
                                "This parameter is used to specify how files in given directory should." +
                                "Possible values for this parameter are,\n" +
                                        "1. TEXT.FULL : to read a text file completely at once.\n" +
                                        "2. BINARY.FULL : to read a binary file completely at once.\n" +
                                        "3. LINE : to read a text file line by line.\n" +
                                        "4. REGEX : to read a text file and extract data using a regex.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "line"
                ),

                @Parameter(
                        // TODO  : tailing should't be provided with full mode : done
                        name = "tailing",
                        description = "" +
                                "This can either have value true or false. By default it will be true. " +
                                "This attribute allows user to specify whether the file should be tailed or not. " +
                                "If tailing is enabled, the first file of the directory will be tailed.\n" +
                                "Also tailing should not be enabled in 'binary.full' or 'text.full' modes.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),

                @Parameter(
                        name = "action.after.process", // TODO : default value == delete : done
                        description = "" +
                                "This parameter is used to specify the action which should be carried out " +
                                "after processing a file in the given directory. \n" +
                                "It can be either DELETE or MOVE and default value will be 'DELETE'.\n" +
                                "If the action.after.process is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.process' parameter.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "delete"
                ),

                @Parameter(
                        name = "action.after.failure", // TODO : default value == delete : done
                        description = "" +
                                "This parameter is used to specify the action which should be carried out " +
                                "if a failure occurred during the process. " +
                                "It can be either DELETE or MOVE and default value will be 'DELETE'.\n" +
                                "If the action.after.failure is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.failure' parameter.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "move.after.process",
                        description = "" +
                                "If action.after.process is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.process' parameter.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "move.after.failure",
                        description = "" +
                                "If action.after.failure is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.failure' parameter.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "move.after.failure",
                        description = "" +
                                "If action.after.failure is MOVE, user must specify the location to " +
                                "move consumed files using 'move.after.failure' parameter.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "begin.regex",
                        description = "" +
                                "This will define the regex to be matched at the beginning of the " +
                                "retrieved content. ",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "None"
                ),

                @Parameter(
                        name = "end.regex",
                        description = "" +
                                "This will define the regex to be matched at the end of the " +
                                "retrieved content. ",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "None"
                ),

                @Parameter(
                        name = "file.polling.interval",
                        description = "" +
                                "This parameter is used to specify the time period (in milliseconds) " +
                                "of a polling cycle for a file.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),

                @Parameter(
                        name = "dir.polling.interval",
                        description = "This parameter is used to specify the time period (in milliseconds) " +
                                "of a polling cycle for a directory.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),

                // TODO : add action.after.failure (default : delete) , move.after.failure : done
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@source(type='file',mode='text.full', tailing='false', " +
                                "dir.uri='/abc/xyz'," +
                                "action.after.process='delete'," +
                                "@map(type='json')) \n" +
                                "define stream FooStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, all the files in directory will be picked and read " +
                                "one by one.\n" +
                                "In this case, it's assumed that all the files contains json valid json strings with " +
                                "keys 'symbol','price' and 'volume'.\n" +
                                "Once a file is read, " +
                                "its content will be converted to an event using siddhi-map-json " +
                                "extension and then, that event will be received to the FooStream.\n" +
                                "Finally, after reading is finished, the file will be deleted."
                ),

                @Example(
                        syntax = "" +
                                "@source(type='file',mode='files.repo.line', tailing='true', " +
                                "dir.uri='/abc/xyz'," +
                                "@map(type='json')) \n" +
                                "define stream FooStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, " +
                                "the first file in directory '/abc/xyz'  will be picked and read " +
                                "line by line.\n" +
                                "In this case, it is assumed that the file contains lines json strings.\n" +
                                "For each line, line content will be converted to an event using siddhi-map-json " +
                                "extension and then, that event will be received to the FooStream.\n" +
                                "Once file content is completely read, " +
                                "it will keep checking whether a new entry is added to the file or not.\n" +
                                "If such entry is added, it will be immediately picked up and processed.\n"
                )
        }
)
public class FileSource extends Source {
    private static final Logger log = Logger.getLogger(FileSource.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private FileSourceServiceProvider fileSourceServiceProvider;
    private ServerConnector fileSystemServerConnector;
    private String filePointer = "0";
    private String[] requiredProperties;
    private boolean isTailingEnabled = true;
    private SiddhiAppContext siddhiAppContext;

    private String mode;
    private String actionAfterProcess;
    private String actionAfterFailure = null;
    private String moveAfterProcess;
    private String moveAfterFailure = null;
    private String tailing;
    private String beginRegex;
    private String endRegex;
    private String tailedFileURI;
    private String dirUri;
    private String fileUri;
    private String dirPollingInterval;
    private String filePollingInterval;

    @Override
    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, String[] requiredProperties,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.siddhiAppContext = siddhiAppContext;
        this.requiredProperties = requiredProperties.clone();
        this.fileSourceConfiguration = new FileSourceConfiguration();

        this.fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
        fileSystemServerConnectorProvider = fileSourceServiceProvider.getFileSystemServerConnectorProvider();

        if (optionHolder.isOptionExists(Constants.DIR_URI)) {
            dirUri = optionHolder.validateAndGetStaticValue(Constants.DIR_URI);
        }
        if (optionHolder.isOptionExists(Constants.FILE_URI)) {
            fileUri = optionHolder.validateAndGetStaticValue(Constants.FILE_URI);
        }

        if (dirUri != null && fileUri != null) {
            throw new SiddhiAppCreationException("Only one of directory uro or file url should be provided. But both " +
                    "have been provided.");
        }

        mode = optionHolder.validateAndGetStaticValue(Constants.MODE, Constants.LINE);

        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.FALSE);
        } else {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        }
        isTailingEnabled = Boolean.parseBoolean(tailing);

        if (isTailingEnabled) {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS,
                    Constants.NONE);
        } else {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS,
                    Constants.DELETE);
        }
        actionAfterFailure = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_FAILURE, Constants.DELETE);
        if (optionHolder.isOptionExists(Constants.MOVE_AFTER_PROCESS)) {
            moveAfterProcess = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_PROCESS);
        }
        if (optionHolder.isOptionExists(Constants.MOVE_AFTER_FAILURE)) {
            moveAfterFailure = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_FAILURE);
        }

        dirPollingInterval = optionHolder.validateAndGetStaticValue(Constants.DIRECTORY_POLLING_INTERVAL, "1000");

        filePollingInterval = optionHolder.validateAndGetStaticValue(Constants.FILE_POLLING_INTERVAL, "1000");

        beginRegex = optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null);
        endRegex = optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null);

        validateParameters();
        createInitialSourceConf();
        updateSourceConf();
    }


    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback) throws ConnectionUnavailableException {
        updateSourceConf();
        deployServers();
    }

    @Override
    public void disconnect() {
        try {
            if (fileSystemServerConnector != null) {
                fileSystemServerConnector.stop();
                fileSystemServerConnector = null;
            }
            //fileSystemServerConnector = null;
            if (isTailingEnabled) {
                fileSourceConfiguration.getFileServerConnector().stop();
                fileSourceConfiguration.setFileServerConnector(null);
            }
        } catch (ServerConnectorException e) {
           throw new SiddhiAppRuntimeException("Failed to stop the file server : " + e.getMessage());
        }
    }


    public void destroy() {

    }

    public void pause() {
        try {
            if (fileSystemServerConnector != null) {
                fileSystemServerConnector.stop();
            }
            if (isTailingEnabled && fileSourceConfiguration.getFileServerConnector() != null) {
                fileSourceConfiguration.getFileServerConnector().stop();
            }
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Failed to stop the file server : " + e.getMessage());
        }
    }

    public void resume() {
        try {
            updateSourceConf();
            deployServers();
        } catch (ConnectionUnavailableException e) {
            throw new SiddhiAppRuntimeException("Failed to resume siddhi app runtime due to " + e.getMessage(), e);
        }
    }

    public Map<String, Object> currentState() {
        Map<String, Object> currentState = new HashMap<>();
        currentState.put(Constants.FILE_POINTER, fileSourceConfiguration.getFilePointer());
        currentState.put(Constants.TAILED_FILE, fileSourceConfiguration.getTailedFileURI());
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        this.filePointer = map.get(Constants.FILE_POINTER).toString();
        this.tailedFileURI = map.get(Constants.TAILED_FILE).toString();
        fileSourceConfiguration.setFilePointer(filePointer);
    }

    private void createInitialSourceConf() {
        fileSourceConfiguration.setDirURI(dirUri);
        fileSourceConfiguration.setFileURI(fileUri);
        fileSourceConfiguration.setMoveAfterProcessUri(moveAfterProcess);
        fileSourceConfiguration.setBeginRegex(beginRegex);
        fileSourceConfiguration.setEndRegex(endRegex);
        fileSourceConfiguration.setMode(mode);
        fileSourceConfiguration.setActionAfterProcess(actionAfterProcess);
        fileSourceConfiguration.setTailingEnabled(Boolean.parseBoolean(tailing));
        fileSourceConfiguration.setFilePollingInterval(filePollingInterval);
        fileSourceConfiguration.setDirPollingInterval(dirPollingInterval);
        fileSourceConfiguration.setActionAfterFailure(actionAfterFailure);
        fileSourceConfiguration.setMoveAfterFailure(moveAfterFailure);
        fileSourceConfiguration.setRequiredProperties(requiredProperties);
    }

    private void updateSourceConf() {
        fileSourceConfiguration.setFilePointer(filePointer);
        fileSourceConfiguration.setTailedFileURI(tailedFileURI);
    }

    private Map<String, String> getFileSystemServerProperties() {
        Map<String, String> map = new HashMap<>();

        map.put(Constants.TRANSPORT_FILE_DIR_URI, dirUri);
        if (actionAfterProcess != null) {
            map.put(Constants.ACTION_AFTER_PROCESS_KEY, actionAfterProcess.toUpperCase(Locale.ENGLISH));
        }
        map.put(Constants.MOVE_AFTER_PROCESS_KEY, moveAfterProcess);
        map.put(Constants.POLLING_INTERVAL, dirPollingInterval);
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
        if (actionAfterFailure != null) {
            map.put(Constants.ACTION_AFTER_FAILURE_KEY, actionAfterFailure);
        }
        if (moveAfterFailure != null) {
            map.put(Constants.MOVE_AFTER_FAILURE_KEY, moveAfterFailure);
        }
        return map;
    }

    private void validateParameters() {
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            if (isTailingEnabled) {
                throw new SiddhiAppRuntimeException("Tailing can't be enabled in '" + mode + "' mode.");
            }
        }

        if (isTailingEnabled && moveAfterProcess != null) {
            throw new SiddhiAppRuntimeException("'moveAfterProcess' cannot be used when tailing is enabled. " +
                    "Hence stopping the SiddhiApp. ");
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

    private void deployServers() throws ConnectionUnavailableException {
        ExecutorService executorService = siddhiAppContext.getExecutorService();
        createInitialSourceConf();
        fileSourceConfiguration.setExecutorService(executorService);

        if (dirUri != null) {
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
                throw new ConnectionUnavailableException("Failed to connect to the file system server due to : " +
                        e.getMessage(), e);
            }
        } else if (fileUri != null) {
            Map<String, String> properties = new HashMap<>();
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.MAX_LINES_PER_POLL, "10");
            properties.put(Constants.POLLING_INTERVAL, filePollingInterval);
            if (actionAfterFailure != null) {
                properties.put(Constants.ACTION_AFTER_FAILURE_KEY, actionAfterFailure);
            }
            if (moveAfterFailure != null) {
                properties.put(Constants.MOVE_AFTER_FAILURE_KEY, moveAfterFailure);
            }

            if (fileSourceConfiguration.isTailingEnabled()) {
                if (fileSourceConfiguration.getTailedFileURI() == null) {
                    fileSourceConfiguration.setTailedFileURI(fileUri);
                }

                if (fileSourceConfiguration.getTailedFileURI().equalsIgnoreCase(fileUri)) {
                    properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
                    properties.put(Constants.PATH, fileUri);

                    FileServerConnectorProvider fileServerConnectorProvider =
                            fileSourceServiceProvider.getFileServerConnectorProvider();
                    FileProcessor fileProcessor = new FileProcessor(sourceEventListener,
                            fileSourceConfiguration);
                    final ServerConnector fileServerConnector = fileServerConnectorProvider
                            .createConnector("file-server-connector", properties);
                    fileServerConnector.setMessageProcessor(fileProcessor);
                    fileSourceConfiguration.setFileServerConnector((FileServerConnector) fileServerConnector);

                    Runnable runnableServer = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                fileServerConnector.start();
                            } catch (ServerConnectorException e) {
                                log.error("Failed to start the server for file " + fileUri + ". " +
                                        "Hence starting to process next file.");
                            }
                        }
                    };
                    fileSourceConfiguration.getExecutorService().execute(runnableServer);
                }
            } else {
                // TODO: 23/7/17 When VFSClient is called alone, it keeps the thread running even after job is done
                properties.put(Constants.URI, fileUri);
                properties.put(Constants.ACK_TIME_OUT, "1000");
                VFSClientConnector vfsClientConnector = new VFSClientConnector();
                FileProcessor fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                vfsClientConnector.setMessageProcessor(fileProcessor);
                VFSClientConnectorCallback vfsClientConnectorCallback = new VFSClientConnectorCallback();

                Runnable runnableClient = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            vfsClientConnector.send(null, vfsClientConnectorCallback, properties);
                            vfsClientConnectorCallback.waitTillDone(2000, fileUri);
                            if (moveAfterProcess != null) {
                                properties.put(Constants.URI, fileUri);
                                properties.put(Constants.ACTION, Constants.MOVE);
                                properties.put(Constants.DESTINATION, moveAfterProcess);
                                vfsClientConnector.send(null, vfsClientConnectorCallback, properties);
                            }
                        } catch (ClientConnectorException e) {
                            log.error("Failed to start the server for file " + fileUri + ". " +
                                    "Hence starting to process next file.");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                };

                fileSourceConfiguration.getExecutorService().execute(runnableClient);
            }
        }
    }

}
