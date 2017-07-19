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

package org.wso2.extension.siddhi.io.file.processors;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.sender.VFSClientConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.extension.siddhi.io.file.util.FileSourceConfiguration;
import org.wso2.extension.siddhi.io.file.util.FileSourceServiceProvider;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.Map;

/**
 * Message processor for handling file uri's provided by FileSystemServer.
 * */
public class FileSystemMessageProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(FileSystemMessageProcessor.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSourceServiceProvider fileSourceServiceProvider;

    public FileSystemMessageProcessor(SourceEventListener sourceEventListener,
                                      FileSourceConfiguration fileSourceConfiguration) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (carbonMessage instanceof TextCarbonMessage) {
            String mode = fileSourceConfiguration.getMode();
            String fileURI = ((TextCarbonMessage) carbonMessage).getText();
            VFSClientConnector vfsClientConnector;
            FileProcessor fileProcessor;
            if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                vfsClientConnector = new VFSClientConnector();
                fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                vfsClientConnector.setMessageProcessor(fileProcessor);

                Map<String, String> properties = new HashMap();
                properties.put(Constants.URI, fileURI);
                properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
                properties.put(Constants.ACTION, Constants.READ);
                properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());

                vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                //carbonCallback.done(carbonMessage);
            } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                vfsClientConnector = new VFSClientConnector();
                fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                vfsClientConnector.setMessageProcessor(fileProcessor);

                Map<String, String> properties = new HashMap();
                properties.put(Constants.URI, fileURI);
                properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
                properties.put(Constants.ACTION, Constants.READ);
                properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());

                vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                //carbonCallback.done(carbonMessage);
            } else if (Constants.LINE.equalsIgnoreCase(mode) || Constants.REGEX.equalsIgnoreCase(mode)) {
                Map<String, String> properties = new HashMap();
                properties.put(Constants.ACTION, Constants.READ);
                properties.put(Constants.MAX_LINES_PER_POLL, "10");
                properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());

                if (fileSourceConfiguration.isTailingEnabled()) {
                    if (fileSourceConfiguration.getTailedFileURI() == null) {
                        fileSourceConfiguration.setTailedFileURI(fileURI);
                    }

                    if (fileSourceConfiguration.getTailedFileURI().equalsIgnoreCase(fileURI)) {
                        fileSourceConfiguration.getFileSystemServerConnector().stop();
                        properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
                        properties.put(Constants.PATH, fileURI);

                        FileServerConnectorProvider fileServerConnectorProvider =
                                fileSourceServiceProvider.getFileServerConnectorProvider();
                        fileProcessor = new FileProcessor(sourceEventListener,
                                fileSourceConfiguration);
                        final ServerConnector fileServerConnector = fileServerConnectorProvider
                                .createConnector("file-server-connector", properties);
                        fileServerConnector.setMessageProcessor(fileProcessor);
                        fileSourceConfiguration.setFileServerConnector((FileServerConnector) fileServerConnector);

                        FileServerExecutor fileServerExecutor = new FileServerExecutor(carbonMessage, carbonCallback,
                                fileServerConnector, fileURI);
                        fileSourceConfiguration.getExecutorService().execute(fileServerExecutor);
                        fileSourceConfiguration.getFileSystemServerConnector().stop();
                    } else {
                        carbonCallback.done(carbonMessage);
                    }
                } else {
                    properties.put(Constants.URI, fileURI);
                    vfsClientConnector = new VFSClientConnector();
                    fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                    vfsClientConnector.setMessageProcessor(fileProcessor);

                    vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                    carbonCallback.done(carbonMessage);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public void setTransportSender(TransportSender transportSender) {

    }

    public void setClientConnector(ClientConnector clientConnector) {

    }

    public String getId() {
        return "file-system-message-processor";
    }

    static class FileServerExecutor implements Runnable {
        ServerConnector fileServerConnector = null;
        CarbonCallback carbonCallback = null;
        CarbonMessage carbonMessage = null;
        String fileURI = null;

        public FileServerExecutor(CarbonMessage carbonMessage, CarbonCallback
                carbonCallback,
                                  ServerConnector fileServerConnector, String fileURI) {
            this.fileURI = fileURI;
            this.fileServerConnector = fileServerConnector;
            this.carbonCallback = carbonCallback;
            this.carbonMessage = carbonMessage;
        }

        @Override
        public void run() {
            try {
                fileServerConnector.start();
            } catch (ServerConnectorException e) {
                log.error("Failed to start the server for file " + fileURI + ". " +
                        "Hence starting to process next file.");
                carbonCallback.done(carbonMessage);
            }
        }
    }
}
