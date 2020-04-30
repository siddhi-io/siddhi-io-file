/*
 * Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.file.listeners;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.file.processors.FileProcessor;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.FileSourceServiceProvider;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;
import org.wso2.transport.file.connector.server.FileServerConnector;
import org.wso2.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.transport.remotefilesystem.listener.RemoteFileSystemListener;
import org.wso2.transport.remotefilesystem.message.RemoteFileSystemBaseMessage;
import org.wso2.transport.remotefilesystem.message.RemoteFileSystemEvent;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.siddhi.extension.io.file.util.GenerateAppProperties.generateProperties;

/**
 * Test {@link RemoteFileSystemListener} implementation for testing purpose.
 */
public class FileSystemListener implements RemoteFileSystemListener {
    private static final Logger log = Logger.getLogger(FileSystemListener.class);
    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSourceServiceProvider fileSourceServiceProvider;

    public FileSystemListener(SourceEventListener sourceEventListener,
                              FileSourceConfiguration fileSourceConfiguration) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
    }

    @Override
    public boolean onMessage(RemoteFileSystemBaseMessage remoteFileSystemBaseEvent) {
        if (remoteFileSystemBaseEvent instanceof RemoteFileSystemEvent) {
            String mode = fileSourceConfiguration.getMode();
            String actionAfterProcess = fileSourceConfiguration.getActionAfterProcess();
            RemoteFileSystemEvent remoteFileSystemEvent = (RemoteFileSystemEvent) remoteFileSystemBaseEvent;
            for (int i = 0; i < remoteFileSystemEvent.getAddedFiles().size(); i++) {
                File a = new File(remoteFileSystemEvent.getAddedFiles().get(i).getPath());
                String fileURI = a.toURI().toString();
                VFSClientConnector vfsClientConnector;
                FileProcessor fileProcessor;
                if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                    vfsClientConnector = new VFSClientConnector();
                    fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                    vfsClientConnector.setMessageProcessor(fileProcessor);
                    Map<String, String> properties = generateProperties(fileSourceConfiguration, fileURI);
                    VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                    BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(
                            ByteBuffer.wrap(fileURI.getBytes(StandardCharsets.UTF_8)), true);
                    try {
                        vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                        try {
                            carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
                        } catch (InterruptedException e) {
                            log.error(String.format("Failed to wait until file '%s' is processed.", fileURI), e);
                            return false;
                        }
                        if (!actionAfterProcess.equalsIgnoreCase(Constants.KEEP)) {
                            reProcessFile(vfsClientConnector, carbonCallback, properties, fileURI);
                        }
                    } catch (ClientConnectorException e) {
                        log.error(String.format("Failed to provide file '%s' for consuming.", fileURI), e);
                        carbonCallback.done(carbonMessage);
                    }
                } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                    vfsClientConnector = new VFSClientConnector();
                    fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                    vfsClientConnector.setMessageProcessor(fileProcessor);
                    Map<String, String> properties = generateProperties(fileSourceConfiguration, fileURI);
                    VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                    BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(
                            ByteBuffer.wrap(fileURI.getBytes(StandardCharsets.UTF_8)), true);
                    try {
                        vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                        try {
                            carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
                        } catch (InterruptedException e) {
                            log.error(String.format("Failed to get callback from vfs-client  for file '%s'.", fileURI),
                                    e);
                            return false;
                        }
                        if (!actionAfterProcess.equalsIgnoreCase(Constants.KEEP)) {
                            reProcessFile(vfsClientConnector, carbonCallback, properties, fileURI);
                        }
                    } catch (ClientConnectorException e) {
                        log.error(String.format("Failed to provide file '%s' for consuming.", fileURI), e);
                    }
                } else if (Constants.LINE.equalsIgnoreCase(mode) || Constants.REGEX.equalsIgnoreCase(mode)) {
                    Map<String, String> properties = generateProperties(fileSourceConfiguration, fileURI);
                    if (fileSourceConfiguration.isTailingEnabled()) {
                        fileSourceConfiguration.setTailedFileURI(fileURI);
                        if (fileSourceConfiguration.getTailedFileURIMap().contains(fileURI)) {
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
                            VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                            BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                                    fileURI.getBytes(StandardCharsets.UTF_8)), true);
                            FileServerExecutor fileServerExecutor = new FileServerExecutor(carbonMessage,
                                    carbonCallback, fileServerConnector, fileURI);
                            if (log.isDebugEnabled()) {
                                log.debug("fileServerExecutor started with file tailing for file: " + fileURI);
                            }
                            fileSourceConfiguration.getExecutorService().execute(fileServerExecutor);
                        }
                    } else {
                        vfsClientConnector = new VFSClientConnector();
                        fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
                        vfsClientConnector.setMessageProcessor(fileProcessor);
                        VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                                fileURI.getBytes(StandardCharsets.UTF_8)), true);
                        try {
                            vfsClientConnector.send(carbonMessage, carbonCallback, properties);
                            try {
                                carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
                            } catch (InterruptedException e) {
                                log.error(String.format("Failed to get callback from vfs-client  for file '%s'.",
                                        fileURI), e);
                                return false;
                            }
                            if (!actionAfterProcess.equalsIgnoreCase(Constants.KEEP)) {
                                reProcessFile(vfsClientConnector, carbonCallback, properties, fileURI);
                            }
                        } catch (ClientConnectorException e) {
                            log.error(String.format("Failed to provide file '%s' for consuming.", fileURI), e);
                        }
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void done() {
    }

    static class FileServerExecutor implements Runnable {
        ServerConnector fileServerConnector = null;
        CarbonCallback carbonCallback = null;
        CarbonMessage carbonMessage = null;
        String fileURI = null;

        FileServerExecutor(CarbonMessage carbonMessage, CarbonCallback carbonCallback,
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
                log.error(String.format("Failed to start the server for file '%s'. " +
                        "Hence starting to process next file.", fileURI));
                carbonCallback.done(carbonMessage);
            }
        }
    }

    private void reProcessFile(VFSClientConnector vfsClientConnector,
                               VFSClientConnectorCallback vfsClientConnectorCallback,
                               Map<String, String> properties, String fileUri) {
        String actionAfterProcess = fileSourceConfiguration.getActionAfterProcess();
        properties.put(Constants.URI, fileUri);
        properties.put(Constants.ACK_TIME_OUT, "1000");
        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                fileUri.getBytes(StandardCharsets.UTF_8)), true);
        String moveAfterProcess = fileSourceConfiguration.getMoveAfterProcess();
        try {
            if (fileSourceConfiguration.getActionAfterProcess() != null) {
                properties.put(Constants.URI, fileUri);
                properties.put(Constants.ACTION, actionAfterProcess);
                if (fileSourceConfiguration.getMoveAfterProcess() != null) {
                    String destination = constructPath(moveAfterProcess, getFileName(fileUri,
                            fileSourceConfiguration.getProtocolForMoveAfterProcess()));
                    if (destination != null) {
                        properties.put(Constants.DESTINATION, destination);
                    }
                }
                vfsClientConnector.send(carbonMessage, vfsClientConnectorCallback, properties);
                vfsClientConnectorCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileUri);
            }
        } catch (ClientConnectorException e) {
            log.error(String.format("Failure occurred in vfs-client while reading the file '%s'.", fileUri), e);
        } catch (InterruptedException e) {
            log.error(String.format("Failed to get callback from vfs-client for file '%s'.", fileUri), e);
        }
    }

    private String getFileName(String uri, String protocol) {
        try {
            URL url = new URL(String.format("%s%s%s", protocol, File.separator, uri));
            return FilenameUtils.getName(url.getPath());
        } catch (MalformedURLException e) {
            log.error(String.format("Failed to extract file name from the uri '%s'.", uri), e);
            return null;
        }
    }

    private String constructPath(String baseUri, String fileName) {
        if (baseUri != null && fileName != null) {
            if (baseUri.endsWith(File.separator)) {
                return String.format("%s%s", baseUri, fileName);
            } else {
                return String.format("%s%s%s", baseUri, File.separator, fileName);
            }
        } else {
            return null;
        }
    }
}
