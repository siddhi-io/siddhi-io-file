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
import io.siddhi.extension.io.file.metrics.SourceMetrics;
import io.siddhi.extension.io.file.metrics.StreamStatus;
import io.siddhi.extension.io.file.processors.FileProcessor;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.FileSourceServiceProvider;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import io.siddhi.extension.util.Utils;
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

import static io.siddhi.extension.io.file.util.Util.constructPath;
import static io.siddhi.extension.io.file.util.Util.generateProperties;
import static io.siddhi.extension.io.file.util.Util.getFileName;
import static io.siddhi.extension.io.file.util.Util.reProcessFileGenerateProperties;

/**
 * Test {@link RemoteFileSystemListener} implementation for testing purpose.
 */
public class FileSystemListener implements RemoteFileSystemListener {
    private static final Logger log = Logger.getLogger(FileSystemListener.class);
    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSourceServiceProvider fileSourceServiceProvider;
    private SourceMetrics metrics;

    public FileSystemListener(SourceEventListener sourceEventListener,
                              FileSourceConfiguration fileSourceConfiguration, SourceMetrics sourceMetrics) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
        this.metrics = sourceMetrics;
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
                fileSourceConfiguration.setCurrentlyReadingFileURI(fileURI);
                String shortenFilePath = Utils.getShortFilePath(fileURI);
                if (metrics != null) {
                    metrics.getSourceFileStatusMap().putIfAbsent(shortenFilePath, StreamStatus.PROCESSING);
                    metrics.setFilePath(fileURI);
                }
                if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                    vfsClientConnector = new VFSClientConnector();
                    fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration, metrics);
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
                        if (metrics != null) {
                            metrics.getSourceFileStatusMap().replace(shortenFilePath, StreamStatus.ERROR);
                        }
                    }
                } else if (Constants.BINARY_CHUNKED.equalsIgnoreCase(mode) ||
                        Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                    vfsClientConnector = new VFSClientConnector();
                    fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration, metrics);
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
                        if (metrics != null) {
                            metrics.getSourceFileStatusMap().replace(shortenFilePath, StreamStatus.ERROR);
                        }
                    }
                } else if (Constants.LINE.equalsIgnoreCase(mode) || Constants.REGEX.equalsIgnoreCase(mode)) {
                    Map<String, String> properties = generateProperties(fileSourceConfiguration, fileURI);
                    if (fileSourceConfiguration.isTailingEnabled()) {
                        fileSourceConfiguration.setTailedFileURI(fileURI);
                        if (metrics != null) {
                            metrics.getTailEnabledFilesMap().putIfAbsent(shortenFilePath,
                                    System.currentTimeMillis());
                        }
                        if (fileSourceConfiguration.getTailedFileURIMap().contains(fileURI)) {
                            properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
                            properties.put(Constants.PATH, fileURI);
                            FileServerConnectorProvider fileServerConnectorProvider =
                                    fileSourceServiceProvider.getFileServerConnectorProvider();
                            fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration, metrics);
                            final ServerConnector fileServerConnector = fileServerConnectorProvider
                                    .createConnector("file-server-connector", properties);
                            fileServerConnector.setMessageProcessor(fileProcessor);
                            fileSourceConfiguration.setFileServerConnector((FileServerConnector) fileServerConnector);
                            VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                            BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                                    fileURI.getBytes(StandardCharsets.UTF_8)), true);
                            FileServerExecutor fileServerExecutor = new FileServerExecutor(carbonMessage,
                                    carbonCallback, fileServerConnector, fileURI, metrics);
                            if (log.isDebugEnabled()) {
                                log.debug("fileServerExecutor started with file tailing for file: " + fileURI);
                            }
                            fileSourceConfiguration.getExecutorService().execute(fileServerExecutor);
                        }
                    } else {
                        vfsClientConnector = new VFSClientConnector();
                        fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration, metrics);
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
                            if (metrics != null) {
                                metrics.getSourceFileStatusMap().replace(shortenFilePath, StreamStatus.ERROR);
                            }
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
        SourceMetrics metrics;

        FileServerExecutor(CarbonMessage carbonMessage, CarbonCallback carbonCallback,
                           ServerConnector fileServerConnector, String fileURI, SourceMetrics metrics) {
            this.fileURI = fileURI;
            this.fileServerConnector = fileServerConnector;
            this.carbonCallback = carbonCallback;
            this.carbonMessage = carbonMessage;
            this.metrics = metrics;
        }

        @Override
        public void run() {
            try {
                fileServerConnector.start();
            } catch (ServerConnectorException e) {
                log.error(String.format("Failed to start the server for file '%s'. " +
                        "Hence starting to process next file.", fileURI));
                carbonCallback.done(carbonMessage);
                if (metrics != null) {
                    metrics.getSourceFileStatusMap().replace(Utils.getShortFilePath(fileURI), StreamStatus.ERROR);
                }
            }
        }
    }

    private void reProcessFile(VFSClientConnector vfsClientConnector,
                               VFSClientConnectorCallback vfsClientConnectorCallback,
                               Map<String, String> properties, String fileUri) {
        Map<String, String> reGeneratedProperties = reProcessFileGenerateProperties(fileSourceConfiguration, fileUri,
                properties);
        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                fileUri.getBytes(StandardCharsets.UTF_8)), true);
        String moveAfterProcess = fileSourceConfiguration.getMoveAfterProcess();
        if (metrics != null) {
            metrics.setFilePath(fileUri);
        }
        try {
            if (fileSourceConfiguration.getActionAfterProcess() != null) {
                if (fileSourceConfiguration.getMoveAfterProcess() != null) {
                    String destination = constructPath(moveAfterProcess, getFileName(fileUri,
                            fileSourceConfiguration.getProtocolForMoveAfterProcess()));
                    if (destination != null) {
                        reGeneratedProperties.put(Constants.DESTINATION, destination);
                    }
                }
                vfsClientConnector.send(carbonMessage, vfsClientConnectorCallback, reGeneratedProperties);
                vfsClientConnectorCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileUri);
                if (metrics != null) {
                    fileSourceConfiguration.getExecutorService().execute(() -> {
                        metrics.getSourceFileStatusMap().replace(Utils.getShortFilePath(fileUri),
                                StreamStatus.COMPLETED);
                        increaseMetricsAfterProcess(fileSourceConfiguration.getMoveAfterProcess(), 1);
                    });
                }
            }

        } catch (ClientConnectorException e) {
            if (metrics != null) {
                increaseMetricsAfterProcess(fileSourceConfiguration.getMoveAfterProcess(), 0);
            }
            log.error(String.format("Failure occurred in vfs-client while reading the file '%s'.", fileUri), e);
        } catch (InterruptedException e) {
            log.error(String.format("Failed to get callback from vfs-client for file '%s'.", fileUri), e);
        } finally {
            if (metrics != null) {
                metrics.setFilePath(fileUri);
                metrics.getCompletedTimeMetric(System.currentTimeMillis());
            }
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

    private void increaseMetricsAfterProcess(String moveAfterProcess, int value) {
        if (moveAfterProcess == null) {
            metrics.getFileDeleteMetrics().setTime(System.currentTimeMillis());
            metrics.getFileDeleteMetrics().getDeleteMetric(value);
        } else {
            metrics.getFileMoveMetrics().setDestination(Utils.getShortFilePath(moveAfterProcess));
            metrics.getFileMoveMetrics().setTime(System.currentTimeMillis());
            metrics.getFileMoveMetrics().getMoveMetric(value);
        }
        metrics.setReadPercentage(100.0);
    }
}
