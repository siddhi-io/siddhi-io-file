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

package org.wso2.extension.siddhi.io.file.util;

import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.query.api.SiddhiApp;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class for keep the configurations of a file source instance.
 * */
public class FileSourceConfiguration {

    private String actionAfterProcess;
    private String moveAfterProcessUri;
    private boolean isTailingEnabled;
    private String dirURI;
    private String mode;
    private String beginRegex = null;
    private String endRegex = null;
    private String filePointer = "0";
    private Executor executor;
    private CarbonMessageProcessor messageProcessor;
    private FileServerConnector fileServerConnector;
    private ServerConnector fileSystemServerConnector;
    private String tailedFileURI = null;
    private ExecutorService executorService = null;
    private String[] requiredProperties = null;


    public FileSourceConfiguration() {
        executor = Executors.newSingleThreadExecutor();
    }

    public String getBeginRegex() {
        return beginRegex;
    }

    public void setBeginRegex(String beginRegex) {
        this.beginRegex = beginRegex;
    }

    public String getEndRegex() {
        return endRegex;
    }

    public void setEndRegex(String endRegex) {
        this.endRegex = endRegex;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getActionAfterProcess() {
        return actionAfterProcess;
    }

    public void setActionAfterProcess(String actionAfterProcess) {
        this.actionAfterProcess = actionAfterProcess;
    }

    public String getMoveAfterProcessUri() {
        return moveAfterProcessUri;
    }

    public void setMoveAfterProcessUri(String moveAfterProcessUri) {
        this.moveAfterProcessUri = moveAfterProcessUri;
    }

    public boolean isTailingEnabled() {
        return isTailingEnabled;
    }

    public void setTailingEnabled(boolean tailingEnabled) {
        isTailingEnabled = tailingEnabled;
    }

    public String getDirURI() {
        return dirURI;
    }

    public void setDirURI(String dirURI) {
        this.dirURI = dirURI;
    }

    public String getFilePointer() {
        return filePointer;
    }

    public void setFilePointer(String filePointer) {
        this.filePointer = filePointer;
    }

    public void updateFilePointer(int valueToAdd) {
        long filePointer = Long.parseLong(this.filePointer);
        filePointer += valueToAdd;
        this.filePointer = Long.toString(filePointer);
    }

    public CarbonMessageProcessor getMessageProcessor() {
        return messageProcessor;
    }

    public void setMessageProcessor(CarbonMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    public FileServerConnector getFileServerConnector() {
        return fileServerConnector;
    }

    public void setFileServerConnector(FileServerConnector fileServerConnector) {
        this.fileServerConnector = fileServerConnector;
    }

    public ServerConnector getFileSystemServerConnector() {
        return fileSystemServerConnector;
    }

    public void setFileSystemServerConnector(ServerConnector fileSystemServerConnector) {
        this.fileSystemServerConnector = fileSystemServerConnector;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public String getTailedFileURI() {
        return tailedFileURI;
    }

    public void setTailedFileURI(String tailedFileURI) {
        this.tailedFileURI = tailedFileURI;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public String[] getRequiredProperties() {
        return requiredProperties;
    }

    public void setRequiredProperties(String[] requiredProperties) {
        this.requiredProperties = requiredProperties;
    }
}
