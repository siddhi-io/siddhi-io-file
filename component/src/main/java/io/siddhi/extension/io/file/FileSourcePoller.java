/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.file;

import org.apache.log4j.Logger;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.server.connector.contract.RemoteFileSystemServerConnector;

/**
 * Polls files in a directory for changes and uses a callback to handle connection unavailable exception
 */
public class FileSourcePoller implements Runnable {
    private static final Logger log = Logger.getLogger(FileSourcePoller.class);
    private CompletionCallback completionCallback;
    private RemoteFileSystemServerConnector fileSystemServerConnector;
    private String siddhiAppName;

    public FileSourcePoller(RemoteFileSystemServerConnector fileSystemServerConnector, String siddhiAppName) {
        this.fileSystemServerConnector = fileSystemServerConnector;
        this.siddhiAppName = siddhiAppName;
    }

    public void setCompletionCallback(CompletionCallback completionCallback) {
        this.completionCallback = completionCallback;
    }

    @Override
    public void run() {
        try {
            fileSystemServerConnector.poll();
        } catch (RemoteFileSystemConnectorException e) {
            completionCallback.handle(e);
            log.error("Failed to connect to the remote file system server through " +
                    "the siddhi app '" + siddhiAppName + "'. ", e);
        } catch (Throwable t) {
            log.error("Exception occurred when processing '" + siddhiAppName + "'. ", t);
        }
    }

    /**
     * A callback function to be notified when {@code FileSourcePoller} throws an Error.
     */
    public interface CompletionCallback {
        /**
         * Handle errors from FileSystemServerConnector pooling}.
         *
         * @param error the error.
         */
        void handle(Throwable error);
    }
}
