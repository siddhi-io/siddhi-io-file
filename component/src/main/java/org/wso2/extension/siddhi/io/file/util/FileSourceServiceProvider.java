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

import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.remotefilesystem.RemoteFileSystemConnectorFactory;
import org.wso2.carbon.transport.remotefilesystem.impl.RemoteFileSystemConnectorFactoryImpl;

/**
 * Class for providing server connectors.
 */
public class FileSourceServiceProvider {
    private static RemoteFileSystemConnectorFactory fileSystemConnectorFactory =
            new RemoteFileSystemConnectorFactoryImpl();
    private static FileServerConnectorProvider fileServerConnectorProvider =
            new FileServerConnectorProvider();
    private static FileSourceServiceProvider fileSourceServiceProvider = new FileSourceServiceProvider();

    private FileSourceServiceProvider() {
    }

    public static FileSourceServiceProvider getInstance() {
        return fileSourceServiceProvider;
    }

    public RemoteFileSystemConnectorFactory getFileSystemConnectorFactory() {
        return fileSystemConnectorFactory;
    }

    public FileServerConnectorProvider getFileServerConnectorProvider() {
        return fileServerConnectorProvider;
    }
}
