package org.wso2.extension.siddhi.io.file.util;

import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;

/**
 * Class for providing server connectors.
 */
public class FileSourceServiceProvider {
    private static FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private static FileServerConnectorProvider fileServerConnectorProvider;
    private static FileSourceServiceProvider fileSourceServiceProvider = new FileSourceServiceProvider();

    private FileSourceServiceProvider() {
        fileServerConnectorProvider = new FileServerConnectorProvider();
        fileSystemServerConnectorProvider = new FileSystemServerConnectorProvider();
    }

    public static FileSourceServiceProvider getInstance() {
        return fileSourceServiceProvider;
    }

    public static FileSystemServerConnectorProvider getFileSystemServerConnectorProvider() {
        return fileSystemServerConnectorProvider;
    }

    public static FileServerConnectorProvider getFileServerConnectorProvider() {
        return fileServerConnectorProvider;
    }
}
