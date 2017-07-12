package org.wso2.extension.siddhi.io.file.util;

import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;

/**
 * Class for providing server connectors.
 */
public class FileSourceServiceProvider {
    private static FileSystemServerConnectorProvider fileSystemServerConnectorProvider =
            new FileSystemServerConnectorProvider();
    private static FileServerConnectorProvider fileServerConnectorProvider =
            new FileServerConnectorProvider();
    private static FileSourceServiceProvider fileSourceServiceProvider = new FileSourceServiceProvider();

    private FileSourceServiceProvider() {
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
