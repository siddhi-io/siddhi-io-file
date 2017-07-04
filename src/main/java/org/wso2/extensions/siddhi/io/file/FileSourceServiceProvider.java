package org.wso2.extensions.siddhi.io.file;

import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;

import java.util.ArrayList;

public class FileSourceServiceProvider {
    private static FileSourceServiceProvider fileSourceServiceProvider = new FileSourceServiceProvider();
    private static FileServerConnectorProvider fileServerConnectorProvider;
    private static FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private static int serverConnectorCount = 0;
    private static int systemServerConnectorCount = 0;
    private static int vfsClientConnectorCount = 0;
    private static ArrayList<String> serverConnectorIDs;
    private static ArrayList<String> systemServerConnectorIDs;
    private static String systemServerConnectorIDPrefex = "file-system-server-connector-";
    private static String serverConnectorIDPrefex = "file-server-connector-";
    private static String vfsClientConnectorIDPrefex = "vfs-client-connector-";

    private FileSourceServiceProvider(){
        fileServerConnectorProvider = new FileServerConnectorProvider();
        fileSystemServerConnectorProvider = new FileSystemServerConnectorProvider();
        serverConnectorIDs = new ArrayList<>();
        systemServerConnectorIDs = new ArrayList<>();
    }

    public static FileSourceServiceProvider getInstance(){
        return fileSourceServiceProvider;
    }

    public  FileServerConnectorProvider getFileServerConnectorProvider(){
        return fileServerConnectorProvider;
    }

    public  FileSystemServerConnectorProvider getFileSystemServerConnectorProvider(){
        return fileSystemServerConnectorProvider;
    }

    public  String getServerConnectorID(){
        String id = serverConnectorIDPrefex + Integer.toString(serverConnectorCount++);
        serverConnectorIDs.add(id);
        return id;
    }

    public String getSystemServerConnectorID(){
        String id = systemServerConnectorIDPrefex + Integer.toString(systemServerConnectorCount++);
        systemServerConnectorIDs.add(id);
        return id;
    }

    public String getVFSClientConnectorID(){
        return vfsClientConnectorIDPrefex + Integer.toString(vfsClientConnectorCount++);
    }

    public ArrayList<String> getServerConnectorIDList(){
        return serverConnectorIDs;
    }

    public ArrayList<String>  getSystemServerConnectorIDList(){
        return systemServerConnectorIDs;
    }


}
