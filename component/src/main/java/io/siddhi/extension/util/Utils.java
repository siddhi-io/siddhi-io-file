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

package io.siddhi.extension.util;

import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.extension.io.file.metrics.Metrics;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.log4j.Logger;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.server.util.FileTransportUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.siddhi.extension.util.Constant.VFS_SCHEME_KEY;

/**
 * Util Class.
 */
public class Utils {
    private static final Logger log = Logger.getLogger(Utils.class);

    /**
     * Returns the FileObject in the given file path
     *
     * @param filePathUri file or directory path
     * @return FileObject retried by the given uri
     */
    public static FileObject getFileObject(String filePathUri, String fileSystemOptions) {
        Map<String, String> fileSystemOptionMap = getFileSystemOptionMap(filePathUri, fileSystemOptions);
        FileSystemOptions sourceFso;
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            sourceFso = FileTransportUtils.attachFileSystemOptions(fileSystemOptionMap);
            SftpFileSystemConfigBuilder configBuilder = SftpFileSystemConfigBuilder.getInstance();
            if (fileSystemOptionMap.get(Constant.SFTP_SESSION_TIMEOUT) != null) {
                configBuilder.setTimeout(sourceFso, Integer.parseInt(fileSystemOptionMap.get("session.timeout")));
                log.info("The session timeout was set successfully, the configured timeout is : "
                        + fileSystemOptionMap.get("session.timeout"));
            }
            return fsManager.resolveFile(filePathUri, sourceFso);
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting VFS manager", e);
        } catch (RemoteFileSystemConnectorException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when parsing scheme file options for path: " +
                    fileSystemOptionMap, e);
        }
    }

    public static Map<String, String> getFileSystemOptionMap(String filePathUri, String fileSystemOptions) {
        Map<String, String> fileSystemOptionMap = new HashMap();
        String scheme = UriParser.extractScheme(filePathUri);
        if (scheme != null) {
            fileSystemOptionMap.put(VFS_SCHEME_KEY, scheme);
        }
        if (fileSystemOptions != null) {
            String[] configs = fileSystemOptions.split(",");
            for (String config : configs) {
                String[] configKeyValue = config.split(":");
                fileSystemOptionMap.put(configKeyValue[0], configKeyValue[1]);
            }
        }
        fileSystemOptionMap.put(org.wso2.transport.remotefilesystem.Constants.URI, filePathUri);
        return fileSystemOptionMap;
    }

    public static Map<String, Object> getFileSystemOptionObjectMap(String filePathUri, String fileSystemOptions) {
        Map<String, Object> fileSystemOptionMap = new HashMap();
        if (filePathUri != null) {
            String scheme = UriParser.extractScheme(filePathUri);
            if (scheme != null) {
                fileSystemOptionMap.put(VFS_SCHEME_KEY, scheme);
            }
        }
        if (fileSystemOptions != null) {
            String[] configs = fileSystemOptions.split(",");
            for (String config : configs) {
                String[] configKeyValue = config.split(":");
                fileSystemOptionMap.put(configKeyValue[0], configKeyValue[1]);
            }
        }
        fileSystemOptionMap.put(org.wso2.transport.remotefilesystem.Constants.URI, filePathUri);
        return fileSystemOptionMap;
    }

    /**
     * Traverse a directory and get all files,
     * and add the file into fileList
     *
     * @param node file or directory
     */
    public static void generateFileList(FileObject node, List<FileObject> fileObjectList,
                                        boolean excludeSubdirectories) {
        try {
            if (node.isFile()) {
                fileObjectList.add(node);
            }
            if (node.isFolder() && !excludeSubdirectories) {
                FileObject[] subNote = node.getChildren();
                if (subNote != null) {
                    for (FileObject file : subNote) {
                        generateFileList(file, fileObjectList, false);
                    }
                }
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking file type for " +
                    node.getName().getPath(), e);
        }
    }

    public static long getFileSize(String filePathUri) {
        filePathUri = getFilePath(filePathUri);
        File file = new File(filePathUri);
        return file.length();
    }

    private static String getFilePath(String uri) {
        if (uri.startsWith("file:")) {
            uri = uri.replaceFirst("file:", "");
        }
        uri = uri.replace("%20", " ");
        return FilenameUtils.separatorsToSystem(uri).replace("\\", "/");
    }

    public static long getLinesCount(String uri) throws IOException {
        if (uri.startsWith("file:")) {
            uri = uri.replaceFirst("file:", "");
        }
        CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.IGNORE);
        Path path = Paths.get(URI.create("file:///" + uri).normalize());
        try (Reader r = Channels.newReader(FileChannel.open(path), dec, -1);
             BufferedReader br = new BufferedReader(r)) {
            return br.lines()
                    .filter(line -> line.length() != 0).count();
        }
    }

    public static String capitalizeFirstLetter(String str) {
        return str.substring(0, 1).toUpperCase(Locale.ENGLISH) + str.substring(1);
    }

    public static String getFileName(String fileURI, Metrics metrics) {
        fileURI = getFilePath(fileURI);
        if (metrics.getFileNames().containsKey(fileURI)) {
            return metrics.getFileNames().get(fileURI);
        }
        String[] arr = fileURI.split("/");
        int n = arr.length;
        StringBuilder fileName = new StringBuilder();
        fileName.append(arr[n - 1]);
        if (!metrics.getFileNames().containsValue(fileName.toString())) {
            return fileName.toString();
        }
        for (int i = n - 2; i >= 0; i--) {
            fileName.insert(0, "/").insert(0, arr[i]);
            if (!metrics.getFileNames().containsValue(fileName.toString())) {
                metrics.getFileNames().put(fileURI, fileName.toString());
                break;
            }
        }
        return fileName.toString();
    }

    public static String getShortFilePath(String fileURI) {
        fileURI = getFilePath(fileURI);
        if (fileURI.length() <= 40) {
            return fileURI;
        }
        int n = fileURI.length();
        int i = n - 41; // to get last 40 characters
        char c = fileURI.charAt(i);
        while (c != '/' && i > 0) {
            i--;
            c = fileURI.charAt(i);
        }
        if (i == 0) {
            return fileURI;
        }
        return ".." + fileURI.substring(i);
    }
}
