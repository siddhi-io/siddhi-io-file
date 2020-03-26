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
import io.siddhi.extension.io.file.util.Metrics;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.wso2.transport.file.connector.server.exception.FileServerConnectorException;
import org.wso2.transport.file.connector.server.util.Constants;
import org.wso2.transport.file.connector.server.util.FileTransportUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
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
import java.util.Properties;

import static io.siddhi.extension.util.Constant.FTP_SCHEME_FILE_OPTION;
import static io.siddhi.extension.util.Constant.SFTP_SCHEME_FILE_OPTION;
import static io.siddhi.extension.util.Constant.VFS_SCHEME_KEY;

/**
 * Util Class.
 */
public class Utils {
    /**
     * Returns the FileObject in the given file path
     *
     * @param filePathUri file or directory path
     * @return FileObject retried by the given uri
     */
    public static FileObject getFileObject(String filePathUri) {
        Map<String, String> sourceOptions = Utils.parseSchemeFileOptions(filePathUri, new Properties());
        FileSystemOptions sourceFso;
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            sourceFso = FileTransportUtils.attachFileSystemOptions(sourceOptions, fsManager);
        } catch (FileServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when parsing scheme file options for path: " +
                    sourceOptions, e);
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting VFS manager", e);
        }
        if (sourceOptions != null && FTP_SCHEME_FILE_OPTION.equals(sourceOptions.get(VFS_SCHEME_KEY))) {
            FtpFileSystemConfigBuilder.getInstance().setPassiveMode(sourceFso, true);
        }
        try {
            return fsManager.resolveFile(filePathUri, sourceFso);
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when resolving path for: " +
                    filePathUri, e);
        }
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

    private static Map<String, String> parseSchemeFileOptions(String fileURI, Properties properties) {
        String scheme = UriParser.extractScheme(fileURI);
        if (scheme == null) {
            return null;
        } else {
            HashMap<String, String> schemeFileOptions = new HashMap();
            schemeFileOptions.put(VFS_SCHEME_KEY, scheme);
            addOptions(scheme, schemeFileOptions, properties);
            return schemeFileOptions;
        }
    }

    private static void addOptions(String scheme, Map<String, String> schemeFileOptions, Properties properties) {
        if (scheme.equals(SFTP_SCHEME_FILE_OPTION)) {
            Constants.SftpFileOption[] sftpFileOptions = Constants.SftpFileOption.values();
            for (Constants.SftpFileOption option : sftpFileOptions) {
                String strValue = (String) properties.get(SFTP_SCHEME_FILE_OPTION + option.toString());
                if (strValue != null && !strValue.isEmpty()) {
                    schemeFileOptions.put(option.toString(), strValue);
                }
            }
        }
    }

    public static double getFileSize(String filePathUri) {
        filePathUri = getFilePath(filePathUri);
        File file = new File(filePathUri);
        return file.length();
    }

    /*public static String getFileName(String uri) {
        String[] path = uri.split("/");
        String fileName = path[path.length - 1];
        return fileName.replace("%20", " ");
    }*/

    private static String getFilePath(String uri) {
        if (uri.startsWith("file:")) {
            uri = uri.replaceFirst("file:", "");
        }
        return uri.replace("%20", " ");
    }

    public static long getLinesCount(String uri) throws IOException {
        /*if (uri.startsWith("file:")) {
            uri = uri.replaceFirst("file:", "");
        }
        uri = uri.replace("%20", " ");
        return Files.lines(Paths.get(uri)).filter(line -> line.length() != 0).count();*/
        if (uri.startsWith("file:")) {
            uri = uri.replaceFirst("file:", "");
        }
        uri = uri.replace("%20", " ");
        CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.IGNORE);
        Path path = Paths.get(uri);
        try (Reader r = Channels.newReader(FileChannel.open(path), dec, -1);
            BufferedReader br = new BufferedReader(r)) {
            return br.lines()
                    .filter(line -> line.length() != 0).count();
        }
    }

    public static String capitalizeFirstLetter(String str) {
        return str.substring(0, 1).toUpperCase(Locale.ENGLISH) + str.substring(1);
    }

    public static void addSiddhiApp(String siddhiAppName) {
        boolean added = Metrics.getInstance().getSiddhiApps().add(siddhiAppName);
        if (added) {
            Metrics.getInstance().getTotalSiddhiApps().labels(siddhiAppName).inc();
        }
    }

    public static String getFileName(String fileURI) {
        fileURI = getFilePath(fileURI);
        if (Metrics.getInstance().getFileNames().containsKey(fileURI)) {
            return Metrics.getInstance().getFileNames().get(fileURI);
        }
        String[] arr = fileURI.split("/");
        int n = arr.length;
        StringBuilder fileName = new StringBuilder();
        fileName.append(arr[n - 1]);
        if (!Metrics.getInstance().getFileNames().containsValue(fileName.toString())) {
            return fileName.toString();
        }
        for (int i = n - 2; i >= 0; i--) {
            fileName.insert(0, "/").insert(0, arr[i]);
            if (!Metrics.getInstance().getFileNames().containsValue(fileName.toString())) {
                Metrics.getInstance().getFileNames().put(fileURI, fileName.toString());
                break;
            }
        }
        return fileName.toString();
    }

    public static String getShortFilePath(String fileURI) {
        fileURI = getFilePath(fileURI);
        if (fileURI.length() < 40) {
            return "../" + fileURI;
        }
        int n = fileURI.length();
        int i = n - 41; // to get last 40 characters
        char c = fileURI.charAt(i);
        while (c != '/' && i > 0) {
            i--;
            c = fileURI.charAt(i);
        }
        return ".." + fileURI.substring(i);
    }
}
