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

import java.util.HashMap;
import java.util.List;
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
}
