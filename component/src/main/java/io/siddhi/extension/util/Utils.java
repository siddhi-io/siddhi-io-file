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

import java.io.File;
import java.util.List;

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
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            return fsManager.resolveFile(filePathUri, opts);
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking type of file in path: " +
                    filePathUri, e);
        }
    }

    /**
     * Traverse a directory and get all files,
     * and add the file into fileList
     *
     * @param node file or directory
     */
    public static void generateFileList(File node, List<String> fileList, boolean excludeSubdirectories) {
        //add file only
        if (node.isFile()) {
            fileList.add(node.getAbsoluteFile().toString());
        }
        if (node.isDirectory() && !excludeSubdirectories) {
            String[] subNote = node.list();
            if (subNote != null) {
                for (String filename : subNote) {
                    generateFileList(new File(node, filename), fileList, false);
                }
            }
        }
    }
}
