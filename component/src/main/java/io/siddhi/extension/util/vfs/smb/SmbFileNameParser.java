/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (c) 2026, WSO2 LLC. (http://www.wso2.org): ported from the
 * Apache Commons VFS sandbox (rel/commons-vfs-2.10.0) to jcifs-ng (org.codelibs:jcifs).
 */

package io.siddhi.extension.util.vfs.smb;

import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.FileNameParser;
import org.wso2.org.apache.commons.vfs2.provider.URLFileNameParser;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.org.apache.commons.vfs2.provider.VfsComponentContext;

/**
 * Parses {@code smb://[domain\]user:password@host[:port]/share/path} URIs.
 */
public class SmbFileNameParser extends URLFileNameParser {

    private static final SmbFileNameParser INSTANCE = new SmbFileNameParser();
    private static final int SMB_PORT = 139;

    public SmbFileNameParser() {
        super(SMB_PORT);
    }

    public static FileNameParser getInstance() {
        return INSTANCE;
    }

    @Override
    public FileName parseUri(VfsComponentContext context, FileName base, String fileName)
            throws FileSystemException {
        StringBuilder name = new StringBuilder();
        Authority auth = extractToPath(fileName, name);
        String username = auth.getUserName();
        String domain = extractDomain(username);
        if (domain != null) {
            username = username.substring(domain.length() + 1);
        }
        UriParser.canonicalizePath(name, 0, name.length(), this);
        UriParser.fixSeparators(name);
        String share = UriParser.extractFirstElement(name);
        if (share == null || share.isEmpty()) {
            throw new FileSystemException("vfs.provider.smb/missing-share-name.error", fileName);
        }
        FileType fileType = UriParser.normalisePath(name);
        return new SmbFileName(auth.getScheme(), auth.getHostName(), auth.getPort(), username, auth.getPassword(),
                domain, share, name.toString(), fileType);
    }

    private static String extractDomain(String username) {
        if (username == null) {
            return null;
        }
        int index = username.indexOf('\\');
        return index < 0 ? null : username.substring(0, index);
    }
}
