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
 * Modifications copyright (c) 2026 WSO2 LLC. (http://www.wso2.com): ported from the
 * Apache Commons VFS sandbox (rel/commons-vfs-2.10.0) to jcifs-ng (org.codelibs:jcifs).
 */

package io.siddhi.extension.util.vfs.smb;

import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.provider.GenericFileName;

/**
 * An SMB file name: a generic host name plus a share and an optional domain.
 */
public class SmbFileName extends GenericFileName {

    private static final int DEFAULT_PORT = 139;

    private final String share;
    private final String domain;
    private String uriWithoutAuth;

    protected SmbFileName(String scheme, String hostName, int port, String userName, String password,
                          String domain, String share, String path, FileType type) {
        super(scheme, hostName, port, DEFAULT_PORT, userName, password, path, type);
        this.share = share;
        this.domain = domain;
    }

    public String getShare() {
        return share;
    }

    public String getDomain() {
        return domain;
    }

    @Override
    protected void appendRootUri(StringBuilder buffer, boolean addPassword) {
        super.appendRootUri(buffer, addPassword);
        buffer.append('/').append(share);
    }

    @Override
    protected void appendCredentials(StringBuilder buffer, boolean addPassword) {
        if (domain != null && !domain.isEmpty() && getUserName() != null && !getUserName().isEmpty()) {
            buffer.append(domain).append('\\');
        }
        super.appendCredentials(buffer, addPassword);
    }

    @Override
    public FileName createName(String path, FileType type) {
        return new SmbFileName(getScheme(), getHostName(), getPort(), getUserName(), getPassword(), domain, share,
                path, type);
    }

    public String getUriWithoutAuth() throws FileSystemException {
        if (uriWithoutAuth == null) {
            StringBuilder uri = new StringBuilder(120).append(getScheme()).append("://").append(getHostName());
            if (getPort() != DEFAULT_PORT) {
                uri.append(':').append(getPort());
            }
            uriWithoutAuth = uri.append('/').append(share).append(getPathDecoded()).toString();
        }
        return uriWithoutAuth;
    }
}
