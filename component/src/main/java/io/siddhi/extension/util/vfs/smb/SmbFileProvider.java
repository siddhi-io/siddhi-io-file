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

import org.wso2.org.apache.commons.vfs2.Capability;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileSystem;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.UserAuthenticationData;
import org.wso2.org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * VFS provider for the {@code smb} scheme, backed by jcifs-ng.
 */
public class SmbFileProvider extends AbstractOriginatingFileProvider {

    static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = {
            UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD, UserAuthenticationData.DOMAIN};

    static final Collection<Capability> CAPABILITIES = Collections.unmodifiableCollection(Arrays.asList(
            Capability.CREATE, Capability.DELETE, Capability.RENAME, Capability.GET_TYPE,
            Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE, Capability.SET_LAST_MODIFIED_FOLDER,
            Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI, Capability.WRITE_CONTENT,
            Capability.APPEND_CONTENT));

    public SmbFileProvider() {
        setFileNameParser(SmbFileNameParser.getInstance());
    }

    @Override
    protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
            throws FileSystemException {
        return new SmbFileSystem(name, fileSystemOptions);
    }

    @Override
    public Collection<Capability> getCapabilities() {
        return CAPABILITIES;
    }
}
