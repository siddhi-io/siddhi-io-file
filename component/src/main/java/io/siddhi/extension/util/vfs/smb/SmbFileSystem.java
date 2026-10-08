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
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileName;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;

/**
 * An SMB file system.
 */
public class SmbFileSystem extends AbstractFileSystem {

    protected SmbFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
        super(rootName, null, fileSystemOptions);
    }

    @Override
    protected void addCapabilities(Collection<Capability> caps) {
        caps.addAll(SmbFileProvider.CAPABILITIES);
    }

    @Override
    protected FileObject createFile(AbstractFileName name) {
        return new SmbFileObject(name, this);
    }
}
