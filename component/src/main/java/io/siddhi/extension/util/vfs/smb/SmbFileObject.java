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

import jcifs.CIFSContext;
import jcifs.context.SingletonContext;
import jcifs.smb.NtStatus;
import jcifs.smb.NtlmPasswordAuthenticator;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileNotFoundException;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.FileTypeHasNoContentException;
import org.wso2.org.apache.commons.vfs2.UserAuthenticationData;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileName;
import org.wso2.org.apache.commons.vfs2.provider.AbstractFileObject;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.org.apache.commons.vfs2.util.UserAuthenticatorUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A file or folder on an SMB share, accessed through jcifs-ng.
 */
public class SmbFileObject extends AbstractFileObject<SmbFileSystem> {

    private SmbFile file;

    protected SmbFileObject(AbstractFileName name, SmbFileSystem fileSystem) {
        super(name, fileSystem);
    }

    @Override
    protected void doAttach() throws Exception {
        if (file == null) {
            file = createSmbFile(getName());
        }
    }

    @Override
    protected void doDetach() {
        file = null;
    }

    private SmbFile createSmbFile(FileName fileName) throws IOException {
        SmbFileName smbFileName = (SmbFileName) fileName;
        String path = smbFileName.getUriWithoutAuth();
        UserAuthenticationData authData = null;
        try {
            authData = UserAuthenticatorUtils.authenticate(getFileSystem().getFileSystemOptions(),
                    SmbFileProvider.AUTHENTICATOR_TYPES);
            String userName = credential(authData, UserAuthenticationData.USERNAME, smbFileName.getUserName());
            CIFSContext context = SingletonContext.getInstance();
            if (userName != null) {
                context = context.withCredentials(new NtlmPasswordAuthenticator(
                        credential(authData, UserAuthenticationData.DOMAIN, smbFileName.getDomain()), userName,
                        credential(authData, UserAuthenticationData.PASSWORD, smbFileName.getPassword())));
            }
            SmbFile smbFile = new SmbFile(path, context);
            if (smbFile.isDirectory() && !smbFile.toString().endsWith("/")) {
                smbFile = new SmbFile(path + "/", context);
            }
            return smbFile;
        } finally {
            UserAuthenticatorUtils.cleanup(authData);
        }
    }

    private static String credential(UserAuthenticationData authData, UserAuthenticationData.Type type,
                                     String fromUri) {
        return UserAuthenticatorUtils.toString(
                UserAuthenticatorUtils.getData(authData, type, UserAuthenticatorUtils.toChar(fromUri)));
    }

    @Override
    protected FileType doGetType() throws Exception {
        if (!file.exists()) {
            return FileType.IMAGINARY;
        }
        if (file.isDirectory()) {
            return FileType.FOLDER;
        }
        if (file.isFile()) {
            return FileType.FILE;
        }
        throw new FileSystemException("vfs.provider.smb/get-type.error", getName());
    }

    @Override
    protected String[] doListChildren() throws Exception {
        if (!file.isDirectory()) {
            return null;
        }
        return UriParser.encode(file.list());
    }

    @Override
    protected boolean doIsHidden() throws Exception {
        return file.isHidden();
    }

    @Override
    protected void doDelete() throws Exception {
        file.delete();
    }

    @Override
    protected void doRename(FileObject newFile) throws Exception {
        file.renameTo(createSmbFile(newFile.getName()));
    }

    @Override
    protected void doCreateFolder() throws Exception {
        file.mkdir();
        file = createSmbFile(getName());
    }

    @Override
    protected long doGetContentSize() throws Exception {
        return file.length();
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception {
        return file.getLastModified();
    }

    @Override
    protected boolean doSetLastModifiedTime(long modtime) throws Exception {
        file.setLastModified(modtime);
        return true;
    }

    @Override
    protected InputStream doGetInputStream() throws Exception {
        try {
            return new SmbFileInputStream(file);
        } catch (SmbException e) {
            if (e.getNtStatus() == NtStatus.NT_STATUS_NO_SUCH_FILE) {
                throw new FileNotFoundException(getName());
            }
            if (file.isDirectory()) {
                throw new FileTypeHasNoContentException(getName());
            }
            throw e;
        }
    }

    @Override
    protected OutputStream doGetOutputStream(boolean append) throws Exception {
        return new SmbFileOutputStream(file, append);
    }
}
