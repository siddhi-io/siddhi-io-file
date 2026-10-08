/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
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

package io.siddhi.extension.io.file;

import io.siddhi.core.SiddhiManager;
import io.siddhi.extension.io.file.util.Util;
import io.siddhi.extension.util.vfs.smb.SmbFileName;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.VFS;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileName;

import java.net.MalformedURLException;

public class SmbProviderTestCase {

    @Test
    public void testSmbProvidersAreRegistered() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        Assert.assertTrue(manager.hasProvider("smb"), "smb provider is not registered");
        Assert.assertTrue(manager.hasProvider("smb2"), "smb2 provider is not registered");
    }

    @Test
    public void testSmbUriParsing() throws FileSystemException {
        FileName name = VFS.getManager().resolveURI(
                "smb://ubuntu:admin@localhost/sambashare/source/published.json");
        Assert.assertTrue(name instanceof SmbFileName, name.getClass().getName());
        SmbFileName smbName = (SmbFileName) name;
        Assert.assertEquals(smbName.getShare(), "sambashare");
        Assert.assertEquals(smbName.getPath(), "/source/published.json");
        Assert.assertEquals(smbName.getUriWithoutAuth(), "smb://localhost/sambashare/source/published.json");
    }

    @Test(expectedExceptions = FileSystemException.class)
    public void testSmbUriWithoutShareIsRejected() throws FileSystemException {
        VFS.getManager().resolveURI("smb://ubuntu:admin@localhost/");
    }

    @Test
    public void testSmb2UriParsing() throws FileSystemException {
        FileName name = VFS.getManager().resolveURI(
                "smb2://ubuntu:admin@localhost:1445/sambashare/source/published.json");
        Assert.assertTrue(name instanceof Smb2FileName, name.getClass().getName());
        Smb2FileName smb2Name = (Smb2FileName) name;
        Assert.assertEquals(smb2Name.getShareName(), "sambashare");
        Assert.assertEquals(smb2Name.getPath(), "/source/published.json");
        Assert.assertEquals(smb2Name.getPort(), 1445);
        Assert.assertFalse(smb2Name.getFriendlyURI().contains("admin"), smb2Name.getFriendlyURI());
    }

    @Test
    public void testSmbSchemesMapToAUrlParsableScheme() {
        Assert.assertEquals(Util.replaceSmbScheme("smb://u:p@h/share/a.txt"), "ftp://u:p@h/share/a.txt");
        Assert.assertEquals(Util.replaceSmbScheme("smb2://u:p@h:1445/share/a.txt"),
                "ftp://u:p@h:1445/share/a.txt");
        Assert.assertEquals(Util.replaceSmbScheme("smb2:"), "ftp:");
        Assert.assertEquals(Util.replaceSmbScheme("sftp://u:p@h/a.txt"), "sftp://u:p@h/a.txt");
        Assert.assertNull(Util.replaceSmbScheme(null));
    }

    @Test
    public void testFileNameOfSmb2Uri() {
        Assert.assertEquals(Util.getFileName("smb2://u:p@h/share/in/sales.csv", "smb2:"), "sales.csv");
    }

    @Test
    public void testSmb2FileUriPassesUrlValidation() {
        String app = "@App:name('Smb2UriValidation') " +
                "@source(type='file', mode='line', tailing='false', action.after.process='keep', " +
                "file.uri='smb2://ubuntu:admin@127.0.0.1:1/sambashare/in.csv', @map(type='csv')) " +
                "define stream InStream (name string, amount double); ";
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            siddhiManager.createSiddhiAppRuntime(app);
            Assert.fail("App creation should fail: nothing listens on 127.0.0.1:1");
        } catch (RuntimeException e) {
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                Assert.assertFalse(cause instanceof MalformedURLException,
                        "smb2 URI was rejected by URL validation: " + cause);
            }
        } finally {
            siddhiManager.shutdown();
        }
    }
}
