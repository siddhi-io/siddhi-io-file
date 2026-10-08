/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.extension.util.Utils;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.Selectors;

import java.util.concurrent.atomic.AtomicInteger;

public class SMBFileSourceSinkTestCase {
    private static final String HOST = System.getProperty("smb.test.host");
    private static final String PORT = System.getProperty("smb.test.port", "445");
    private static final String USER = System.getProperty("smb.test.user", "ubuntu");
    private static final String PASSWORD = System.getProperty("smb.test.password", "admin");
    private static final String SHARE = System.getProperty("smb.test.share", "sambashare");
    private static final int WAIT_TIME = 10000;
    private static final int TIMEOUT = 30000;
    private final AtomicInteger count = new AtomicInteger();

    @BeforeClass
    public void init() {
        if (HOST == null || HOST.isEmpty()) {
            throw new SkipException("Set -Dsmb.test.host to run the SMB tests against a Samba server");
        }
    }

    @DataProvider(name = "schemes")
    public Object[][] schemes() {
        return new Object[][]{{"smb"}, {"smb2"}};
    }

    @BeforeMethod
    public void resetCount() {
        count.set(0);
    }

    private static String sourceDir(String scheme) {
        return scheme + "://" + USER + ":" + PASSWORD + "@" + HOST + ":" + PORT + "/" + SHARE + "/" + scheme
                + "-source/";
    }

    private static void resetSourceDir(String scheme) throws FileSystemException {
        FileObject dir = Utils.getFileObject(sourceDir(scheme), null);
        dir.delete(Selectors.SELECT_ALL);
        dir.createFolder();
    }

    private static void writeEvents(String sinkUri, String append, Object[][] events)
            throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime runtime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('SmbSinkApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='" + append + "', file.uri='" + sinkUri + "') " +
                "define stream BarStream (symbol string, price float, volume long); " +
                "from FooStream select * insert into BarStream; ");
        InputHandler input = runtime.getInputHandler("FooStream");
        runtime.start();
        for (Object[] event : events) {
            input.send(event);
        }
        Thread.sleep(1000);
        siddhiManager.shutdown();
    }

    private void readEvents(String sourceOption, int expected) throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime runtime = siddhiManager.createSiddhiAppRuntime(
                "@App:name('SmbSourceApp')" +
                "@source(type='file', mode='line', " + sourceOption + ", action.after.process='keep', " +
                "tailing='false', @map(type='json')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); " +
                "from FooStream select * insert into BarStream; ");
        runtime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.addAndGet(events.length);
            }
        });
        runtime.start();
        SiddhiTestHelper.waitForEvents(WAIT_TIME, expected, count, TIMEOUT);
        siddhiManager.shutdown();
        AssertJUnit.assertEquals("Number of events", expected, count.get());
    }

    @Test(dataProvider = "schemes")
    public void testAppendingSinkThenFileSource(String scheme) throws Exception {
        resetSourceDir(scheme);
        String file = sourceDir(scheme) + "published.json";
        writeEvents(file, "true", new Object[][]{{"WSO2", 55.6f, 100L}, {"IBM", 57.678f, 200L}});
        readEvents("file.uri='" + file + "'", 2);
    }

    @Test(dataProvider = "schemes")
    public void testOverwritingSinkThenFileSource(String scheme) throws Exception {
        resetSourceDir(scheme);
        String file = sourceDir(scheme) + "published.json";
        writeEvents(file, "false", new Object[][]{{"WSO2", 55.6f, 100L}, {"IBM", 57.678f, 200L}});
        readEvents("file.uri='" + file + "'", 1);
    }

    @Test(dataProvider = "schemes")
    public void testDynamicSinkThenDirectorySource(String scheme) throws Exception {
        resetSourceDir(scheme);
        writeEvents(sourceDir(scheme) + "{{symbol}}.json", "true",
                new Object[][]{{"WSO2", 55.6f, 1L}, {"WSO2", 55.7f, 2L}, {"IBM", 57.6f, 3L}, {"IBM", 57.1f, 4L}});
        readEvents("dir.uri='" + sourceDir(scheme) + "'", 4);
    }
}
