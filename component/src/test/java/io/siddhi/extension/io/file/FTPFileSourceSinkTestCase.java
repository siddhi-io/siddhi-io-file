/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class FTPFileSourceSinkTestCase {
    private static final Logger log = LogManager.getLogger(FTPFileSourceSinkTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private FileObject sourceLocalRoot, tempFTPSource, ftpDestination;
    String fileOptions;
    private int waitTime = 10000;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        fileOptions = "PASSIVE_MODE:true";
        sourceLocalRoot = Utils.getFileObject((rootPath + "/repo/function/"), null);
        tempFTPSource = Utils.getFileObject(
                "ftp://bob:password@localhost:21/source/", fileOptions);
    }

    @BeforeMethod
    public void doBeforeMethod() throws InterruptedException, FileSystemException {
        count.set(0);
        try {
            tempFTPSource.delete(Selectors.SELECT_ALL);
            tempFTPSource.createFolder();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void fileSinkSourceTest1() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 1");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='ftp://bob:password@localhost:21/source/published.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 200L});

        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.json");
        symbolNames.add("IBM.json");
        symbolNames.add("GOOGLE.json");
        symbolNames.add("REDHAT.json");

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

        streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='ftp://bob:password@localhost:21/source/published.json', " +
                "action.after.process='keep', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(200L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkSourceTest2() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 1");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='false', " +
                "file.uri='ftp://bob:password@localhost:21/source/published.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 200L});

        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.json");
        symbolNames.add("IBM.json");
        symbolNames.add("GOOGLE.json");
        symbolNames.add("REDHAT.json");

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

        streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='ftp://bob:password@localhost:21/source/published.json', " +
                "action.after.process='keep', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(200L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileDynamicSinkDirectorySourceTest() throws InterruptedException {
        log.info("test SiddhiIoFile Sink for dynamic paths and reading from a directory");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, fileName string); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='ftp://bob:password@localhost:21/source/{{fileName}}.json') " +
                "define stream BarStream (symbol string, price float, fileName string); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, "file1"});
        stockStream.send(new Object[]{"WSO4", 55.7f, "file1"});
        stockStream.send(new Object[]{"IBM", 57.678f, "file2"});
        stockStream.send(new Object[]{"IBM2", 57.123f, "file2"});

        Thread.sleep(100);

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

        streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='ftp://bob:password@localhost:21/source/', " +
                "file.system.options='PASSIVE_MODE:true', " +
                "action.after.process='keep', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, fileName string); " +
                "define stream BarStream (symbol string, price float, fileName string); ";

        query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.getAndIncrement();
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }
}
