/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 * mode = line.
 */
public class FileSourceLineModeTestCase {
    private static final Logger log = Logger.getLogger(FileSourceLineModeTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 10000;
    private int timeout = 30000;
    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        sourceRoot = new File(rootPath + "/repo");
        dirUri = rootPath + "/new";
        newRoot = new File(dirUri);
        moveAfterProcessDir = rootPath + "/moved_files";
    }

    @BeforeMethod
    public void doBeforeMethod() {
        count.set(0);
        try {
            FileUtils.copyDirectory(sourceRoot, newRoot);
            movedFiles = new File(moveAfterProcessDir);
        } catch (IOException e) {
            throw new TestException("Failed to copy files from " +
                    sourceRoot.getAbsolutePath() +
                    " to " +
                    newRoot.getAbsolutePath() +
                    " which are required for tests. Hence aborting tests.", e);
        }
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.deleteDirectory(newRoot);
            FileUtils.deleteDirectory(movedFiles);
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
    }

    /**
     * Test cases for 'mode = line'.
     */

    @Test
    public void siddhiIoFileTest1() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json', " +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 40, count, timeout);
        File file = new File(dirUri + "/line/json");
        AssertJUnit.assertEquals(0, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest1")
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 40, count, timeout);
        File file = new File(dirUri + "/line/json");
        AssertJUnit.assertEquals(0, file.list().length);
        File movedDir = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, movedDir.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest2")
    public void siddhiIoFileTest3() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/xml', " +
                "tailing='true', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(1000L, event.getData(2));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(2000L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                siddhiAppRuntime.start();
            }
        });
        t1.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dirUri + "/line/xml/xml_line.txt");
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("GOOGLE").append("</symbol>")
                            .append("<price>").append("100").append("</price>")
                            .append("<volume>").append("1000").append("</volume>")
                            .append("</event>")
                            .append("</events>\n");
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("YAHOO").append("</symbol>")
                            .append("<price>").append("200").append("</price>")
                            .append("<volume>").append("2000").append("</volume>")
                            .append("</event>")
                            .append("</events>");
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write(sb.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest3")
    public void siddhiIoFileTest4() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/xml', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(1000L, event.getData(2));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(2000L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                siddhiAppRuntime.start();
            }
        });
        t1.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dirUri + "/line/xml/xml_line.txt");
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("GOOGLE").append("</symbol>")
                            .append("<price>").append("100").append("</price>")
                            .append("<volume>").append("1000").append("</volume>")
                            .append("</event>")
                            .append("</events>\n");
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("YAHOO").append("</symbol>")
                            .append("<price>").append("200").append("</price>")
                            .append("<volume>").append("2000").append("</volume>")
                            .append("</event>")
                            .append("</events>");
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write(sb.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest4")
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json', " +
                "action.after.process='delete'," +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 40, count, timeout);
        File file = new File(dirUri + "/line/json");
        AssertJUnit.assertEquals(0, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "siddhiIoFileTest5")
    public void siddhiIoFileTest6() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json', " +
                "action.after.process='move', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "siddhiIoFileTest6")
    public void siddhiIoFileTest7() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json_invalid_path', " +
                "action.after.process='move', " +
                "tailing='false', " +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest7")
    public void siddhiIoFileTest8() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/invalid', " +
                "action.after.process='delete'," +
                "tailing='false', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        File file = new File(dirUri + "/line/invalid");
        AssertJUnit.assertEquals(0, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "siddhiIoFileTest8")
    public void siddhiIoFileTest9() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/invalid', " +
                "tailing='false'," +
                "action.after.process='delete', " +
                "move.after.process='file://abc/def/', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest9")
    public void siddhiIoFileTest10() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:/" + dirUri + "/line/xml/xml_line.txt', " +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        File file = new File(dirUri + "/line/xml/");
        AssertJUnit.assertEquals(0, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest10")
    public void siddhiIoFileTest11() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 11");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/xml', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='false', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement() % 5;
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        File file = new File(dirUri + "/line/xml");
        AssertJUnit.assertEquals(0, file.list().length);
        File movedDir = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(1, movedDir.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest11")
    public void siddhiIoFileTest12() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 12");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/text', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='false', " +
                "@map(type='text',fail.on.missing.attribute='false', " +
                "regex.A='(\\w+),([-.0-9]+),([-.0-9]+)', event.grouping.enabled='false', " +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]')))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
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
                        case 2:
                            AssertJUnit.assertEquals(300L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(400L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(500L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 40, count, timeout);
        File file = new File(dirUri + "/line/text");
        AssertJUnit.assertEquals(0, file.list().length);
        File movedDir = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, movedDir.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    //@Test (dependsOnMethods = "siddhiIoFileTest12")
    public void siddhiIoFileTest13() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 13");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:/" + dirUri + "/line/xml/xml_line.txt', " +
                "tailing='true', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(1000L, event.getData(2));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(2000L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dirUri + "/line/xml/xml_line.txt");
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("GOOGLE").append("</symbol>")
                            .append("<price>").append("100").append("</price>")
                            .append("<volume>").append("1000").append("</volume>")
                            .append("</event>")
                            .append("</events>\n");
                    sb.append("<events>")
                            .append("<event>")
                            .append("<symbol>").append("YAHOO").append("</symbol>")
                            .append("<price>").append("200").append("</price>")
                            .append("<volume>").append("2000").append("</volume>")
                            .append("</event>")
                            .append("</events>");
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write(sb.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "siddhiIoFileTest12")
    public void siddhiIoFileTest14() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 14");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:/" + dirUri + "/line/xml/xml_line.txt', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/xml_line.txt', " +
                "action.after.failure='move', " +
                "move.after.failure='file:/" + moveAfterProcessDir + "/xml_line.txt', " +
                "tailing='false', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        File file = new File(dirUri + "/line/xml/");
//        AssertJUnit.assertEquals(7, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class, dependsOnMethods = "siddhiIoFileTest14")
    public void siddhiIoFileTest15() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 15");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/text', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='true', " +
                "@map(type='text',fail.on.missing.attribute='false', " +
                "regex.A='(\\w+),([-.0-9]+),([-.0-9]+)', event.grouping.enabled='false', " +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]')))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    //@Test (dependsOnMethods = "siddhiIoFileTest15")
    public void siddhiIoFileTest16() throws InterruptedException, CannotRestoreSiddhiAppStateException {
        log.info("test SiddhiIoFile [mode=line] Test 16");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:/" + dirUri + "/line/xml/xml_line.txt', " +
                "tailing='true', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10001L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10002L, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;

                        default:
                            AssertJUnit.fail();

                    }
                }
            }
        });

        siddhiAppRuntime2.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 6:
                            AssertJUnit.assertEquals(1000L, event.getData(2));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(2000L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail();

                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 5, count, timeout);
        byte[] snapshot = siddhiAppRuntime.snapshot();
        siddhiAppRuntime.shutdown();

        File file = new File(dirUri + "/line/xml/xml_line.txt");
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("<events>")
                    .append("<event>")
                    .append("<symbol>").append("GOOGLE").append("</symbol>")
                    .append("<price>").append("100").append("</price>")
                    .append("<volume>").append("1000").append("</volume>")
                    .append("</event>")
                    .append("</events>\n");
            sb.append("<events>")
                    .append("<event>")
                    .append("<symbol>").append("YAHOO").append("</symbol>")
                    .append("<price>").append("200").append("</price>")
                    .append("<volume>").append("2000").append("</volume>")
                    .append("</event>")
                    .append("</events>");
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            bufferedWriter.write(sb.toString());
            bufferedWriter.newLine();
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        siddhiAppRuntime2.restore(snapshot);
        siddhiAppRuntime2.start();
        SiddhiTestHelper.waitForEvents(waitTime, 7, count, timeout);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileReadLineInBinaryFileTest() {
        log.info("test SiddhiIoFile [mode = line] when mapping is binary");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line'," +
                "dir.uri='file:/" + dirUri + "/binary', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "@map(type='binary'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
    }

    @Test
    public void siddhiIoFileTestForEOFAndFileName() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test for EOF and File Name");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:/" + dirUri + "/line/json/logs.txt', " +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map(type='json', enclosing.element=\"$.event\", " +
                "@attributes(symbol = \"symbol\", price = \"price\", volume = \"volume\", " +
                "eof = 'trp:eof', fp = 'trp:file.path')))\n" +
                "define stream FooStream (symbol string, price float, volume long, eof String, fp String); " +
                "define stream BarStream (symbol string, price float, volume long, eof String, fp String); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    if (n < 5) {
                        AssertJUnit.assertEquals("false", event.getData(3));
                        AssertJUnit.assertTrue(((String) event.getData(4)).
                                contains("test-classes/files/new/line/json/logs.txt"));
                    } else if (n == 5) {
                        AssertJUnit.assertEquals("true", event.getData(3));
                    } else {
                        AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(10000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTestForSkipHeader() throws InterruptedException {
        log.info("test SiddhiIoFile header.present parameter Test");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:" + newRoot + "/line/header/test.txt', " +
                "header.present='true'," +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map( type='csv', delimiter='|', " +
                "@attributes(code = '0', serialNo = '1', amount = '2', fileName = 'trp:file.path', " +
                "eof = 'trp:eof')))\n" +
                "define stream FileReaderStream (code string, serialNo string, amount double, " +
                "fileName string, eof string); " +
                "define stream FileResultStream (code string, serialNo string, amount double, " +
                "fileName string, eof string); ";
        String query = "" +
                "from FileReaderStream " +
                "select * " +
                "insert into FileResultStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("FileResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(10000);
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTestWithoutSkipHeader() throws InterruptedException {
        log.info("test SiddhiIoFile without header.present parameter Test");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:" + newRoot + "/line/header/test.txt', " +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map( type='csv', delimiter='|', " +
                "@attributes(code = '0', serialNo = '1', amount = '2', fileName = 'trp:file.path', " +
                "eof = 'trp:eof')))\n" +
                "define stream FileReaderStream (code string, serialNo string, amount double, " +
                "fileName string, eof string); " +
                "define stream FileResultStream (code string, serialNo string, amount double, " +
                "fileName string, eof string); ";
        String query = "" +
                "from FileReaderStream " +
                "select * " +
                "insert into FileResultStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("FileResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(10000);
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test// (dependsOnMethods = "siddhiIoFileTestForKeepAfterProcess")
    public void siddhiIoFileTestForKeepAfterProcess() throws InterruptedException {
        log.info("test SiddhiIoFile for keep after process");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + newRoot + "/line/invalid', " +
//                "action.after.process='delete'," +
                "tailing='false', " +
                "@map(type='xml'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(10003L, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(10004L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 3, count, timeout);
        File file = new File(dirUri + "/line/invalid");
        AssertJUnit.assertEquals(1, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIOFileTestForSkipReadOnlyHeader() throws InterruptedException {
        log.info("test SiddhiIOFile read.only.header=false parameter Test");
        String streams = "" +
                "@App:name('TestSiddhiApp')\n" +
                "@source(type='file', mode='line', file.uri='file:" + newRoot + "/line/header/test.txt', " +
                "read.only.header='false', tailing='false', " +
                "@map(type='csv', delimiter='|'))\n" +
                "define stream FileReaderStream (code string, serialNo string, amount string);\n" +
                "@sink(type='log')\n" +
                "define stream FileResultStream (code string, serialNo string, amount string);\n";

        String query = "" +
                "from FileReaderStream\n" +
                "select *\n" +
                "insert into FileResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("FileResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 7, count.get(), 6000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIOFileTestForReadOnlyHeader() throws InterruptedException {
        log.info("test SiddhiIOFile read.only.header parameter Test");
        String streams = "" +
                "@App:name('TestSiddhiApp')\n" +
                "@source(type='file', mode='line', file.uri='file:" + newRoot + "/line/header/test.txt', " +
                "read.only.header='true', action.after.process='keep', tailing='false', \n" +
                "@map(type='csv', delimiter='|'))\n" +
                "define stream FileReaderStream (code string, serialNo string, amount string);\n" +
                "@sink(type='log')\n" +
                "define stream FileResultStream (code string, serialNo string, amount string);\n";

        String query = "" +
                "from FileReaderStream\n" +
                "select *\n" +
                "insert into FileResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("FileResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIOFileTestCronSupportForFile() throws InterruptedException {
        log.info("Siddhi IO File test for Cron support via file.uri");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='file:" + newRoot + "/line/header/test.txt', cron.expression='*/5 * * * * ?', " +
                "action.after.process='move', tailing='false', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/line/header/test.txt', " +
                "@map( type='csv', delimiter='|'))" +
                "define stream FileReaderStream (code string, serialNo string, amount double); " +
                "define stream FileResultStream (code string, serialNo string, amount double); ";
        String query = "" +
                "from FileReaderStream " +
                "select * " +
                "insert into FileResultStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("FileResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 7, count.get(), 6000);
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void regexFileNameTest() throws InterruptedException {
        log.info("Siddhi IO File test for file name patterns with regex");
        AtomicInteger eventCount = new AtomicInteger();
        String streams = "@App:name('FileTest')\n" +
                "\n" +
                "@source(type='file',\n" +
                "\tmode='line',\n" +
                "\taction.after.process='NONE',\n" +
                "\ttailing='true',\n" +
                "\tdir.uri='file:/" + newRoot + "/name_pattern',\n" +
                "\tfile.name.pattern='^test-[\\d]+', \n" +
                "\t@map(type='json'))\n" +
                "define stream FooStream(symbol string, price float, volume long);\n" +
                "\n" +
                "@sink(type='log')\n" +
                "define stream BarStream(symbol string, price float, volume long);\n";

        String query = "" +
                "from FooStream\n" +
                "select * \n" +
                "insert into BarStream;\n";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                eventCount.incrementAndGet();
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 2, eventCount.get(), 6000);
        AssertJUnit.assertEquals("Number of events", 2, eventCount.get());
        siddhiAppRuntime.shutdown();
    }
}
