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

package org.wso2.extension.siddhi.io.file;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 * mode = line.
 * */
public class FileSourceLineModeTestCase {
    private static final Logger log = Logger.getLogger(FileSourceLineModeTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 2000;
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
                "dir.uri='" + dirUri + "/line/json', " +
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

    @Test
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/json', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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

    @Test
    public void siddhiIoFileTest3() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/xml', " +
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
                File file = new File(dirUri + "/line/xml/xml_line (3rd copy).txt");
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
    @Test
    public void siddhiIoFileTest4() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/xml', " +
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
                File file = new File(dirUri + "/line/xml/xml_line (3rd copy).txt");
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

    @Test
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/json', " +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest6() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/json', " +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest7() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/json_invalid_path', " +
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

    @Test
    public void siddhiIoFileTest8() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/invalid', " +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest9() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/invalid', " +
                "tailing='false'," +
                "action.after.process='delete', " +
                "move.after.process='/abc/def/', " +
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

    @Test
    public void siddhiIoFileTest10() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "file.uri='" + dirUri + "/line/xml/xml_line.txt', " +
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
        AssertJUnit.assertEquals(7, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 5, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest11() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 11");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/xml', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
        SiddhiTestHelper.waitForEvents(waitTime, 40, count, timeout);
        File file = new File(dirUri + "/line/xml");
        AssertJUnit.assertEquals(0, file.list().length);
        File movedDir = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, movedDir.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest12() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test 12");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='" + dirUri + "/line/text', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
}
