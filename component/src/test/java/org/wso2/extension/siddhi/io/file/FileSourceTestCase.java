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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 * */
public class FileSourceTestCase {
    // TODO: 20/7/17 Improve Thread.sleep() to use SiddhiTestHelper.waitForEvents().
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;
    
    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceTestCase.class.getClassLoader();
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
    * Test cases for 'mode = text.full'.
    * */
    @Test
    public void siddhiIoFileTest1() throws InterruptedException {
        log.info("test SiddhiIoFile 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='" + dirUri + "/text_full', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals("apache", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("cloudbees", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("google", event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ibm", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("intel", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("microsoft", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("redhat", event.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("test SiddhiIoFile 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='" + dirUri + "/text_full', " +
                "action.after.process='delete', " +
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
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals("apache", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals("cloudbees", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("google", event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ibm", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("intel", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("microsoft", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("redhat", event.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        File file = new File(dirUri + "/text_full");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest3() throws InterruptedException {
        log.info("test SiddhiIoFile 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='" + dirUri + "/text_full_single', " +
                "action.after.process='delete', " +
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
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals("apache", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest4() throws InterruptedException {
        log.info("test SiddhiIoFile 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='" + dirUri + "/text_full_single', " +
                "action.after.process='delete', " +
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
                int n = count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals("apache", event.getData(0));
                            break;
                        case 2 :
                            AssertJUnit.assertEquals("google", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                siddhiAppRuntime.start();
            }
        });
        t1.start();

        Thread.sleep(1000);

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File source = new File(dirUri + "/text_full/google.json");
                File dest = new File(dirUri + "/text_full_single/google.json");
                while (true) {
                    if (count.intValue() == 1) {
                        try {
                            FileUtils.copyFile(source, dest);
                        } catch (IOException e) {
                            AssertJUnit.fail("Failed to add a new file to directory '" +
                                    dirUri + "/text_full_single'.");
                        }
                    }
                }
            }
        });
        t2.start();

        Thread.sleep(1000);

        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }


    /**
     * Test cases for 'mode = 'regex'.
     * */

    @Test
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex', " +
                "begin.regex='(\\{)', " +
                "end.regex='(}})', " +
                "tailing='false', " +
                "action.after.process='delete', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(1000);

        File file = new File(dirUri + "/regex");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest6() throws InterruptedException {
        log.info("test SiddhiIoFile 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex', " +
                "tailing='false', " +
                "action.after.process='delete', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(1000);

        File file = new File(dirUri + "/regex");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest7() throws InterruptedException {
        log.info("test SiddhiIoFile 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "end.regex='(</events>)', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(2000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest8() throws InterruptedException {
        log.info("test SiddhiIoFile 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/json', " +
                "begin.regex='(\\{)', " +
                "end.regex='(}})', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(1000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest9() throws InterruptedException {
        log.info("test SiddhiIoFile 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(2000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest10() throws InterruptedException {
        log.info("test SiddhiIoFile 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml_single/', " +
                "begin.regex='(<events>)', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = events.length;
                count.incrementAndGet();
                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(10000L, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest11() throws InterruptedException {
        log.info("test SiddhiIoFile 11");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "end.regex='(</events>)', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='" + moveAfterProcessDir + "', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(1000);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest12() throws InterruptedException {
        log.info("test SiddhiIoFile 12");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "end.regex='(</events>)', " +
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

        Thread.sleep(5000);

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dirUri + "/regex/xml/xml_logs (3rd copy).txt");
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append("<events>\n")
                            .append("<event>\n")
                            .append("<symbol>").append("GOOGLE").append("</symbol>\n")
                            .append("<price>").append("100").append("</price>\n")
                            .append("<volume>").append("1000").append("</volume>\n")
                            .append("</event>\n")
                            .append("</events>\n");
                    sb.append("<events>\n")
                            .append("<event>\n")
                            .append("<symbol>").append("YAHOO").append("</symbol>\n")
                            .append("<price>").append("200").append("</price>\n")
                            .append("<volume>").append("2000").append("</volume>\n")
                            .append("</event>\n")
                            .append("</events>\n");
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
        Thread.sleep(2000);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 7, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest13() throws InterruptedException {
        log.info("test SiddhiIoFile 13");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "tailing='false', " +
                "action.after.process='delete', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(2000);

        File file = new File(dirUri + "/regex/xml");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test (expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest14() throws InterruptedException {
        log.info("test SiddhiIoFile 14");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "begin.regex='(<events>)', " +
                "tailing='false', " +
                "action.after.process='delete', " +
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

        Thread.sleep(2000);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest15() throws InterruptedException {
        log.info("test SiddhiIoFile 15");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "end.regex='(</events>)', " +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(2000);

        File file = new File(dirUri + "/regex/xml");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest16() throws InterruptedException {
        log.info("test SiddhiIoFile 16");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/xml', " +
                "begin.regex='(<events>)', " +
                "end.regex='(</events>)', " +
                "action.after.process='move' ," +
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
                int n = count.intValue() % 5;
                count.incrementAndGet();
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

        Thread.sleep(2000);

        File file = new File(dirUri + "/regex/xml");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 40, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest17() throws InterruptedException {
        log.info("test SiddhiIoFile 17");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='regex'," +
                "dir.uri='" + dirUri + "/regex/invalid', " +
                "begin.regex='(<events>)', " +
                "end.regex='(</events>)', " +
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

        Thread.sleep(2000);

        File file = new File(dirUri + "/regex/invalid");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest20() throws InterruptedException {
        log.info("test SiddhiIoFile 20");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', action.after.process='none'," +
                "file.uri='" + dirUri + "/line/logs.txt'," +
                "file.polling.interval='1000', " +
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
                int n = count.incrementAndGet();

                for (Event event : events) {
                    switch (n) {
                        case 1:
                            AssertJUnit.assertEquals(0f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(1f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(2f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(3f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(4f, event.getData(1));
                            break;
                        case 10:
                            AssertJUnit.assertEquals(9f, event.getData(1));
                            break;
                        case 12:
                            AssertJUnit.assertEquals(11f, event.getData(1));
                            break;
                        case 15:
                            AssertJUnit.assertEquals(14f, event.getData(1));
                            break;
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(3000);

        //assert event count
         AssertJUnit.assertEquals("Number of events", 15, count.get());
        siddhiAppRuntime.shutdown();
    }

    //from here, tests will be implemented later

    /*@Test
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='true', " +
                "file.uri='" + dir + "/tailing/logs.txt'," +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select *  " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        final SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);


        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                int n = count.incrementAndGet();
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (n) {
                        case 1:
                        case 2:
                        case 3:
                        case 4:
                        case 5:
                            AssertJUnit.assertEquals("WSO2", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("GOOGLE", event.getData(0));
                            break;
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

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dir + "/tailing/logs.txt");
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write("{\"event\":{\"symbol\":\"IBM\",\"price\":2000,\"volume\":30000}}");
                    bufferedWriter.newLine();
                    bufferedWriter.write("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":3000,\"volume\":40000}}");
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();

        Thread.sleep(3000);
    }*/
}
