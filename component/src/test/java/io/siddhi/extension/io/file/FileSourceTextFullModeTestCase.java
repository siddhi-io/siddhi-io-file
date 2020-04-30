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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 */
public class FileSourceTextFullModeTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTextFullModeTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 2000;
    private int timeout = 30000;

    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;
    private List<String> companies = new ArrayList<>();
    private List<String> companies2 = new ArrayList<>();

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceTextFullModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        sourceRoot = new File(rootPath + "/repo");
        dirUri = rootPath + "/new";
        newRoot = new File(dirUri);
        moveAfterProcessDir = rootPath + "/moved_files";
        companies.add("redhat");
        companies.add("apache");
        companies.add("cloudbees");
        companies.add("ibm");
        companies.add("intel");
        companies.add("microsoft");
        companies.add("google");
        companies.add("wso2");
        companies2.add("apache");
        companies2.add("google");
    }

    @BeforeMethod
    public void doBeforeMethod() {
        count.set(0);
        try {
            FileUtils.copyDirectory(sourceRoot, newRoot);
            movedFiles = new File(moveAfterProcessDir);
            FileUtils.forceMkdir(movedFiles);
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
            FileUtils.forceDelete(newRoot);
            FileUtils.forceDelete(movedFiles);
        } catch (IOException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    /**
     * Test cases for 'mode = text.full'.
     */
    @Test
    public void siddhiIoFileTest1() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
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
                    AssertJUnit.assertEquals(true, companies.contains(event.getData(0).toString()));
                }
            }
        });

        siddhiAppRuntime.start();

        SiddhiTestHelper.waitForEvents(waitTime, 8, count, timeout);

        File file = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(8, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full', " +
                "action.after.process='delete', timeout='100000'," +
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
                    AssertJUnit.assertEquals(true, companies.contains(event.getData(0).toString()));
                }
            }
        });

        siddhiAppRuntime.start();

        SiddhiTestHelper.waitForEvents(waitTime, 8, count, timeout);

        File file = new File(dirUri + "/text_full");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest3() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full_single', " +
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

        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest4() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full_single', " +
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
                    AssertJUnit.assertEquals(true, companies.contains(event.getData(0).toString()));
                }
            }
        });

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                siddhiAppRuntime.start();
            }
        });
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(t1);

        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File source = new File(dirUri + "/text_full/google.json");
                File dest = new File(dirUri + "/text_full_single/google.json");
                while (true) {
                    if (count.intValue() == 1) {
                        try {
                            FileUtils.copyFile(source, dest);
                            break;
                        } catch (IOException e) {
                            AssertJUnit.fail("Failed to add a new file to directory '" +
                                    dirUri + "/text_full_single'.");
                        }
                    }
                }
            }
        });

        executorService.execute(t2);

        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);

        executorService.shutdown();

        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
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

        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest6() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/apache.json', " +
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

        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);

        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);

        File movedFile = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(1, movedFile.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest7() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "action.after.process='delete'," +
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
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        File file = new File(dirUri + "/text_full_single");
        AssertJUnit.assertEquals(0, file.list().length);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTestForEOFAndFileNameForTextFull() throws InterruptedException {
        log.info("test SiddhiIoFile [mode=line] Test for EOF and File Path");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full/google.json', " +
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
                    if (n == 1) {
                        AssertJUnit.assertEquals("true", event.getData(3));
                        AssertJUnit.assertTrue(((String) event.getData(4)).
                                contains("test-classes/files/new/text_full/google.json"));
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest8() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "tailing='true', " +
                "@map(type='json'))" +
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest9() throws InterruptedException {
        log.info("test SiddhiIoFile [mode = text.full] 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='text.full'," +
                "file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "tailing='true', timeout='100000x'," +
                "@map(type='json'))" +
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

    @Test
    public void siddhiIoFileTestCronSupportForDirectory() throws InterruptedException, IOException {
        log.info("Siddhi IO File Test for cron Support for directory");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full_single', " +
                "action.after.process='move', tailing='false', cron.expression='*/5 * * * * ?', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/text_full', " +
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
                log.info("Count is : " + n + " Time is : " + new Date(System.currentTimeMillis()));
                for (Event event : events) {
                    AssertJUnit.assertTrue(companies.contains(event.getData(0).toString()));
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        File file2 = new File(dirUri + "/text_full/cloudbees.json");
        File file3 = new File(dirUri + "/text_full_single/cloudbees.json");
        FileUtils.copyFile(file2, file3);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);

        File file = new File(moveAfterProcessDir + "/text_full");
        AssertJUnit.assertEquals(2, (Objects.requireNonNull(file.list()).length));

        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIOFileTestCronExpressionInDirectory() throws InterruptedException, IOException {
        log.info("Siddhi IO File for Cron Support to check whether Cron Expression satisfies the time");
        long[] time = new long[3];
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', dir.uri='file:/" + dirUri + "/text_full_single', " +
                "action.after.process='move', tailing='false', cron.expression='*/5 * * * * ?', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/text_full', " +
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
                time[n] = System.currentTimeMillis();
                log.info("Count is : " + n + " Time is : " + new Date(System.currentTimeMillis()));
                for (Event event : events) {
                    AssertJUnit.assertTrue(companies.contains(event.getData(0).toString()));
                }
            }
        });

        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 1, count, timeout);
        File file2 = new File(dirUri + "/text_full/cloudbees.json");
        File file3 = new File(dirUri + "/text_full_single/cloudbees.json");
        FileUtils.copyFile(file2, file3);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        long val = time[2] - time[1];
        log.info("Difference between " + time[2] + " and " + time[1] + " : " + val);
        if (val < 4000) {
            AssertJUnit.fail("Cron Time is not satisfied");
        } else {
            //assert event count
            AssertJUnit.assertEquals("Number of events", 2, count.get());
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest10() throws InterruptedException {
        log.info("Cron is not null and action.after.process is in default value");
        String streams = "" +
                "@App:name('SiddhiApp')" +
                "@source(type='file', file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/apache.json', " +
                "cron.expression='*/5 * * * * ?', tailing='false', @map(type='csv'))\n" +
                "define stream InputStream (symbol string, price float, volume long);" +
                "@sink(type='log')" +
                "define stream OutputStream (symbol string, price float, volume long);";

        String query = "" +
                "from InputStream " +
                "select * " +
                "insert into OutputStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest11() throws InterruptedException {
        log.info("Cron is not null tailing is true");
        String streams = "" +
                "@App:name('SiddhiApp')" +
                "@source(type='file', file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "action.after.process='MOVE', cron.expression='*/5 * * * * ?', " +
                "move.after.process='file:/" + moveAfterProcessDir + "/apache.json', " +
                "tailing='true', @map(type='csv'))\n" +
                "define stream InputStream (symbol string, price float, volume long);" +
                "@sink(type='log')" +
                "define stream OutputStream (symbol string, price float, volume long);";

        String query = "" +
                "from InputStream " +
                "select * " +
                "insert into OutputStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest12() throws InterruptedException {
        log.info("Move after process is null but cron is not null");
        String streams = "" +
                "@App:name('SiddhiApp')" +
                "@source(type='file', file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "action.after.process='MOVE', " +
                "cron.expression='*/5 * * * * ?', tailing='false', @map(type='csv'))\n" +
                "define stream InputStream (symbol string, price float, volume long);" +
                "@sink(type='log')" +
                "define stream OutputStream (symbol string, price float, volume long);";

        String query = "" +
                "from InputStream " +
                "select * " +
                "insert into OutputStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTest13() throws InterruptedException {
        log.info("Cron expression is not valid");
        String streams = "" +
                "@App:name('SiddhiApp')" +
                "@source(type='file', file.uri='file:/" + dirUri + "/text_full_single/apache.json', " +
                "action.after.process='MOVE', move.after.process='file:/" + moveAfterProcessDir + "/apache.json', " +
                "cron.expression='* 5 * * * * ?', tailing='false', @map(type='csv'))\n" +
                "define stream InputStream (symbol string, price float, volume long);" +
                "@sink(type='log')" +
                "define stream OutputStream (symbol string, price float, volume long);";

        String query = "" +
                "from InputStream " +
                "select * " +
                "insert into OutputStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }
}
