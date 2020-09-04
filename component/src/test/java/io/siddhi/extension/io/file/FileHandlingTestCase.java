/*
 * Copyright (c)  2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FileHandlingTestCase {
    private static final Logger log = Logger.getLogger(FileHandlingTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    private File newRoot;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileHandlingTestCase.class.getClassLoader();
        String rootPath = Objects.requireNonNull(classLoader.getResource("files")).getFile();
        String dirUri = rootPath + "/new";
        newRoot = new File(dirUri);
    }

    @BeforeMethod
    public void doBeforeMethod() throws TestException {
        count.set(0);
        try {
            FileUtils.forceMkdir(newRoot);
        } catch (IOException e) {
            throw new TestException("Failed to make directory in the" + newRoot, e);
        }
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.forceDelete(newRoot);
        } catch (IOException e) {
            throw new TestException("Failed to delete files from the " + newRoot, e);
        }
        count.set(0);
    }

    /**
     * Test cases for 'type = 'fileeventlistener
     */
    @Test
    public void siddhiIOFileTest1() throws InterruptedException {
        log.info("Siddhi IO File Testcase 1");
        String app = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener', " +
                "dir.uri='file:" + newRoot + "')\n" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);\n" +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0 || n == 1) {
                        Assert.assertEquals(newRoot + "/destination", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        File newFile = new File(newRoot + "/destination");
        if (newFile.mkdir()) {
            log.debug("New folder has been created. ");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        if (newFile.delete()) {
            log.debug("New folder is deleted. ");
        }
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(2, count.get());
    }

    @Test
    public void fileHandlerPauseAndResume() throws InterruptedException, IOException {
        log.info("Test PauseAndResume - 2");
        File newFolder = new File(newRoot + "/destination");
        File newFile = new File(newRoot + "/action.txt");
        String app = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener', " +
                "dir.uri='file:" + newRoot + "')\n" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);\n" +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        Collection<List<Source>> sources = siddhiAppRuntime.getSources();
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0 || n == 1) {
                        Assert.assertEquals(newRoot + "/destination", event.getData(0));
                    } else if (n == 2 || n == 3) {
                        Assert.assertEquals(newRoot + "/action.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        if (newFolder.mkdir()) {
            log.debug("New folder has been created. ");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        if (newFolder.delete()) {
            log.debug("New folder is deleted. ");
        }
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        sources.forEach(e -> e.forEach(Source::pause));
        if (newFile.createNewFile()) {
            log.debug("New file has been created");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count.get(), 3000);
        if (newFile.delete()) {
            log.debug("File is deleted ");
        }
        sources.forEach(e -> e.forEach(Source::resume));
        SiddhiTestHelper.waitForEvents(100, 4, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(4, count.get());
    }

    @Test
    public void siddhiIOFileTest3() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 3");
        File newFile = new File(newRoot + "/action.txt");
        if (newFile.createNewFile()) {
            log.debug("New file has been created to validate the path");
        }
        String app = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener', " +
                "dir.uri='" + newRoot + "', file.name.list = 'action.txt')\n" +
                "define stream FileListenerStream (filepath string, filename string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, filename string, status string); " +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n <= 2) {
                        Assert.assertEquals(newRoot + "/action.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(200, 1, count.get(), 3000);
        FileWriter fw = new FileWriter(newFile);
        fw.write("Hi this is added newly");
        fw.close();
        SiddhiTestHelper.waitForEvents(200, 2, count.get(), 3000);
        if (newFile.delete()) {
            log.debug("File is deleted ");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(3, count.get());
    }

    @Test
    public void siddhiIOFileTest4() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 4 rename or move");
        File currentFile = new File(newRoot + "/destination.txt");
        File newFile = new File(newRoot + "/changedDestination.txt");
        String app = "" +
                "@App:name('TestFileEventListener') " +
                "@source(type='fileeventlistener', " +
                "dir.uri='" + newRoot + "')\n" +
                "define stream FileListenerStream (filepath string, filename string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, filename string, status string); " +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0 || n == 2) {
                        Assert.assertEquals(newRoot + "/destination.txt", event.getData(0));
                    } else if (n == 1) {
                        Assert.assertEquals(newRoot + "/changedDestination.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        if (currentFile.createNewFile()) {
            log.debug("New file has been created");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        if (currentFile.renameTo(newFile)) {
            log.debug("File has been renamed");
        }
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        if (newFile.delete()) {
            log.debug("Files are deleted");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(3, count.get());
    }

    @Test
    public void siddhiIOFileTest5() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 5");
        File newFile = new File(newRoot + "/destination.txt");
        File newFolder = new File(newRoot + "/destination");
        if (newFile.createNewFile()) {
            log.debug("New file has been created");
        }
        if (newFolder.createNewFile()) {
            log.debug("New file has been created");
        }
        String app = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener', " +
                "dir.uri='" + newRoot + "', file.name.list= 'destination, destination.txt')\n" +
                "define stream FileListenerStream (filepath string, filename string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, filename string, status string); " +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0 || n == 2) {
                        Assert.assertEquals(newRoot + "/destination.txt", event.getData(0));
                    } else if (n == 1) {
                        Assert.assertEquals(newRoot + "/destination", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        if (newFile.delete()) {
            log.debug("File is deleted");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(3, count.get());
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIOFileTest6() throws InterruptedException {
        log.info("Test Siddhi IO File Function 6 URI must be folder");
        String app = "" +
                "@App:name('SiddhiAppFileNotFound') @source(type='fileeventlistener', " +
                "dir.uri='file:" + newRoot + "/action')" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);\n" +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void siddhiIOFileTest7() throws InterruptedException {
        log.info("Siddhi IO File Exception 7- Monitoring value must in integer");
        String streams = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener'," +
                "dir.uri='" + newRoot + "', monitoring.interval= 'interval')\n" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);";

        String query = "" +
                "from FileListenerStream " +
                "select * " +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIOFileTest8() throws InterruptedException, IOException {
        log.info("Test Siddhi IO File 8 Function URI must be folder");
        File newFile = new File(newRoot + "/action.txt");
        if (newFile.createNewFile()) {
            log.debug("File is created");
        }
        String app = "" +
                "@App:name('SiddhiAppFileNotFound') @source(type='fileeventlistener', " +
                "dir.uri='file:" + newRoot + "/action.txt')" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);\n" +
                "from FileListenerStream\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIOFileTest9() throws InterruptedException {
        log.info("Siddhi IO File 9 Exception - Folder is not found");
        String streams = "" +
                "@App:name('SiddhiAppURINotFound') @source(type='fileeventlistener'," +
                "dir.uri='" + newRoot + "', file.name.list= 'interval')\n" +
                "define stream FileListenerStream(filepath string, length int, last_modified string);\n" +
                "define stream ResultStream(filepath string,length int, last_modified string);";

        String query = "" +
                "from FileListenerStream " +
                "select * " +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIOFileTest10() throws InterruptedException {
        log.info("Siddhi IO File Exception 10 - URI must provided");
        String streams = "" +
                "@App:name('SiddhiAppURINotFound') @source(type='fileeventlistener')" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);";

        String query = "" +
                "from FileListenerStream " +
                "select * " +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIOFileTest11() throws InterruptedException {
        log.info("Siddhi IO File Exception 11 - File given in the fileNameList should be available");
        String streams = "" +
                "@App:name('TestFileEventListener') @source(type='fileeventlistener'," +
                "dir.uri='" + newRoot + "', file.name.list = 'action.txt')\n" +
                "define stream FileListenerStream(filepath string, filename string, status string);\n" +
                "define stream ResultStream(filepath string, filename string, status string);";

        String query = "" +
                "from FileListenerStream " +
                "select * " +
                "insert into ResultStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }
}
