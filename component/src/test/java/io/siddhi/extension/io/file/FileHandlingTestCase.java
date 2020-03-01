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
import org.testng.Assert;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FileHandlingTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTextFullModeTestCase.class);
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
            throw new TestException("Failed to make directory " + newRoot, e);
        }
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.forceDelete(newRoot);
        } catch (IOException e) {
            throw new TestException("Failed to delete files in the " + newRoot, e);
        }
        count.set(0);
    }

    /**
     * Test cases for 'type = 'file_handler'
     * */
    @Test
    public void siddhiIOFileTest1() throws InterruptedException {
        log.info("Siddhi IO File Testcase 1 - Directory");
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='file:" + newRoot + "', @map(type='json'))\n" +
                "define stream CreateFileStream(filepath string, length long, last_modified string, status string);\n" +
                "define stream ResultStream(filepath string, length long, last_modified string, status string);\n" +
                "from CreateFileStream\n" +
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
                    if (n == 0) {
                        Assert.assertEquals(newRoot + "/destination", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        File newFile = new File(newRoot +  "/destination");
        if (newFile.mkdir()) {
            log.info (" New folder  has been created. ");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        if (newFile.delete()) {
            log.info("New folder has been deleted");
        }
        Assert.assertEquals(1, count.get());
    }


    @Test
    public void siddhiIOFileTest2() throws InterruptedException {
        log.info("Siddhi IO File Testcase 1 - Directory without file:/");
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='" + newRoot + "', @map(type='json'))\n" +
                "define stream CreateFileStream(filepath string, length int, last_modified string, status string);\n" +
                "define stream ResultStream(filepath string, length int, last_modified string, status string);\n" +
                "from CreateFileStream\n" +
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
                    if (n == 0) {
                        Assert.assertEquals(newRoot + "/destination", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        File newFile = new File(newRoot +  "/destination");
        if (newFile.mkdir()) {
            log.info (" New folder  has been created. ");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        if (newFile.delete()) {
            log.info("New folder has been deleted");
        }
        Assert.assertEquals(1, count.get());
    }



    @Test
    public void siddhiIOFileTest3() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 3 - file");
        File newFile = new File(newRoot + "/action.txt");
        if (newFile.createNewFile()) {
            log.info("New file has been created to validate the path");
        }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='file:" + newRoot + "/action.txt', @map(type='json'))\n" +
                "define stream CreateFileStream (filepath string, length int, last_modified string, " +
                "status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, length int, last_modified string, status string); " +
                "from CreateFileStream\n" +
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
        FileWriter fw = new FileWriter(newRoot + "/action.txt");
        fw.write("Hi this is added newly");
        fw.close();
        SiddhiTestHelper.waitForEvents(200, 3, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(3, count.get());
        if (newFile.delete()) {
            log.info("File is deleted");
        }
    }


    @Test
    public void siddhiIOFileTest4() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 4 - file");
        String path1 = newRoot +  "/action.txt";
        File newFile = new File(path1);
        if (newFile.createNewFile()) {
            log.info("New file has been created to validate the path");
        }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='" + newRoot + "/action.txt', @map(type='json'))\n" +
                "define stream CreateFileStream (filepath string, length int," +
                " last_modified string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, length int, last_modified string, status string); " +
                "from CreateFileStream\n" +
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
                    if (n == 0) {
                        Assert.assertEquals(newRoot + "/action.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        if (newFile.delete()) {
            log.info("File is deleted ");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(1, count.get());
    }

    @Test
    public void siddhiIOFileTest5() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase rename or move");
        String path = newRoot + "/destination.txt";
        File currentFile = new File(path);
        String path1 = newRoot + "/changeddestination.txt";
        File newFile = new File(path1);
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='" + newRoot + "', @map(type='json'))\n" +
                "define stream CreateFileStream (filepath string, length int," +
                " last_modified string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, length int, last_modified string, status string); " +
                "from CreateFileStream\n" +
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
                        Assert.assertEquals(newRoot + "/changeddestination.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        if (currentFile.createNewFile()) {
            log.info("New file has been created");
        }
        SiddhiTestHelper.waitForEvents(100, 1, count.get(), 3000);
        if (currentFile.renameTo(new File(path1))) {
            log.info("File has been renamed");
        }
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        if (newFile.delete()) {
            log.info("Files are deleted");
        }
        SiddhiTestHelper.waitForEvents(100, 3, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(3, count.get());
    }

    @Test
    public void siddhiIoFileTest6() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 6 delete() ");
        String path = newRoot + "/destination.txt";
        File currentFile = new File(path);
        if (currentFile.createNewFile()) {
            log.info("New file has been created");
        }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener', " +
                "uri='" + newRoot + "', @map(type='json'))\n" +
                "define stream CreateFileStream (filepath string, length int," +
                " last_modified string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, length int, last_modified string, status string); " +
                "from CreateFileStream\n" +
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
                        Assert.assertEquals(newRoot + "/destination.txt", event.getData(0));
                    } else {
                        Assert.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        if (currentFile.delete()) {
            log.info("File is deleted");
        }
        SiddhiTestHelper.waitForEvents(100, 2, count.get(), 3000);
        siddhiAppRuntime.shutdown();
        Assert.assertEquals(2, count.get());
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhifiletest7exception() throws InterruptedException {
        log.info("Test Siddhi Io File Function File Not found");
        log.info(newRoot);
        String app = "" +
                "@App:name('SiddhiAppFileNotFound') @source(type='fileeventlistener', " +
                "uri='file:" + newRoot + "/exists', @map(type='json'))" +
                "define stream CreateFileStream(filepath string, length int, last_modified string);\n" +
                "define stream ResultStream(filepath string,length int, last_modified string);\n" +
                "from CreateFileStream\n" +
                "select *\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhifiletest8exception() throws InterruptedException {
        log.info("test SiddhiIoFile Exception");
        String streams = "" +
                "@App:name('TestSiddhiApp') @source(type='fileeventlistener'," +
                "uri='file:/home/ajanthy/wso2/Trial1/productiondns.csv' ,@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(100, 0, count.get(), 1000);
        siddhiAppRuntime.shutdown();
    }
}
