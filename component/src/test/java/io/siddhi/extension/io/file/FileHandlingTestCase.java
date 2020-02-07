package io.siddhi.extension.io.file;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class FileHandlingTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTextFullModeTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 2000;
    private int timeout = 30000;

    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceTextFullModeTestCase.class.getClassLoader();
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
        count.set(0);
    }

    /**
     * Test cases for 'type = 'file_handler'
     * */
    @Test
    public void siddhiIoFileTest1() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 1 - Directory");
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "dir.uri='file:" + sourceRoot + "', @map(type='json'))\n" +
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
                        AssertJUnit.assertEquals(sourceRoot + "/destination", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(100);
        File newFile = new File(sourceRoot +  "/destination");
        if (newFile.mkdir()) {
            log.info (" New folder  has been created. ");
        }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        if (newFile.delete()) {
            log.info("New folder has been deleted");
        }
        AssertJUnit.assertEquals(1, count.get());
    }


    private boolean isFileExist(String filePathUri, boolean isDirectory) {
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(filePathUri, opts);
            if (!isDirectory) {
                return fileObj.isFile();
            } else {
                return fileObj.isFolder();
            }
        } catch (org.apache.commons.vfs2.FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking existence for path: " +
                    filePathUri, e);
        }
    }
    @Test
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("Siddhi IO File Testcase 1 - Directory without file:/");
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "dir.uri='" + sourceRoot + "', @map(type='json'))\n" +
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
                        AssertJUnit.assertEquals(sourceRoot + "/destination", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(100);
        File newFile = new File(sourceRoot +  "/destination");
        if (newFile.mkdir()) {
            log.info (" New folder  has been created. ");
        }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        if (newFile.delete()) {
            log.info("New folder has been deleted");
        }
        AssertJUnit.assertEquals(1, count.get());
    }



    @Test
    public void siddhiIoFileTest3() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 2 - file");
        String path = sourceRoot + "/destination.txt";
        File currentFile = new File(path);
        File newFile = new File(sourceRoot +  "/action.txt");
       if (newFile.createNewFile()) {
           log.info("New file has been created to validate the path");
       }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "file.uri='file:" + sourceRoot + "/action.txt', @map(type='json'))\n" +
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
                    if (n == 0) {
                        AssertJUnit.assertEquals(sourceRoot + "/destination.txt", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        if (currentFile.createNewFile()) {
            log.info("New file has been created");
        }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(1, count.get());
        if (currentFile.delete() && newFile.delete()) {
            log.info("Files are deleted");
        }
    }


    @Test
    public void siddhiIoFileTest4() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 2 - file");
        String path = sourceRoot + "/destination.txt";
        File currentFile = new File(path);
        String path1 = sourceRoot +  "/action.txt";
        File newFile = new File(path1);
        if (newFile.createNewFile()) {
            log.info("New file has been created to validate the path");
        }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "file.uri='" + sourceRoot + "/action.txt', @map(type='json'))\n" +
                "define stream CreateFileStream (filepath string, length int," +
                " last_modified string, status string); \n" +
                "@sink(type='log')" +
                "define stream ResultStream (filepath string, length int, last_modified string, status string); " +
                "from CreateFileStream\n" +
                "select *\n" +
                "insert into ResultStream;";


        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("CreateFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(sourceRoot + "/destination.txt", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
       if (currentFile.createNewFile()) {
           log.info("New file has been created");
       }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(1, count.get());
        if (currentFile.delete() && newFile.delete()) {
            log.info("Files are deleted");
        }
    }

    @Test
    public void siddhiIoFileTest5() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase rename or move");
        String path = sourceRoot + "/destination.txt";
        File currentFile = new File(path);
        String path1 = sourceRoot + "/changeddestination.txt";
        File newFile = new File(path1);
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "dir.uri='" + sourceRoot + "', @map(type='json'))\n" +
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
                        AssertJUnit.assertEquals(sourceRoot + "/changeddestination.txt", event.getData(0));
                    } else if (n == 1) {
                        AssertJUnit.assertEquals(sourceRoot + "/destination.txt", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        if (currentFile.createNewFile()) {
            log.info("New file has been created");
        }
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        if (currentFile.renameTo(new File(path1))) {
            log.info("File has been renamed");
        }
        Thread.sleep(1000);
        if (newFile.delete()) {
            log.info("Files are deleted");
        }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(3, count.get());
    }

    @Test
    public void siddhiIoFileTest6() throws InterruptedException, IOException {
        log.info("Siddhi IO File Testcase 6 delete() ");
        String path = sourceRoot + "/destination.txt";
        File currentFile = new File(path);
        if (currentFile.createNewFile()) {
            log.info("New file has been created");
        }
        String app = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler', " +
                "dir.uri='" + sourceRoot + "', @map(type='json'))\n" +
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
                        AssertJUnit.assertEquals(sourceRoot + "/destination.txt", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        if (currentFile.delete()) {
            log.info("File is deleted");
        }
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(1, count.get());
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhifiletest7exception() throws InterruptedException {
        log.info("Test Siddhi Io File Function File Not found");
        log.info(sourceRoot);
        String app = "" +
                "@App:name('SiddhiAppFileNotFound') @source(type='file_handler', " +
               "dir.uri='file:" + sourceRoot + "/exists', @map(type='json'))" +
                "define stream CreateFileStream(filepath string, length int, last_modified string);\n" +
                "define stream ResultStream(filepath string,length int, last_modified string);\n" +
                "from CreateFileStream\n" +
                "select *\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhifiletest8exception() throws InterruptedException {
        log.info("test SiddhiIoFile Exception");
        String streams = "" +
                "@App:name('TestSiddhiApp') @source(type='file_handler'," +
                "file.uri='file:/home/ajanthy/wso2/Trial1/productiondns.csv' ,@map(type='json'))" +
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
}
