package io.siddhi.extension.io.file;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class FileFunctionsTestCase {
    private static final Logger log = Logger.getLogger(FileSinkTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private File sourceRoot, tempSource, destination;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        sourceRoot = new File(rootPath + "/repo/function/");
        tempSource = new File(rootPath + "/repo/function/tempSource");
        destination = new File(rootPath + "/repo/function/destination/");
        try {
            FileUtils.forceMkdir(destination);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @BeforeMethod
    public void doBeforeMethod() {
        count.set(0);
        try {
            FileUtils.copyDirectory(sourceRoot, tempSource);
            FileUtils.cleanDirectory(destination);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void fileIsFileFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isFile().");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CheckIsFileStream(sample string);\n" +
                "from CheckIsFileStream\n" +
                "select file:isFile('" + sourceRoot + "/testFile/test.txt') as fileExist\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("CheckIsFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileIsDirectoryFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isDirectory().");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CheckIsFileStream(sample string);\n" +
                "from CheckIsFileStream\n" +
                "select file:isDirectory('" + tempSource + "/testFile/') as directoryExist\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("CheckIsFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileArchiveFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for archiving, listing files in an archived file, " +
                "unarchive and search files with or without regex");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ArchiveFileStream(sample string);\n" +
                "from ArchiveFileStream\n" +
                "select file:archive" +
                "('" + sourceRoot + "/archive/', '" + destination + "/file.zip', " + true + ") as isArchived\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("ArchiveFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();

        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ListArchivedFileStream(sample string);\n" +
                "from ListArchivedFileStream#file:listFilesInZip('" + destination + "/file.zip')\n" +
                "select fileName\n" +
                "insert into ResultStream;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("ListArchivedFileStream");
        List<String> fileList = new ArrayList<>();
        fileList.add("test2.txt");
        fileList.add("folder2/test3.txt");
        fileList.add("test.txt");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n > 0 && n <= 3) {
                        AssertJUnit.assertTrue(fileList.contains(event.getData(0)));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();

        File unzipLocation = new File(destination.getAbsolutePath() + "/decompressed");
        try {
            FileUtils.forceMkdir(unzipLocation);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        count.set(0);
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream UnArchiveFileStream(sample string);\n" +
                "from UnArchiveFileStream\n" +
                "select file:unarchive" +
                "('" + destination + "/file.zip', '" + unzipLocation.getAbsolutePath() + "') as isArchived\n" +
                "insert into ResultStream;";

        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("UnArchiveFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    log.info("UnArchiveFileStream: " + event.getData(0) + n);
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();

        count.set(0);
        List<String> fileList2 = new ArrayList<>();
        fileList2.add("test-classes/files/repo/function/destination/decompressed/test2.txt");
        fileList2.add("test-classes/files/repo/function/destination/decompressed/folder2/test3.txt");
        fileList2.add("test-classes/files/repo/function/destination/decompressed/test.txt");

        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ListFileStream(sample string);\n" +
                "from " +
                "ListFileStream#file:searchAndList('" + unzipLocation.getAbsolutePath() + "', '', " + true + ")\n" +
                "select fileName\n" +
                "insert into ResultStream;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("ListFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n >= 0 && n < 3) {
                        boolean fileFound = false;
                        for (String s : fileList2) {
                            if (event.getData(0).toString().endsWith(s)) {
                                fileFound = true;
                            }
                        }
                        AssertJUnit.assertTrue(fileFound);
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();

        count.set(0);
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ListFileStream(sample string);\n" +
                "from ListFileStream#file:searchAndList('" + unzipLocation.getAbsolutePath() + "', '.*test3.txt$', " +
                true + ")\n" +
                "select fileName\n" +
                "insert into ResultStream;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("ListFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        boolean fileFound = false;
                        for (String s : fileList2) {
                            if (event.getData(0).toString().endsWith(s)) {
                                fileFound = true;
                            }
                        }
                        AssertJUnit.assertTrue(fileFound);
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination"));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream\n" +
                "select file:copy('file:" + sourceRoot + "/testFile/test.txt', '" + sourceRoot + "/destination') " +
                "as moved\ninsert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("CopyFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/test.txt"));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/testFile/test.txt"));
    }

    @Test
    public void fileMoveFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for move()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination"));
        count.set(0);
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream MoveFileStream(sample string);\n" +
                "from MoveFileStream\n" +
                "select file:move('file:" + tempSource + "/testFile/test.txt', '" + sourceRoot + "/destination') " +
                "as moved\ninsert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("MoveFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/test.txt"));
        AssertJUnit.assertFalse(isFileExist(tempSource + "/testFile/test.txt"));
    }

    @Test
    public void fileCreateAndDeleteFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for create() and delete()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt"));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CreateFileStream(sample string);\n" +
                "from CreateFileStream\n" +
                "select file:create('file:" + sourceRoot + "/destination/created.txt') as created\n" +
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
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
        count.set(0);
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/created.txt"));
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream DeleteFileStream(sample string);\n" +
                "from DeleteFileStream\n" +
                "select file:delete('file:" + sourceRoot + "/destination/created.txt') as deleted\n" +
                "insert into ResultStream;";
        siddhiManager = new SiddhiManager();
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("DeleteFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt"));
    }

    @Test
    public void fileExistsFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isExists()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt"));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isExist('file:" + sourceRoot + "/archive/') as created\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("IsExistFileStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSizeFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for size()");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FileSizeStream(sample string);\n" +
                "from FileSizeStream\n" +
                "select file:size('file:" + sourceRoot + "/archive/test.txt') as fileSize\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FileSizeStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals(1235L, (long) event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileLastModifiedFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for lastModifiedTime()");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FileLastModifiedStream(sample string);\n" +
                "from FileLastModifiedStream\n" +
                "select file:lastModifiedTime('file:" + sourceRoot + "/archive/test.txt') as lastModifiedTime\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FileLastModifiedStream");
        siddhiAppRuntime.addCallback("ResultStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        Pattern pattern = Pattern.compile("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] " +
                                "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");
                        AssertJUnit.assertTrue(pattern.matcher((String) event.getData(0)).lookingAt());
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }

    private boolean isFileExist(String filePathUri) {
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(filePathUri, opts);
            return fileObj.isFile();
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking type of file in path: " +
                    filePathUri, e);
        }
    }
}
