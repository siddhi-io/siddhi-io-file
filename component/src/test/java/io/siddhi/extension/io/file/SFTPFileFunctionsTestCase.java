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
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class SFTPFileFunctionsTestCase {
    private static final Logger log = LogManager.getLogger(SFTPFileFunctionsTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private FileObject sourceLocalRoot, tempFTPSource, ftpDestination;
    String fileOptions;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        fileOptions = "USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true";
        sourceLocalRoot = Utils.getFileObject((rootPath + "/repo/function/"), null);
        tempFTPSource = Utils.getFileObject(
                "sftp://demo:demo@localhost:22/sftp/source", fileOptions);
        ftpDestination = Utils.getFileObject(
                "sftp://demo:demo@localhost:22/sftp/destination", fileOptions);
    }

    @BeforeMethod
    public void doBeforeMethod() throws InterruptedException, FileSystemException {
        count.set(0);
        try {
            tempFTPSource.delete(Selectors.SELECT_ALL);
            ftpDestination.delete(Selectors.SELECT_ALL);
            tempFTPSource.createFolder();
            ftpDestination.createFolder();
            tempFTPSource.copyFrom(sourceLocalRoot, Selectors.SELECT_ALL);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void sftpFileCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() for file");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                    "('sftp://demo:demo@localhost:22/sftp/source/move/moveFolder/test.txt', " +
                    "'sftp://demo:demo@localhost:22/sftp/destination/', '', " + false + ", " +
                    "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
                "insert into ResultStream;";
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/source/move/moveFolder/test.txt", false, fileOptions));
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination/test.txt", false, fileOptions));
    }

    @Test
    public void ftpFolderCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() for folder");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('sftp://demo:demo@localhost:22/sftp/source/archive', " +
                "'sftp://demo:demo@localhost:22/sftp/destination/', '', " + false +
                ", 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
                "insert into ResultStream;";
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/source/archive/", true, fileOptions));
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//archive/", true, fileOptions));
    }

    @Test
    public void ftpFolderCopyWithoutRootFolderFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() without the root folder (only the content)");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('sftp://demo:demo@localhost:22/sftp/source/archive', " +
                "'sftp://demo:demo@localhost:22/sftp/destination/', '', " + true +
                ", 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
                "insert into ResultStream;";
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//subFolder/", true,
                fileOptions));
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//test.txt", false,
                fileOptions));
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/source/archive/", true, fileOptions));
    }

    @Test
    public void folderCopyWithRegexFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() only files adheres to a regex");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('sftp://demo:demo@localhost:22/sftp/source/archive', " +
                "'sftp://demo:demo@localhost:22/sftp/destination/', '.*test2.txt$', " + false +
                ", 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
                "insert into ResultStream;";
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//archive/test2.txt", false, fileOptions));
        AssertJUnit.assertFalse(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//archive/test.txt", false, fileOptions));
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/source/archive/test.txt", false, fileOptions));
    }

    @Test
    public void fileCreateAndDeleteFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for create() and delete()");

        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CreateFileStream(sample string);\n" +
                "from CreateFileStream#" +
                "file:create('sftp://demo:demo@localhost:22/sftp/destination//created.txt', " +
                "" + false + ", 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination//created.txt", false, fileOptions));
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream DeleteFileStream(sample string);\n" +
                "from DeleteFileStream#file:delete(" +
                "'sftp://demo:demo@localhost:22/sftp/destination/created.txt', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertFalse(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination/created.txt", false, fileOptions));
    }

    @Test
    public void fileCreateFolderAndDeleteFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for create() and delete()");

        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CreateFileStream(sample string);\n" +
                "from CreateFileStream#" +
                "file:create('sftp://demo:demo@localhost:22/sftp/destination/created', " + true +
                ", 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertTrue(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination/created", true, fileOptions));

        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream DeleteFileStream(sample string);\n" +
                "from " +
                "DeleteFileStream#file:delete('sftp://demo:demo@localhost:22/sftp/destination/created'," +
                " 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true')\n" +
                "select *\n" +
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
                        AssertJUnit.assertEquals("WSO2", event.getData(0));
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
        AssertJUnit.assertFalse(isFileExist(
                "sftp://demo:demo@localhost:22/sftp/destination/created", true, fileOptions));
    }

    @Test
    public void fileIsFileFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isFile().");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CheckIsFileStream(sample string);\n" +
                "from CheckIsFileStream\n" +
                "select " +
                "file:isFile('sftp://demo:demo@localhost:22/sftp/source/archive/subFolder/test3.txt', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as fileExist\n" +
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
                "select " +
                "file:isDirectory('sftp://demo:demo@localhost:22/sftp/source/archive/', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as directoryExist\n" +
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
    public void fileExistsForFileFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isExists()");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isExist('sftp://demo:demo@localhost:22/sftp/source/archive/subFolder/test3.txt', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as exists\n" +
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
    public void fileExistsForFolderFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function isExists() for folder");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isExist('sftp://demo:demo@localhost:22/sftp/source/archive/', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as exists\n" +
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
                "select file:size('sftp://demo:demo@localhost:22/sftp/source/move/test.txt', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as fileSize\n" +
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
                        AssertJUnit.assertEquals(760L, (long) event.getData(0));
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
    public void fileSizeOfFolderFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for size() for a folder");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FileSizeStream(sample string);\n" +
                "from FileSizeStream\n" +
                "select file:size('sftp://demo:demo@localhost:22/sftp/source/move/', " +
                "'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as fileSize\n" +
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
                        AssertJUnit.assertEquals(2470L, (long) event.getData(0));
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
                "select file:lastModifiedTime(" +
                "'sftp://demo:demo@localhost:22/sftp/source/archive/test.txt', " +
                "'', 'USER_DIR_IS_ROOT:false,AVOID_PERMISSION_CHECK:true') as lastModifiedTime\n" +
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

    private boolean isFileExist(String filePathUri, boolean isDirectory, String fileSystemOptions) {
        try {
            FileObject fileObj = Utils.getFileObject(filePathUri, fileSystemOptions);
            if (!isDirectory) {
                return fileObj.isFile();
            } else {
                return fileObj.isFolder();
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking type of file in path: " +
                    filePathUri, e);
        }
    }
}
