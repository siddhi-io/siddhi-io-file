/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
    private static final Logger log = Logger.getLogger(FileFunctionsTestCase.class);
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
            FileUtils.deleteDirectory(destination);
            FileUtils.forceMkdir(destination);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Test
    public void fileCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('file:" + sourceRoot + "/testFile/test.txt', '" + sourceRoot + "/destination', '')\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/testFile/test.txt", false));
    }

    @Test
    public void folderCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('" + sourceRoot + "/archive', '" + sourceRoot + "/destination', '', " + false + ")\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/archive/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/testFile/test.txt", false));
    }

    @Test
    public void folderCopyWithoutRootFolderFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() without the root folder");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('" + sourceRoot + "/archive', '" + sourceRoot + "/destination', '', " + true + ")\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/testFile/test.txt", false));
    }

    @Test
    public void folderCopyWithRegexFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy() only files adheres to a regex");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('" + sourceRoot + "/archive', '" + sourceRoot + "/destination', '.*test3.txt$')\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/archive/subFolder/test3.txt", false));
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/archive/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/archive/test.txt", false));
    }

    @Test
    public void fileCreateAndDeleteFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for create() and delete()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CreateFileStream(sample string);\n" +
                "from CreateFileStream#" +
                "file:create('file:" + sourceRoot + "/destination/created.txt', " + false + ")\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/created.txt", false));
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream DeleteFileStream(sample string);\n" +
                "from DeleteFileStream#file:delete('file:" + sourceRoot + "/destination/created.txt')\n" +
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
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt", false));
    }

    @Test
    public void fileCreateFolderAndDeleteFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for create() and delete()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CreateFileStream(sample string);\n" +
                "from CreateFileStream#" +
                "file:create('file:" + sourceRoot + "/destination/created', " + true + ")\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/created", true));

        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream DeleteFileStream(sample string);\n" +
                "from DeleteFileStream#file:delete('file:" + sourceRoot + "/destination/created')\n" +
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
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created", true));
    }

    @Test
    public void fileArchiveTarFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for archiving, listing files in an archived file, " +
                "unarchive and search files with or without regex");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ArchiveFileStream(sample string);\n" +
                "from ArchiveFileStream#" +
                "file:archive('" + sourceRoot + "/archive/', '" + destination + "', 'tar')\n" +
                "select *\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler archiveFileStream = siddhiAppRuntime1.getInputHandler("ArchiveFileStream");
        siddhiAppRuntime1.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals("TarArchiveFileStream", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime1.start();
        archiveFileStream.send(new Object[]{"TarArchiveFileStream"});
        Thread.sleep(100);
        siddhiAppRuntime1.shutdown();

        count.set(0);

        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ListArchivedFileStream(sample string);\n" +
                "from ListArchivedFileStream#file:searchInArchive('" + destination + "/archive.tar')\n" +
                "select fileNameList \n" +
                "insert into FileNameListStream;\n" + //;// +
                "from FileNameListStream#list:tokenize(fileNameList)\n" +
                "select index, value \n" +
                "insert into ResultStream;";
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler listArchivedFileStream = siddhiAppRuntime2.getInputHandler("ListArchivedFileStream");
        List<String> fileList = new ArrayList<>();
        fileList.add("test2.txt");
        fileList.add("subFolder/test3.txt");
        fileList.add("test.txt");
        siddhiAppRuntime2.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                int n = count.getAndIncrement();
                for (Event event : events) {
                    log.info(event.toString());
                    if (n >= 0 && n < 3) {
                        AssertJUnit.assertTrue(fileList.contains(event.getData(1)));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime2.start();
        listArchivedFileStream.send(new Object[]{"ListTarArchivedFileStream"});
        Thread.sleep(100);
        siddhiAppRuntime2.shutdown();

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
                "from UnArchiveFileStream#" +
                "file:unarchive('" + destination + "/archive.tar', '" + unzipLocation.getAbsolutePath() + "')\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiAppRuntime siddhiAppRuntime3 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler unArchiveFileStream = siddhiAppRuntime3.getInputHandler("UnArchiveFileStream");
        siddhiAppRuntime3.addCallback("ResultStream", new StreamCallback() {
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
        siddhiAppRuntime3.start();
        unArchiveFileStream.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime3.shutdown();

        count.set(0);
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream UnArchiveFileStream(sample string);\n" +
                "from UnArchiveFileStream#" +
                "file:unarchive" +
                "('" + destination + "/archive.tar', '" + unzipLocation.getAbsolutePath() + "', " + true + ")\n" +
                "select *\n" +
                "insert into ResultStream;";

        SiddhiAppRuntime siddhiAppRuntime4 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler unArchiveFileStream2 = siddhiAppRuntime4.getInputHandler("UnArchiveFileStream");
        siddhiAppRuntime4.addCallback("ResultStream", new StreamCallback() {
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
        siddhiAppRuntime4.start();
        unArchiveFileStream2.send(new Object[]{"WSO2"});
        Thread.sleep(100);
        siddhiAppRuntime4.shutdown();
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/decompressed/archive/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/decompressed/test.txt", false));
    }

    @Test
    public void fileArchiveTarFunction2() throws InterruptedException {
        log.info("test Siddhi Io File Function for archiving, listing files in an archived file, " +
                "unarchive and search files with or without regex");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ArchiveFileStream(sample string);\n" +
                "from ArchiveFileStream#" +
                "file:archive('" + sourceRoot + "/archive/', '" + destination + "/', 'zip')\n" +
                "select *\n" +
                "insert into ResultStream;";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime1 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler archiveFileStream = siddhiAppRuntime1.getInputHandler("ArchiveFileStream");
        siddhiAppRuntime1.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n == 0) {
                        AssertJUnit.assertEquals("TarArchiveFileStream", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime1.start();
        archiveFileStream.send(new Object[]{"TarArchiveFileStream"});
        Thread.sleep(100);
        siddhiAppRuntime1.shutdown();
    }

    @Test
    public void fileArchiveFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for archiving, listing files in an archived file, " +
                "unarchive and search files with or without regex");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ArchiveFileStream(sample string);\n" +
                "from ArchiveFileStream#file:archive('" + sourceRoot + "/archive/', '" + destination + "')\n" +
                "select *\n" +
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
                        AssertJUnit.assertEquals("Archive", event.getData(0));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"Archive"});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
        count.set(0);
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream ListArchivedFileStream(sample string);\n" +
                "from ListArchivedFileStream#file:searchInArchive('" + destination + "/archive.zip')\n" +
                "select sample, fileNameList " +
                "insert into FileNameListStream;\n" +
                "from FileNameListStream#list:tokenize(fileNameList)\n" +
                "select index, value " +
                "insert into ResultStream;";
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(app);
        InputHandler listArchivedFileStream = siddhiAppRuntime2.getInputHandler("ListArchivedFileStream");
        List<String> fileList = new ArrayList<>();
        fileList.add("test2.txt");
        fileList.add("subFolder/test3.txt");
        fileList.add("test.txt");
        siddhiAppRuntime2.addCallback("ResultStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                int n = count.getAndIncrement();
                for (Event event : events) {
                    if (n >= 0 && n < 3) {
                        AssertJUnit.assertTrue(fileList.contains(event.getData(1)));
                    } else {
                        AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });
        siddhiAppRuntime2.start();
        listArchivedFileStream.send(new Object[]{"ListArchive"});
        Thread.sleep(100);
        siddhiAppRuntime2.shutdown();
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
                "from UnArchiveFileStream#" +
                "file:unarchive" +
                "('" + destination + "/archive.zip', '" + unzipLocation.getAbsolutePath() + "', " + false + ")\n" +
                "select *\n" +
                "insert into ResultStream;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("UnArchiveFileStream");
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
        app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream UnArchiveFileStream(sample string);\n" +
                "from UnArchiveFileStream#" +
                "file:unarchive" +
                "('" + destination + "/archive.zip', '" + unzipLocation.getAbsolutePath() + "', " + true + ")\n" +
                "select *\n" +
                "insert into ResultStream;";
        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);
        stockStream = siddhiAppRuntime.getInputHandler("UnArchiveFileStream");
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/decompressed/archive/test.txt", false));
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/decompressed/test.txt", false));
    }

    @Test
    public void folderMoveFunction() throws InterruptedException, IOException {
        FileUtils.copyDirectory(sourceRoot, tempSource);
        log.info("test Siddhi Io File Function for move()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        count.set(0);
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream MoveFileStream(sample string);\n" +
                "from MoveFileStream" +
                "#file:move('" + tempSource + "/archive/', '" + sourceRoot + "/destination', '')\n" +
                "select * \n" +
                "insert into ResultStream;";
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/archive/test.txt", false));
        AssertJUnit.assertFalse(isFileExist(tempSource + "/archive/test.txt", false));
    }

    @Test
    public void folderMoveWithRegexFunction() throws InterruptedException, IOException {
        FileUtils.copyDirectory(sourceRoot, tempSource);
        log.info("test Siddhi Io File Function for copy() only files adheres to a regex");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:move" +
                "('" + tempSource + "/archive', '" + sourceRoot + "/destination', '.*test3.txt$')\n" +
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/archive/subFolder/test3.txt", false));
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/archive/test.txt", false));
        AssertJUnit.assertFalse(isFileExist(tempSource + "/archive/subFolder/test3.txt", false));
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
                "select file:isDirectory('" + sourceRoot + "/testFile/') as directoryExist\n" +
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
    public void fileMoveFunction() throws InterruptedException, IOException {
        FileUtils.copyDirectory(sourceRoot, tempSource);
        log.info("test Siddhi Io File Function for move()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream MoveFileStream(sample string);\n" +
                "from MoveFileStream" +
                "#file:move('file:" + tempSource + "/testFile/test.txt', '" + sourceRoot + "/destination', '')\n" +
                "select * \n" +
                "insert into ResultStream;";
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
        AssertJUnit.assertTrue(isFileExist(sourceRoot + "/destination/test.txt", false));
        AssertJUnit.assertFalse(isFileExist(tempSource + "/testFile/test.txt", false));
    }

    @Test
    public void fileExistsForFileFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isExists()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isExist('file:" + sourceRoot + "/archive/test.txt') as created\n" +
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
    public void isFileExistsFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isFile()");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isFile('file:" + sourceRoot + "/archive/test.txt') as isFileExist\n" +
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
    public void isDirectoryExistsFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for isFile()");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream IsExistFileStream(sample string);\n" +
                "from IsExistFileStream\n" +
                "select file:isDirectory('file:" + sourceRoot + "/archive/') as isDirectoryExist\n" +
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
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination/created.txt", false));
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
                "select file:size('" + sourceRoot + "/archive/test.txt') as fileSize\n" +
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
    public void fileSizeOfFolderFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for size() for a folder");
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FileSizeStream(sample string);\n" +
                "from FileSizeStream\n" +
                "select file:size('" + sourceRoot + "/archive/') as fileSize\n" +
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
                        AssertJUnit.assertEquals(1710L, (long) event.getData(0));
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

    @Test
    public void ftpFileCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('ftp://bob:password@localhost:21/test1/function/testFile/test.txt', " +
                    "'ftp://bob:password@localhost:21/test1/function/destination', '')\n" +
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
    }

    @Test
    public void ftpFolderCopyFunction() throws InterruptedException {
        log.info("test Siddhi Io File Function for copy()");
        AssertJUnit.assertFalse(isFileExist(sourceRoot + "/destination", false));
        String app = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream CopyFileStream(sample string);\n" +
                "from CopyFileStream#file:copy" +
                "('ftp://bob:password@localhost:21/test1/function/archive', " +
                    "'ftp://bob:password@localhost:21/test1/function/destination', '')\n" +
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
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking existence for path: " +
                    filePathUri, e);
        }
    }
}
