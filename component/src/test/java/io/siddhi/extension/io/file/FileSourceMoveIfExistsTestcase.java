/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.extension.io.file.util.FileTestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides the test cases for the parameter move.if.exist.mode
 */
public class FileSourceMoveIfExistsTestcase {
    private static final Logger log = LogManager.getLogger(FileSourceMoveIfExistsTestcase.class);
    private AtomicInteger count = new AtomicInteger();
    private int waitTime = 2000;
    private int timeout = 30000;

    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;
    private List<String> companies = new ArrayList<>();

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceRegexModeTestCase.class.getClassLoader();
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

    @Test(expectedExceptions = { SiddhiAppCreationException.class })
    public void fileSourceMoveIfExistsTest1() {
        log.info("fileSourceMoveIfExistsTest1: " +
                "This is a negative testcase which asserts that a SiddhiAppCreationException is thrown " +
                "when the parameter move.if.exist.mode is specified but the action.after.process parameter " +
                "is not equal to MOVE");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='line'," +
                "dir.uri='file:/" + dirUri + "/line/json', " +
                "action.after.process='delete', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='false', " +
                "move.if.exist.mode='KEEP'," +
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
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMoveIfExistsTest2() throws InterruptedException, IOException {
        log.info("fileSourceMoveIfExistsTest2: move.if.exist.mode=KEEP");
        File originalFile = new File(dirUri + "/text_full/apache.json");
        String originalFileContent = FileUtils.readFileToString(originalFile);

        File beforeProcessFile = new File(dirUri + "/move_if_exists/apache.json");
        String beforeProcessFileContent = FileUtils.readFileToString(beforeProcessFile);

        FileUtils.copyFileToDirectory(beforeProcessFile, new File(moveAfterProcessDir));
        AssertJUnit.assertTrue(FileTestUtils.isFileExist(
                moveAfterProcessDir + "/apache.json", false));

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "move.if.exist.mode='KEEP'," +
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
        AssertJUnit.assertEquals(9, file.list().length);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());

        AssertJUnit.assertTrue(FileTestUtils.isFileExist(
                moveAfterProcessDir + "/apache.json", false));
        File afterProcessFile = new File(moveAfterProcessDir + "/apache.json");
        String afterProcessFileContent = FileUtils.readFileToString(afterProcessFile);
        AssertJUnit.assertEquals("File content has changed after the process!",
                beforeProcessFileContent, afterProcessFileContent);

        String movedFileName = getMovedFileName(file.list(), "apache.json");
        if (movedFileName != null) {
            File movedFile = new File(moveAfterProcessDir + "/" + movedFileName);
            String movedFileContent = FileUtils.readFileToString(movedFile);
            AssertJUnit.assertEquals("Moved file content is different from the original file content!"
                    , movedFileContent, originalFileContent);
        } else {
            AssertJUnit.fail("No file was found with name starting apache.json-. " +
                    "Looks like a new file has not got created!");
        }
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "fileSourceMoveIfExistsTest2")
    public void fileSourceMoveIfExistsTest3() throws InterruptedException, IOException {
        log.info("fileSourceMoveIfExistsTest3: move.if.exist.mode=OVERWRITE");
        File originalFile = new File(dirUri + "/text_full/apache.json");
        String originalFileContent = FileUtils.readFileToString(originalFile);

        /**
         * We move a file (beforeProcessFile) to the moveAfterProcessDir with the same name but with different content.
         * At the end of the process, the file content in moveAfterProcessDir/apache.json
         * should be equal to the originalFileContent. Then we can conclude that the file got successfully overwritten.
         */
        File beforeProcessFile = new File(dirUri + "/move_if_exists/apache.json");
        FileUtils.copyFileToDirectory(beforeProcessFile, new File(moveAfterProcessDir));
        AssertJUnit.assertTrue(FileTestUtils.isFileExist(
                moveAfterProcessDir + "/apache.json", false));

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='file:/" + dirUri + "/text_full', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "move.if.exist.mode='OVERWRITE'," +
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

        AssertJUnit.assertTrue(FileTestUtils.isFileExist(
                moveAfterProcessDir + "/apache.json", false));
        File afterProcessFile = new File(moveAfterProcessDir + "/apache.json");
        String afterProcessFileContent = FileUtils.readFileToString(afterProcessFile);
        AssertJUnit.assertEquals("Moved file content is not equal to the original file content!",
                originalFileContent, afterProcessFileContent);
        siddhiAppRuntime.shutdown();
    }

    private String getMovedFileName(String[] fileList, String originalFileName) {
        for (String fileName: fileList) {
            if (!fileName.equalsIgnoreCase(originalFileName) && fileName.contains(originalFileName)) {
                return fileName;
            }
        }
        return null;
    }
}
