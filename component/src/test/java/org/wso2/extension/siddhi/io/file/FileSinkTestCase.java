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
import org.wso2.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file sink.
 * */
public class FileSinkTestCase {
    private static final Logger log = Logger.getLogger(FileSinkTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    private String dirUri, sinkUri;
    private File sourceRoot, fileToAppend, sinkRoot;

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = classLoader.getResource("files").getFile();
        sourceRoot = new File(rootPath + "/repo/sink_repo");
        dirUri = rootPath + "/new";
        sinkUri = rootPath + "/sink";
        fileToAppend = new File(dirUri);
        sinkRoot = new File(sinkUri);
    }

    @BeforeMethod
    public void doBeforeMethod() {
        count.set(0);
        try {
            FileUtils.copyDirectory(sourceRoot, fileToAppend);
        } catch (IOException e) {
            throw new TestException("Failed to copy files from " +
                    sourceRoot.getAbsolutePath() +
                    " to " +
                    fileToAppend.getAbsolutePath() +
                    " which are required for tests. Hence aborting tests.", e);
        }
    }

    @AfterMethod
    public void doAfterMethod() {
        try {
            FileUtils.deleteDirectory(fileToAppend);
            FileUtils.deleteDirectory(sinkRoot);
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
    }

    @Test
    public void fileSinkTest1() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 1");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='false', " +
                "file.uri='" + sinkUri + "/{{symbol}}.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.json");
        symbolNames.add("IBM.json");
        symbolNames.add("GOOGLE.json");
        symbolNames.add("REDHAT.json");

        File sink = new File(sinkUri);
        if (sink.isDirectory()) {
            for (File file : sink.listFiles()) {
                if (symbolNames.contains(file.getName())) {
                    count.incrementAndGet();
                }
            }
            AssertJUnit.assertEquals(4, count.intValue());
        } else {
            AssertJUnit.fail(sinkUri + " is not a directory.");
        }

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest2() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 2");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='false', " +
                "file.uri='" + dirUri + "/empty.txt') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        File file = new File(dirUri + "/empty.txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                count.incrementAndGet();
                AssertJUnit.assertEquals(line, "{\"event\":{\"symbol\":\"REDHAT\",\"price\":50.0,\"volume\":100}}");
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(1, count.intValue());

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest3() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 3");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='false', " +
                "file.uri='" + dirUri + "/apache.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                count.incrementAndGet();
                AssertJUnit.assertEquals(line, "{\"event\":{\"symbol\":\"REDHAT\",\"price\":50.0,\"volume\":100}}");
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(1, count.intValue());

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest4() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 4");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='" + dirUri + "/newFile.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> msgs = new ArrayList<>();
        msgs.add("{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"IBM\",\"price\":57.678,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":50.0,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"REDHAT\",\"price\":50.0,\"volume\":100}}");

        File file = new File(dirUri + "/newFile.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (msgs.contains(line)) {
                    count.incrementAndGet();
                } else {
                    AssertJUnit.fail("Message " + line + " is not supposed to be written.");
                }
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(4, count.intValue());

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest5() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 5");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='" + dirUri + "/empty.txt') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> msgs = new ArrayList<>();
        msgs.add("{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"IBM\",\"price\":57.678,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":50.0,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"REDHAT\",\"price\":50.0,\"volume\":100}}");

        File file = new File(dirUri + "/empty.txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (msgs.contains(line)) {
                    count.incrementAndGet();
                } else {
                    AssertJUnit.fail("Message " + line + " is not supposed to be written.");
                }
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(4, count.intValue());

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest6() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 6");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='" + dirUri + "/apache.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> msgs = new ArrayList<>();
        msgs.add("{\"event\":{\"symbol\":\"apache\",\"price\":80.0,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"IBM\",\"price\":57.678,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":50.0,\"volume\":100}}");
        msgs.add("{\"event\":{\"symbol\":\"REDHAT\",\"price\":50.0,\"volume\":100}}");

        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (msgs.contains(line)) {
                    count.incrementAndGet();
                } else {
                    AssertJUnit.fail("Message " + line + " is not supposed to be written.");
                }
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(5, count.intValue());

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest7() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 7");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', " +
                "file.uri='" + sinkUri + "/{{symbol}}.json') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        stockStream.send(new Object[]{"WSO2", 56.6f, 200L});
        stockStream.send(new Object[]{"IBM", 58.678f, 200L});

        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.json");
        symbolNames.add("IBM.json");
        symbolNames.add("GOOGLE.json");
        symbolNames.add("REDHAT.json");

        File sink = new File(sinkUri);
        if (sink.isDirectory()) {
            for (File file : sink.listFiles()) {
                if (symbolNames.contains(file.getName())) {
                    count.incrementAndGet();
                }
            }
            AssertJUnit.assertEquals(4, count.intValue());
        } else {
            AssertJUnit.fail(sinkUri + " is not a directory.");
        }

        File wso2File = new File(sinkUri + "/WSO2.json");
        File ibmFile = new File(sinkUri + "/IBM.json");

        try {
            BufferedReader bufferedReader1 = new BufferedReader(new FileReader(wso2File));
            BufferedReader bufferedReader2 = new BufferedReader(new FileReader(ibmFile));

            int n = 0;
            String line = null;
            while ((line = bufferedReader1.readLine()) != null) {
                switch (n) {
                    case 0:
                        AssertJUnit.assertEquals("{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}}",
                                line);
                        break;
                    case 1:
                        AssertJUnit.assertEquals("{\"event\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":200}}",
                                line);
                        break;
                }
                n++;
            }

            AssertJUnit.assertEquals(2, n);

            n = 0;
            while ((line = bufferedReader2.readLine()) != null) {
                switch (n) {
                    case 0:
                        AssertJUnit.assertEquals("{\"event\":{\"symbol\":\"IBM\",\"price\":57.678,\"volume\":100}}",
                                line);
                        break;
                    case 1:
                        AssertJUnit.assertEquals("{\"event\":{\"symbol\":\"IBM\",\"price\":58.678,\"volume\":200}}",
                                line);
                        break;
                }
                n++;
            }

            AssertJUnit.assertEquals(2, n);

        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail(e.getMessage());
        }

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest8() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 8");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='text'), append='false', " +
                "file.uri='" + sinkUri + "/{{symbol}}.txt') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.txt");
        symbolNames.add("IBM.txt");
        symbolNames.add("GOOGLE.txt");
        symbolNames.add("REDHAT.txt");

        File sink = new File(sinkUri);
        if (sink.isDirectory()) {
            for (File file : sink.listFiles()) {
                if (symbolNames.contains(file.getName())) {
                    count.incrementAndGet();
                }
            }
            AssertJUnit.assertEquals(4, count.intValue());
        } else {
            AssertJUnit.fail(sinkUri + " is not a directory.");
        }

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest9() throws InterruptedException {
        log.info("test SiddhiIoFile Sink 9");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='xml'), append='false', " +
                "file.uri='" + sinkUri + "/{{symbol}}.xml') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.xml");
        symbolNames.add("IBM.xml");
        symbolNames.add("GOOGLE.xml");
        symbolNames.add("REDHAT.xml");

        File sink = new File(sinkUri);
        if (sink.isDirectory()) {
            for (File file : sink.listFiles()) {
                if (symbolNames.contains(file.getName())) {
                    count.incrementAndGet();
                }
            }
            AssertJUnit.assertEquals(4, count.intValue());
        } else {
            AssertJUnit.fail(sinkUri + " is not a directory.");
        }

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSinkTest10() throws InterruptedException, CannotRestoreSiddhiAppStateException {
        log.info("test SiddhiIoFile Sink 10");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='xml'), append='false', " +
                "file.uri='" + sinkUri + "/{{symbol}}.xml') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        SiddhiAppRuntime siddhiAppRuntime2 = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler stockStream2 = siddhiAppRuntime2.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        byte[] snapshot = siddhiAppRuntime.snapshot();
        siddhiAppRuntime.shutdown();
        Thread.sleep(1000);

        siddhiAppRuntime2.restore(snapshot);
        siddhiAppRuntime2.start();

        stockStream2.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream2.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);

        ArrayList<String> symbolNames = new ArrayList<>();
        symbolNames.add("WSO2.xml");
        symbolNames.add("IBM.xml");
        symbolNames.add("GOOGLE.xml");
        symbolNames.add("REDHAT.xml");

        File sink = new File(sinkUri);
        if (sink.isDirectory()) {
            for (File file : sink.listFiles()) {
                if (symbolNames.contains(file.getName())) {
                    count.incrementAndGet();
                }
            }
            AssertJUnit.assertEquals(4, count.intValue());
        } else {
            AssertJUnit.fail(sinkUri + " is not a directory.");
        }

        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }
}
