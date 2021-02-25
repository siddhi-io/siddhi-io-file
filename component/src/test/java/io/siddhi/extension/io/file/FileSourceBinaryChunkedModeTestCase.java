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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSourceBinaryChunkedModeTestCase {
    private static final Logger log = Logger.getLogger(FileSourceBinaryModeTestCase.class);
    private final AtomicInteger count = new AtomicInteger();
    private final int waitTime = 2000;
    private final int timeout = 30000;

    private String dirUri, moveAfterProcessDir;
    private File sourceRoot, newRoot, movedFiles;
    private List<String> companies = new ArrayList<>();

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceBinaryModeTestCase.class.getClassLoader();
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
     * Test cases for 'mode = binary.chunked'.
     */
    @Test
    public void siddhiIoFileTestForBinaryChunkedWithDirUri() throws InterruptedException {
        log.info("Siddhi IO File Test with binary.chunked mode and binaryPassThrough Mapper with Directory Uri");
        File file = new File(dirUri + "/binary");
        long fileSize = file.length();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='binary.chunked'," +
                "dir.uri='file:/" + dirUri + "/binary', " +
                "action.after.process='move', " +
                "move.after.process='file:/" + moveAfterProcessDir + "', " +
                "tailing='false', " +
                "@map(type='binaryPassThrough', " +
                "@attributes(buffer='0', eof = 'trp:eof', fileName = 'trp:file.name', " +
                "sequenceNumber = 'trp:sequence.number', length = 'trp:content.length')))\n" +
                "define stream FooStream (buffer object, eof bool, fileName string, sequenceNumber int, " +
                "length int);\n" +
                "@sink(type='log')\n" +
                "define stream BarStream (eof bool, fileName string, sequenceNumber int, length int); ";
        String query = "" +
                "from FooStream " +
                "select eof, fileName, sequenceNumber, length " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                int n = count.incrementAndGet();
                for (Event event : events) {
                    String fileName = (String) event.getData(1);
                    if (n <= 8) {
                        AssertJUnit.assertEquals(true, event.getData(0));
                        AssertJUnit.assertTrue(companies.contains(fileName.substring(0, fileName.length() - 4)));
                    } else {
                        AssertJUnit.fail();
                    }
                }
            }
        });
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 8, count, timeout);
        File afterProcessFile = new File(moveAfterProcessDir);
        AssertJUnit.assertEquals(fileSize, afterProcessFile.length());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTestForBinaryChunkedWithFileUri() throws InterruptedException {
        log.info("Siddhi IO File Test with binary.chunked mode and binaryPassThrough Mapper with File Uri");
        File file = new File(dirUri + "/binary/apache.bin");
        long fileSize = file.length();
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='binary.chunked'," +
                "file.uri='file:/" + dirUri + "/binary/apache.bin', " +
                "action.after.process='delete', " +
                "tailing='false', " +
                "@map(type='binaryPassThrough', " +
                "@attributes(buffer='0', eof = 'trp:eof', fileName = 'trp:file.name', " +
                "sequenceNumber = 'trp:sequence.number', length = 'trp:content.length')))\n" +
                "define stream FooStream (buffer object, eof bool, fileName string, sequenceNumber int, " +
                "length int);\n" +
                "@sink(type='log')\n" +
                "define stream BarStream (eof bool, fileName string, sequenceNumber int, length int); ";
        String query = "" +
                "from FooStream " +
                "select eof, fileName, sequenceNumber, length " +
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
                        AssertJUnit.assertEquals("apache.bin", event.getData(1));
                        AssertJUnit.assertEquals((int) fileSize, event.getData(3));
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
    public void siddhiIoFileTestForBinaryChunkedWithException() throws InterruptedException {
        log.info("Tailing has to be enabled with Binary.Chunked Mode");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='binary.chunked'," +
                "file.uri='file:/" + dirUri + "/binary/apache.bin', " +
                "tailing='true', " +
                "@map(type='binaryPassThrough', " +
                "@attributes(buffer='0', eof = 'trp:eof', fileName = 'trp:file.name', " +
                "sequenceNumber = 'trp:sequence.number', length = 'trp:content.length')))\n" +
                "define stream FooStream (buffer object, eof bool, fileName string, sequenceNumber int, " +
                "length int);\n" +
                "define stream BarStream (eof bool, fileName string, sequenceNumber int, length int); ";
        String query = "" +
                "from FooStream " +
                "select eof, fileName, sequenceNumber, length " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void siddhiIoFileTestBinaryChunkedModeException() throws InterruptedException {
        log.info("Regex shouldn't be given with Binary.Chunked Mode");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', mode='binary.chunked'," +
                "dir.uri='file:/" + dirUri + "/binary', " +
                "begin.regex='<event>'," +
                "end.regex='</event>'," +
                "@map(type='binary'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        siddhiAppRuntime.shutdown();
    }
}
