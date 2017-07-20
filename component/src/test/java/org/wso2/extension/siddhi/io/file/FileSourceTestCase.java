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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 * */
public class FileSourceTestCase {
    // TODO: 20/7/17 Improve Thread.sleep() to use SiddhiTestHelper.waitForEvents().
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    File dir;
    
    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceTestCase.class.getClassLoader();
        dir = new File(classLoader.getResource("files").getFile());
    }

    @BeforeMethod
    public void doBeforeMethod() {
        count.set(0);
    }

    @Test
    public void siddhiIoFileTest1() throws InterruptedException {
        log.info("test SiddhiIoFile 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full'," +
                "dir.uri='" + dir + "/text_full', " +
                "action.after.process='none'," +
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
                        case 2:
                            AssertJUnit.assertEquals("cloudbees", event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals("google", event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ibm", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("intel", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("microsoft", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("redhat", event.getData(0));
                            break;
                        case 8:
                            AssertJUnit.assertEquals("wso2", event.getData(0));
                            break;
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        //assert event count
        AssertJUnit.assertEquals("Number of events", 8, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void siddhiIoFileTest2() throws InterruptedException {
        log.info("test SiddhiIoFile 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', action.after.process='none'," +
                "file.uri='" + dir + "/line/logs.txt'," +
                "file.polling.interval='1000', " +
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
                            AssertJUnit.assertEquals(0f, event.getData(1));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(1f, event.getData(1));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(2f, event.getData(1));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(3f, event.getData(1));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(4f, event.getData(1));
                            break;
                        case 10:
                            AssertJUnit.assertEquals(9f, event.getData(1));
                            break;
                        case 12:
                            AssertJUnit.assertEquals(11f, event.getData(1));
                            break;
                        case 15:
                            AssertJUnit.assertEquals(14f, event.getData(1));
                            break;
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(3000);

        //assert event count
         AssertJUnit.assertEquals("Number of events", 15, count.get());
        siddhiAppRuntime.shutdown();
    }

    //from here, tests will be implemented later

    /*@Test
    public void siddhiIoFileTest5() throws InterruptedException {
        log.info("test SiddhiIoFile 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='true', " +
                "file.uri='" + dir + "/tailing/logs.txt'," +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select *  " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        final SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);


        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                int n = count.incrementAndGet();
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (n) {
                        case 1:
                        case 2:
                        case 3:
                        case 4:
                        case 5:
                            AssertJUnit.assertEquals("WSO2", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals("GOOGLE", event.getData(0));
                            break;
                    }
                }

            }
        });

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                siddhiAppRuntime.start();
            }
        });

        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File(dir + "/tailing/logs.txt");
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write("{\"event\":{\"symbol\":\"IBM\",\"price\":2000,\"volume\":30000}}");
                    bufferedWriter.newLine();
                    bufferedWriter.write("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":3000,\"volume\":40000}}");
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();

        Thread.sleep(3000);
    }*/
}
