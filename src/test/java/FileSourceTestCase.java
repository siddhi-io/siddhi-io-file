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
import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSourceTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    @Test
    public void fileSourceMapperTest1() throws InterruptedException {
        log.info("test FileSourceMapper 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full', tailing='false',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/text_full'," +
                "action.after.process='delete'," +
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
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        //assert event count
       // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest2() throws InterruptedException {
        log.info("test FileSourceMapper 2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line'," +
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
            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest3() throws InterruptedException {
        log.info("test FileSourceMapper 3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full',action.after.process='move',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/text_full'," +
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

            }
        });

        siddhiAppRuntime.start();
        System.out.println("started ");

        Thread.sleep(1000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
       siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest4() throws InterruptedException {
        log.info("test FileSourceMapper 4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line',tailing='false',action.after.process='move',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line'," +
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
            }
        });

        siddhiAppRuntime.start();
        System.out.println("started ");

        Thread.sleep(1000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest5() throws InterruptedException {
        log.info("test FileSourceMapper 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line',tailing='true',action.after.process='move',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line'," +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/line'," +
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

            }
        });

        siddhiAppRuntime.start();
        System.err.println("________________________________started ");

        Thread.sleep(1000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());

        siddhiAppRuntime.shutdown();
    }


    @Test
    public void fileSourceMapperTest6() throws InterruptedException {
        log.info("test FileSourceMapper 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='regex',begin.regex='<begin>',end.regex='<end>',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/regex'," +
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

            }
        });

        siddhiAppRuntime.start();
        System.out.println("started ");

        Thread.sleep(1000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest7() throws InterruptedException {
        log.info("test FileSourceMapper 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='true', uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot'," +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/line'," +
                "@map(type='json'))" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select *  " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);


        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                int n = count.incrementAndGet();
                System.err.println("####################### "+n);
                EventPrinter.print(events);

            }
        });

        siddhiAppRuntime.start();

        System.out.println("test");
        Thread.sleep(3000);


        System.err.println("############## shutting down");

        byte[] snapshot = siddhiAppRuntime.snapshot();
        siddhiAppRuntime.shutdown();

        Thread.sleep(5000);

        File file = new File("/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot/logs.txt");
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            bufferedWriter.write("{\"event\":{\"symbol\":\"IBM\",\"price\":2000,\"volume\":30000}}");
            bufferedWriter.newLine();
            bufferedWriter.write("{\"event\":{\"symbol\":\"GOOGLE\",\"price\":3000,\"volume\":40000}}");
            bufferedWriter.newLine();
            System.err.println("############## writing file");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread.sleep(5000);

        System.err.println("###################### starting..");
        siddhiAppRuntime.restore(snapshot);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                int n = count.incrementAndGet();
                System.err.println("******************* "+n);
                EventPrinter.print(events);

                if(n==7)
                    siddhiAppRuntime.shutdown();

            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(1000000);


        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        //siddhiAppRuntime.shutdown();
    }
}
