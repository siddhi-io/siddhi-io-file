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
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file source.
 * */
public class FileSourceTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    @Test
    public void fileSourceMapperTest1() throws InterruptedException {
        log.info("test FileSourceMapper 1");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full', tailing='false'," +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/text_full', " +
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
    public void fileSourceMapperTest8() throws InterruptedException {
        log.info("test FileSourceMapper 8");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full', action.after.process='delete'," +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/text_full'," +
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
    public void fileSourceMapperTest9() throws InterruptedException {
        log.info("test FileSourceMapper 9");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='text.full', action.after.process='move', " +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/text_full' ," +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/text_full'," +
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
                "@source(type='file',mode='line', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line'," +
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
    public void fileSourceMapperTest10() throws InterruptedException {
        log.info("test FileSourceMapper 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='false', " +
                "action.after.process='delete'," +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line'," +
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
    public void fileSourceMapperTest11() throws InterruptedException {
        log.info("test FileSourceMapper 10");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='false', " +
                "action.after.process='move', " +
                "move.after.process = '/home/minudika/Projects/WSO2/siddhi-io-file/read/line' ," +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/line', " +
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
    public void fileSourceMapperTest5() throws InterruptedException {
        log.info("test FileSourceMapper 5");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='true', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot', " +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/line', " +
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
                File file = new File("/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot/logs.txt");
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

        Thread.sleep(10000);
    }

    @Test
    public void fileSourceMapperTest6() throws InterruptedException {
        log.info("test FileSourceMapper 6");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='regex', " +
                "tailing='false', " +
                "action.after.process='delete', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/regex', " +
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

        Thread.sleep(5000);

        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest12() throws InterruptedException {
        log.info("test FileSourceMapper 12");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='regex', " +
                "tailing='false', " +
                "action.after.process='move', " +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/regex', " +
                "begin.regex='<begin>', " +
                "end.regex='<end>', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/regex', " +
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

        Thread.sleep(10000000);


        //assert event count
        // Assert.assertEquals("Number of events", 4, count.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void fileSourceMapperTest13() throws InterruptedException {
        log.info("test FileSourceMapper 13");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='regex', " +
                "begin.regex='<begin>', " +
                "end.regex='<end>', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/regex_tailing', " +
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

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                siddhiAppRuntime.start();
            }
        });

        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                File file = new File("/home/minudika/Projects/WSO2/siddhi-io-file/testDir/regex_tailing/regex.txt");
                String str1 = "We’ve seen<begin> a lot from the eight tails, all of which has been very impressive." +
                        " It was noted by Kisame and the Nine <end>Tails to be the second most " +
                        "<begin>powerful of the nine" +
                        " tailed beasts, and it’s held its own against two other tailed beasts<end> despite suffering" +
                        " a past injury.";
                String str2 = "In Eight Tails<begin> form B. was able to nearly kill Sasuke <end>and friends multiple" +
                        " times. The Eight <begin>Tails is probably the smartest of all Tailed Beasts as well as" +
                        " it is shown being highly tactical in <end>battle.";
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                    bufferedWriter.write(str1);
                    bufferedWriter.newLine();
                    bufferedWriter.write(str2);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    bufferedWriter.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        });
        t2.start();

        Thread.sleep(10000);
    }

    @Test
    public void fileSourceMapperTest7() throws InterruptedException {
        log.info("test FileSourceMapper 7");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file',mode='line', tailing='true', " +
                "uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot', " +
                "move.after.process='/home/minudika/Projects/WSO2/siddhi-io-file/read/line', " +
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
                EventPrinter.print(events);

            }
        });

        siddhiAppRuntime.start();

        Thread.sleep(2000);

        byte[] snapshot = siddhiAppRuntime.snapshot();

        Thread.sleep(2000);

        siddhiAppRuntime.shutdown();

        File file = new File("/home/minudika/Projects/WSO2/siddhi-io-file/testDir/snapshot/logs.txt");
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

        Thread.sleep(5000);

        siddhiAppRuntime.restore(snapshot);
        siddhiAppRuntime.start();

        Thread.sleep(5000);

        siddhiAppRuntime.shutdown();
    }
}
