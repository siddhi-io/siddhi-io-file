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
import org.wso2.siddhi.core.util.snapshot.PersistenceReference;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.concurrent.atomic.AtomicInteger;

public class FileSinkTestCase {
    private static final Logger log = Logger.getLogger(FileSinkTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    @Test
    public void fileSinkTest1() throws InterruptedException {
        log.info("test FileSinkMapper 1");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='text'), append='false', uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/out_test1.txt') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};

        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        Thread.sleep(100);

        //assert event count
       // Assert.assertEquals(5, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        //InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void fileSinkTest2() throws InterruptedException {
        log.info("test FileSinkMapper 2");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='text'), append='true', uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/out_test1.txt') " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};

        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.678f, 100L});
        stockStream.send(new Object[]{"WSO2", 50f, 100L});
        stockStream.send(new Object[]{"WSO2#$%", 50f, 100L});
        Thread.sleep(100);

        //assert event count
        // Assert.assertEquals(5, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        //InMemoryBroker.unsubscribe(subscriberWSO2);
    }

    @Test
    public void fileSinkTest3() throws InterruptedException {
        log.info("test FileSinkMapper 3");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file', @map(type='json'), append='true', uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/{{symbol}}.json')" +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        Event wso2Event = new Event();
        Event ibmEvent = new Event();
        Object[] wso2Data = {"WSO2", 55.6f, 100L};
        Object[] ibmData = {"IBM", 32.6f, 160L};

        wso2Event.setData(wso2Data);
        ibmEvent.setData(ibmData);
        stockStream.send(new Object[]{"wso2", 55.6f, 100L});
        stockStream.send(new Object[]{"ibm", 57.678f, 100L});
        stockStream.send(new Object[]{"google", 50f, 100L});
        stockStream.send(new Object[]{"microsoft", 50f, 100L});
        stockStream.send(new Object[]{"intel", 75.6f, 100L});
        stockStream.send(new Object[]{"redhat", 57.678f, 100L});
        stockStream.send(new Object[]{"cloudbees", 54.4f, 100L});
        stockStream.send(new Object[]{"apache", 80f, 100L});

        PersistenceReference str = siddhiAppRuntime.persist();
        Thread.sleep(100);
        siddhiAppRuntime.restoreRevision(str.getRevision());

        //assert event count
        // Assert.assertEquals(5, wso2Count.get());
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        //InMemoryBroker.unsubscribe(subscriberWSO2);
    }
}
