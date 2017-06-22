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

import java.util.concurrent.atomic.AtomicInteger;

public class FileSourceTestCase {
    private static final Logger log = Logger.getLogger(FileSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    @Test
    public void fileSourceMapperTest1() throws InterruptedException {
        log.info("test FileSourceMapper 1");
        String streams = "" +
                "@Plan:name('TestSiddhiApp')" +
                "@source(type='file',uri='/home/minudika/Projects/WSO2/siddhi-io-file/testDir/input.json'" +
                ")" +
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
                "@Plan:name('TestSiddhiApp')" +
                "@source(type='file',uri='/home/minudika/Projects/WSO2/LogWriterForTests/logs/tmp.txt'," +
                "tail.file='true', @map(type='json'))" +
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
        System.out.println("Stopped : 1");
        Thread.sleep(1000);
        siddhiAppRuntime.start();
        System.out.println("Restarted : 1");
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
        System.out.println("Stopped : 2");
    }
}
