/*
 * Copyright (c)  2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import com.codahale.metrics.MetricRegistry;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.config.StatisticsConfiguration;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.extension.io.file.metrics.SinkMetrics;
import io.siddhi.extension.io.file.metrics.StreamStatus;
import io.siddhi.extension.io.file.util.TestUtils;
import io.siddhi.extension.util.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.TestException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.carbon.config.ConfigurationException;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.metrics.core.MetricManagementService;
import org.wso2.carbon.metrics.core.MetricNotFoundException;
import org.wso2.carbon.metrics.core.MetricService;
import org.wso2.carbon.metrics.core.Metrics;
import org.wso2.carbon.si.metrics.core.MetricsFactory;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for siddhi-io-file sink to test sink level metrics.
 */
public class FileSinkMetricsTestCase {

    private static final Logger log = LogManager.getLogger(FileSinkTestCase.class);
    private final AtomicInteger count = new AtomicInteger();

    private String dirUri;
    private File sourceRoot, fileToAppend, sinkRoot;
    private Metrics metrics;
    private MetricService metricService;
    private final String siddhiAppName = "TestSiddhiApp";

    @BeforeClass
    public void init() {
        ClassLoader classLoader = FileSourceLineModeTestCase.class.getClassLoader();
        String rootPath = Objects.requireNonNull(classLoader.getResource("files")).getFile();
        sourceRoot = new File(rootPath + "/repo/sink_repo");
        dirUri = rootPath + "/new";
        String sinkUri = rootPath + "/sink";
        fileToAppend = new File(dirUri);
        sinkRoot = new File(sinkUri);
    }

    @BeforeMethod
    public void doBeforeMethod() throws ConfigurationException {
        metrics = new Metrics(TestUtils.getConfigProvider("conf/metrics-prometheus.yaml"));
        metrics.activate();
        metricService = metrics.getMetricService();
        MetricManagementService metricManagementService = metrics.getMetricManagementService();
        metricManagementService.setRootLevel(Level.ALL);
        metricManagementService.stopReporters();
        MetricsDataHolder.getInstance().setMetricService(metricService);
        MetricsDataHolder.getInstance().setMetricManagementService(metricManagementService);
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
            log.info("Deactivating Metrics");
            metrics.deactivate();
        } catch (IOException e) {
            throw new TestException("Failed to delete files in due to " + e.getMessage(), e);
        }
    }


    @Test
    public void testEventCountMetric() throws InterruptedException, MetricNotFoundException {
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
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(5, count.intValue());
        String shortenFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String eventCounterName = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.event.count.%s.%s.%s.%s",
                siddhiAppName, "apache.json" + ".filename", "Json", "BarStream", shortenFilePath);
        Assert.assertEquals(metricService.counter(eventCounterName).getCount(), 4);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testEventCountMetric")
    public void testLineCountMetric() throws InterruptedException, MetricNotFoundException {
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
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(5, count.intValue());
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String lineCounterMetric = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "lines_count", shortenedFilePath);
        Assert.assertEquals(metricService.counter(lineCounterMetric).getCount(), 4);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testLineCountMetric")
    public void testWrittenByteMetric() throws InterruptedException, MetricNotFoundException {
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
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }

        AssertJUnit.assertEquals(5, count.intValue());
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String writtenByteMetric = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "total_written_byte", shortenedFilePath);
        Assert.assertEquals(metricService.counter(writtenByteMetric).getCount(), 221);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testWrittenByteMetric")
    public void testFileSizeMetric() throws InterruptedException, MetricNotFoundException {
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
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }
        AssertJUnit.assertEquals(5, count.intValue());
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String writtenByteMetric = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "file_size", shortenedFilePath);
        Assert.assertEquals(metricService.counter(writtenByteMetric).getCount(), 277);
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testFileSizeMetric")
    public void testElapsedTime() throws InterruptedException, NoSuchFieldException,
            IllegalAccessException {
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

        Field f1 = metricService.getClass().getDeclaredFields()[0]; //get MetricManager object from MetricsServiceIml
        f1.setAccessible(true);
        Field f2 = f1.getType().getDeclaredField("metricRegistry"); //get MetricRegistry Object from MetricManger
        f2.setAccessible(true);
        MetricRegistry metricRegistry = (MetricRegistry) f2.get(f1.get(metricService));
        SiddhiManager siddhiManager = new SiddhiManager();
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        long startedTime = System.currentTimeMillis();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }
        AssertJUnit.assertEquals(5, count.intValue());
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String elapsedTime = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "elapsed_time", shortenedFilePath);
        Assert.assertTrue((long) metricRegistry.getGauges().get(elapsedTime).getValue() <
                (System.currentTimeMillis() - startedTime));
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testElapsedTime")
    public void testLastPublishedTime() throws InterruptedException, NoSuchFieldException,
            IllegalAccessException {
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

        Field f1 = metricService.getClass().getDeclaredFields()[0]; //get MetricManager object from MetricsServiceIml
        f1.setAccessible(true);
        Field f2 = f1.getType().getDeclaredField("metricRegistry"); //get MetricRegistry Object from MetricManger
        f2.setAccessible(true);
        MetricRegistry metricRegistry = (MetricRegistry) f2.get(f1.get(metricService));
        SiddhiManager siddhiManager = new SiddhiManager();
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        long lastPublishedTime = System.currentTimeMillis();
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        Thread.sleep(100);
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }
        AssertJUnit.assertEquals(5, count.intValue());
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String lastPublishedTimeMetricName = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "last_published_time", shortenedFilePath);
        Assert.assertTrue((long) metricRegistry.getGauges().get(lastPublishedTimeMetricName).getValue() >
                (System.currentTimeMillis() - lastPublishedTime));
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "testLastPublishedTime")
    public void testFileStatus() throws InterruptedException, NoSuchFieldException,
            IllegalAccessException {
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

        Field f1 = metricService.getClass().getDeclaredFields()[0]; //get MetricManager object from MetricsServiceIml
        f1.setAccessible(true);
        Field f2 = f1.getType().getDeclaredField("metricRegistry"); //get MetricRegistry Object from MetricManger
        f2.setAccessible(true);
        MetricRegistry metricRegistry = (MetricRegistry) f2.get(f1.get(metricService));
        SiddhiManager siddhiManager = new SiddhiManager();
        StatisticsConfiguration statisticsConfiguration = new StatisticsConfiguration(new MetricsFactory());
        siddhiManager.setStatisticsConfiguration(statisticsConfiguration);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.getSinks().forEach(sinks -> {
            try {
                SinkMetrics sinkMetrics = new SinkMetrics(siddhiAppName, "Json", "BarStream");
                Field metrics = sinks.get(0).getClass().getDeclaredField("metrics");
                metrics.setAccessible(true);
                metrics.set(sinks.get(0), sinkMetrics);

            } catch (NoSuchFieldException | IllegalAccessException ignored) {

            }
        });
        siddhiAppRuntime.start();
        String shortenedFilePath = Utils.getShortFilePath(dirUri + "/apache.json");
        String fileStatusMetric = String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                siddhiAppName, "file_status", shortenedFilePath);
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 57.678f, 100L});
        stockStream.send(new Object[]{"GOOGLE", 50f, 100L});
        Assert.assertEquals(metricRegistry.getGauges().get(fileStatusMetric).getValue(),
                StreamStatus.PROCESSING.ordinal());
        stockStream.send(new Object[]{"REDHAT", 50f, 100L});
        File file = new File(dirUri + "/apache.json");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            while ((bufferedReader.readLine()) != null) {
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            AssertJUnit.fail(e.getMessage());
        } catch (IOException e) {
            AssertJUnit.fail("Error occurred during reading the file '" + file.getAbsolutePath());
        }
        AssertJUnit.assertEquals(5, count.intValue());
        Thread.sleep(10000); //wait more than 8 seconds to change the status into 'IDLE' mode
        Assert.assertEquals(metricRegistry.getGauges().get(fileStatusMetric).getValue(),
                StreamStatus.IDLE.ordinal());
        siddhiAppRuntime.shutdown();
    }
}
