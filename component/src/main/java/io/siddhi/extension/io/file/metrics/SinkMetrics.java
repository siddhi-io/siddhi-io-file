/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.file.metrics;

import com.google.common.base.Stopwatch;
import io.siddhi.extension.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Gauge;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Class which is holds the metrics to monitor sinks operations. This acts like a metrics handler for file sink.
 */
public class SinkMetrics extends Metrics {
    private static final Logger log = LogManager.getLogger(SinkMetrics.class);
    private final Map<String, StreamStatus> sinkFileStatusMap = new HashMap<>(); // string -> fileURI
    private final Map<String, Long> sinkFileLastPublishedTimeMap = new HashMap<>();
    private final Map<String, Stopwatch> sinkElapsedTimeMap =  new HashMap<>();
    private boolean isStarted;
    private String filePath;
    private final String mapType;
    private final String streamName;
    private String fileName;

    public SinkMetrics(String siddhiAppName, String mapType, String streamName) {
        super(siddhiAppName);
        this.mapType = mapType;
        this.streamName = streamName;
    }

    public Counter getTotalWriteMetrics() { //to count the total writes from siddhi app level.
        /* This is a common for all the extensions which performs writes.
        Siddhi app name is dynamic and extension name should be given manually. */
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Writes.%s", siddhiAppName, "file"),
                        Level.INFO);
    }

    public Counter getSinkFilesEventCount() { //to get hte total writes in sink level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.event.count.%s.%s.%s.%s",
                        siddhiAppName, fileName + ".filename", mapType, streamName, filePath), Level.INFO);
    }

    public Counter getSinkLinesCount() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "lines_count", filePath), Level.INFO);
    }

    public Counter getWriteBytes() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "total_written_byte", filePath), Level.INFO);
    }

    public Counter getErrorCount() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "total_error_count", filePath), Level.INFO);
    }

    public Counter getSinkDroppedEvents() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "dropped_events", filePath), Level.INFO);
    }

    public Counter getSinkFileSize() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "file_size", filePath), Level.INFO);
    }

    public void setSinkLastPublishedTime() {
        /* We register the reference to the gauge here, and this should be done only once.
         In order to update the gauge we have to update the registered reference. */
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "last_published_time", filePath), Level.INFO,
                        // We should always pass a reference of a local variable, not as a parameter
                        () -> sinkFileLastPublishedTimeMap.getOrDefault(filePath, 0L));
    }

    public void setSinkElapsedTime(String fileURI) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "elapsed_time", filePath),
                        Level.INFO, () -> {
                            if (sinkElapsedTimeMap.containsKey(fileURI)) {
                                return sinkElapsedTimeMap.get(fileURI).elapsed().toMillis();
                            } else {
                                return 0;
                            }
                        });
    }

    public void setSinkFileStatusMetrics() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "file_status", filePath), Level.INFO, new FileStatusGauge(filePath));
    }

    public void updateMetrics(ExecutorService executorService) {
        if (!isStarted) {
            executorService.execute(() -> {
                isStarted = true;
                while (isStarted) {
                    try {
                        if (!sinkFileStatusMap.isEmpty()) {
                            sinkFileLastPublishedTimeMap.forEach((filePath, lastPublishedTime) -> {
                                long idleTime = System.currentTimeMillis() - lastPublishedTime;
                                if (idleTime / 1000 >= 8) {
                                    sinkFileStatusMap.replace(filePath, StreamStatus.IDLE);
                                }
                            });
                        }
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error("Error while updating the status of files.", e);
                    }
                }
            });

        }
    }

    public void setFilePath(String fileURI) {
        this.filePath = Utils.getShortFilePath(fileURI);;
        this.fileName = Utils.getFileName(fileURI, this);

    }

    public Map<String, StreamStatus> getSinkFileStatusMap() {
        return sinkFileStatusMap;
    }

    public Map<String, Long> getSinkFileLastPublishedTimeMap() {
        return sinkFileLastPublishedTimeMap;
    }

    public Map<String, Stopwatch> getSinkElapsedTimeMap() {
        return sinkElapsedTimeMap;
    }

    /**
     * Class which is used to get the status of the file.
     */
    public class FileStatusGauge implements Gauge<Integer> {
        private final String filePath;

        public FileStatusGauge(String filePath) {
            this.filePath = Utils.getShortFilePath(filePath);
        }


        @Override
        public Integer getValue() {
            if (sinkFileStatusMap.containsKey(filePath)) {
                return sinkFileStatusMap.get(filePath).ordinal();
            } else {
                return 0;
            }
        }
    }
}
