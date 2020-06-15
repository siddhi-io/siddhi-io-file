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

import io.siddhi.extension.util.Utils;
import org.apache.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Gauge;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Class which is holds the metrics to monitor sink operations.
 */
public class SourceMetrics extends Metrics {
    private static final Logger log = Logger.getLogger(SourceMetrics.class);
    private final Map<String, StreamStatus> sourceFileStatusMap = new HashMap<>();
    private final Map<String, Long> lastConsumedTimeMap = new HashMap<>(); //to get the last consumed time

    private boolean isStarted;
    private String filePath;
    private String fileName;
    private final String readingMode;
    private final String streamName;
    private final FileDeleteMetrics fileDeleteMetrics;
    private final FileMoveMetrics fileMoveMetrics;
    private double readPercentage;

    public SourceMetrics(String siddhiAppName, String readingMode, String streamName) {
        super(siddhiAppName);
        this.readingMode = readingMode;
        this.streamName = streamName;
        this.fileDeleteMetrics = new FileDeleteMetrics(siddhiAppName);
        this.fileMoveMetrics = new FileMoveMetrics(siddhiAppName);
    }

    public Counter getTotalReadsMetrics() { //to count the total reads from siddhi app level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.Total.Reads.%s", siddhiAppName, "file"),
                        Level.INFO);
    }

    public Counter getTotalFileReadCount() { //to count the total reads from source level.
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.Total.Reads.%s.%s.%s.%s",
                        siddhiAppName, fileName + ".filename", readingMode, streamName, filePath), Level.INFO);
    }

    public Counter getReadByteMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "total_read_byte", filePath), Level.INFO);
    }

    public Counter getValidEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService().counter(
                String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "total_valid_events_count", filePath), Level.INFO);
    }

    public Counter getReadLineCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "lines_count", filePath), Level.INFO);
    }

    public void getElapseTimeMetric(Gauge gauge) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "elapse_time", filePath), Level.INFO, gauge);
    }

    public Counter getDroppedEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "dropped_events", filePath), Level.INFO);
    }

    public Counter getTotalErrorCount() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "error_count", filePath), Level.INFO);
    }


    public void getFileSizeMetric(Gauge gauge) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "file_size", filePath), Level.INFO, gauge);
    }

    public void getFileStatusMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "file_status", filePath), Level.INFO, new FileStatusGauge(filePath));
    }

    public void getStartedTimeMetric(long startTime) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "started_time", filePath), Level.INFO, () -> startTime);
    }

    public void getCompletedTimeMetric(long completedTime) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "completed_time", filePath), Level.INFO, () -> completedTime);
    }

    public void getTailEnabledMetric(int enable) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "tailing_enable", filePath), Level.INFO, () -> enable);
    }

    public void getReadPercentageMetric() {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "read_percentage", filePath), Level.INFO, () -> readPercentage);
    }

    public void setFilePath(String fileURI) {
        this.filePath = Utils.getShortFilePath(fileURI);
        this.fileName = Utils.getFileName(fileURI, this);
        this.fileMoveMetrics.set_source(filePath);
        this.fileDeleteMetrics.setSource(filePath);

    }

    public void updateMetrics(ExecutorService executorService) {
        if (!isStarted) {
            executorService.execute(() -> {
                isStarted = true;
                while (isStarted) {
                    try {
                        if (!lastConsumedTimeMap.isEmpty()) {
                            lastConsumedTimeMap.forEach((filePath, lastModifiedTime) -> {
                                long idleTime = System.currentTimeMillis() - lastModifiedTime;
                                if (idleTime / 1000 >= 8) {
                                    sourceFileStatusMap.replace(filePath, StreamStatus.IDLE);
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

    public FileDeleteMetrics getFileDeleteMetrics() {
        return fileDeleteMetrics;
    }

    public FileMoveMetrics getFileMoveMetrics() {
        return fileMoveMetrics;
    }

    public Map<String, StreamStatus> getSourceFileStatusMap() {
         return sourceFileStatusMap;
     }

     public Map<String, Long> getTailEnabledFilesMap() {
         return lastConsumedTimeMap;
     }

    public void setReadPercentage(double readPercentage) {
        this.readPercentage = readPercentage;
    }

    /**
     * Gauge implementation to get the status of the file.
     */
    private class FileStatusGauge implements Gauge<Integer> {

        private final String fileURI;

        private FileStatusGauge(String fileURI) {
            this.fileURI = fileURI;
        }

         @Override
         public Integer getValue() {
             if (sourceFileStatusMap.containsKey(fileURI)) {
                 return sourceFileStatusMap.get(fileURI).ordinal();
             }
             return 0;
         }
     }
}
