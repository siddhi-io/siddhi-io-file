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

package io.siddhi.extension.io.file.util;

import com.google.common.base.Stopwatch;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Class which holds all the metrics which is expose to prometheus.
 */
public class Metrics {
    private static final Logger log = Logger.getLogger(Metrics.class); // TODO: 3/12/20 use singleton pattern
    private static volatile boolean isServerStarted;
    private static boolean isMetricsRegistered;
    private static HTTPServer server;
    private static Set<String> filesURI = new HashSet<>();
    private static Set<String> siddhiApps = new HashSet<>();
    private static Map<String, String> fileNames = new HashMap<>(); //fileURI, fileName
    private static final Map<SourceDetails, StreamStatus> sourceFileStatus =  new HashMap<>();
    private static final Map<String, StreamStatus> sinkStreamStatus =  new HashMap<>();
    private static final Map<String, List<SinkDetails>> sinkFilesMap = new HashMap<>();
    private static final Map<String, Long> sinkFilesIdleStatus = new HashMap<>();
    private static final Map<String, Stopwatch> sinkElapsedTimeMap = new HashMap<>();
    private static final Map<SourceDetails, Long> tailEnabledFiles = new HashMap<>();
    // TODO: 3/12/20 Append map to varialbes
    private static Counter sinkFilesEventCount;
    private static Gauge sinkFileSize;
    private static Counter sinkDroppedEvents;
    private static Gauge sinkStreamStatusMetrics;
    private static Counter sinkLinesCount;
    private static Gauge sinkElapsedTime;
    private static Gauge sinkLastPublishedTime;
    private static Counter writeBytes;
    private static Counter sourceFilesEventCount;
    private static Gauge sourceFileSize;
    private static Gauge sourceStartedTime;
    private static Gauge sourceCompletedTime;
    private static Counter sourceDroppedEvents;
    private static Gauge sourceStreamStatusMetrics;
    private static Counter sourceNoOfLines;
    private static Counter readBytes;
    private static Gauge sourceElapsedTime;
    private static Gauge sourceTailingEnable;
    private static Gauge sourceReadPercentage;
    private static Counter numberOfCopy;
    private static Counter numberOfDeletion;
    private static Counter numberOfMoves;
    private static Counter numberOfArchives;
    private static Counter totalSiddhiApps;

    private static void registerMetrics() {
        if (!isMetricsRegistered) {
            numberOfArchives = Counter.build().help("total number of archived files").name("created_archived_count")
                    .register();
            numberOfMoves = Counter.build().help("total number of moved files").name("moved_files_count").register();
            numberOfDeletion = Counter.build().help("total number of deleted files").name("deleted_file_count")
                    .register();
            numberOfCopy = Counter.build().help("total no of copied files").name("copied_file_count").register();
            sourceFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path",
                    "file_name", "mode", "stream_name").name("source_file_event_count").register();
            readBytes = Counter.build().help("no of bytes completed").labelNames("app_name", "file_path")
                    .name("source_file_total_read_byte").register();
            sourceNoOfLines = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                    .name("source_file_lines_count").register();
            sourceElapsedTime = Gauge.build().help("no of seconds taken to complete reading").labelNames("app_name",
                    "file_path").name("source_file_elapse_time").register();
            sourceStreamStatusMetrics = Gauge.build()
                    .help("Stream status").labelNames("app_name", "file_path").name("source_stream_status").register();
            sourceDroppedEvents = Counter.build().help("no of dropped events")
                    .labelNames("app_name", "file_path").name("source_file_dropped_events").register();
            sourceFileSize = Gauge.build().help("size of files which are used by source").labelNames("app_name",
                    "file_path").name("source_file_size").register();
            sourceStartedTime = Gauge.build().help("Source started time in millis").labelNames("app_name", "file_path")
                    .name("source_file_started_time").register();
            sourceCompletedTime = Gauge.build().help("Source completed time in millis").labelNames("app_name",
                    "file_path").name("source_file_completed_time").register();
            sourceTailingEnable = Gauge.build().help("To check if file is reading while tailing enable").labelNames
                    ("app_name", "file_path").name("source_file_tailing_enable").register();
            sourceReadPercentage = Gauge.build().name("source_file_read_percentage").help("Read percentage of the " +
                    "currently reading file").labelNames("app_name", "file_path").register();
            sinkFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path", "file_name",
                    "mode", "stream_name").name("sink_file_event_count").register();
            sinkLinesCount = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                    .name("sink_file_lines_count")
                    .register();
            writeBytes = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                    .name("sink_file_total_written_byte").register();
            sinkStreamStatusMetrics = Gauge.build().help("Stream status").labelNames("app_name", "file_path")
                    .name("sink_stream_status").register();
            sinkDroppedEvents = Counter.build().help("no of dropped events").labelNames("app_name", "file_path")
                    .name("sink_file_dropped_events").register();
            sinkLastPublishedTime = Gauge.build().help("Time of last events written at.").labelNames("app_name",
                    "file_path").name("sink_file_last_published_time").register();
            sinkFileSize = Gauge.build().help("size of files which are used by sink").labelNames("app_name",
                    "file_path").name("sink_file_size").register();
            sinkElapsedTime = Gauge.build().help("elapse time of the sink").name(
                    "sink_file_elapsed_time").labelNames("file_path").register();
            totalSiddhiApps = Counter.build().name("siddhi_app_count").help("Number of siddhi apps which has source " +
                    "or sink with siddhi-io-file extension.").labelNames("app_name").register();
            isMetricsRegistered = true;
        }
    }


    public static synchronized void createServer(ExecutorService executorService) {
        if (!isServerStarted) {
            try {
                server = new HTTPServer(8898);
                isServerStarted = true;
                registerMetrics();
                executorService.execute(() -> {
                    while (isServerStarted) {
                        try {
                            if (!sinkStreamStatus.isEmpty()) {
                                setSinkStreamStatus();
                            }
                            sinkElapsedTimeMap.forEach((uri, stopwatch) -> sinkElapsedTime.labels(uri).set(
                                    stopwatch.elapsed().toMillis())); /*FileSink -->>*/
                            setSourceFileStatus();
                            Thread.sleep(1000);
                        } catch (InterruptedException ignored) {

                        }
                    }
                });
            } catch (IOException e) {
                log.error("Error while starting the prometheus server.", e);
            }
        } else {
            log.debug("Prometheus server has already been started.");
        }
    }

    public static synchronized void stopServer() {
        if (siddhiApps.isEmpty()) {
            if (server != null && isServerStarted) {
                CollectorRegistry.defaultRegistry.clear();
                server.stop();
                isServerStarted = false;
                isMetricsRegistered = false;
                filesURI.clear();
                siddhiApps.clear();
                sourceFileStatus.clear();
                sinkStreamStatus.clear();
                sinkFilesMap.clear();
                sinkFilesIdleStatus.clear();
                sinkElapsedTimeMap.clear();
                tailEnabledFiles.clear();
            }
        }
    }

    private static void setSinkStreamStatus() {
        sinkFilesIdleStatus.forEach((sinkID, lastModifiedTime) -> {
            if (!sinkFilesMap.isEmpty()) {
                long idleTime = System.currentTimeMillis() - lastModifiedTime;
                if (idleTime / 1000 >= 5) {
                    sinkStreamStatus.replace(sinkID, StreamStatus.IDLE);
                }
                int streamStatus = sinkStreamStatus.get(sinkID).ordinal();
                List<SinkDetails> sinkFiles = sinkFilesMap.get(sinkID);
                if (sinkFiles != null) {
                    sinkFiles.forEach(sinkDetails-> sinkStreamStatusMetrics.labels(sinkDetails.siddhiAppName,
                            sinkDetails.filePath).set(streamStatus));
                }

            }
        });
    }

    private static void setSourceFileStatus() {
        tailEnabledFiles.forEach((sourceDetails, lastModifiedTime) -> {
            long idleTime = System.currentTimeMillis() - lastModifiedTime;
            if (idleTime / 1000 >= 5) {
                sourceFileStatus.replace(sourceDetails, StreamStatus.IDLE);
            }
        });
        sourceFileStatus.forEach((sourceDetails, streamStatus) -> sourceStreamStatusMetrics.labels(
                sourceDetails.siddhiAppName, sourceDetails.filePath).set(streamStatus.ordinal()));

    }


    public static Set<String> getFilesURI() {
        return filesURI;
    }

    public static Map<SourceDetails, StreamStatus> getSourceFileStatus() {
        return sourceFileStatus;
    }

    public static Counter getSinkFilesEventCount() {
        return sinkFilesEventCount;
    }

    public static Gauge getSinkFileSize() {
        return sinkFileSize;
    }

    public static Counter getSinkDroppedEvents() {
        return sinkDroppedEvents;
    }

    public static Counter getSinkLinesCount() {
        return sinkLinesCount;
    }

    public static Counter getSourceFilesEventCount() {
        return sourceFilesEventCount;
    }

    public static Gauge getSourceFileSize() {
        return sourceFileSize;
    }

    public static Counter getSourceDroppedEvents() {
        return sourceDroppedEvents;
    }

    public static Counter getSourceNoOfLines() {
        return sourceNoOfLines;
    }

    public static Counter getReadBytes() {
        return readBytes;
    }

    public static Gauge getSourceElapsedTime() {
        return sourceElapsedTime;
    }

    public static Counter getNumberOfCopy() {
        return numberOfCopy;
    }

    public static Counter getNumberOfDeletion() {
        return numberOfDeletion;
    }

    public static Counter getNumberOfMoves() {
        return numberOfMoves;
    }

    public static Counter getNumberOfArchives() {
        return numberOfArchives;
    }

    public static Counter getWriteBytes() {
        return writeBytes;
    }

    public static Map<String, Long> getSinkFilesIdleStatus() {
        return sinkFilesIdleStatus;
    }

    public static Map<String, StreamStatus> getSinkStreamStatus() {
        return sinkStreamStatus;
    }

    public static Map<String, List<SinkDetails>> getSinkFilesMap() {
        return sinkFilesMap;
    }

    public static Map<String, Stopwatch> getSinkElapsedTimeMap() {
        return sinkElapsedTimeMap;
    }

    public static Map<SourceDetails, Long> getTailEnabledFiles() {
        return tailEnabledFiles;
    }

    public static Gauge getSourceStartedTime() {
        return sourceStartedTime;
    }

    public static Gauge getSourceCompletedTime() {
        return sourceCompletedTime;
    }

    public static Set<String> getSiddhiApps() {
        return siddhiApps;
    }

    public static Counter getTotalSiddhiApps() {
        return totalSiddhiApps;
    }

    public static Gauge getSinkLastPublishedTime() {
        return sinkLastPublishedTime;
    }

    public static Gauge getSourceTailingEnable() {
        return sourceTailingEnable;
    }

    public static Gauge getSinkStreamStatusMetrics() {
        return sinkStreamStatusMetrics;
    }

    public static Gauge getSinkElapsedTime() {
        return sinkElapsedTime;
    }

    public static Gauge getSourceStreamStatusMetrics() {
        return sourceStreamStatusMetrics;
    }

    public static Map<String, String> getFileNames() {
        return fileNames;
    }

    public static Gauge getSourceReadPercentage() {
        return sourceReadPercentage;
    }

    /**
     * Enum that defines stream status.
     */
    public enum StreamStatus {
        CONNECTING,
        PROCESSING,
        COMPLETED,
        IDLE,
        RETRY,
        ERROR,
    }

    /**
     * Class which is used to holds the details of a file used by sink.
     */
    public static class SinkDetails {
        private String siddhiAppName;
        private String filePath;

        public SinkDetails(String siddhiAppName, String filePath) {
            this.siddhiAppName = siddhiAppName;
            this.filePath = filePath;
        }
    }

    /**
     * Class which is used to save the details of a reading file.
     */
    public static class SourceDetails {
        private String siddhiAppName;
        private String filePath;

        public SourceDetails(String siddhiAppName, String filePath) {
            this.siddhiAppName = siddhiAppName;
            this.filePath = filePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SourceDetails that = (SourceDetails) o;
            return filePath.equals(that.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(siddhiAppName, filePath);
        }
    }




}
