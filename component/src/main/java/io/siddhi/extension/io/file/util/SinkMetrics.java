package io.siddhi.extension.io.file.util;

import com.google.common.base.Stopwatch;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Class which is holds the metrics to monitor source operations.
 */
public class SinkMetrics extends Metrics {
    private static final SinkMetrics INSTANCE = new SinkMetrics();

    private Map<SinkDetails, StreamStatus> sinkFileStatusMap;
    private Map<SinkDetails, Long> sinkFileLastPublishedTimeMap;
    private Map<String, Stopwatch> sinkElapsedTimeMap;
    private boolean isStatusUpdated;

    private Counter sinkFilesEventCount;
    private Gauge sinkFileSize;
    private Counter sinkDroppedEvents;
    private Gauge sinkFileStatusMetrics;
    private Counter sinkLinesCount;
    private Gauge sinkElapsedTime;
    private Gauge sinkLastPublishedTime;
    private Counter writeBytes;

    private SinkMetrics() {
        sinkFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path", "file_name",
                "mode", "stream_name").name("sink_file_event_count").register();
        sinkLinesCount = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                .name("sink_file_lines_count")
                .register();
        writeBytes = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                .name("sink_file_total_written_byte").register();
        sinkFileStatusMetrics = Gauge.build().help("Stream status").labelNames("app_name", "file_path")
                .name("sink_file_status").register();
        sinkDroppedEvents = Counter.build().help("no of dropped events").labelNames("app_name", "file_path")
                .name("sink_file_dropped_events").register();
        sinkLastPublishedTime = Gauge.build().help("Time of last events written at.").labelNames("app_name",
                "file_path").name("sink_file_last_published_time").register();
        sinkFileSize = Gauge.build().help("size of files which are used by sink").labelNames("app_name",
                "file_path").name("sink_file_size").register();
        sinkElapsedTime = Gauge.build().help("elapse time of the sink").name(
                "sink_file_elapsed_time").labelNames("file_path").register();
        sinkFileStatusMap = new HashMap<>();
        sinkFileLastPublishedTimeMap = new HashMap<>();
        sinkElapsedTimeMap = new HashMap<>();
    }

    private void setSinkStreamStatus() {
        sinkFileLastPublishedTimeMap.forEach((sinkDetails, lastPublishedTime) -> {
            long idleTime = System.currentTimeMillis() - lastPublishedTime;
            if (idleTime / 1000 == 8) {
                sinkFileStatusMap.replace(sinkDetails, StreamStatus.IDLE);
            }
        });
        sinkFileStatusMap.forEach((sinkDetails, streamStatus) -> sinkFileStatusMetrics.labels(
                sinkDetails.siddhiAppName, sinkDetails.filePath).set(streamStatus.ordinal()));
    }

    public synchronized void updateMetrics(ExecutorService executorService) {
        if (!isStatusUpdated) {
            executorService.execute(() -> {
                isStatusUpdated = true;
                while (isServerStarted) {
                    try {
                        if (!sinkFileStatusMap.isEmpty()) {
                            setSinkStreamStatus();
                        }
                        sinkElapsedTimeMap.forEach((uri, stopwatch) -> sinkElapsedTime.labels(uri).set(
                                stopwatch.elapsed().toMillis()));
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {

                    }
                }
            });

        }
    }

    @Override
    public synchronized void stopServer() {
        super.stopServer();
        if (siddhiApps.isEmpty() && !isServerStarted) {
            sinkFileStatusMap.clear();
            sinkElapsedTimeMap.clear();
//            sinkFileStreamMap.clear();
            sinkFileLastPublishedTimeMap.clear();
        }
    }

    public static SinkMetrics getInstance() {
        return INSTANCE;
    }

    public Map<SinkDetails, StreamStatus> getSinkFileStatusMap() {
        return sinkFileStatusMap;
    }



    public Map<SinkDetails, Long> getSinkFileLastPublishedTimeMap() {
        return sinkFileLastPublishedTimeMap;
    }

    public Map<String, Stopwatch> getSinkElapsedTimeMap() {
        return sinkElapsedTimeMap;
    }

    public Counter getSinkFilesEventCount() {
        return sinkFilesEventCount;
    }

    public Gauge getSinkFileSize() {
        return sinkFileSize;
    }

    public Counter getSinkDroppedEvents() {
        return sinkDroppedEvents;
    }

    public Gauge getSinkFileStatusMetrics() {
        return sinkFileStatusMetrics;
    }

    public Counter getSinkLinesCount() {
        return sinkLinesCount;
    }

    public Gauge getSinkElapsedTime() {
        return sinkElapsedTime;
    }

    public Gauge getSinkLastPublishedTime() {
        return sinkLastPublishedTime;
    }

    public Counter getWriteBytes() {
        return writeBytes;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SinkDetails that = (SinkDetails) o;
            return Objects.equals(siddhiAppName, that.siddhiAppName) &&
                    Objects.equals(filePath, that.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(siddhiAppName, filePath);
        }
    }
}
