package io.siddhi.extension.io.file.util;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Class which is holds the metrics to monitor sink operations.
 */
public class SourceMetrics extends Metrics {
    private static final SourceMetrics INSTANCE = new SourceMetrics();
    private Map<SourceDetails, StreamStatus> sourceFileStatusMap;
    private Map<SourceDetails, Long> tailEnabledFilesMap; //to get the last consumed time
    private boolean isStatusUpdated;

    private Counter sourceFilesEventCount;
    private Gauge sourceFileSize;
    private Gauge sourceStartedTime;
    private Gauge sourceCompletedTime;
    private Counter sourceDroppedEvents;
    private Gauge sourceFileStatusMetrics;
    private Counter sourceNoOfLines;
    private Counter readBytes;
    private Gauge sourceElapsedTime;
    private Gauge sourceTailingEnable;
    private Gauge sourceReadPercentage;


    private SourceMetrics() {
        super();
        sourceFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path",
                "file_name", "mode", "stream_name").name("source_file_event_count").register();
        readBytes = Counter.build().help("no of bytes completed").labelNames("app_name", "file_path")
                .name("source_file_total_read_byte").register();
        sourceNoOfLines = Counter.build().help("no of lines in the file").labelNames("app_name", "file_path")
                .name("source_file_lines_count").register();
        sourceElapsedTime = Gauge.build().help("no of seconds taken to complete reading").labelNames("app_name",
                "file_path").name("source_file_elapse_time").register();
        sourceFileStatusMetrics = Gauge.build()
                .help("Stream status").labelNames("app_name", "file_path").name("source_file_status").register();
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
        sourceFileStatusMap = new HashMap<>();
        tailEnabledFilesMap = new HashMap<>();
    }

    private void setSourceFileStatus() {
         tailEnabledFilesMap.forEach((sourceDetails, lastModifiedTime) -> {
             long idleTime = System.currentTimeMillis() - lastModifiedTime;
             if (idleTime / 1000 >= 8) {
                 sourceFileStatusMap.replace(sourceDetails, StreamStatus.IDLE);
             }
         });
         sourceFileStatusMap.forEach((sourceDetails, streamStatus) -> sourceFileStatusMetrics.labels(
                 sourceDetails.siddhiAppName, sourceDetails.filePath).set(streamStatus.ordinal()));
     }

    public synchronized void updateMetrics(ExecutorService executorService) {
        if (!isStatusUpdated) {
            executorService.execute(() -> {
                isStatusUpdated = true;
                while (isServerStarted) {
                    try {
                        setSourceFileStatus();
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
            sourceFileStatusMap.clear();
            tailEnabledFilesMap.clear();
        }
    }

    public static SourceMetrics getInstance() {
        return INSTANCE;
    }

     public Map<SourceDetails, StreamStatus> getSourceFileStatusMap() {
         return sourceFileStatusMap;
     }

     public Map<SourceDetails, Long> getTailEnabledFilesMap() {
         return tailEnabledFilesMap;
     }

     public Counter getSourceFilesEventCount() {
         return sourceFilesEventCount;
     }

     public Gauge getSourceFileSize() {
         return sourceFileSize;
     }

     public Gauge getSourceStartedTime() {
         return sourceStartedTime;
     }

     public Gauge getSourceCompletedTime() {
         return sourceCompletedTime;
     }

     public Counter getSourceDroppedEvents() {
         return sourceDroppedEvents;
     }

     public Gauge getSourceFileStatusMetrics() {
         return sourceFileStatusMetrics;
     }

     public Counter getSourceNoOfLines() {
         return sourceNoOfLines;
     }

     public Counter getReadBytes() {
         return readBytes;
     }

     public Gauge getSourceElapsedTime() {
         return sourceElapsedTime;
     }

     public Gauge getSourceTailingEnable() {
         return sourceTailingEnable;
     }

     public Gauge getSourceReadPercentage() {
         return sourceReadPercentage;
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
