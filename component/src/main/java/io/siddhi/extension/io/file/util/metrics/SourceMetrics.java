package io.siddhi.extension.io.file.util.metrics;

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
    private Map<String, StreamStatus> sourceFileStatusMap = new HashMap<>();
    private Map<String, Long> lastConsumedTimeMap = new HashMap<>(); //to get the last consumed time

    private boolean isStarted;
    private String filePath;
    private String fileName;
    private String readingMode;
    private String streamName;
    private FileDeleteMetrics fileDeleteMetrics;
    private FileMoveMetrics fileMoveMetrics;

    public SourceMetrics(String siddhiAppName, String readingMode, String streamName) {
        super(siddhiAppName);
        this.readingMode = readingMode;
        this.streamName = streamName;
        this.fileDeleteMetrics = new FileDeleteMetrics(siddhiAppName);
        this.fileMoveMetrics = new FileMoveMetrics(siddhiAppName);
        /*sourceFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path",
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
                "currently reading file").labelNames("app_name", "file_path").register();*/
    }

    public Counter getSourceFileEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.event.count.%s.%s.%s.%s",
                        siddhiAppName, fileName, readingMode, streamName, filePath), Level.INFO);
    }

    public Counter getReadByteMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "total_read_byte", filePath), Level.INFO);
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

    public void getElapseTimeMetric(long elapseTime) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "elapse_time", filePath), Level.INFO, () -> elapseTime);
    }

    public Counter getDroppedEventCountMetric() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "dropped_events", filePath), Level.INFO);
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

    public void getReadPercentageMetric(Gauge gauge) {
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Source.%s.%s",
                        siddhiAppName, "read_percentage", filePath), Level.INFO, gauge);
    }

    public void setFilePath(String fileURI) {
        this.filePath = Utils.getShortFilePath(fileURI);;
        this.fileName = Utils.getFileName(fileURI, this).split("\\.")[0];
        this.fileMoveMetrics.set_source(filePath);
        this.fileDeleteMetrics.set_source(filePath);

    }

    public synchronized void updateMetrics(ExecutorService executorService) {
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
                            Thread.sleep(500);
                        }
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

    /**
     * Gauge implementation to get the status of the file.
     */
    private class FileStatusGauge implements Gauge<Integer> {

        private String fileURI;

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
