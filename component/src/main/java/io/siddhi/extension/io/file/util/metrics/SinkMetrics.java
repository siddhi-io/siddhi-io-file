package io.siddhi.extension.io.file.util.metrics;

import com.google.common.base.Stopwatch;
import io.siddhi.extension.util.Utils;
import org.apache.log4j.Logger;
import org.wso2.carbon.metrics.core.Counter;
import org.wso2.carbon.metrics.core.Gauge;
import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;
import org.wso2.carbon.si.metrics.core.internal.MetricsManagement;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Class which is holds the metrics to monitor source operations.
 */
public class SinkMetrics extends Metrics {
    private static final Logger log = Logger.getLogger(SinkMetrics.class);
    private Map<String, StreamStatus> sinkFileStatusMap = new HashMap<>(); // string -> fileURI
    private Map<String, Long> sinkFileLastPublishedTimeMap = new HashMap<>();
    private Map<String, Stopwatch> sinkElapsedTimeMap =  new HashMap<>();
    private boolean isStarted;
    private String filePath;
    private String mapType;
    private String streamName;
    private String fileName;

    public SinkMetrics(String siddhiAppName, String mapType, String streamName) {
        super(siddhiAppName);
        this.mapType = mapType;
        this.streamName = streamName;
        /*sinkFilesEventCount = Counter.build().help("file summary").labelNames("app_name", "file_path", "file_name",
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
                "sink_file_elapsed_time").labelNames("file_path").register();*/
    }

    public Counter getSinkFilesEventCount() {
        return MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.event.count.%s.%s.%s.%s",
                        siddhiAppName, fileName, mapType, streamName, filePath), Level.INFO);
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
        MetricsDataHolder.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "last_published_time", filePath),
                        Level.INFO, () -> sinkFileLastPublishedTimeMap.getOrDefault(filePath, 0L));
    }

    public void setSinkElapsedTime(String fileURI) {
        MetricsManagement.getInstance().getMetricService()
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

    public void setSinkFileStatusMetrics(String fileURI) {
        MetricsManagement.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Sinks.%s.%s",
                        siddhiAppName, "file_status", filePath), Level.INFO, () -> {
                            if (sinkFileStatusMap.containsKey(fileURI)) {
                                return sinkFileStatusMap.get(fileURI).ordinal();
                            } else {
                                return 0;
                            }
                        });
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
        this.fileName = Utils.getFileName(fileURI, this).split("\\.")[0];

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
        private String filePath;

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
