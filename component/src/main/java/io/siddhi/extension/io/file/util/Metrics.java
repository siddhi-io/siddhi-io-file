package io.siddhi.extension.io.file.util;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Class which holds all the metrics which is expose to prometheus.
 */
public class Metrics {
    private static final Logger log = Logger.getLogger(Metrics.class);
    private static volatile boolean isServerStarted;
    private static HTTPServer server;
    private static HashSet<String> filesURI = new HashSet<>();
    private static final Map<String, StreamStatus> streamStatus =  new HashMap<>();

    private static Counter totalPublishedEvents;
    private static Counter totalReceivedEvents;
    private static Gauge sourceFileCount;
    private static Gauge sinkFileCount;
    private static Counter sinkFiles;
    private static Gauge sinkFileSize;
    private static Counter sinkDroppedEvents;
    private static Gauge sinkStreamStatusMetrics;
    private static Counter sinkNoOfLines;
    private static Counter sourceFiles;
    private static Gauge sourceFileSize;
    private static Counter sourceDroppedEvents;
    private static Gauge sourceStreamStatusMetrics;
    private static Counter sourceNoOfLines;
    private static Counter readByte;
    private static Gauge sourceElapsedTime;
    private static Counter totalCopy;
    private static Counter totalDeletion;
    private static Counter totalMove;
    private static Counter totalNumberOfArchive;

    private static void registerMetrics() {
        totalNumberOfArchive = Counter.build().help("total number of archived files").name("number_of_archived")
                .register();
        totalMove  = Counter.build().help("total number of moved files").name("number_of_moves").register();
        totalDeletion = Counter.build().help("total number of deleted files").name("number_of_delete").register();
        totalCopy  = Counter.build().help("total no of copied files").name("number_of_copy").register();
        sourceElapsedTime = Gauge.build().help("no of seconds taken to complete reading").labelNames("app_name",
                "file_path", "file_name")
                .name("source_elapse_time").register();
        readByte = Counter.build().help("no of bytes completed").labelNames("app_name","file_path", "file_name")
                .name("source_total_read_byte")
                .register();
        sourceNoOfLines = Counter.build().help("no of lines in the file").labelNames("app_name","file_path", "file_name")
                .name("source_no_of_lines").register();
        sourceStreamStatusMetrics = Gauge.build()
                .help("Stream status").labelNames("app_name","file_path", "file_name", "stream_name").name("source_stream_status").register();
        sourceDroppedEvents = Counter.build()
                .help("no of dropped events")
                .labelNames("app_name","file_path", "file_name")
                .name("source_dropped_events").register();
        sourceFileSize = Gauge.build().help("size of files which are used by source").labelNames("app_name","file_path",
                "file_name")
                .name("source_file_size").register();
        sourceFiles = Counter.build().help("file summary").labelNames("app_name","file_path", "file_name", "mode", "stream_name", "stream_type")
                .name("source_file").register();
        sinkNoOfLines = Counter.build().help("no of lines in the file").labelNames("app_name","file_path", "file_name")
                .name("sink_no_of_lines")
                .register();
        sinkStreamStatusMetrics = Gauge.build().help("Stream status").labelNames("app_name","file_path", "file_name", "stream_name")
                .name("sink_stream_status").register();
        sinkDroppedEvents = Counter.build().help("no of dropped events").labelNames("app_name","file_path", "file_name")
                .name("sink_dropped_events").register();
        totalPublishedEvents = Counter.build().help("total number of events send").name("published_events").register();
        totalReceivedEvents = Counter.build().help("total number of events received").name("received_events")
                .register();
        sourceFileCount = Gauge.build().help("number of file in sources").name("source_number_of_files").register();
        sinkFileCount = Gauge.build().help("number of file in sinks").name("sink_number_of_files").register();
        sinkFiles = Counter.build().help("file summary").labelNames("app_name","file_path", "file_name", "mode", "stream_name", "stream_type")
                .name("sink_file").register();
        sinkFileSize = Gauge.build().help("size of files which are used by sink").labelNames("app_name","file_path", "file_name")
                .name("sink_file_size").register();
    }


    public static synchronized void createServer() {
        if (isServerStarted) {
            log.info("Prometheus server has already been started.");
        }
        if (!isServerStarted) {
            try {
                registerMetrics();
                server = new HTTPServer(8898);
                isServerStarted = true;
            } catch (IOException e) {
                log.error("Error while starting the prometheus server.", e);
            }
        }
    }

    public static synchronized void stopServer() {
        if (server != null && isServerStarted) {
            CollectorRegistry.defaultRegistry.clear();
            server.stop();
            isServerStarted = false;
        }
    }


    public static HashSet<String> getFilesURI() {
        return filesURI;
    }

    public static Map<String, StreamStatus> getStreamStatus() {
        return streamStatus;
    }

    public static Counter getTotalPublishedEvents() {
        return totalPublishedEvents;
    }

    public static Counter getTotalReceivedEvents() {
        return totalReceivedEvents;
    }

    public static Gauge getSourceFileCount() {
        return sourceFileCount;
    }

    public static Gauge getSinkFileCount() {
        return sinkFileCount;
    }

    public static Counter getSinkFiles() {
        return sinkFiles;
    }

    public static Gauge getSinkFileSize() {
        return sinkFileSize;
    }

    public static Counter getSinkDroppedEvents() {
        return sinkDroppedEvents;
    }

    public static Gauge getSinkStreamStatusMetrics() {
        return sinkStreamStatusMetrics;
    }

    public static Counter getSinkNoOfLines() {
        return sinkNoOfLines;
    }

    public static Counter getSourceFiles() {
        return sourceFiles;
    }

    public static Gauge getSourceFileSize() {
        return sourceFileSize;
    }

    public static Counter getSourceDroppedEvents() {
        return sourceDroppedEvents;
    }

    public static Gauge getSourceStreamStatusMetrics() {
        return sourceStreamStatusMetrics;
    }

    public static Counter getSourceNoOfLines() {
        return sourceNoOfLines;
    }

    public static Counter getReadByte() {
        return readByte;
    }

    public static Gauge getSourceElapsedTime() {
        return sourceElapsedTime;
    }

    public static Counter getTotalCopy() {
        return totalCopy;
    }

    public static Counter getTotalDeletion() {
        return totalDeletion;
    }

    public static Counter getTotalMove() {
        return totalMove;
    }

    public static Counter getTotalNumberOfArchive() {
        return totalNumberOfArchive;
    }

    /**
     * Enum that defines stream status.
     */
    public enum StreamStatus {
        CONNECTING,
        PROCESSING,
        COMPLETED,
        RETRY,
        ERROR,
    }




}
