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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Parent class of the SinkMetrics class and the SourceMetrics class. This class also holds the metrics to monitor
 * the file operations.
 */
public class Metrics {

    private static final Metrics instance = new Metrics(1);

    private static final Logger log = Logger.getLogger(Metrics.class);
    protected static volatile boolean isServerStarted;
    //    private boolean isMetricsRegistered;
    private static HTTPServer server;
    protected Set<String> filesURI;
    protected Set<String> siddhiApps;
    protected Map<String, String> fileNamesMap; //fileURI, fileName

    private Gauge numberOfCopy;
    private Gauge numberOfDeletion;
    private Gauge numberOfMoves;
    private Gauge numberOfArchives;
    private Counter totalSiddhiApps;

    private Metrics(int self) {
        numberOfArchives = Gauge.build().help("total number of archived files").name("created_archived")
                .labelNames("app_name", "_source", "destination", "type", "time").register();
        numberOfMoves = Gauge.build().help("total number of moved files").name("moved_files_count")
                .labelNames("app_name", "_source", "destination", "time").register();
        numberOfDeletion = Gauge.build().help("total number of deleted files").name("deleted_file_count")
                .labelNames("app_name", "_source", "time").register();
        numberOfCopy = Gauge.build().help("total no of copied files").name("copied_file")
                .labelNames("app_name", "_source", "destination", "time").register();
        totalSiddhiApps = Counter.build().name("siddhi_app_count").help("Number of siddhi apps which has source " +
                "or sink with siddhi-io-file extension.").labelNames("app_name").register();
        filesURI = new HashSet<>();
        siddhiApps = new HashSet<>();
        fileNamesMap = new HashMap<>();
    }

    protected Metrics() {
        numberOfArchives = instance.numberOfArchives;
        numberOfMoves = instance.numberOfMoves;
        numberOfDeletion = instance.numberOfDeletion;
        numberOfCopy = instance.numberOfCopy;
        filesURI = instance.filesURI;
        siddhiApps = instance.siddhiApps;
        fileNamesMap = instance.fileNamesMap;
    }

    public static Metrics getInstance() {
        return instance;
    }

    public synchronized void createServer() {
        if (!isServerStarted && server == null) {
            try {
                server = new HTTPServer(8898);
                isServerStarted = true;
            } catch (IOException e) {
                log.error("Error while starting the prometheus server.", e);
            }
        } else {
            log.debug("Prometheus server has already been started.");
        }
    }

    public synchronized void stopServer() {
        if (siddhiApps.isEmpty()) {
            if (server != null && isServerStarted) {
                server.stop();
                server = null;
                isServerStarted = false;
                filesURI.clear();
                siddhiApps.clear();
            }
        }
    }

    public Gauge getNumberOfCopy() {
        return numberOfCopy;
    }

    public Gauge getNumberOfDeletion() {
        return numberOfDeletion;
    }

    public Gauge getNumberOfMoves() {
        return numberOfMoves;
    }

    public Gauge getNumberOfArchives() {
        return numberOfArchives;
    }


    public Set<String> getFilesURI() {
        return filesURI;
    }

    public Set<String> getSiddhiApps() {
        return siddhiApps;
    }

    public Counter getTotalSiddhiApps() {
        return totalSiddhiApps;
    }

    public Map<String, String> getFileNames() {
        return fileNamesMap;
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

}
