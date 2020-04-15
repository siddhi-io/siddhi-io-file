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
package io.siddhi.extension.io.file.util.metrics;

import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsManagement;

/**
 * Class which is holds the metrics to monitor Archive files operations.
 */
public class FileArchiveMetrics extends Metrics {

    private String destination;
    private String type;
    protected String source;
    protected Long time;

    public FileArchiveMetrics(String siddhiAppName) {
        super(siddhiAppName);
    }

    public void getArchiveMetric(int status) {
        MetricsManagement.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Operations.Archived.%s.%s.%s.%s",
                        siddhiAppName, type, time + ".time", source + ".source", destination + ".destination"),
                        Level.INFO, () -> status);
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setTime(Long time) {
        this.time = time;
    }
}
