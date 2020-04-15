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
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Parent class of the SinkMetrics, SourceMetrics, File Operation metrics classes.
 */
public class Metrics {

    protected Set<String> filesURI;
    protected Map<String, String> fileNamesMap; //fileURI, fileName
    protected String siddhiAppName;

    public Metrics(String siddhiAppName) {
        this.siddhiAppName = siddhiAppName;
        filesURI = new HashSet<>();
        fileNamesMap = new HashMap<>();
        MetricsDataHolder.getInstance().getMetricService()
                .counter(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File", siddhiAppName), Level.INFO).inc();
    }

    public Set<String> getFilesURI() {
        return filesURI;
    }

    public Map<String, String> getFileNames() {
        return fileNamesMap;
    }

}

