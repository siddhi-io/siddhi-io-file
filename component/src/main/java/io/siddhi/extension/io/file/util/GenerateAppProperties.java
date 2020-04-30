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

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * GenerateAppProperties class is used to generate properties Map according to the modes used in the Siddhi App
 */
public class GenerateAppProperties {
    private static final Logger log = Logger.getLogger(GenerateAppProperties.class);

    public static Map<String, String> generateProperties(FileSourceConfiguration fileSourceConfiguration,
                                                         String fileURI) {
        Map<String, String> properties;
        String mode = fileSourceConfiguration.getMode();
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
            properties = new HashMap<>();
            properties.put(Constants.URI, fileURI);
            properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
        } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            properties = new HashMap<>();
            properties.put(Constants.URI, fileURI);
            properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
        } else {
            properties = new HashMap<>();
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.MAX_LINES_PER_POLL, "10");
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.HEADER_PRESENT, fileSourceConfiguration.getHeaderPresent());
            properties.put(Constants.READ_ONLY_HEADER, fileSourceConfiguration.getReadOnlyHeader());
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
            properties.put(Constants.URI, fileURI);
        }
        return properties;
    }

    public static Map<String, String> reProcessFileGenerateProperties(FileSourceConfiguration fileSourceConfiguration,
                                                                      String fileURI, Map<String, String> properties) {
        String actionAfterProcess = fileSourceConfiguration.getActionAfterProcess();
        properties.put(Constants.URI, fileURI);
        properties.put(Constants.ACK_TIME_OUT, "1000");
        properties.put(Constants.ACTION, actionAfterProcess);
        return properties;
    }

    public static String getFileName(String uri, String protocol) {
        try {
            URL url = new URL(String.format("%s%s%s", protocol, File.separator, uri));
            return FilenameUtils.getName(url.getPath());
        } catch (MalformedURLException e) {
            log.error(String.format("Failed to extract file name from the uri '%s '.", uri), e);
            return null;
        }
    }

    public static String constructPath(String baseUri, String fileName) {
        if (baseUri != null && fileName != null) {
            if (baseUri.endsWith(File.separator)) {
                return String.format("%s%s", baseUri, fileName);
            } else {
                return String.format("%s%s%s", baseUri, File.separator, fileName);
            }
        } else {
            return null;
        }
    }
}
