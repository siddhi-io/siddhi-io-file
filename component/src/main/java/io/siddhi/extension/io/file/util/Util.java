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

import io.siddhi.core.event.Event;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util Class.
 * This method used to get the fileHandlerEvent
 */
public class Util {
    private static final Logger log = LogManager.getLogger(Util.class);
    public static Event getFileHandlerEvent(final File file, List<String> fileObjectList, Status enumStatus) {
        boolean listenerEventsURLValidated = false;
        String status;
        switch (enumStatus) {
            case STATUS_NEW: status = "created"; break;
            case STATUS_PROCESS: status = "modifying"; break;
            case STATUS_DONE: status = "modifyingCompleted"; break;
            case STATUS_REMOVE: status = "removed"; break;
            default: throw new IllegalStateException("Unexpected value: " + enumStatus);
        }
        if (fileObjectList.contains(file.getAbsolutePath())) {
            listenerEventsURLValidated = true;
        } else {
            //If the fileObjectList contains this file
            for (String fileObjectPath : fileObjectList) {
                File fileObject = new File(fileObjectPath);
                if (fileObject.isDirectory()) {
                    //If a fileObject is a folder then the events for the files in the folder should thrown.
                    listenerEventsURLValidated = true;
                }
            }
        }
        if (listenerEventsURLValidated) {
            Object[] obj = {file.getAbsolutePath(), file.getName(), status};
            return new Event(System.currentTimeMillis(), obj);
        }
        return null;
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
        } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode) || Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
            properties = new HashMap<>();
            properties.put(Constants.URI, fileURI);
            properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
            properties.put(Constants.BUFFER_SIZE_IN_BINARY_CHUNKED, fileSourceConfiguration.getBufferSize());
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
            properties.put(Constants.READ_ONLY_TRAILER, fileSourceConfiguration.getReadOnlyTrailer());
            properties.put(Constants.SKIP_TRAILER, fileSourceConfiguration.getSkipTrailer());
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
        if (actionAfterProcess.equalsIgnoreCase(Constants.MOVE)) {
            properties.put(Constants.MOVE_IF_EXIST_MODE, fileSourceConfiguration.getMoveIfExistMode());
        }
        return properties;
    }
}
