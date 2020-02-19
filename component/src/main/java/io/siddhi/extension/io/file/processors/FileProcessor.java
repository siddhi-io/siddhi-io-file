/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.file.processors;

import com.google.common.base.Stopwatch;
import io.prometheus.client.Counter;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.Metrics;
import io.siddhi.extension.util.Utils;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TransportSender;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message processor for handling data retrieved from consumed files.
 */
public class FileProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(FileProcessor.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private String mode;
    private Pattern pattern;
    private int readBytes;
    private StringBuilder sb;
    private String[] requiredProperties;
    private String fileName;
    private Counter.Child eventCounter;
    private Counter.Child readBytesMetrics;
    private Stopwatch stopwatch;
    private String streamName;
    private String siddhiAppName;
    private String filePath;

    public FileProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSourceConfiguration,
                         String siddhiAppName) {
        this.sourceEventListener = sourceEventListener;
        this.streamName = sourceEventListener.getStreamDefinition().getId();
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.requiredProperties = fileSourceConfiguration.getRequiredProperties();
        this.mode = fileSourceConfiguration.getMode();
        this.siddhiAppName = siddhiAppName;
        if (Constants.REGEX.equalsIgnoreCase(mode) && fileSourceConfiguration.isTailingEnabled()) {
            sb = fileSourceConfiguration.getTailingRegexStringBuilder();
        } else {
            sb = new StringBuilder();
        }
        pattern = fileSourceConfiguration.getPattern();
        String fileURI = fileSourceConfiguration.getCurrentlyReadingFileURI();
        filePath = Utils.getFilePath(fileURI);
        fileName = Utils.getFileName(fileURI);
        fileSourceConfiguration.getExecutorService().execute(() -> {
            stopwatch = Stopwatch.createStarted();
            double fileSize = Utils.getFileSize(fileURI) / 1024.0; //converts into KB
            String streamName = sourceEventListener.getStreamDefinition().getId();
            eventCounter = Metrics.getSourceFiles().labels(siddhiAppName, filePath,
                    fileName, mode, streamName, "Source");
            readBytesMetrics = Metrics.getReadByte().labels(siddhiAppName, filePath, fileName);
            Metrics.getSourceFileSize().labels(siddhiAppName, filePath, fileName).set(fileSize);
            Metrics.getSourceStreamStatusMetrics().labels(siddhiAppName, filePath, fileName, streamName).set(
                    Metrics.getStreamStatus().get(fileURI).ordinal());
            Metrics.getSourceDroppedEvents().labels(siddhiAppName, filePath, fileName);
            boolean add = Metrics.getFilesURI().add(fileURI);
            if (add) {
                Metrics.getSourceFileCount().inc();
                try {
                    Metrics.getSourceNoOfLines().labels(siddhiAppName, filePath, fileName).inc(Utils.getLinesCount(fileURI));
                } catch (IOException e) {
                    log.error("Error occurred while getting the lines count in '" + fileURI + "'.", e);
                }
            }
        });
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (carbonMessage instanceof BinaryCarbonMessage) {
            byte[] content = ((BinaryCarbonMessage) carbonMessage).readBytes().array();
            String msg = new String(content, Constants.UTF_8);
            if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    carbonCallback.done(carbonMessage);
                    carbonMessage.setProperty
                            (org.wso2.transport.file.connector.server.util.Constants.EOF, true);
                    sourceEventListener.onEvent(new String(content, Constants.UTF_8),
                            getRequiredPropertyValues(carbonMessage));
                }
            } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    carbonCallback.done(carbonMessage);
                    sourceEventListener.onEvent(content, getRequiredPropertyValues(carbonMessage));
                }
            } else if (Constants.LINE.equalsIgnoreCase(mode)) {
                if (!fileSourceConfiguration.isTailingEnabled()) {
                    InputStream is = new ByteArrayInputStream(content);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (line.length() > 0) {
                            readBytes = line.length();
                            sourceEventListener.onEvent(line.trim(), getRequiredPropertyValues(carbonMessage));
                        }
                    }
                    carbonCallback.done(carbonMessage);
                } else {
                    if (msg.length() > 0) {
                        readBytes = msg.getBytes(Constants.UTF_8).length;
                        fileSourceConfiguration.updateFilePointer(
                                (Long) carbonMessage.getProperties().get(Constants.CURRENT_POSITION));
                        sourceEventListener.onEvent(msg, getRequiredPropertyValues(carbonMessage));
                    }
                }
            } else if (Constants.REGEX.equalsIgnoreCase(mode)) {
                int lastMatchedIndex = 0;
                int remainedLength = 0;
                if (!fileSourceConfiguration.isTailingEnabled()) {
                    char[] buf = new char[10];
                    InputStream is = new ByteArrayInputStream(content);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));
                    int endOfStream = bufferedReader.read(buf); //number of characters read
                    String event = null;
                    String prevEvent = null;
                    while (endOfStream != -1) {
                        lastMatchedIndex = 0;
                        sb.append(new String(buf));
                        Matcher matcher = pattern.matcher(sb.toString().trim());
                        boolean matchFound = false;
                        while (matcher.find()) {
                            matchFound = true;
                            prevEvent = event;
                            event = matcher.group(0);
                            lastMatchedIndex = matcher.end();
                            if (fileSourceConfiguration.getEndRegex() == null) {
                                try {
                                    lastMatchedIndex -= (fileSourceConfiguration.getBeginRegex().length() - 2);
                                    event = event.substring(0, lastMatchedIndex);
                                } catch (StringIndexOutOfBoundsException e) {
                                    log.error(e.getMessage());
                                }
                            } else if (fileSourceConfiguration.getBeginRegex() == null) {
                                if (remainedLength < lastMatchedIndex) {
                                    lastMatchedIndex += remainedLength;
                                }
                                remainedLength = sb.length() - event.length() - remainedLength - 1;
                            }
                            buf = new char[10]; // to clean existing content of buffer
                            endOfStream = bufferedReader.read(buf);
                        }
                        if (matchFound && endOfStream == -1) {
                            if (prevEvent != null) {
                                carbonMessage.setProperty
                                        (org.wso2.transport.file.connector.server.util.Constants.EOF, false);
                                sourceEventListener.onEvent(prevEvent, getRequiredPropertyValues(carbonMessage));
                            }
                            carbonMessage.setProperty
                                    (org.wso2.transport.file.connector.server.util.Constants.EOF, true);
                            sourceEventListener.onEvent(event, getRequiredPropertyValues(carbonMessage));
                        } else if (matchFound) {
                            if (prevEvent != null) {
                                carbonMessage.setProperty
                                        (org.wso2.transport.file.connector.server.util.Constants.EOF, false);
                                sourceEventListener.onEvent(prevEvent, getRequiredPropertyValues(carbonMessage));
                            }
                            prevEvent = event;
                        } else {
                            buf = new char[10]; // to clean existing content of buffer
                            endOfStream = bufferedReader.read(buf);
                            if (endOfStream == -1) {
                                if (prevEvent != null) {
                                    carbonMessage.setProperty
                                            (org.wso2.transport.file.connector.server.util.Constants.EOF, true);
                                    sourceEventListener.onEvent(prevEvent, getRequiredPropertyValues(carbonMessage));
                                }
                            }
                        }
                        String tmp;
                        tmp = sb.substring(lastMatchedIndex);
                        sb.setLength(0);
                        sb.append(tmp);
                    }

                    /*
                    Handling last regex match since it will be having only one begin regex
                    instead of two to match.
                    */
                    if (fileSourceConfiguration.getBeginRegex() != null &&
                            fileSourceConfiguration.getEndRegex() == null) {
                        Pattern p = Pattern.compile(fileSourceConfiguration.getBeginRegex() + "((.|\n)*?)");
                        Matcher m = p.matcher(sb.toString());
                        while (m.find()) {
                            event = m.group(0);
                            sourceEventListener.onEvent
                                    (sb.substring(sb.indexOf(event)), getRequiredPropertyValues(carbonMessage));
                        }
                    }
                    if (carbonCallback != null) {
                        carbonCallback.done(carbonMessage);
                    }
                } else {
                    fileSourceConfiguration.updateFilePointer(readBytes);

                    sb.append(new String(content, Constants.UTF_8));
                    Matcher matcher = pattern.matcher(sb.toString().trim());
                    while (matcher.find()) {
                        String event = matcher.group(0);
                        lastMatchedIndex = matcher.end();
                        if (fileSourceConfiguration.getEndRegex() == null) {
                            lastMatchedIndex -= (fileSourceConfiguration.getBeginRegex().length() - 2);
                            event = event.substring(0, lastMatchedIndex);
                        } else if (fileSourceConfiguration.getBeginRegex() == null) {
                            if (remainedLength < lastMatchedIndex) {
                                lastMatchedIndex += remainedLength;
                            }
                            remainedLength = sb.length() - event.length() - remainedLength - 1;
                        }
                        sourceEventListener.onEvent(event, getRequiredPropertyValues(carbonMessage));
                        readBytes += content.length;
                    }
                    String tmp;
                    tmp = sb.substring(lastMatchedIndex);
                    sb.setLength(0);
                    sb.append(tmp);

                    if (carbonCallback != null) {
                        carbonCallback.done(carbonMessage);
                    }
                }
            }
            increaseMetrics(content.length, stopwatch.elapsed().toMillis());
            return true;
        } else {
            Metrics.getSourceDroppedEvents().labels(fileName).inc();
            return false;
        }
    }

    public void setTransportSender(TransportSender transportSender) {

    }

    public void setClientConnector(ClientConnector clientConnector) {

    }

    public String getId() {
        return "file-message-processor";
    }

    private String[] getRequiredPropertyValues(CarbonMessage carbonMessage) {
        String[] values = new String[requiredProperties.length];
        int i = 0;
        for (String propertyKey : requiredProperties) {
            Object value = carbonMessage.getProperty(propertyKey);
            if (value != null) {
                values[i++] = value.toString();
            } else {
                log.error("Failed to find required transport property '" + propertyKey + "'");
            }
        }
        return values;
    }

    private void increaseMetrics(int byteLength, long elapseTime) {
        eventCounter.inc();
        readBytesMetrics.inc(byteLength);
        Metrics.getTotalReceivedEvents().inc();
        Metrics.getSourceElapsedTime().labels(siddhiAppName, filePath, fileName).set(elapseTime);
        Metrics.getSourceStreamStatusMetrics().labels(siddhiAppName, filePath, fileName, streamName).set(Metrics.getStreamStatus().get(
                fileSourceConfiguration.getCurrentlyReadingFileURI()).ordinal());
    }

}
