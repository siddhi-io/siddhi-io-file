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
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.extension.io.file.metrics.SourceMetrics;
import io.siddhi.extension.io.file.metrics.StreamStatus;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message processor for handling data retrieved from consumed files.
 */
public class FileProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(FileProcessor.class);

    private final SourceEventListener sourceEventListener;
    private final FileSourceConfiguration fileSourceConfiguration;
    private final String mode;
    private final Pattern pattern;
    private int readBytes;
    private final StringBuilder sb;
    private final String[] requiredProperties;

    private Stopwatch stopwatch;
    private long lineCount;
    private long readingLine;
    private long totalReadByteSize;
    private double fileSize;
    private String fileURI;
    private SourceMetrics metrics;
    private long startedTime;
    private long completedTime;
    private boolean send;
    private long previousEventCount;

    public FileProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSourceConfiguration,
                         SourceMetrics sourceMetrics) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.requiredProperties = fileSourceConfiguration.getRequiredProperties();
        this.mode = fileSourceConfiguration.getMode();
        if (Constants.REGEX.equalsIgnoreCase(mode) && fileSourceConfiguration.isTailingEnabled()) {
            sb = fileSourceConfiguration.getTailingRegexStringBuilder();
        } else {
            sb = new StringBuilder();
        }
        pattern = fileSourceConfiguration.getPattern();
        if (sourceMetrics != null) {
            this.metrics = sourceMetrics;
            this.fileURI = fileSourceConfiguration.getCurrentlyReadingFileURI();
            previousEventCount = ((SourceMapper) sourceEventListener).getEventCount();
            stopwatch = Stopwatch.createStarted();
            startedTime = System.currentTimeMillis();
            fileSize = Utils.getFileSize(fileURI);
            metrics.getStartedTimeMetric(System.currentTimeMillis());
            boolean add = metrics.getFilesURI().add(fileURI);
            if (add) {
                try {
                    lineCount = Utils.getLinesCount(fileURI);
                    metrics.getFileSizeMetric(() -> fileSize);
                    metrics.getReadPercentageMetric(fileURI);
                    metrics.getReadLineCountMetric().inc(lineCount);
                    metrics.getValidEventCountMetric();
                    metrics.getTotalErrorCount();
                    if (fileSourceConfiguration.isTailingEnabled()) {
                        metrics.getTailEnabledMetric(1);
                        metrics.getElapseTimeMetric(() -> stopwatch.elapsed().toMillis());
                    } else {
                        metrics.getTailEnabledMetric(0);
                        metrics.getElapseTimeMetric(() -> {
                            if (completedTime != 0) {
                                return completedTime - startedTime;
                            }
                            return 0L;
                        });
                    }
                    metrics.getFileStatusMetric();
                } catch (IOException e) {
                    log.error("Error occurred while getting the lines count in '" + fileURI + "'.", e);
                }
            }
        }
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (carbonMessage instanceof BinaryCarbonMessage) {
            byte[] content = ((BinaryCarbonMessage) carbonMessage).readBytes().array();

            String[] requiredPropertyValues = new String[0];
            Map requiredPropertiesMap = new HashMap();

            /**
             * EOF property will only be supported under REGEX mode, going forward.
             * TEXT_FULL mode is supported in this version to keep backward compatibility
             */
            if (Constants.REGEX.equalsIgnoreCase(mode) || Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                extractRequiredProperties(carbonMessage, requiredPropertiesMap);
            } else {
                requiredPropertyValues = getRequiredPropertyValues(carbonMessage);
            }

            long filePointer = 0;
            if (Constants.LINE.equalsIgnoreCase(mode) && fileSourceConfiguration.isTailingEnabled()) {
                filePointer = (Long) carbonMessage.getProperties().get(Constants.CURRENT_POSITION);
            }
            if (carbonCallback != null) {
                carbonCallback.done(carbonMessage);
            }

            String msg = new String(content, StandardCharsets.UTF_8);
            if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    sourceEventListener.onEvent(new String(content, StandardCharsets.UTF_8),
                            getRequiredPropertyValuesInRegexMode(true, requiredPropertiesMap));
                    send = true;
                }
            } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    sourceEventListener.onEvent(content, requiredPropertyValues);
                    send = true;
                }
            } else if (Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    sourceEventListener.onEvent(content, requiredPropertyValues);
                }
            } else if (Constants.LINE.equalsIgnoreCase(mode)) {
                if (!fileSourceConfiguration.isTailingEnabled()) {
                    InputStream is = new ByteArrayInputStream(content);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (line.length() > 0) {
                            readBytes = line.length();
                            sourceEventListener.onEvent(line.trim(), requiredPropertyValues);
                            send = true;
                        }
                    }
                } else {
                    if (msg.length() > 0) {
                        readBytes = msg.getBytes(StandardCharsets.UTF_8).length;
                        fileSourceConfiguration.updateFilePointer(filePointer);
                        sourceEventListener.onEvent(msg, requiredPropertyValues);
                        send = true;
                        if (metrics != null) {
                            increaseTailingMetrics();
                        }
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
                                sourceEventListener.onEvent(prevEvent,
                                        getRequiredPropertyValuesInRegexMode(false, requiredPropertiesMap));
                                send = true;
                            }
                            sourceEventListener.onEvent(event,
                                    getRequiredPropertyValuesInRegexMode(true, requiredPropertiesMap));
                            send = true;
                        } else if (matchFound) {
                            if (prevEvent != null) {
                                sourceEventListener.onEvent(prevEvent,
                                        getRequiredPropertyValuesInRegexMode(false, requiredPropertiesMap));
                                send = true;
                            }
                            prevEvent = event;
                        } else {
                            buf = new char[10]; // to clean existing content of buffer
                            endOfStream = bufferedReader.read(buf);
                            if (endOfStream == -1) {
                                if (prevEvent != null) {
                                    sourceEventListener.onEvent(prevEvent,
                                            getRequiredPropertyValuesInRegexMode(true, requiredPropertiesMap));
                                    send = true;
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
                                    (sb.substring(sb.indexOf(event)), requiredPropertyValues);
                            send = true;
                        }
                    }
                } else {
                    fileSourceConfiguration.updateFilePointer(readBytes);

                    sb.append(new String(content, StandardCharsets.UTF_8));
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
                        sourceEventListener.onEvent(event, requiredPropertyValues);
                        send = true;
                        readBytes += content.length;
                        if (metrics != null) {
                            increaseTailingMetrics();
                        }
                    }
                    String tmp;
                    tmp = sb.substring(lastMatchedIndex);
                    sb.setLength(0);
                    sb.append(tmp);
                }
            }
            if (metrics != null && send) {
                increaseMetrics(content.length);
                totalReadByteSize += content.length;
                readingLine++;
                completedTime = System.currentTimeMillis();
                send = false;
            }
            return true;
        } else {
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
                log.error("Failed to find required transport property '" + propertyKey + "'. Assigning null value");
                values[i++] = null;
            }
        }
        return values;
    }

    private void increaseMetrics(int byteLength) {
        metrics.getTotalFileReadCount().inc();
        metrics.getTotalReadsMetrics().inc();
        metrics.getReadByteMetric().inc(byteLength);
        metrics.getElapseTimeMetric(() -> stopwatch.elapsed().toMillis());
        metrics.setReadPercentage(totalReadByteSize / fileSize * 100, fileURI);
        long eventCount = ((SourceMapper) sourceEventListener).getEventCount() - previousEventCount;
        metrics.getValidEventCountMetric().inc(eventCount);
        previousEventCount = ((SourceMapper) sourceEventListener).getEventCount();
    }

    private void increaseTailingMetrics() {
        if (readingLine >= lineCount) {
            metrics.getReadLineCountMetric().inc();
        }
        fileSize = Utils.getFileSize(fileSourceConfiguration.getCurrentlyReadingFileURI());
        metrics.getSourceFileStatusMap().replace(Utils.getShortFilePath(fileURI), StreamStatus.PROCESSING);
        metrics.getTailEnabledFilesMap().replace(Utils.getShortFilePath(fileURI), System.currentTimeMillis());
    }

    /**
     * In Regex mode, the user may request for property trp:eol which is not extracted from the carbonMessage.
     * @param eof Whether EOF reached or not
     * @return required properties array
     */
    private String[] getRequiredPropertyValuesInRegexMode(boolean eof, Map requiredPropertyValues) {
        String[] values = new String[requiredProperties.length];
        int i = 0;
        for (String propertyName: requiredProperties) {
            if (propertyName.equalsIgnoreCase(Constants.EOF)) {
                values[i++] = String.valueOf(eof);
            } else {
                Object value = requiredPropertyValues.get(propertyName);
                if (value == null) {
                    log.error("Failed to find required transport property '" + propertyName +
                            "'. Assigning null value");
                    values[i++] = null;
                } else {
                    values[i++] = value.toString();
                }
            }
        }
        return values;
    }

    private void extractRequiredProperties(CarbonMessage carbonMessage, Map requiredPropertyValues) {
        for (String property: requiredProperties) {
            Object value = carbonMessage.getProperty(property);
            if (value != null) {
                requiredPropertyValues.put(property, value);
            } else {
                //We assume it is okay to proceed even though a trp property value will be null
                log.error("Failed to find required transport property '" + property + "'");
            }
        }
    }

}
