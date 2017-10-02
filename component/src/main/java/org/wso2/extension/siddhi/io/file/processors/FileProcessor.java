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

package org.wso2.extension.siddhi.io.file.processors;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.extension.siddhi.io.file.util.Constants;
import org.wso2.extension.siddhi.io.file.util.FileSourceConfiguration;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message processor for handling data retrieved from consumed files.
 * */
public class FileProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(FileProcessor.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private String mode;
    private Pattern pattern;
    private int readBytes;
    private StringBuilder sb;
    private String[] requiredProperties;

    public FileProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSourceConfiguration) {
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
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (carbonMessage instanceof BinaryCarbonMessage) {
            byte[] content = ((BinaryCarbonMessage) carbonMessage).readBytes().array();
            String msg = new String(content, Constants.UTF_8);
            String[] requiredPropertyValues = getRequiredPropertyValues(carbonMessage);
            if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    sourceEventListener.onEvent(new String(content, Constants.UTF_8),
                            fileSourceConfiguration.getRequiredProperties());
                }
                carbonCallback.done(carbonMessage);
            } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
                if (msg.length() > 0) {
                    sourceEventListener.onEvent(content, requiredPropertyValues);
                    // todo : handle trps here
                }
                carbonCallback.done(carbonMessage);
            } else if (Constants.LINE.equalsIgnoreCase(mode)) {
                if (!fileSourceConfiguration.isTailingEnabled()) {
                    InputStream is = new ByteArrayInputStream(content);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (line.length() > 0) {
                            readBytes = line.length();
                            sourceEventListener.onEvent(line.trim(), requiredPropertyValues);
                        }
                    }
                    carbonCallback.done(carbonMessage);
                } else {
                    if (msg.length() > 0) {
                        readBytes = msg.getBytes(Constants.UTF_8).length;
                        fileSourceConfiguration.updateFilePointer(readBytes);
                        sourceEventListener.onEvent(msg, requiredPropertyValues);
                    }
                }
            } else if (Constants.REGEX.equalsIgnoreCase(mode)) {
                int lastMatchedIndex = 0;
                int remainedLength = 0;
                if (!fileSourceConfiguration.isTailingEnabled()) {
                    char[] buf = new char[10];
                    InputStream is = new ByteArrayInputStream(content);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, Constants.UTF_8));

                    while (bufferedReader.read(buf) != -1) {
                        lastMatchedIndex = 0;
                        sb.append(new String(buf));
                        Matcher matcher = pattern.matcher(sb.toString().trim());
                        while (matcher.find()) {
                            String event = matcher.group(0);
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
                            sourceEventListener.onEvent(event, requiredPropertyValues);
                        }
                        String tmp;
                        tmp = sb.substring(lastMatchedIndex);

                        sb.setLength(0);
                        sb.append(tmp);
                        buf = new char[10]; // to clean existing content of buffer
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
                            String event = m.group(0);
                            sourceEventListener.onEvent(sb.substring(sb.indexOf(event)), requiredPropertyValues);
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
                        sourceEventListener.onEvent(event, requiredPropertyValues);
                        readBytes += content.length;
                    }
                    String tmp;
                    tmp = sb.substring(lastMatchedIndex);

                    sb.setLength(0);
                    sb.append(tmp);

                }
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
                log.error("Failed to find required transport property '" + propertyKey + "'");
            }
        }
        return  values;
    }
}
