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

package org.wso2.extensions.siddhi.io.file;

import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.MapCarbonMessage;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.extensions.siddhi.io.file.utils.Constants;
import org.wso2.extensions.siddhi.io.file.utils.FileSourceConfiguration;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class FileMessageProcessor implements CarbonMessageProcessor {
    private CountDownLatch latch = new CountDownLatch(1);
    private String fileContent;
    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private Pattern defaultPattern;
    private Pattern endRegexPattern;
    private Matcher matcher;
    private int regexType = -1;

    public FileMessageProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSinkConfiguration) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSinkConfiguration;
    }

    public FileMessageProcessor(){
        int x = 10;
    }

    private void configureFileMessageProcessor(){
        String beginRegex = fileSourceConfiguration.getBeginRegex();
        String endRegex = fileSourceConfiguration.getEndRegex();
        if(beginRegex != null && endRegex != null){
            //pattern = Pattern.compile("<tag>(.+?)</tag>");
            defaultPattern = Pattern.compile(beginRegex + "(.+?)" + endRegex);
            regexType = 2;
        } else if (beginRegex != null && endRegex == null){
            defaultPattern = Pattern.compile(beginRegex + "(.+?)" + beginRegex);
            regexType = 0;
        } else if (beginRegex == null && endRegex != null) {
            defaultPattern = Pattern.compile(".+?" + endRegex);
            endRegexPattern = Pattern.compile(endRegex + "(.+?)" + endRegex);
            regexType = 1;
        }
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        //fileContent = getStringFromInputStream(carbonMessage.getInputStream());
        fileContent = readFile(carbonMessage);
        System.err.println(fileContent);
        carbonCallback.done(carbonMessage);
        done();
        return false;
    }

    public void setTransportSender(TransportSender transportSender) {

    }

    public void setClientConnector(ClientConnector clientConnector) {

    }

    public String getId() {
        return "test-file-message-processor";
    }

    /**
     * To wait till file reading operation is finished.
     *
     * @throws InterruptedException Interrupted Exception.
     */
    public void waitTillDone() throws InterruptedException {
        latch.await();
    }

    /**
     * To make sure the reading the file content is done.
     */
    private void done() {
        latch.countDown();
    }

    /**
     * To get the string from the input stream.
     *
     * @param in Input stream to be converted to String.
     * @return the String value of the input stream
     * @throws IOException IO exception when reading the input stream
     */
    private static String getStringFromInputStream(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder(4096);
        InputStreamReader reader = new InputStreamReader(in);
        BufferedReader bufferedReader = new BufferedReader(reader);
        int x = 10;
        try {
            String str;
            while ((str = bufferedReader.readLine()) != null) {
                sb.append(str);
            }
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // Do nothing.
            }
            try {
                reader.close();
            } catch (IOException e) {
                // Do nothing.
            }
            try {
                bufferedReader.close();
            } catch (IOException e) {
                // Do nothing.
            }
        }
        return sb.toString();
    }

    /**
     * To get the file content of the relevant file.
     *
     * @return the file content.
     */
    public String getFileContent() {
        return fileContent;
    }

    private void processMessage(CarbonMessage carbonMessage) {

        if (carbonMessage.getClass() == TextCarbonMessage.class) {
            String event = ((TextCarbonMessage) carbonMessage).getText();
            sourceEventListener.onEvent(event);
        } else if (carbonMessage.getClass() == MapCarbonMessage.class) {
            Map<String, String> event = new HashMap<String, String>();
            MapCarbonMessage mapCarbonMessage = (MapCarbonMessage) carbonMessage;
            Enumeration<String> mapNames = mapCarbonMessage.getMapNames();
            while (mapNames.hasMoreElements()) {
                String key = mapNames.nextElement();
                event.put(key, mapCarbonMessage.getValue(key));
            }
            sourceEventListener.onEvent(event);
        }
    }

    private String readFile(CarbonMessage carbonMessage){
        String mode = fileSourceConfiguration.getMode();
        InputStream inputStream = carbonMessage.getInputStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String content = null;
        if(Constants.TEXT_FULL.equalsIgnoreCase(mode)){
            //readFileLineByLine(bufferedReader);
            content = readFullFile(bufferedReader);
        } else if(Constants.BINARY_FULL.equalsIgnoreCase(mode)){

        } else if(Constants.REGEX.equalsIgnoreCase(mode)){

        } else if(Constants.LINE.equalsIgnoreCase(mode)){

        }
        return content;
    }

    private void readFileLineByLine(BufferedReader reader){
        String line;
        try {
            while((line = reader.readLine()) != null) {
                sourceEventListener.onEvent(line);
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Failed to read line."+e.getMessage());
        }
    }

    private String readFullFile(BufferedReader reader){
        char[] buf = new char[2048];
        StringBuilder sb = new StringBuilder();
        try {
            while(reader.read(buf) != -1){
                sb.append(new String(buf));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(buf);
    }

    private void readFileUsingRegex(BufferedReader reader){
        char[] buf = new char[10];
        StringBuilder sb = new StringBuilder();
        StringBuilder eventStringBuilder = new StringBuilder();
        try {
            while(reader.read(buf) != -1){
                sb.append(new String(buf));
                Matcher matcher = defaultPattern.matcher(sb.toString().trim());
                while(matcher.find()){
                    eventStringBuilder.append(matcher.group(0));
                    String tmp = sb.substring(matcher.end() + 1);
                    sb.setLength(0);
                    sb.append(tmp);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
