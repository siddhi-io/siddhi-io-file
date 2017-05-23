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
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class FileMessageProcessor implements CarbonMessageProcessor {
    private CountDownLatch latch = new CountDownLatch(1);
    private String fileContent;
    private SourceEventListener sourceEventListener;

    public FileMessageProcessor(SourceEventListener sourceEventListener) {
        this.sourceEventListener = sourceEventListener;
    }

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        fileContent = getStringFromInputStream(carbonMessage.getInputStream());
        System.out.println(fileContent);
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
}
