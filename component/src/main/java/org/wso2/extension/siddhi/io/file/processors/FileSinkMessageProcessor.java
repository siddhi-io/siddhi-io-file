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
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;

/**
 * A Message Processor class to be used for File connector pass through scenarios.
 */
public class FileSinkMessageProcessor implements CarbonMessageProcessor {
    private static final Logger log = Logger.getLogger(FileSinkMessageProcessor.class);

    private TextCarbonMessage textCarbonMessage;
    private BinaryCarbonMessage binaryCarbonMessage;

    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        if (carbonMessage instanceof TextCarbonMessage) {
            textCarbonMessage = (TextCarbonMessage) carbonMessage;
        } else if (carbonMessage instanceof BinaryCarbonMessage) {
            binaryCarbonMessage = (BinaryCarbonMessage) carbonMessage;
        }
        return true;
    }

    public void setTransportSender(TransportSender transportSender) {

    }

    public void setClientConnector(ClientConnector clientConnector) {

    }

    public String getId() {
        return null;
    }

    public TextCarbonMessage getTextCarbonMessage() {
        return textCarbonMessage;
    }

    public BinaryCarbonMessage getBinaryCarbonMessage() {
        return binaryCarbonMessage;
    }

}
