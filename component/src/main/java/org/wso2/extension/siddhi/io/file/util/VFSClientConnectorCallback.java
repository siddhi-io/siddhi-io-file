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

package org.wso2.extension.siddhi.io.file.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.transport.file.connector.server.util.FileTransportUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Class for CarbonCall back to be used in VFSClientConnector.
 */
public class VFSClientConnectorCallback implements CarbonCallback {
    private static final Logger log = LoggerFactory.getLogger(VFSClientConnectorCallback.class);
    private CountDownLatch latch = new CountDownLatch(1);

    public void done(CarbonMessage carbonMessage) {
        if (log.isDebugEnabled()) {
            log.debug("Message processor acknowledgement received.");
        }
        this.latch.countDown();
    }

    public void waitTillDone(long timeOutInterval, String fileURI) throws InterruptedException {
        boolean isCallbackReceived = this.latch.await(timeOutInterval, TimeUnit.MILLISECONDS);
        if (!isCallbackReceived) {
            log.warn("The time for waiting for the acknowledgement exceeded " + timeOutInterval +
                    "ms. Proceeding to post processing the file: " +
                    FileTransportUtils.maskURLPassword(fileURI));
        }

    }
}
