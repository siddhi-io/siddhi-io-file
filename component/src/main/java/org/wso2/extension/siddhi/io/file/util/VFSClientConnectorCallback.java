package org.wso2.extension.siddhi.io.file.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.transport.filesystem.connector.server.exception.FileSystemServerConnectorException;
import org.wso2.carbon.transport.filesystem.connector.server.util.FileTransportUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by minudika on 23/7/17.
 */
public class VFSClientConnectorCallback implements CarbonCallback {
    private static final Logger log = LoggerFactory.getLogger(VFSClientConnectorCallback.class);
    private CountDownLatch latch = new CountDownLatch(1);

    public void done(CarbonMessage carbonMessage) {
        if(log.isDebugEnabled()) {
            log.debug("Message processor acknowledgement received.");
        }

        this.latch.countDown();
    }

    public void waitTillDone(long timeOutInterval, String fileURI) throws InterruptedException {
        boolean isCallbackReceived = this.latch.await(timeOutInterval, TimeUnit.MILLISECONDS);
        if(!isCallbackReceived) {
            log.warn("The time for waiting for the acknowledgement exceeded " + timeOutInterval + "ms. Proceeding to post processing the file: " + FileTransportUtils.maskURLPassword(fileURI));
        }

    }
}
