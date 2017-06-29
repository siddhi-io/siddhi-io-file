package org.wso2.extensions.siddhi.io.file;

import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.carbon.transport.file.connector.sender.VFSClientConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.extensions.siddhi.io.file.utils.Constants;
import org.wso2.extensions.siddhi.io.file.utils.FileSourceConfiguration;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by minudika on 28/6/17.
 */
public class FileProcessor implements CarbonMessageProcessor {
    private CountDownLatch latch = new CountDownLatch(1);
    SourceEventListener sourceEventListener;
    FileSourceConfiguration fileSourceConfiguration;
    private String mode;
    private int count = 0;


    public FileProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSourceConfiguration) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSourceConfiguration;
        this.mode = fileSourceConfiguration.getMode();
    }

    @Override
    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {

        byte[] content = ((BinaryCarbonMessage) carbonMessage).readBytes().array();
        String msg = new String(content);
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>"+ (count++)+ ">>>>" + msg);
        //sourceEventListener.onEvent(new String(content));
        //done();

        if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
            sourceEventListener.onEvent(new String(content));
            done();
        } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {

        } else if (Constants.LINE.equalsIgnoreCase(mode)) {
            sourceEventListener.onEvent(msg);
        }
        return false;
    }

    @Override
    public void setTransportSender(TransportSender transportSender) {

    }

    @Override
    public void setClientConnector(ClientConnector clientConnector) {

    }

    @Override
    public String getId() {
        return null;
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
}
