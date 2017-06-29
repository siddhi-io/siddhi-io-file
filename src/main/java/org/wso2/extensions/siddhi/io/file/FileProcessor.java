package org.wso2.extensions.siddhi.io.file;

import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.CarbonMessageProcessor;
import org.wso2.carbon.messaging.ClientConnector;
import org.wso2.carbon.messaging.TextCarbonMessage;
import org.wso2.carbon.messaging.TransportSender;
import org.wso2.extensions.siddhi.io.file.utils.FileSourceConfiguration;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;

import java.util.concurrent.CountDownLatch;

/**
 * Created by minudika on 28/6/17.
 */
public class FileProcessor implements CarbonMessageProcessor {
    private CountDownLatch latch = new CountDownLatch(1);
    SourceEventListener sourceEventListener;
    FileSourceConfiguration fileSourceConfiguration;


    public FileProcessor(SourceEventListener sourceEventListener, FileSourceConfiguration fileSinkConfiguration) {
        this.sourceEventListener = sourceEventListener;
        this.fileSourceConfiguration = fileSinkConfiguration;
    }

    @Override
    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        byte[] content = ((BinaryCarbonMessage) carbonMessage).readBytes().array();
        sourceEventListener.onEvent(new String(content));
        done();
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
