package org.wso2.extension.siddhi.io.file.util;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.wso2.carbon.messaging.ServerConnectorErrorHandler;

/**
 * Created by minudika on 3/8/17.
 */
public class ErrorHandler implements ServerConnectorErrorHandler {
    private static final Logger log = Logger.getLogger(ErrorHandler.class);

    @Override
    public void handleError(Exception e, CarbonMessage carbonMessage, CarbonCallback carbonCallback) {
        log.error(e.getMessage());
    }

    @Override
    public String getProtocol() {
        return "fs";
    }
}
