package io.siddhi.extension.io.file.util;

import java.util.HashMap;
import java.util.Map;

/**
 * FileSystemPropertiesUtil
 */
public class GenerateAppProperties {
    public static Map<String, String> generateProperties(FileSourceConfiguration fileSourceConfiguration,
                                                         String fileURI) {
        Map<String, String> properties;
        String mode = fileSourceConfiguration.getMode();
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode)) {
            properties = new HashMap<>();
            properties.put(Constants.URI, fileURI);
            properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
        } else if (Constants.BINARY_FULL.equalsIgnoreCase(mode)) {
            properties = new HashMap<>();
            properties.put(Constants.URI, fileURI);
            properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
        } else {
            properties = new HashMap<>();
            properties.put(Constants.ACTION, Constants.READ);
            properties.put(Constants.MAX_LINES_PER_POLL, "10");
            properties.put(Constants.POLLING_INTERVAL, fileSourceConfiguration.getFilePollingInterval());
            properties.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY,
                    fileSourceConfiguration.getFileReadWaitTimeout());
            properties.put(Constants.MODE, mode);
            properties.put(Constants.HEADER_PRESENT, fileSourceConfiguration.getHeaderPresent());
            properties.put(Constants.READ_ONLY_HEADER, fileSourceConfiguration.getReadOnlyHeader());
            properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
            properties.put(Constants.URI, fileURI);
        }
        return properties;
    }
}
