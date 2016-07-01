/*
* Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.wso2.carbon.extension.analytics.publisher.file;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapter;
import org.wso2.carbon.event.output.adapter.core.OutputEventAdapterConfiguration;
import org.wso2.carbon.event.output.adapter.core.exception.ConnectionUnavailableException;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;
import org.wso2.carbon.event.output.adapter.core.exception.TestConnectionNotSupportedException;
import org.wso2.carbon.extension.analytics.publisher.file.internal.util.FileEventAdapterConstants;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class FileEventAdapter implements OutputEventAdapter {
    private static final Log log = LogFactory.getLog(FileEventAdapter.class);
    private OutputEventAdapterConfiguration eventAdapterConfiguration;
    private Map<String, FileChannel> fileChannelMap = new HashMap<>();

    public FileEventAdapter(OutputEventAdapterConfiguration eventAdapterConfiguration) {
        this.eventAdapterConfiguration = eventAdapterConfiguration;
    }

    /**
     * This method is called when initiating event publisher bundle.
     * Relevant code segments which are needed when loading OSGI bundle can be included in this method.
     *
     * @throws OutputEventAdapterException
     */
    public void init() throws OutputEventAdapterException {

    }

    /**
     * This method is used to test the connection of the publishing server.
     *
     * @throws TestConnectionNotSupportedException
     * @throws ConnectionUnavailableException
     */
    public void testConnect() throws TestConnectionNotSupportedException, ConnectionUnavailableException {

    }

    /**
     * Can be called to connect to back end before events are published.
     *
     * @throws ConnectionUnavailableException
     */
    public void connect() throws ConnectionUnavailableException {

    }

    /**
     * Publish events. Throws ConnectionUnavailableException if it cannot connect to the back end.
     *
     * @param message           Message object.
     * @param dynamicProperties Dynamic properties list.
     * @throws ConnectionUnavailableException
     */
    public void publish(Object message, Map<String, String> dynamicProperties) throws ConnectionUnavailableException {
        ByteBuffer buffer = null;
        String file = dynamicProperties.get(FileEventAdapterConstants.ADAPTER_FILE_NAME);
        try {
            buffer = ByteBuffer.allocate(Integer.parseInt(dynamicProperties
                    .get(FileEventAdapterConstants.ADAPTER_BUFFER_SIZE)));
            buffer.put(message.toString().getBytes());
            buffer.flip();
            if (fileChannelMap.get(file) == null) {
                createNewChannel(file);
            }
            fileChannelMap.get(file).write(buffer);

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (buffer != null) {
                buffer.clear();
            }
        }
    }

    /**
     * Will be called after publishing is done, or when ConnectionUnavailableException is thrown.
     */
    public void disconnect() {
        for (String keyList : fileChannelMap.keySet()) {
            try {
                fileChannelMap.get(keyList).close();
            } catch (IOException e) {
                log.error("Error occurred while close the FileChannel", e);
            }
        }
    }

    /**
     * The method can be used to clean all the resources consumed.
     */
    public void destroy() {

    }

    /**
     * Checks whether events get accumulated at the adapter and clients connect to it to collect events.
     *
     * @return
     */
    public boolean isPolled() {
        return false;
    }

    /**
     * Create new FileChannel.
     *
     * @param file Name of the file with path.
     * @throws FileNotFoundException
     */
    private void createNewChannel(String file) throws FileNotFoundException {
        FileChannel fileChannel = new FileOutputStream(file).getChannel();
        fileChannelMap.put(file, fileChannel);
    }
}