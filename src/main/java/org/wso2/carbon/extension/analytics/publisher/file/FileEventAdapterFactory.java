/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.carbon.extension.analytics.publisher.file;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.event.output.adapter.core.*;
import org.wso2.carbon.extension.analytics.publisher.file.internal.util.FileEventAdapterConstants;

import java.util.*;

/**
 * The File event adapter factory class to create
 * an File output adapter
 */
public class FileEventAdapterFactory extends OutputEventAdapterFactory {
    private static final Log log = LogFactory.getLog(FileEventAdapterFactory.class);
    private ResourceBundle resourceBundle = ResourceBundle.getBundle("Resources", Locale.getDefault());

    /**
     * This string will be displayed in the publisher interface in the adapter type drop down list.
     *
     * @return
     */
    @Override
    public String getType() {
        return FileEventAdapterConstants.ADAPTER_TYPE;
    }

    /**
     * Specify supported message formats for the created publisher type.
     *
     * @return supported message formats.
     */
    @Override
    public List<String> getSupportedMessageFormats() {
        List<String> supportedMessageFormats = new ArrayList<>();
        supportedMessageFormats.add(MessageType.TEXT);
        supportedMessageFormats.add(MessageType.XML);
        supportedMessageFormats.add(MessageType.JSON);

        return supportedMessageFormats;
    }

    /**
     * Here static properties have to be specified.
     * These properties will use the values assigned when creating a publisher.
     * For more information on adapter properties see Event Publisher Configuration.
     *
     * @return Static property.
     */
    @Override
    public List<Property> getStaticPropertyList() {
        return new ArrayList<>();
    }

    /**
     * You can define dynamic properties similar to static properties,
     * the only difference is dynamic property values can be derived by events handling by publisher.
     * For more information on adapter properties see Event Publisher Configuration.
     *
     * @return Dynamic property.
     */
    @Override
    public List<Property> getDynamicPropertyList() {
        List<Property> dynamicProperties = new ArrayList<>();

        Property fileName = new Property(FileEventAdapterConstants.ADAPTER_FILE_NAME);
        fileName.setDisplayName(resourceBundle
                .getString(FileEventAdapterConstants.ADAPTER_FILE_NAME));
        fileName.setHint(resourceBundle.getString(FileEventAdapterConstants.ADAPTER_FILE_NAME_HINT));
        dynamicProperties.add(fileName);

        Property bufferSize = new Property(FileEventAdapterConstants.ADAPTER_BUFFER_SIZE);
        bufferSize.setDisplayName(resourceBundle
                .getString(FileEventAdapterConstants.ADAPTER_BUFFER_SIZE));
        bufferSize.setHint(resourceBundle.getString(FileEventAdapterConstants.ADAPTER_BUFFER_HINT));
        bufferSize.setDefaultValue(FileEventAdapterConstants.ADAPTER_FILE_BUFFER_SIZE_DEFAULT);
        dynamicProperties.add(bufferSize);

        return dynamicProperties;
    }

    /**
     * Specify any hints to be displayed in the management console.
     *
     * @return
     */
    @Override
    public String getUsageTips() {
        return null;
    }

    /**
     * This method creates the publisher by specifying event adapter configuration
     * and global properties which are common to every adapter type.
     *
     * @param eventAdapterConfiguration OutputEventAdapterConfiguration.
     * @param globalProperties          The globalProperties.
     * @return
     */
    @Override
    public OutputEventAdapter createEventAdapter(OutputEventAdapterConfiguration eventAdapterConfiguration,
                                                 Map<String, String> globalProperties) {
        return new FileEventAdapter(eventAdapterConfiguration);
    }
}