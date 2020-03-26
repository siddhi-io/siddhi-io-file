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

package io.siddhi.extension.io.file;

import com.google.common.base.Stopwatch;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.Metrics;
import io.siddhi.extension.io.file.util.SinkMetrics;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the implementation of siddhi-io-file sink which provides the functionality of
 * publishing data to files through siddhi.
 */

@Extension(
        name = "file" ,
        namespace = "sink" ,
        description = "" +
                "File Sink can be used to publish (write) event data which is processed within siddhi " +
                "to files. \nSiddhi-io-file sink provides support to write both textual and binary data into files\n",
        parameters = {
                @Parameter(name = "file.uri",
                        description =
                                "Used to specify the file for data to be written. ",
                        type = {DataType.STRING},
                        dynamic = true
                ),

                @Parameter(name = "append",
                        description = "" +
                                "This parameter is used to specify whether the data should be " +
                                "append to the file or not.\n" +
                                "If append = 'true', " +
                                "data will be write at the end of the file without " +
                                "changing the existing content.\n" +
                                "If file does not exist, a new fill will be crated and then data will be written.\n" +
                                "If append append = 'false', \n" +
                                "If given file exists, existing content will be deleted and then data will be " +
                                "written back to the file.\n" +
                                "If given file does not exist, a new file will be created and " +
                                "then data will be written on it.\n",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),
                @Parameter(
                        name = "add.line.separator",
                        description = "This parameter is used to specify whether events added to the file should " +
                                "be separated by a newline.\n" +
                                "If add.event.separator= 'true'," +
                                "then a newline will be added after data is added to the file.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true. (However, if csv mapper is used, it is false)"
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@sink(type='file', @map(type='json'), " +
                                "append='false', " +
                                "file.uri='/abc/{{symbol}}.txt') " +
                                "define stream BarStream (symbol string, price float, volume long); ",

                        description = "" +
                                "Under above configuration, for each event, " +
                                "a file will be generated if there's no such a file," +
                                "and then data will be written to that file as json messages" +
                                "output will looks like below.\n" +
                                "{\n" +
                                "    \"event\":{\n" +
                                "        \"symbol\":\"WSO2\",\n" +
                                "        \"price\":55.6,\n" +
                                "        \"volume\":100\n" +
                                "    }\n" +
                                "}\n")
        }
)
public class FileSink extends Sink {
    private static final Logger log = Logger.getLogger(FileSink.class);

    private VFSClientConnector vfsClientConnector = null;
    private Map<String, String> properties = null;
    private Option uriOption;
    private SiddhiAppContext siddhiAppContext;
    private boolean addEventSeparator;
    private String streamName;
    private String mapType;
    private String siddhiAppName;
    private SinkMetrics metrics;


    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, byte[].class, Object.class};
    }

    public String[] getSupportedDynamicOptions() {
        return new String[]{Constants.FILE_URI};
    }

    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.siddhiAppName = siddhiAppContext.getName();
        uriOption = optionHolder.validateAndGetOption(Constants.FILE_URI);
        String append = optionHolder.validateAndGetStaticValue(Constants.APPEND, Constants.TRUE);
        properties = new HashMap<>();
        properties.put(Constants.ACTION, Constants.WRITE);
        if (Constants.TRUE.equalsIgnoreCase(append)) {
            properties.put(Constants.APPEND, append);
        }
        mapType = streamDefinition.getAnnotations().get(0).getAnnotations().get(0).getElements().get(0)
                .getValue();
        streamName = streamDefinition.getId();
        addEventSeparator = optionHolder.isOptionExists(Constants.ADD_EVENT_SEPARATOR) ?
                Boolean.parseBoolean(optionHolder.validateAndGetStaticValue(Constants.ADD_EVENT_SEPARATOR)) :
                !mapType.equalsIgnoreCase("csv");
        mapType = Utils.capitalizeFirstLetter(mapType);
        metrics = SinkMetrics.getInstance();
        return null;
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    public void connect() throws ConnectionUnavailableException {
//        metrics.getSinkFileStatusMap().putIfAbsent(getID() , Metrics.StreamStatus.CONNECTING);
        vfsClientConnector = new VFSClientConnector();
        metrics.createServer();
        metrics.updateMetrics(siddhiAppContext.getExecutorService());
        Utils.addSiddhiApp(siddhiAppName);
//        metrics.getSinkFileStatusMap().put(getID(), System.currentTimeMillis());
//        metrics.getSinkFileStatusMap().replace(getID(), Metrics.StreamStatus.PROCESSING);
    }

    public void disconnect() {
        metrics.getSiddhiApps().remove(siddhiAppName);
        /*for (String filePath: files) {
            String fileName = Utils.getFileName(filePath);
            Metrics.getSinkFiles().remove(siddhiAppName, filePath, fileName, mapType, streamName);
            Metrics.getSinkNoOfLines().remove(siddhiAppName, filePath, fileName);
            Metrics.getWriteBytes().remove(siddhiAppName, filePath, fileName);
            Metrics.getSinkDroppedEvents().remove(siddhiAppName, filePath, fileName);
            Metrics.getSinkLastPublishedTime().remove(siddhiAppName, filePath);
            Metrics.getSinkFileSize().remove(siddhiAppName, filePath, fileName);
            Metrics.getSinkElapsedTimeMap().remove(filePath);
            Metrics.getFilesURI().remove(filePath);
            Metrics.getSinkStreamStatusMetrics().remove(siddhiAppName, filePath, fileName, streamName);
            Metrics.getSinkElapsedTime().remove(filePath);
        }
        Metrics.getSinkStreamStatus().remove(getID());
        Metrics.getSinkFilesMap().remove(getID());
        Metrics.getSinkFilesIdleStatus().remove(getID());*/
//        metrics.stopServer();
    }

    public void destroy() {
    }

    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        byte[] byteArray = new byte[0];
        boolean canBeWritten = true;
        String uri = uriOption.getValue(dynamicOptions);
        String fileName = Utils.getFileName(uri);
        String shortenUri = Utils.getShortFilePath(uri);
        if (payload instanceof byte[]) {
            byteArray = (byte[]) payload;
        } else {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append(payload.toString());
                if (this.addEventSeparator) {
                    sb.append("\n");
                }
                byteArray = sb.toString().getBytes(Constants.UTF_8);
            } catch (UnsupportedEncodingException e) {
                canBeWritten = false;
                log.error("Received payload does not support UTF-8 encoding. Hence dropping the event." , e);
                metrics.getSinkDroppedEvents().labels(siddhiAppName, shortenUri).inc();
            }
        }

        if (canBeWritten) {
            BinaryCarbonMessage binaryCarbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(byteArray), true);
            properties.put(Constants.URI, uri);
            int byteSize = byteArray.length;
            SinkMetrics.SinkDetails sinkDetails = new SinkMetrics.SinkDetails(siddhiAppName, shortenUri);
            try {
                boolean send = vfsClientConnector.send(binaryCarbonMessage, null, properties);
                if (send) {
                    siddhiAppContext.getExecutorService().execute(() -> {
                        double fileSize = Utils.getFileSize(uri);
                        metrics.getSinkFilesEventCount().labels(siddhiAppName, shortenUri, fileName, mapType,
                                streamName).inc();
                        metrics.getSinkDroppedEvents().labels(siddhiAppName, shortenUri);
                        metrics.getWriteBytes().labels(siddhiAppName, shortenUri).inc(byteSize);
                        metrics.getSinkFileSize().labels(siddhiAppName, shortenUri).set(fileSize);
                        boolean added = metrics.getFilesURI().add(uri);
                        if (added) {
                            metrics.getSinkElapsedTimeMap().put(shortenUri, Stopwatch.createStarted());
                        }
                        if (metrics.getSinkFileLastPublishedTimeMap().containsKey(sinkDetails)) {
                            metrics.getSinkFileLastPublishedTimeMap().replace(sinkDetails, System.currentTimeMillis());
                            metrics.getSinkFileStatusMap().replace(sinkDetails, Metrics.StreamStatus.PROCESSING);
                        } else {
                            metrics.getSinkFileLastPublishedTimeMap().put(sinkDetails, System.currentTimeMillis());
                            metrics.getSinkFileStatusMap().put(sinkDetails, Metrics.StreamStatus.PROCESSING);
                        }
                        metrics.getSinkLastPublishedTime().labels(siddhiAppName, shortenUri).set(
                                System.currentTimeMillis());
                        metrics.getSinkLinesCount().labels(siddhiAppName, shortenUri).inc();
                    });
                }
            } catch (ClientConnectorException e) {
                metrics.getSinkFileStatusMap().replace(sinkDetails, Metrics.StreamStatus.RETRY);
                metrics.getSinkDroppedEvents().labels(siddhiAppName, shortenUri).inc();
                throw new ConnectionUnavailableException("Writing data into the file " + uri + " failed during the " +
                        "execution of '" + siddhiAppName + "' SiddhiApp, due to " +
                        e.getMessage(), e);
            }
        }
    }
}

// TODO: 2/19/20 Think about a way to set streamstatus, should be able to get appName, path in the metrics

// TODO: 2/5/20 Check sink with connectionUnavailable
