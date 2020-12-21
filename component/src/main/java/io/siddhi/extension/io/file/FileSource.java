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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.file.listeners.FileSystemListener;
import io.siddhi.extension.io.file.metrics.SourceMetrics;
import io.siddhi.extension.io.file.metrics.StreamStatus;
import io.siddhi.extension.io.file.processors.FileProcessor;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.FileSourceServiceProvider;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import org.apache.commons.vfs2.FileObject;
import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;
import org.wso2.transport.file.connector.sender.VFSClientConnector;
import org.wso2.transport.file.connector.server.FileServerConnector;
import org.wso2.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.transport.remotefilesystem.RemoteFileSystemConnectorFactory;
import org.wso2.transport.remotefilesystem.exception.RemoteFileSystemConnectorException;
import org.wso2.transport.remotefilesystem.server.connector.contract.RemoteFileSystemServerConnector;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static io.siddhi.extension.io.file.listeners.FileCronExecutor.scheduleJob;
import static org.quartz.CronExpression.isValidExpression;


/**
 * Implementation of siddhi-io-file source.
 */
@Extension(
        name = "file",
        namespace = "source",
        description = "" +
                "The File Source component of the 'siddhi-io-fie' extension allows you to receive the input data to" +
                " be processed by Siddhi via files. Both text files and binary files are supported.",
        parameters = {
                @Parameter(
                        name = "dir.uri",
                        description =
                                "The path to the directory to be processed. During execution time, Siddhi by default" +
                                        " processes all the files within this directory. However, if you have entered" +
                                        " specific files to be processed via the 'file.name.list' parameter, only " +
                                        "those files are processed. The URI specified must " +
                                        "include the file handling protocol to be used for file processing.\n" +
                                        "e.g., If the file handling protocol to be used is 'ftp', the URI must be " +
                                        "provided as 'ftp://<DIRECTORY_PATH>>'.\n" +
                                        "At a given time, you should provide a value only for one out of the " +
                                        "'dir.uri' and 'file.uri' parameters. You can provide the directory URI if " +
                                        "you have multiple files that you want to process within a directory. You " +
                                        "can provide the file URI if you only need to process one file.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "file:/var/tmp"
                ),

                @Parameter(
                        name = "file.uri",
                        description =
                                "The path to the file to be processed. The URI specified must include the file " +
                                        "handling protocol to be used for file processing.\n " +
                                        " Only one of 'dir.uri' and 'file.uri' should be provided.\n e.g., If the " +
                                        "file handling protocol to be used is 'ftp', the URI must be provided as " +
                                        "'ftp://<FILE_PATH>>'.\n" +
                                        "At a given time, you should provide a value only for one out of the " +
                                        "'dir.uri' and 'file.uri' parameters. You can provide the directory URI if " +
                                        "you have multiple files that you want to process within a directory. You can" +
                                        " provide the file URI if you only need to process one file.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "file:/var/temp/tmp.text"
                ),

                @Parameter(
                        name = "mode",
                        description =
                                "This specifies the mode in which the files in given directory must be read." +
                                        "Possible values for this parameter are as follows:\n" +
                                        "- TEXT.FULL : to read a text file completely at once.\n" +
                                        "- BINARY.FULL : to read a binary file completely at once.\n" +
                                        "- BINARY.CHUNKED : to read a binary file chunk by chunk.\n" +
                                        "- LINE : to read a text file line by line.\n" +
                                        "- REGEX : to read a text file and extract data using a regex.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "line"
                ),

                @Parameter(
                        name = "tailing",
                        description = "" +
                                "If this parameter is set to 'true', the file/the first file of the directory " +
                                "is tailed. \n" +
                                "Do not set the parameter to 'true' and enable tailing if the mode is 'binary.full'," +
                                " 'text.full' or 'binary.chunked'.\n",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"
                ),

                @Parameter(
                        name = "action.after.process",
                        description = "" +
                                "The action to be carried out after processing the file/directory. Possible values " +
                                "are 'DELETE' and 'MOVE'. 'DELETE' is default. If you specify 'MOVE', you need to " +
                                "specify a value for the 'move.after.process' parameter to indicate the location to " +
                                "which the consumed files should be moved.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "delete"
                ),

                @Parameter(
                        name = "action.after.failure",
                        description = "" +
                                "The action to be taken if a failure occurs while the file/directory is being " +
                                "processed. Possible values are 'DELETE' and 'MOVE'. 'DELETE' is default. If you " +
                                "specify 'MOVE', you need to specify a value for the 'move.after.failure' parameter " +
                                "to indicate the location to which the files that could not be read need to be moved",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "delete"
                ),

                @Parameter(
                        name = "move.after.process",
                        description = "" +
                                "If you specify 'MOVE' as the value for the 'action.after.process' parameter, use " +
                                "this parameter to specify the location to which the consumed files need to be moved." +
                                "This should be the absolute path of the file that is going to be created after the " +
                                "moving is done.\n" +
                                "This URI must include the file handling protocol used for file processing.\n" +
                                "e.g., If the file handling protocol is 'ftp', the URI must be provided as " +
                                "'ftp://<FILE_PATH>>'.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),

                @Parameter(
                        name = "move.after.failure",
                        description = "" +
                                "If you specify 'MOVE' as the value for the 'action.after.failure' parameter, use " +
                                "this parameter to specify the location to which the files should be moved after the" +
                                " failure\n" +
                                "This should be the absolute path of the file that is going to be created after the " +
                                "failure.\n" +
                                "This URI must include the file handling protocol used for file processing.\n" +
                                "e.g., If the file handling protocol is 'ftp', the URI must be provided as " +
                                "'ftp://<FILE_PATH>>'.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),

                @Parameter(
                        name = "begin.regex",
                        description = "" +
                                "The regex to be matched at the beginning of the retrieved content.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "None"
                ),

                @Parameter(
                        name = "end.regex",
                        description = "" +
                                "The regex to be matched at the end of the retrieved content.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "None"
                ),

                @Parameter(
                        name = "file.polling.interval",
                        description = "" +
                                "The time interval (in milliseconds) of a polling cycle for a file.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),

                @Parameter(
                        name = "dir.polling.interval",
                        description = "The time period (in milliseconds) of a polling cycle for a directory.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),

                @Parameter(
                        name = "timeout",
                        description = "The maximum time duration (in milliseconds) that the system should wait until" +
                                " a file is processed.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "5000"
                ),
                @Parameter(
                        name = "file.read.wait.timeout",
                        description = "The maximum time duration (in milliseconds) that the system should wait " +
                                "before retrying to read the full file content.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),
                @Parameter(
                        name = "header.present",
                        description = "If this parameter is set to 'true', it indicates the file(s) to be processed " +
                                "includes a header line. In such a scenario, the header line is not processed.\n",
                        optional = true, defaultValue = "false",
                        type = {DataType.BOOL}
                ),
                @Parameter(
                        name = "read.only.header",
                        description = "This parameter is applicable only if the value for the 'mode' parameter is" +
                                " 'LINE'. If this parameter is set to 'true', only the first line (i.e., the header" +
                                " line) of a text file (e.g., CSV) is read. If it is set to 'false', the full content" +
                                " of the file is read line by line.",
                        optional = true,
                        type = {DataType.BOOL},
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "buffer.size",
                        description = "This parameter used to get the buffer size for binary.chunked mode.",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "65536"
                ),
                @Parameter(
                        name = "cron.expression",
                        description = "This is used to specify a timestamp in cron expression. " +
                                "The file or files in the given dir.uri or file.uri will be processed when the " +
                                "given expression satisfied by the system time.",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "None"
                ),
                @Parameter(
                        name = Constants.FILE_NAME_PATTERN,
                        description = "Regex pattern for the filenames that should be read from the directory. " +
                                "Note: This parameter is applicable only if the connector is reading from a directory",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. " +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@source(type='file',\n" +
                                "mode='text.full',\n" +
                                "tailing='false'\n " +
                                "dir.uri='file://abc/xyz',\n" +
                                "action.after.process='delete',\n" +
                                "@map(type='json')) \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n",

                        description = "" +
                                "In the above configuration, all the files in the given directory are picked and " +
                                "read one by one.\n" +
                                "Here, it is assumed that all the files contain valid json strings with " +
                                "'symbol', 'price', and 'volume' keys.\n" +
                                "Once a file is read, its content is converted to events via the 'siddhi-map-json' " +
                                "extension. Those events are then received as input events in the the 'FooStream' " +
                                "stream.\n" +
                                "Finally, after the reading is completed, the file is deleted.\n"
                ),

                @Example(
                        syntax = "" +
                                "@source(type='file',\n" +
                                "mode='files.repo.line',\n" +
                                "tailing='true',\n" +
                                "dir.uri='file://abc/xyz',\n" +
                                "@map(type='json')) \n" +
                                "define stream FooStream (symbol string, price float, volume long);\n ",

                        description = "" +
                                "In the above configuration, the first file in '/abc/xyz' directory is picked and " +
                                "read line by line.\n" +
                                "Here, it is assumed that the file contains lines json strings.\n" +
                                "For each line, the line content is converted to an event via the 'siddhi-map-json' " +
                                "extension. Those events are then received as input events in the the 'FooStream' " +
                                "stream.\n" +
                                "Once the file content is completely read, the system keeps checking for new " +
                                "entries added to the file. If it detects a new entry, it immediately picks it up " +
                                "and processes it.\n"
                ),
                @Example(
                        syntax = "" +
                                "@source(type='file',\n" +
                                "mode='text.full',\n" +
                                "tailing='false'\n " +
                                "dir.uri='file://abc/xyz',\n" +
                                "action.after.process='delete',\n" +
                                "@map(type='csv' @attributes(eof = 'trp:eof', fp = 'trp:file.path'))) \n" +
                                "define stream FooStream (symbol string, price float, volume long); \n",

                        description = "" +
                                "In the above configuration, all the files in the given directory are picked and " +
                                "read one by one.\n" +
                                "Here, it is assumed that each file contains valid json strings with 'symbol', and " +
                                "'price' keys.\n" +
                                "Once a file is read, its content is converted to an event via the 'siddhi-map-json'" +
                                " extension with the additional 'eof' attribute. Then, that event is received as an " +
                                "input event in the 'FooStream' stream.\n" +
                                "Once a file is completely read, it is deleted.\n"
                )
        }
)
public class FileSource extends Source<FileSource.FileSourceState> {
    private static final Logger log = Logger.getLogger(FileSource.class);
    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private RemoteFileSystemConnectorFactory fileSystemConnectorFactory;
    private FileSourceServiceProvider fileSourceServiceProvider;
    private RemoteFileSystemServerConnector fileSystemServerConnector;
    private String filePointer = "0";
    private String[] requiredProperties;
    private boolean isTailingEnabled = true;
    private SiddhiAppContext siddhiAppContext;
    private String mode;
    private String actionAfterProcess;
    private String actionAfterFailure = null;
    private String moveAfterProcess;
    private String moveAfterFailure = null;
    private String tailing;
    private String beginRegex;
    private String endRegex;
    private List<String> tailedFileURIMap;
    private String uri;
    private String dirUri;
    private String fileUri;
    private String dirPollingInterval;
    private String filePollingInterval;
    private String fileReadWaitTimeout;
    private long timeout = 5000;
    private boolean fileServerConnectorStarted = false;
    private ScheduledFuture scheduledFuture;
    private ConnectionCallback connectionCallback;
    private String headerPresent;
    private String readOnlyHeader;
    private String bufferSizeInBinaryChunked;
    private SourceMetrics metrics;
    private String cronExpression;
    private String fileNamePattern;
    private String fileSystemOptions;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<FileSourceState> init(SourceEventListener sourceEventListener,
                                              OptionHolder optionHolder,
                                              String[] requiredProperties,
                                              ConfigReader configReader,
                                              SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.siddhiAppContext = siddhiAppContext;
        this.requiredProperties = requiredProperties.clone();
        this.fileSourceConfiguration = new FileSourceConfiguration();
        this.fileSourceServiceProvider = FileSourceServiceProvider.getInstance();
        this.fileSystemConnectorFactory = fileSourceServiceProvider.getFileSystemConnectorFactory();
        this.fileSystemOptions = optionHolder.validateAndGetStaticValue(Constants.FILE_SYSTEM_OPTIONS, null);
        if (optionHolder.isOptionExists(Constants.DIR_URI)) {
            dirUri = optionHolder.validateAndGetStaticValue(Constants.DIR_URI);
            validateURL(dirUri, "dir.uri");
            FileObject listeningFileObject = Utils.getFileObject(dirUri, fileSystemOptions);
            uri = listeningFileObject.getName().getPath();
        }
        if (optionHolder.isOptionExists(Constants.FILE_URI)) {
            fileUri = optionHolder.validateAndGetStaticValue(Constants.FILE_URI);
            validateURL(fileUri, "file.uri");
            FileObject listeningFileObject = Utils.getFileObject(fileUri, fileSystemOptions);
            uri = listeningFileObject.getName().getPath();
        }

        if (dirUri != null && fileUri != null) {
            throw new SiddhiAppCreationException("Only one of directory uri or file uri should be provided. But both " +
                    "have been provided.");
        }
        if (dirUri == null && fileUri == null) {
            throw new SiddhiAppCreationException("Either directory uri or file uri must be provided. But none of them" +
                    "found.");
        }

        mode = optionHolder.validateAndGetStaticValue(Constants.MODE, Constants.LINE);
        List<Annotation> annotations = getMapper().getStreamDefinition().getAnnotations();
        annotations.forEach((Annotation annotation) -> {
            if (annotation.getName().equalsIgnoreCase(Constants.STREAM_DEFINITION_SOURCE_ANNOTATION_NAME)) {
                List<Element> sourceElements = annotation.getElements();
                sourceElements.forEach((element) -> {
                    if (element.getKey().equalsIgnoreCase(Constants.ANNOTATION_TYPE_ELEMENT_NAME)) {
                        String sourceType = element.getValue();
                        if (sourceType.equalsIgnoreCase(Constants.SOURCE_ANNOTATION_FILE_TYPE_NAME)) {
                            List<Annotation> sourceAnnotations = annotation.getAnnotations();
                            sourceAnnotations.forEach((sourceAnnotation) -> {
                                if (sourceAnnotation.getName().
                                        equalsIgnoreCase(Constants.STREAM_DEFINITION_MAP_ANNOTATION_NAME)) {
                                    List<Element> mapElements = sourceAnnotation.getElements();
                                    mapElements.forEach((mapElement) -> {
                                        if (mapElement.getKey().
                                                equalsIgnoreCase(Constants.ANNOTATION_TYPE_ELEMENT_NAME)) {
                                            String mapType = mapElement.getValue();
                                            if (mapType.equalsIgnoreCase(Constants.MAP_ANNOTATION_BINARY_TYPE)
                                                    && mode.equalsIgnoreCase(Constants.LINE)) {
                                                throw new SiddhiAppCreationException(
                                                        "'Binary' file mapping cannot be used with file mode '" +
                                                                Constants.BINARY_FULL + "'");
                                            }
                                        }
                                    });
                                }
                            });
                        }
                    }
                });
            }
        });

        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode) ||
                Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.FALSE);
        } else {
            tailing = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        }
        isTailingEnabled = Boolean.parseBoolean(tailing);

        if (isTailingEnabled) {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS,
                    Constants.NONE);
        } else {
            actionAfterProcess = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_PROCESS,
                    Constants.KEEP);
        }
        actionAfterFailure = optionHolder.validateAndGetStaticValue(Constants.ACTION_AFTER_FAILURE, Constants.DELETE);
        // TODO : When file.uri has been provided, the file uri should be provided for move.after.process parameter.
        // TODO : Fix this in carbon transport
        if (optionHolder.isOptionExists(Constants.MOVE_AFTER_PROCESS)) {
            moveAfterProcess = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_PROCESS);
            validateURL(moveAfterProcess, "moveAfterProcess");
        }
        if (optionHolder.isOptionExists(Constants.MOVE_AFTER_FAILURE)) {
            moveAfterFailure = optionHolder.validateAndGetStaticValue(Constants.MOVE_AFTER_FAILURE);
            validateURL(moveAfterFailure, "moveAfterFailure");
        }

        dirPollingInterval = optionHolder.validateAndGetStaticValue(Constants.DIRECTORY_POLLING_INTERVAL,
                "1000");
        filePollingInterval = optionHolder.validateAndGetStaticValue(Constants.FILE_POLLING_INTERVAL,
                "1000");

        String timeoutValue = optionHolder.validateAndGetStaticValue(Constants.TIMEOUT, "5000");
        try {
            timeout = Long.parseLong(timeoutValue);
        } catch (NumberFormatException e) {
            throw new SiddhiAppRuntimeException("Value provided for timeout, " + timeoutValue + " is invalid.", e);
        }
        beginRegex = optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null);
        endRegex = optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null);
        fileReadWaitTimeout = optionHolder.validateAndGetStaticValue(Constants.FILE_READ_WAIT_TIMEOUT, "1000");
        headerPresent = optionHolder.validateAndGetStaticValue(Constants.HEADER_PRESENT, "false");
        readOnlyHeader = optionHolder.validateAndGetStaticValue(Constants.READ_ONLY_HEADER, "false");
        bufferSizeInBinaryChunked = optionHolder.validateAndGetStaticValue(Constants.BUFFER_SIZE_IN_BINARY_CHUNKED,
                "65536");
        fileNamePattern = optionHolder.validateAndGetStaticValue(Constants.FILE_NAME_PATTERN, null);

        if (optionHolder.isOptionExists(Constants.CRON_EXPRESSION)) {
            cronExpression = optionHolder.validateAndGetStaticValue(Constants.CRON_EXPRESSION, null);
            if (!isValidExpression(cronExpression)) {
                throw new SiddhiAppCreationException("Cron Expression " + cronExpression + " is not valid.");
            }
        } else {
            cronExpression = null;
        }
        validateParameters();
        createInitialSourceConf();
        updateSourceConf();
        getPattern();
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        Constants.PROMETHEUS_REPORTER_NAME)) {
                    metrics = new SourceMetrics(siddhiAppContext.getName(), Utils.capitalizeFirstLetter(mode),
                            sourceEventListener.getStreamDefinition().getId());
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence file metrics will not be initialized.");
            }
        }
        return () -> new FileSourceState();
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, FileSourceState fileSourceState)
            throws ConnectionUnavailableException {
        this.connectionCallback = connectionCallback;
        if (metrics != null) {
            metrics.updateMetrics(siddhiAppContext.getExecutorService());
        }
        updateSourceConf();
        deployServers();
    }

    @Override
    public void disconnect() {
        try {
            fileSystemServerConnector = null;
            if (isTailingEnabled && fileSourceConfiguration.getFileServerConnector() != null) {
                fileSourceConfiguration.getFileServerConnector().stop();
                fileSourceConfiguration.setFileServerConnector(null);
            }
            ExecutorService executorService = fileSourceConfiguration.getExecutorService();
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
            }
            Scheduler scheduler = fileSourceConfiguration.getScheduler();
            if (scheduler != null) {
                scheduler.deleteJob(new JobKey(Constants.JOB_NAME, Constants.JOB_GROUP));
            }
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Failed to stop the file server when shutting down the siddhi app '" +
                    siddhiAppContext.getName() + "' due to " + e.getMessage(), e);
        } catch (SchedulerException e) {
            throw new SiddhiAppRuntimeException("Failed to delete the cron job of the siddhi app '" +
                    siddhiAppContext.getName() + "' due to " + e.getMessage(), e);
        }
    }

    public void destroy() {
    }

    public void pause() {
        try {
            if (isTailingEnabled && fileSourceConfiguration.getFileServerConnector() != null) {
                fileSourceConfiguration.getFileServerConnector().stop();
                this.fileServerConnectorStarted = false;
            }
            if (dirUri != null && scheduledFuture != null) {
                scheduledFuture.cancel(true);
            }
        } catch (ServerConnectorException e) {
            throw new SiddhiAppRuntimeException("Failed to stop the file server when pausing the siddhi app '" +
                    siddhiAppContext.getName() + "'.", e);
        }
    }

    public void resume() {
        try {
            updateSourceConf();
            deployServers();
        } catch (ConnectionUnavailableException e) {
            throw new SiddhiAppRuntimeException("Failed to resume siddhi app runtime.", e);
        }
    }

    private void createInitialSourceConf() {
        fileSourceConfiguration.setUri(uri);
        fileSourceConfiguration.setBeginRegex(beginRegex);
        fileSourceConfiguration.setEndRegex(endRegex);
        fileSourceConfiguration.setMode(mode);
        fileSourceConfiguration.setTailingEnabled(Boolean.parseBoolean(tailing));
        fileSourceConfiguration.setFilePollingInterval(filePollingInterval);
        fileSourceConfiguration.setRequiredProperties(requiredProperties);
        fileSourceConfiguration.setActionAfterProcess(actionAfterProcess);
        fileSourceConfiguration.setActionAfterFailure(actionAfterFailure);
        fileSourceConfiguration.setMoveAfterProcess(moveAfterProcess);
        fileSourceConfiguration.setTimeout(timeout);
        fileSourceConfiguration.setFileReadWaitTimeout(fileReadWaitTimeout);
        fileSourceConfiguration.setHeaderPresent(headerPresent);
        fileSourceConfiguration.setReadOnlyHeader(readOnlyHeader);
        fileSourceConfiguration.setBufferSize(bufferSizeInBinaryChunked);
        fileSourceConfiguration.setCronExpression(cronExpression);
    }

    private void updateSourceConf() {
        fileSourceConfiguration.setFilePointer(filePointer);
        fileSourceConfiguration.setTailedFileURIMap(tailedFileURIMap);
    }

    private Map<String, String> getFileSystemServerProperties() {
        Map<String, String> map = new HashMap<>();
        map.put(Constants.TRANSPORT_FILE_URI, dirUri);
        map.put(Constants.MODE, mode);
        if (actionAfterProcess != null) {
            map.put(Constants.ACTION_AFTER_PROCESS_KEY, actionAfterProcess.toUpperCase(Locale.ENGLISH));
        }
        map.put(Constants.MOVE_AFTER_PROCESS_KEY.toUpperCase(Locale.ENGLISH), moveAfterProcess);
        map.put(Constants.POLLING_INTERVAL, dirPollingInterval);
        map.put(Constants.FILE_SORT_ATTRIBUTE, Constants.NAME);
        map.put(Constants.FILE_SORT_ASCENDING, Constants.TRUE.toUpperCase(Locale.ENGLISH));
        map.put(Constants.CREATE_MOVE_DIR, Constants.TRUE.toUpperCase(Locale.ENGLISH));
        map.put(Constants.ACK_TIME_OUT, "5000");
        map.put(Constants.FILE_READ_WAIT_TIMEOUT_KEY, fileReadWaitTimeout);
        map.put(Constants.BUFFER_SIZE_IN_BINARY_CHUNKED, bufferSizeInBinaryChunked);
        map.put(Constants.CRON_EXPRESSION, cronExpression);
        map.put(Constants.FILE_NAME_PATTERN_PROPERTY_NAME, fileNamePattern);

        if (Constants.BINARY_FULL.equalsIgnoreCase(mode) ||
                Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE.toUpperCase(Locale.ENGLISH));
        } else {
            map.put(Constants.READ_FILE_FROM_BEGINNING, Constants.FALSE.toUpperCase(Locale.ENGLISH));
        }
        if (actionAfterFailure != null) {
            map.put(Constants.ACTION_AFTER_FAILURE_KEY, actionAfterFailure.toUpperCase(Locale.ENGLISH));
        }
        if (moveAfterFailure != null) {
            map.put(Constants.MOVE_AFTER_FAILURE_KEY, moveAfterFailure.toUpperCase(Locale.ENGLISH));
        }
        return map;
    }

    private void validateParameters() {
        if (Constants.TEXT_FULL.equalsIgnoreCase(mode) || Constants.BINARY_FULL.equalsIgnoreCase(mode) ||
                Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
            if (isTailingEnabled) {
                throw new SiddhiAppCreationException("In 'file' source of the siddhi app '" +
                        siddhiAppContext.getName() + "', tailing has been enabled by user or by default. " +
                        "But tailing can't be enabled in '" + mode + "' mode.");
            }

            if (Constants.BINARY_FULL.equalsIgnoreCase(mode) || Constants.BINARY_CHUNKED.equalsIgnoreCase(mode)) {
                if (beginRegex != null && endRegex != null) {
                    throw new SiddhiAppCreationException("'begin.regex' and 'end.regex' can be only provided if the" +
                            " mode is 'regex'. But in 'file' source of the siddhi app '" +
                            siddhiAppContext.getName() + "', provided mode is '" + mode + "'.");
                }
            }
        }

        if (isTailingEnabled && cronExpression != null) {
            throw new SiddhiAppCreationException("Tailing has been enabled by user or by default. " +
                    "'cron.expression' cannot be used when tailing is enabled. " +
                    "Hence stopping the siddhi app '" + siddhiAppContext.getName() + "'.");
        }

        if (isTailingEnabled && moveAfterProcess != null) {
            throw new SiddhiAppCreationException("Tailing has been enabled by user or by default." +
                    "'moveAfterProcess' cannot be used when tailing is enabled. " +
                    "Hence stopping the siddhi app '" + siddhiAppContext.getName() + "'.");
        }

        if (Constants.DELETE.equalsIgnoreCase(actionAfterProcess) && moveAfterProcess != null) {
            throw new SiddhiAppCreationException("'moveAfterProcess' can only be used when " +
                    "'action.after.process' is 'move'. But it has been used when 'action.after.process' is 'delete'." +
                    "Hence stopping the siddhi app '" + siddhiAppContext.getName() + "'.");
        }

        if (!(Constants.MOVE.equalsIgnoreCase(actionAfterProcess)) && (cronExpression != null)) {
            throw new SiddhiAppCreationException("'cronExpression' can only be used when 'action.after.process' " +
                    "is 'move'. Hence stopping the siddhi app '" + siddhiAppContext.getName() + "'.");
        }

        if (cronExpression != null && moveAfterProcess == null) {
            throw new SiddhiAppCreationException("'move.after.process' has not been provided where it is mandatory " +
                    "when 'cron.expression' is given. Hence stopping the siddhi app " +
                    siddhiAppContext.getName() + ".");
        }

        if (Constants.MOVE.equalsIgnoreCase(actionAfterProcess) && (moveAfterProcess == null)) {
            throw new SiddhiAppCreationException("'moveAfterProcess' has not been provided where it is mandatory when" +
                    " 'actionAfterProcess' is 'move'. Hence stopping the siddhi app '" +
                    siddhiAppContext.getName() + "'.");
        }

        if (Constants.REGEX.equalsIgnoreCase(mode)) {
            if (beginRegex == null && endRegex == null) {
                mode = Constants.LINE;
            }
        }
    }

    private void deployServers() throws ConnectionUnavailableException {
        ExecutorService executorService = siddhiAppContext.getExecutorService();
        createInitialSourceConf();
        fileSourceConfiguration.setExecutorService(executorService);
        if (fileSourceConfiguration.getCronExpression() != null) {
            scheduleJob(fileSourceConfiguration, sourceEventListener, siddhiAppContext);
        } else {
            if (dirUri != null) {
                Map<String, String> properties = getFileSystemServerProperties();
                FileSystemListener fileSystemListener = new FileSystemListener(sourceEventListener,
                        fileSourceConfiguration, metrics);
                try {
                    fileSystemServerConnector = fileSystemConnectorFactory.createServerConnector(
                            siddhiAppContext.getName(), properties, fileSystemListener);
                    fileSourceConfiguration.setFileSystemServerConnector(fileSystemServerConnector);
                    FileSourcePoller.CompletionCallback fileSourceCompletionCallback = (Throwable error) ->
                    {
                        if (error.getClass().equals(RemoteFileSystemConnectorException.class)) {
                            connectionCallback.onError(new ConnectionUnavailableException(
                                    "Connection to the file directory is lost.", error));
                        } else {
                            throw new SiddhiAppRuntimeException("File Polling mode run failed.", error);
                        }
                    };
                    FileSourcePoller fileSourcePoller =
                            new FileSourcePoller(fileSystemServerConnector, siddhiAppContext.getName());
                    fileSourcePoller.setCompletionCallback(fileSourceCompletionCallback);
                    this.scheduledFuture = siddhiAppContext.getScheduledExecutorService().
                            scheduleAtFixedRate(fileSourcePoller, 0, 1, TimeUnit.SECONDS);
                } catch (RemoteFileSystemConnectorException e) {
                    throw new ConnectionUnavailableException("Connection to the file directory is lost.", e);
                }
            } else if (fileUri != null && !fileServerConnectorStarted) {
                Map<String, String> properties = new HashMap<>();
                properties.put(Constants.ACTION, Constants.READ);
                properties.put(Constants.MAX_LINES_PER_POLL, "10");
                properties.put(Constants.POLLING_INTERVAL, filePollingInterval);
                properties.put(Constants.HEADER_PRESENT, headerPresent);
                properties.put(Constants.READ_ONLY_HEADER, readOnlyHeader);
                if (actionAfterFailure != null) {
                    properties.put(Constants.ACTION_AFTER_FAILURE_KEY, actionAfterFailure);
                }
                if (moveAfterFailure != null) {
                    properties.put(Constants.MOVE_AFTER_FAILURE_KEY, moveAfterFailure);
                }
                if (metrics != null) {
                    fileSourceConfiguration.setCurrentlyReadingFileURI(fileUri);
                    metrics.setFilePath(fileUri);
                    metrics.getSourceFileStatusMap().putIfAbsent((Utils.getShortFilePath(fileUri)),
                            StreamStatus.CONNECTING);
                    metrics.getStartedTimeMetric(System.currentTimeMillis());
                }
                if (fileSourceConfiguration.isTailingEnabled()) {
                    if (fileSourceConfiguration.getTailedFileURIMap() == null) {
                        fileSourceConfiguration.setTailedFileURI(fileUri);
                    }
                    if (fileSourceConfiguration.getTailedFileURIMap().get(0).toString().equalsIgnoreCase(fileUri)) {
                        properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
                        properties.put(Constants.PATH, fileUri);
                        FileServerConnectorProvider fileServerConnectorProvider =
                                fileSourceServiceProvider.getFileServerConnectorProvider();
                        FileProcessor fileProcessor = new FileProcessor(sourceEventListener,
                                fileSourceConfiguration, metrics);
                        final ServerConnector fileServerConnector = fileServerConnectorProvider
                                .createConnector("file-server-connector", properties);
                        fileServerConnector.setMessageProcessor(fileProcessor);
                        fileSourceConfiguration.setFileServerConnector((FileServerConnector) fileServerConnector);
                        Runnable runnableServer = () -> {
                            try {
                                fileServerConnector.start();
                            } catch (ServerConnectorException e) {
                                log.error(String.format("For the siddhi app '" + siddhiAppContext.getName() +
                                        ",' failed to start the server for file '%s'." +
                                        "Hence starting to process next file.", fileUri));
                            }
                        };
                        fileSourceConfiguration.getExecutorService().execute(runnableServer);
                        this.fileServerConnectorStarted = true;
                        if (metrics != null) {
                            metrics.getTailEnabledFilesMap().putIfAbsent(Utils.getShortFilePath(fileUri),
                                    System.currentTimeMillis());
                        }
                    }
                } else {
                    properties.put(Constants.URI, fileUri);
                    properties.put(Constants.ACK_TIME_OUT, "1000");
                    properties.put(Constants.MODE, fileSourceConfiguration.getMode());
                    properties.put(Constants.HEADER_PRESENT, headerPresent);
                    properties.put(Constants.READ_ONLY_HEADER, readOnlyHeader);
                    properties.put(Constants.BUFFER_SIZE_IN_BINARY_CHUNKED, bufferSizeInBinaryChunked);
                    VFSClientConnector vfsClientConnector = new VFSClientConnector();
                    Map<String, Object> schemeFileOptions = Utils.getFileSystemOptionObjectMap(fileUri,
                            fileSystemOptions);
                    try {
                        vfsClientConnector.init(null, null, schemeFileOptions);
                        FileProcessor fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration,
                                metrics);
                        vfsClientConnector.setMessageProcessor(fileProcessor);
                        VFSClientConnectorCallback vfsClientConnectorCallback = new VFSClientConnectorCallback();
                        Runnable runnableClient = () -> {
                            try {
                                vfsClientConnector.send(null, vfsClientConnectorCallback, properties);
                                vfsClientConnectorCallback.waitTillDone(timeout, fileUri);
                                if (actionAfterProcess != null) {
                                    properties.put(Constants.URI, fileUri);
                                    properties.put(Constants.ACTION, actionAfterProcess);
                                    if (moveAfterProcess != null) {
                                        properties.put(Constants.DESTINATION, moveAfterProcess);
                                    }
                                    vfsClientConnector.send(null, vfsClientConnectorCallback, properties);
                                    vfsClientConnectorCallback.waitTillDone(timeout, fileUri);
                                    if (metrics != null) {
                                        metrics.getSourceFileStatusMap().replace(Utils.getShortFilePath(fileUri),
                                                StreamStatus.COMPLETED);
                                        if (actionAfterProcess.equals(Constants.DELETE)) {
                                            metrics.getFileDeleteMetrics().setSource(Utils.getShortFilePath(fileUri));
                                            metrics.getFileDeleteMetrics().setTime(System.currentTimeMillis());
                                            metrics.getFileDeleteMetrics().getDeleteMetric(1);
                                        } else if (actionAfterProcess.equals(Constants.MOVE)) {
                                            metrics.getFileMoveMetrics().setTime(System.currentTimeMillis());
                                            metrics.getFileMoveMetrics().set_source(Utils.getShortFilePath(fileUri));
                                            metrics.getFileMoveMetrics().setDestination(Utils.getShortFilePath(
                                                    moveAfterProcess));
                                            metrics.getFileMoveMetrics().getMoveMetric(1);
                                        }
                                        metrics.setReadPercentage(100);
                                        metrics.getCompletedTimeMetric(System.currentTimeMillis());
                                    }
                                }
                            } catch (ClientConnectorException e) {
                                log.error(String.format("Failure occurred in vfs-client while reading the file '%s' " +
                                        "through siddhi app '%s'.", fileUri, siddhiAppContext.getName()), e);
                            } catch (InterruptedException e) {
                                log.error(String.format("Failed to get callback from vfs-client  for file '%s' " +
                                        "through siddhi app '%s'.", fileUri, siddhiAppContext.getName()), e);
                            }
                        };
                        fileSourceConfiguration.getExecutorService().execute(runnableClient);
                    } catch (ClientConnectorException e) {
                        log.error(String.format("Failure occurred when initializing vfs-client for the file '%s' " +
                                "through siddhi app '%s'.", fileUri, siddhiAppContext.getName()), e);
                    }
                }
                if (metrics != null) {
                    metrics.getSourceFileStatusMap().replace(Utils.getShortFilePath(fileUri),
                            StreamStatus.PROCESSING);
                }
            }
        }
    }

    private void getPattern() {
        String beginRegex = fileSourceConfiguration.getBeginRegex();
        String endRegex = fileSourceConfiguration.getEndRegex();
        Pattern pattern;
        try {
            if (beginRegex != null && endRegex != null) {
                pattern = Pattern.compile(beginRegex + "((.|\n)*?)" + endRegex);
            } else if (beginRegex != null) {
                pattern = Pattern.compile(beginRegex + "((.|\n)*?)" + beginRegex);
            } else if (endRegex != null) {
                pattern = Pattern.compile("((.|\n)*?)(" + endRegex + ")");
            } else {
                pattern = Pattern.compile("(\n$)"); // this will not be reached
            }
        } catch (PatternSyntaxException e) {
            throw new SiddhiAppCreationException("Cannot compile the regex '" + beginRegex +
                    "' and '" + endRegex + "'. Hence shutting down the siddhi app '" +
                    siddhiAppContext.getName() + "'.");
        }
        fileSourceConfiguration.setPattern(pattern);
    }

    private void validateURL(String uri, String parameterName) {
        try {
            if (uri.startsWith("sftp")) {
                uri = uri.replaceFirst("s", "");
            }
            new URL(uri);
            String splitRegex = File.separatorChar == '\\' ? "\\\\" : File.separator;
            fileSourceConfiguration.setProtocolForMoveAfterProcess(uri.split(splitRegex)[0]);
        } catch (MalformedURLException e) {
            throw new SiddhiAppCreationException(String.format("In 'file' source of siddhi app '" +
                            siddhiAppContext.getName() + "', provided uri for '%s' parameter '%s' is invalid.",
                    parameterName, uri), e);
        }
    }

    class FileSourceState extends State {

        private final Map<String, Object> state;

        private FileSourceState() {
            state = new HashMap<>();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            filePointer = FileSource.this.fileSourceConfiguration.getFilePointer();
            state.put(Constants.FILE_POINTER, fileSourceConfiguration.getFilePointer());
            state.put(Constants.TAILED_FILE, fileSourceConfiguration.getTailedFileURIMap());
            state.put(Constants.TAILING_REGEX_STRING_BUILDER,
                    fileSourceConfiguration.getTailingRegexStringBuilder());
            return state;
        }

        @Override
        public void restore(Map<String, Object> map) {
            filePointer = map.get(Constants.FILE_POINTER).toString();
            tailedFileURIMap = (List<String>) map.get(Constants.TAILED_FILE);
            fileSourceConfiguration.setFilePointer(filePointer);
            fileSourceConfiguration.setTailedFileURIMap(tailedFileURIMap);
            fileSourceConfiguration.updateTailingRegexStringBuilder(
                    (StringBuilder) map.get(Constants.TAILING_REGEX_STRING_BUILDER));
        }
    }
}
