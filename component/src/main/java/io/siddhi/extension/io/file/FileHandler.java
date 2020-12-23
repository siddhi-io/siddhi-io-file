/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.Status;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.siddhi.extension.io.file.util.Util.getFileHandlerEvent;
/**
 * Implementation of siddhi-io-file Event Listener.
 * FileEventListener  provides the functionality for user to get the details of files which have been created,
 * modified or deleted in the execution time.
 */
@Extension(
        name = "fileeventlistener" ,
        namespace = "source" ,
        description = "" +
                "The 'fileeventlistener' component of the 'siddhi-io-fie' extension allows you to get the details of" +
                " files that have been created, modified or deleted during execution time.Supports listening to local "
                + "folder/file paths." ,
        parameters = {
                @Parameter(
                        name = "dir.uri",
                        description =
                                "The path to the directory to be processed. During execution time, Siddhi by default" +
                                        " processes all the files within this directory. However, if you have entered" +
                                        " specific files to be processed via the 'file.name.list' parameter, only " +
                                        "those files are processed. The URI specified must " +
                                        "include the file handling protocol to be used for file processing.\n",
                        optional = false,
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "monitoring.interval",
                        description =
                                "The time duration (in milliseconds) for which the system must monitor changes to" +
                                        " the files in the specified directory.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "100"
                ),
                @Parameter(
                        name = "file.name.list",
                        description = "If you want to carry out processing for only for one or more specific files in" +
                                " the the given directory URI, you can use this parameter to specify those files as " +
                                "a comma-separated list. \n" +
                                "e.g., 'abc.txt,xyz.csv'",
                        optional = true,
                        type = {DataType.STRING},
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. " +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener', dir.uri='file://abc/xyz, file.name.list = " +
                                "'xyz.txt, test') \n" +
                                "define stream FileListenerStream (filepath string, filename string, " +
                                "status string);\n" +
                                "@sink(type='log')\n" +
                                "define stream FooStream (filepath string, filename string, status string); \n" +
                                "from FileListenerStream\n" +
                                "select *\n" +
                                "insert into FooStream;",

                        description = "" +
                                "In the above configuration, the system monitors the given directory URI to check " +
                                "whether any file named either 'xyz.txt' or 'test' gets created, modified or deleted." +
                                " If any such activity is detected, an input event is generated in the 'FooStream' " +
                                "stream. The information included in the event are the filepath, filename, and the " +
                                "status of the file.\n"
                ),
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener',dir.uri='file://abc/xyz') \n" +
                                "define stream FileListenerStream (filepath string, filename string, " +
                                "status string);\n" +
                                "@sink(type='log')\n" +
                                "define stream FooStream (filepath string, filename string, status string); \n" +
                                "from FileListenerStream\n" +
                                "select *\n" +
                                "insert into FooStream;",

                        description = "" +
                                "In the above configuration, the system monitors the given directory URI to check " +
                                "whether any file gets created, modified or deleted. If any such activity is " +
                                "detected, an input event is generated in the 'FooStream' stream. The information " +
                                "included in the event are the filepath, filename, and the status of the file.\n"
                ),
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener',dir.uri='file://abc/xyz', " +
                                "monitoring.interval='200')\n" +
                                "define stream FileListenerStream (filepath string, filename string, " +
                                "status string);\n" +
                                "@sink(type='log')\n" +
                                "define stream FooStream (filepath string, filename string, status string);\n" +
                                "from FileListenerStream\n" +
                                "select *\n" +
                                "insert into FooStream;",

                        description = "" +
                                "In the above configuration, the system monitors the given directory URI every 200" +
                                " milliseconds to check whether any file gets created, modified or deleted. If any" +
                                " such activity is detected, an input event is generated in the 'FooStream' stream. " +
                                "The information included in the event are the filepath, filename, and the status of" +
                                " the file.\n"
                ),
        }
)

public class FileHandler extends Source<FileHandler.FileHandlerState> {
    private static final Logger log = Logger.getLogger(FileHandler.class);
    private static final String EMPTY_STRING = "";
    private SourceEventListener sourceEventListener;
    private long monitoringInterval = 100;
    private String listeningDirUri;
    private FileAlterationMonitor monitor;
    private Map<String, Long> fileObjectMap = new ConcurrentHashMap<>();
    private static final String CURRENT_MAP_KEY = "current.map.key";
    private List<String> fileObjectList;
    private String fileSystemOptions;

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<FileHandlerState> init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                                               String[] requiredProperties, ConfigReader configReader,
                                               SiddhiAppContext siddhiAppContext) throws SiddhiAppValidationException {
        this.sourceEventListener = sourceEventListener;
        String tmpURL;
        if (optionHolder.isOptionExists(Constants.DIR_URI)) {
            listeningDirUri = optionHolder.validateAndGetStaticValue(Constants.DIR_URI);
        }
        this.fileSystemOptions = optionHolder.validateAndGetStaticValue(Constants.FILE_SYSTEM_OPTIONS, null);
        //Validation for URI
        if (listeningDirUri == null || listeningDirUri.isEmpty()) {
            throw new SiddhiAppCreationException("URI must be provided.");
        }
        FileObject listeningFileObject = Utils.getFileObject(listeningDirUri, fileSystemOptions);
        try {
            if (!listeningFileObject.exists()) {
                throw new SiddhiAppCreationException("Directory " + listeningFileObject.getPublicURIString()
                        + " is not found.");
            }
            if (listeningFileObject.isFile()) {
                throw new SiddhiAppCreationException("URI must belongs to a folder");
            }
            tmpURL = listeningDirUri;
            listeningDirUri = listeningFileObject.getName().getPath();
        } catch (FileSystemException e) {
            throw new SiddhiAppValidationException("Directory " + listeningFileObject.getPublicURIString()
                    + " is not found.", e);
        }

        // Validation for fileNameList
        String fileNameList = optionHolder.validateAndGetStaticValue(Constants.FILE_NAME_LIST, EMPTY_STRING);
        fileNameList = fileNameList.replaceAll("\\s", "");
        fileObjectList = Arrays.asList(fileNameList.split(","));
        for (int i = 0; i < fileObjectList.size(); i++) {
            String fileObjectPath = tmpURL + File.separator + fileObjectList.get(i);
            listeningFileObject = Utils.getFileObject(fileObjectPath, fileSystemOptions);
            String filePath = null;
            try {
                filePath = listeningFileObject.getURL().toURI().toURL().getPath();
                if (!filePath.contains(File.separator)) {
                    //this flow will only executed on windows environment
                    filePath = filePath.replace("/", File.separator).substring(1);
                }
                if (!listeningFileObject.exists()) {
                    throw new SiddhiAppCreationException("File/Folder " +
                            listeningFileObject.getPublicURIString() + " is not found.");
                }
            } catch (FileSystemException e) {
                log.error("File/Folder " + listeningFileObject.getPublicURIString() + " is not found.", e);
            } catch (MalformedURLException e) {
                throw new SiddhiAppCreationException("Path  " + fileObjectPath + " is malformed." + e.getMessage(), e);
            } catch (URISyntaxException e) {
                throw new SiddhiAppCreationException("Path " + fileObjectPath + " syntax is not correct." +
                        e.getMessage(), e);
            }
            if (filePath != null) {
                fileObjectList.set(i, filePath);
            }
        }
        // Validation for MonitoringInterval
        String monitoringValue = optionHolder.validateAndGetStaticValue(Constants.MONITORING_INTERVAL, "100");
        try {
            monitoringInterval = Long.parseLong(monitoringValue);
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Value provided for monitoring, " + monitoringValue +
                    " is invalid.", e);
        }
        return FileHandlerState::new;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Event.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, FileHandler.FileHandlerState fileHandlerState) {
        initiateFileAlterationObserver();
        File[] listOfFiles = new File(listeningDirUri).listFiles();
        if (listOfFiles != null) {
            for (File listOfFile : listOfFiles) {
                //If the file is in list of files and not in initial map add it to the map
                if (!fileObjectMap.containsKey(listOfFile.getAbsolutePath())) {
                    fileObjectMap.put(listOfFile.getAbsolutePath(), listOfFile.length());
                    sourceEventListener.onEvent(getFileHandlerEvent(listOfFile, fileObjectList, Status.STATUS_NEW),
                            null);
                }
            }
            for (Map.Entry<String, Long> entry : fileObjectMap.entrySet()) {
                //If the file is not in the list of files and it is in the initial map it has to be removed from the
                // map
                List<File> fileList = Arrays.asList(listOfFiles);
                File fileObjectMapEntry = new File(entry.getKey());
                if (!fileList.contains(fileObjectMapEntry)) {
                    fileObjectMap.remove(fileObjectMapEntry.getAbsolutePath());
                    sourceEventListener.onEvent(getFileHandlerEvent(fileObjectMapEntry, fileObjectList,
                            Status.STATUS_REMOVE), null);
                }
            }
        }
    }

    public void initiateFileAlterationObserver() {
        FileAlterationObserver observer = new FileAlterationObserver(listeningDirUri);
        observer.addListener(new FileAlterationImpl(sourceEventListener, fileObjectList));
        monitor = new FileAlterationMonitor(monitoringInterval);
        monitor.addObserver(observer);
        try {
            monitor.start();
            log.debug("Directory monitoring has been started for folder/file : " + listeningDirUri + " .");
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception occurred when starting server to monitor "
                    + listeningDirUri + ".", e);
        }
    }

    @Override
    public void disconnect() {
        if (monitor != null) {
            try {
                monitor.stop();
                fileObjectMap.clear();
                log.debug("Directory monitoring has been stopped for folder/file : " + listeningDirUri + " .");
            } catch (Exception e) {
                log.error("Exception occurred when stopping server while monitoring " + listeningDirUri + " .", e);
            }
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public void pause() {
        if (monitor != null) {
            log.debug("Directory monitoring has been paused for folder/file : " + listeningDirUri + " .");
        }
    }

    @Override
    public void resume() {
        if (monitor != null) {
            log.debug("Directory monitoring has been resumed for folder/file : " + listeningDirUri + " .");
        }
    }

    /**
     * State class for  FileHandler
     */
    public  class FileHandlerState extends State {

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> currentState = new HashMap<>();
            currentState.put(CURRENT_MAP_KEY, fileObjectMap);
            return currentState;
        }

        @Override
        public void restore(Map<String, Object> state) {
            fileObjectMap = (Map<String, Long>) state.get(CURRENT_MAP_KEY);
        }
    }
}
