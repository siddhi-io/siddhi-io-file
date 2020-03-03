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
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.siddhi.extension.io.file.util.Util.getFileHandlerEvent;
import static io.siddhi.extension.util.Constant.STATUS_DONE;
import static io.siddhi.extension.util.Constant.STATUS_NEW;
import static io.siddhi.extension.util.Constant.STATUS_PROCESS;


/**
 * Implementation of siddhi-io-file source.
 */
@Extension(
        name = "fileeventlistener",
        namespace = "source",
        description = "" +
                "Fileeventlistener provides the functionality for user to get the details of files " +
                "which have been created or modified or deleted in the execution time" ,

        parameters = {
                @Parameter(
                        name = "uri",
                        description =
                                "Used to specify a file to be processed. \nThe 'uri'  should be provided.\n" +
                                        "This uri MUST have the respective protocol specified.\n",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "monitoring.interval",
                        description = "This parameter is used to specify the time interval (in milliseconds) " +
                                " that the process monitor the changes for.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "1000"
                ),
        },
        examples = {
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener', uri='file://abc/xyz.txt',@map(type='json')) \n" +
                                "define stream CreateFileStream (filepath string, length long, " +
                                "last_modified string, status string);" +
                                "@sink(type='log')" +
                                "define stream FooStream (filepath string, length int, " +
                                "last_modified string, status string); \n",

                        description = "" +
                                "Under above configuration, After monitoring has been started, If the specific " +
                                "file given in the uri gets modified or deleted. an event is created" +
                                " with the filepath, length, last modified date and status of the file. " +
                                "Then that will be received by the FooStream..\n"
                ),
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener',uri='file://abc/xyz',@map(type='json')) \n" +
                                "define stream CreateFileStream (filepath string, length long, " +
                                "last_modified string, status string);\n" +
                                "@sink(type='log')" +
                                "define stream FooStream (filepath string, length int, " +
                                "last_modified string, status string); \n",

                        description = "" +
                                "Under above configuration, all the files in the specific uri " +
                                "which has been  modified or created or deleted in the execution time " +
                                "will be identified and displayed.\n After monitoring has been started, " +
                                "If any files in the specific uri is newly created, modified or deleted.\n " +
                                "an event is created with the filepath, length, last modified date " +
                                "and status of the file. Then that will be received by the FooStream..\n"
                ),
                @Example(
                        syntax = "" +
                                "@source(type='fileeventlistener',uri='file://abc/xyz', monitoring.interval='200', " +
                                "@map(type='json')) \n" +
                                "define stream CreateFileStream (filepath string, length long, " +
                                "last_modified string, status string);\n" +
                                "@sink(type='log')" +
                                "define stream FooStream (filepath string, length int, " +
                                "last_modified string, status string); \n",

                        description = "" +
                                "Under above configuration, all the files in the specific uri \n" +
                                "which has been  modified or created or deleted in the execution time " +
                                "will be identified and displayed. \n After monitoring has been started," +
                                " If any files in the specific uri is newly created, modified or deleted.\n " +
                                "an event is created with the filepath, length, last modified date " +
                                "and status of the file. Then that will be received by the FooStream. If there " +
                                "are any changes a new event will be generated in every 200 milliseconds.\n"
                ),

        }
)

public class FileHandler extends Source<FileHandler.FileHandlerState> {
    private static final Logger log = Logger.getLogger(FileHandler.class);
    private SourceEventListener sourceEventListener;
    private long monitoringInterval = 1000;
    private String listeningUri;
    private String listeningDirUri;
    private String listeningFileUri;
    private FileAlterationMonitor monitor;
    private Map<String, Long> initialMap = new ConcurrentHashMap<>();
    private Map<String, Long> onChangeMap = new ConcurrentHashMap<>();
    private SiddhiAppContext siddhiAppContext;
    private List<ThreadOnContinuousChange> threadList = new ArrayList<>();
    private static final String CURRENT_MAP_KEY = "current.map.key";
    private  Map<String, Long> stateMap = new HashMap<>();

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public StateFactory<FileHandlerState> init(SourceEventListener sourceEventListener,
                                               OptionHolder optionHolder,
                                               String[] requiredProperties,
                                               ConfigReader configReader,
                                               SiddhiAppContext siddhiAppContext) throws SiddhiAppValidationException {
        this.siddhiAppContext = siddhiAppContext;
        this.sourceEventListener = sourceEventListener;
        if (optionHolder.isOptionExists(Constants.URI)) {
            listeningUri = optionHolder.validateAndGetStaticValue(Constants.URI);
        }
        if (listeningUri == null) {
            throw new SiddhiAppCreationException(" uri must be provided.");
        }
        FileObject listeningFileObject = Utils.getFileObject(listeningUri);
        try {
            if (!listeningFileObject.exists()) {
                throw new SiddhiAppCreationException(" Directory/File " + listeningFileObject.getPublicURIString()
                        + " is not found ");
            }
            if (listeningFileObject.isFile()) {
                listeningFileUri = listeningFileObject.getName().getPath();
                listeningFileObject = listeningFileObject.getParent();
            }
            listeningDirUri = listeningFileObject.getName().getPath();
        } catch (FileSystemException e) {
            throw new SiddhiAppValidationException("Directory/File " + listeningFileObject.getPublicURIString()
                    + " is not found.", e);
        }

        String monitoringValue = optionHolder.validateAndGetStaticValue(Constants.MONITORING_INTERVAL, "1000");
        try {
            monitoringInterval = Long.parseLong(monitoringValue);
        } catch (NumberFormatException e) {
            throw new SiddhiAppRuntimeException("Value provided for monitoring " + monitoringValue +
                    " is invalid.", e);
        }
        return FileHandlerState::new;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, FileHandler.FileHandlerState fileHandlerState) {
        initiateFileAlterationObserver();
        File[] listOfFiles = new File(listeningDirUri).listFiles();
        if (listOfFiles != null) {
            for (File listOfFile : listOfFiles) {
                initialMap.put(listOfFile.getName(), listOfFile.length());
                if (!stateMap.containsKey(listOfFile.getName())) {
                    sourceEventListener.onEvent(getFileHandlerEvent(listOfFile, listeningFileUri,
                            STATUS_NEW), null);
                }
            }
        }
    }

    /**
     * Implementation of  FileAlterationImpl
     */
    public class FileAlterationImpl implements FileAlterationListener {

        @Override
        public void onStart(final FileAlterationObserver observer) {
        }

        @Override
        public void onDirectoryCreate(final File directory) {
            log.debug(directory.getAbsolutePath() + " was created.");
            initialMap.put(directory.getName(), directory.length());
            sourceEventListener.onEvent
                    (getFileHandlerEvent(directory, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onDirectoryChange(final File directory) {
            log.debug(directory.getAbsolutePath() + " was modified");
            synchronized (this) {
                if (!onChangeMap.containsKey(directory.getName())) {
                    initialMap.put(directory.getName(), directory.length());
                    sourceEventListener.onEvent
                            (getFileHandlerEvent(directory, listeningFileUri, STATUS_PROCESS),
                                    null);
                    ThreadOnContinuousChange threadOnContinuousChange = new ThreadOnContinuousChange(directory);
                    siddhiAppContext.getScheduledExecutorService().schedule(threadOnContinuousChange,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(threadOnContinuousChange);
                }
            }
        }

        @Override
        public void onDirectoryDelete(final File directory) {
            log.debug(directory.getAbsolutePath() + " was deleted.");
            initialMap.remove(directory.getName(), directory.length());
            sourceEventListener.onEvent
                    (getFileHandlerEvent(directory, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onFileCreate(final File file) {
            log.debug(file.getAbsolutePath() + " was created.");
            initialMap.put(file.getName(), file.length());
            sourceEventListener.onEvent
                    (getFileHandlerEvent(file, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onFileChange(final File file) {
            log.debug(file.getAbsolutePath() + " was modified.");
            synchronized (this) {
                if (!onChangeMap.containsKey(file.getName())) {
                    onChangeMap.put(file.getName(), file.length());
                    sourceEventListener.onEvent
                            (getFileHandlerEvent(file, listeningFileUri, STATUS_PROCESS), null);
                    ThreadOnContinuousChange threadOnContinuousChange = new ThreadOnContinuousChange(file);
                    siddhiAppContext.getScheduledExecutorService().schedule(threadOnContinuousChange,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(threadOnContinuousChange);
                }
            }
        }

        @Override
        public void onFileDelete(final File file) {
            log.debug(file.getAbsolutePath() + " was deleted.");
            initialMap.remove(file.getName());
            sourceEventListener.onEvent
                    (getFileHandlerEvent(file, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onStop(final FileAlterationObserver observer) {

        }
    }

    public void initiateFileAlterationObserver() {
        FileAlterationObserver observer = new FileAlterationObserver(listeningDirUri);
        observer.addListener(new FileAlterationImpl());
        monitor = new FileAlterationMonitor(monitoringInterval);
        monitor.addObserver(observer);
        try {
            monitor.start();
            log.debug("Directory monitoring has been started for folder : " + listeningUri + " .");
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception occurred when starting server to monitor "
                    + listeningUri + ".", e);
        }
    }

    @Override
    public void disconnect() {
        if (monitor != null) {
            try {
                monitor.stop();
                initialMap.clear();
                log.debug("Directory monitoring has been stopped for folder : " + listeningUri + " .");
            } catch (Exception e) {
                log.error("Exception occurred when stopping server  while monitoring " + listeningUri + " .", e);
            }
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public void pause() {
        if (monitor != null) {
            threadList.forEach(ThreadOnContinuousChange::pause);
            log.debug("Directory monitoring has been paused for folder : " + listeningUri + " .");
        }
    }

    @Override
    public void resume() {
        if (monitor != null) {
            threadList.forEach(ThreadOnContinuousChange::resume);
            log.debug("Directory monitoring has been resumed for folder : " + listeningUri + " .");
        }
    }

    /**
     * Implementation of SubThread
     */
    public class ThreadOnContinuousChange implements Runnable {
        String status;
        private final File file;
        private volatile boolean paused;
        private ReentrantLock lock;
        private Condition condition;
        private volatile boolean inactive;

        ThreadOnContinuousChange(File file) {
            this.file = file;
            inactive = false;
            lock = new ReentrantLock();
            condition = lock.newCondition();
        }

        @Override
        public void run() {
            while (!inactive) {
                if (paused) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
                long val = onChangeMap.get(file.getName());
                if (val < file.length()) {
                    status = STATUS_PROCESS;
                    onChangeMap.put(file.getName(), file.length());
                    ThreadOnContinuousChange threadOnContinuousChange = new ThreadOnContinuousChange(file);
                    siddhiAppContext.getScheduledExecutorService().schedule(threadOnContinuousChange,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(threadOnContinuousChange);
                } else {
                    status = STATUS_DONE;
                    onChangeMap.remove(file.getName());
                }
                sourceEventListener.onEvent(getFileHandlerEvent(file, listeningFileUri, status), null);
            }
        }

        public void pause() {
            paused = true;
        }

        public void resume() {
            paused = false;
            try {
                lock.lock();
                condition.signalAll();
            } finally {
                lock.unlock();
            }
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
            currentState.put(CURRENT_MAP_KEY, initialMap);
            return currentState;
        }

        @Override
        public void restore(Map<String, Object> state) {
            stateMap = (Map<String, Long>) state.get(CURRENT_MAP_KEY);
        }
    }
}
