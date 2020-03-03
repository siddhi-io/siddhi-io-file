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
                        defaultValue = "500"
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
    private long monitoringInterval = 500;
    private String uri;
    private String listeningDirUri;
    private String listeningFileUri;
    private FileAlterationMonitor monitor;
    private Map<String, Long> initialMap = new ConcurrentHashMap<>();
    private SiddhiAppContext siddhiAppContext;
    private List<SubThread> threadList = new ArrayList<>();
    private static final String CURRENT_MAP_KEY = "current.map.key";

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
            uri = optionHolder.validateAndGetStaticValue(Constants.URI);
            FileObject file = Utils.getFileObject(uri);
            try {
                if (!file.exists()) {
                    throw new SiddhiAppCreationException(" Directory/File " + file.getPublicURIString()
                            + " is not found ");
                }
                if (file.isFile()) {
                    listeningFileUri = file.getName().getPath();
                    file = file.getParent();
                }
                listeningDirUri = file.getName().getPath();
            } catch (FileSystemException e) {
                throw new SiddhiAppValidationException("Directory/File " + file.getPublicURIString()
                        + " is not found.", e);
            }
        }
        if (uri == null) {
            throw new SiddhiAppCreationException("uri is not found.");
        }
        String monitoringValue = optionHolder.validateAndGetStaticValue(Constants.MONITORING_INTERVAL, "500");
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
    public void connect(ConnectionCallback connectionCallback,
                        FileHandler.FileHandlerState fileHandlerState) {
        initiateFileAlterationObserver();
        File[] listOfFiles = new File(listeningDirUri).listFiles();
        if (listOfFiles != null) {
            for (File listOfFile : listOfFiles) {
                initialMap.put(listOfFile.getName(), listOfFile.length());
                sourceEventListener.onEvent(getFileHandlerEvent(listOfFile, listeningFileUri,
                        STATUS_DONE), null);
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
            sourceEventListener.onEvent
                    (getFileHandlerEvent(directory, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onDirectoryChange(final File directory) {
            log.debug(directory.getAbsolutePath() + " was modified");
            synchronized (this) {
                if (!initialMap.containsKey(directory.getName())) {
                    initialMap.put(directory.getAbsolutePath(), directory.length());
                    sourceEventListener.onEvent
                            (getFileHandlerEvent(directory, listeningFileUri, STATUS_PROCESS),
                                    null);
                    SubThread subThread = new SubThread(directory);
                    siddhiAppContext.getScheduledExecutorService().schedule(subThread,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(subThread);
                }
            }
        }

        @Override
        public void onDirectoryDelete(final File directory) {
            log.debug(directory.getAbsolutePath() + " was deleted.");
            sourceEventListener.onEvent
                    (getFileHandlerEvent(directory, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onFileCreate(final File file) {
            log.debug(file.getAbsolutePath() + " was created.");
            sourceEventListener.onEvent
                    (getFileHandlerEvent(file, listeningFileUri, STATUS_DONE), null);
        }

        @Override
        public void onFileChange(final File file) {
            log.info(file.getAbsolutePath() + " was modified.");
            synchronized (this) {
                if (!initialMap.containsKey(file.getAbsolutePath())) {
                    initialMap.put(file.getAbsolutePath(), file.length());
                    sourceEventListener.onEvent
                            (getFileHandlerEvent(file, listeningFileUri, STATUS_PROCESS), null);
                    SubThread subThread = new SubThread(file);
                    siddhiAppContext.getScheduledExecutorService().schedule(subThread,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(subThread);
                }
            }
        }

        @Override
        public void onFileDelete(final File file) {
            log.debug(file.getAbsolutePath() + " was deleted.");
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
            log.info("Directory monitoring has been started for folder : " + uri + " .");
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception occurred when starting server to monitor " + uri + ".", e);
        }
    }

    @Override
    public void disconnect() {

        if (monitor != null) {
            try {
                monitor.stop();
                initialMap.clear();
                log.info("Directory monitoring has been stopped for folder : " + uri + " .");
            } catch (Exception e) {
                log.error("Exception occurred when stopping server  while monitoring " + uri + " .", e);
            }
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public void pause() {
        if (monitor != null) {
            threadList.forEach(SubThread::pause);
            log.info("Directory monitoring has been paused for folder : " + uri + " .");
        }
    }

    @Override
    public void resume() {
        if (monitor != null) {
            threadList.forEach(SubThread::resume);
            log.info("Directory monitoring has been resumed for folder : " + uri + " .");
        }
    }

    /**
     * Implementation of SubThread
     */
    public class SubThread implements Runnable {
        String status;
        private final File file;
        private volatile boolean paused;
        private ReentrantLock lock;
        private Condition condition;
        private volatile boolean inactive;

        SubThread(File file) {
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
                long val = initialMap.get(file.getAbsolutePath());
                if (val < file.length()) {
                    status = STATUS_PROCESS;
                    initialMap.put(file.getAbsolutePath(), file.length());
                    SubThread subThread = new SubThread(file);
                    siddhiAppContext.getScheduledExecutorService().schedule(subThread,
                            monitoringInterval, TimeUnit.MILLISECONDS);
                    threadList.add(subThread);
                } else {
                    status = STATUS_DONE;
                    initialMap.remove(file.getAbsolutePath());
                }
                sourceEventListener.onEvent
                        (getFileHandlerEvent(file, listeningFileUri, status), null);
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
        public static class FileHandlerState extends State {

            private  Map<String, Long> stateMap = new HashMap<>();

            @Override
            public boolean canDestroy() {
                return false;
            }

            @Override
            public Map<String, Object> snapshot() {
                Map<String, Object> currentState = new HashMap<>();
                currentState.put(CURRENT_MAP_KEY, stateMap);
                return currentState;
            }

            @Override
            public void restore(Map<String, Object> state) {
                stateMap = (Map<String, Long>) state.get(CURRENT_MAP_KEY);
            }
        }
    }
