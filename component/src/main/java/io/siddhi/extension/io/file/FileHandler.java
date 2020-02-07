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
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.FileSourceServiceProvider;
import io.siddhi.extension.util.Utils;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.wso2.transport.remotefilesystem.RemoteFileSystemConnectorFactory;
import org.wso2.transport.remotefilesystem.server.connector.contract.RemoteFileSystemServerConnector;


import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * Implementation of siddhi-io-file source.
 */
@Extension(
        name = "file_handler",
        namespace = "source",
        description = "" +
                "File Source provides the functionality for user to feed data to siddhi from " +
                "files. Both text and binary files are supported by file source.",
        parameters = {
                @Parameter(
                        name = "dir.uri",
                        description =
                                "Used to specify a directory to be processed. \n" +
                                        "All the files inside this directory will be processed. \n" +
                                        "Only one of 'dir.uri' and 'file.uri' should be provided.\n" +
                                        "This uri MUST have the respective protocol specified.",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "file.uri",
                        description =
                                "Used to specify a file to be processed. \n" +
                                        " Only one of 'dir.uri' and 'file.uri' should be provided.\n" +
                                        "This uri MUST have the respective protocol specified.\n",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "timeout",
                        description = "This parameter is used to specify the maximum time period (in milliseconds) " +
                                " for waiting until a file is processed.\n",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "5000"
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
                                "Under above configuration, all the files in directory will be " +
                                "picked and read one by one.\n" +
                                "In this case, it's assumed that all the files contains json valid json" +
                                " strings with keys 'symbol','price' and 'volume'.\n" +
                                "Once a file is read, " +
                                "its content will be converted to an event using siddhi-map-json " +
                                "extension and then, that event will be received to the FooStream.\n" +
                                "Finally, after reading is finished, the file will be deleted.\n"
                ),

        }
)
public class FileHandler extends Source<FileHandler.FileSourceState> {
    private static final Logger log = Logger.getLogger(FileHandler.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private RemoteFileSystemConnectorFactory fileSystemConnectorFactory;
    private FileSourceServiceProvider fileSourceServiceProvider;
    private RemoteFileSystemServerConnector fileSystemServerConnector;
    private String filePointer = "0";
    private String[] requiredProperties;
    private boolean isTailingEnabled = true;
    private SiddhiAppContext siddhiAppContext;
    private List<String> tailedFileURIMap;
    private String dirUri;
    private String fileUri;
    private String uri;
    private String dirPollingInterval;
    private String filePollingInterval;
    private String fileReadWaitTimeout;

    private long timeout = 5000;
    private boolean fileServerConnectorStarted = false;
    private ScheduledFuture scheduledFuture;
    private ConnectionCallback connectionCallback;
    private FileAlterationMonitor monitor;

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
        if (optionHolder.isOptionExists(Constants.DIR_URI)) {
            dirUri = optionHolder.validateAndGetStaticValue(Constants.DIR_URI);
            FileObject directory = Utils.getFileObject(dirUri);
            try {
                validateUri(directory);
            } catch (FileSystemException e) {
                log.error("Directory is not found");
            }
        }
        if (optionHolder.isOptionExists(Constants.FILE_URI)) {
            fileUri = optionHolder.validateAndGetStaticValue(Constants.FILE_URI);
            FileObject directory = Utils.getFileObject(fileUri);
            try {
                validateUri(directory);
            } catch (FileSystemException e) {
                log.error("File is not found");
            }
        }
        if (dirUri != null && fileUri != null) {
            throw new SiddhiAppCreationException("Only one of directory uri or file uri" +
                    " should be provided. But both have been provided.");
        }
        if (dirUri == null && fileUri == null) {
            throw new SiddhiAppCreationException("Either directory uri or file uri must " +
                    "be provided. But none of them found.");
        }
        if (dirUri != null) {
            uri = dirUri;
        } else {
            uri = fileUri;
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

        return () -> new FileSourceState();
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class, byte[].class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback,
                        FileSourceState fileSourceState) {
        try {
            usingFileAlterationObserver(sourceEventListener);
        } catch (IOException e) {
            log.info(e.getMessage());
        }
    }


    /**
     * Implementation of siddhi-io-file source.
     */

    public static class FileAlterationImpl implements FileAlterationListener {


        private SourceEventListener sourceEventListener;

        public FileAlterationImpl(SourceEventListener sourceEventListener) {
            this.sourceEventListener = sourceEventListener;
        }

        @Override
        public void onStart(final FileAlterationObserver observer) {
        }

        @Override
        public void onDirectoryCreate(final File directory) {
            log.info(directory.getAbsolutePath() + " was created.");
            String status = "Done";
            JSONObject obj = new JSONObject();
            obj.put("filepath", directory.getAbsoluteFile());
            obj.put("length", directory.length());
            obj.put("last_modified", new Date(directory.lastModified()));
            obj.put("status", status);
            String properties = "filepath:" + directory.getAbsoluteFile() + " length:" + directory.length() +
                    "last_modified" + new  Date(directory.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onDirectoryChange(final File directory) {
            String status = null;
            log.info(directory.getAbsolutePath() + " was modified");
            JSONObject obj = new JSONObject();
            obj.put("filepath", directory.getAbsoluteFile());
            obj.put("length", directory.length());
            obj.put("last_modified", new Date(directory.lastModified()));
            Long fileSizeBefore = directory.length();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e);
            }
            Long fileSizeAfter = directory.length();
            if (fileSizeBefore.equals(fileSizeAfter)) {
                status = "Done";
            } else {
                status = "In Process";
            }
            obj.put("status", status);
            String properties = "filepath:" + directory.getAbsoluteFile() + " length:" + directory.length() +
                    "last_modified" + new  Date(directory.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onDirectoryDelete(final File directory) {
            log.info(directory.getAbsolutePath() + " was deleted.");
            String status = "Done";
            JSONObject obj = new JSONObject();
            obj.put("filepath", directory.getAbsoluteFile());
            obj.put("length", directory.length());
            obj.put("last_modified", new Date(directory.lastModified()));
            obj.put("status", status);
            String properties = "filepath:" + directory.getAbsoluteFile() + " length:" + directory.length() +
                    "last_modified" + new  Date(directory.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onFileCreate(final File file) {
            log.info(file.getAbsoluteFile() + " was created.");
            JSONObject obj = new JSONObject();
            String status = "Done";
            obj.put("filepath", file.getAbsoluteFile());
            obj.put("length", file.length());
            obj.put("last_modified", new Date(file.lastModified()));
            obj.put("status", status);
            String properties = "filepath:" + file.getAbsoluteFile() + " length:" + file.length() +
                    "last_modified : " + new Date(file.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onFileChange(final File file) {
            String status = null;
            log.info(file.getAbsoluteFile() + " was modified.");
            JSONObject obj = new JSONObject();
            obj.put("filepath", file.getAbsoluteFile());
            obj.put("length", file.length());
            Long fileSizeBefore = file.length();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error(e);
            }
            Long fileSizeAfter = file.length();
            if (fileSizeBefore.equals(fileSizeAfter)) {
                status = "Done";
            } else {
                status = "In process";
            }
            obj.put("status", status);
            obj.put("last_modified", new Date(file.lastModified()));
            String properties = "filepath:" + file.getAbsoluteFile() + " length:" + file.length() +
                    "last_modified : " + new Date(file.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onFileDelete(final File file) {
            log.info(file.getAbsoluteFile() + " was deleted.");
            String status = "Done";
            JSONObject obj = new JSONObject();
            obj.put("filepath", file.getAbsoluteFile());
            obj.put("length", file.length());
            obj.put("last_modified", new Date(file.lastModified()));
            obj.put("status", status);
            String properties = "filepath:" + file.getAbsoluteFile() + " length:" + file.length() +
                    "last_modified : " + new Date(file.lastModified()) + "status:" + status;
            String[] propertiesArr = new String[]{properties};
            sourceEventListener.onEvent (obj.toString(), propertiesArr);
        }

        @Override
        public void onStop(final FileAlterationObserver observer) {
        }
    }

    public void usingFileAlterationObserver(SourceEventListener sourceEventListener) throws IOException {

        String folder = uri;
        String pattern = "file:/.*";

        FileObject directory = Utils.getFileObject(folder);
        if (validateUri(directory)) {
            if (directory.isFile()) {
                directory = directory.getParent();
            }
            folder = directory.getPublicURIString();
            if (folder.matches(pattern)) {
                folder = folder.substring(7);
            }


            FileAlterationObserver observer = new FileAlterationObserver(folder);

            observer.addListener(new FileAlterationImpl(sourceEventListener));


            //create a monitor to check changes after every 500 ms
            monitor = new FileAlterationMonitor(50);
            monitor.addObserver(observer);

            try {
                monitor.start();
                log.info("Starting monitor (" + folder + "). ");
            } catch (Exception e) {
                log.info(e.getMessage());
            }
        }
    }

    @Override
    public void disconnect() {
        try {
            monitor.stop(10000);
        } catch (Exception e) {
            log.error("Unable to stop");
        }
        log.info("Monitoring stopped");
    }

    public void destroy() {
    }

    public void pause() {

    }

    public void resume() {

    }

    private  boolean validateUri(FileObject directory) throws FileSystemException {
        boolean value = directory.exists();
        if (!value) {
            log.error(" Directory is not found ");
            throw new SiddhiAppCreationException(" Directory/File " + directory.getPublicURIString()
                    + " is not found ");

        }
        return value;
    }
    /**
     * FileSourceState
     */
    public class FileSourceState extends State {

        private final Map<String, Object> state;

        public FileSourceState() {
            state = new HashMap<>();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            filePointer = FileHandler.this.fileSourceConfiguration.getFilePointer();
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
