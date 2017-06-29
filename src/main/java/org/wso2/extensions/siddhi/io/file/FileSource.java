package org.wso2.extensions.siddhi.io.file;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.ServerConnector;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.carbon.transport.file.connector.server.FileServerConnectorProvider;
import org.wso2.carbon.transport.filesystem.connector.server.FileSystemServerConnectorProvider;
import org.wso2.extensions.siddhi.io.file.utils.Constants;
import org.wso2.extensions.siddhi.io.file.utils.FileSourceConfiguration;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.stream.input.source.Source;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by minudika on 18/5/17.
 */
@Extension(
        name = "file",
        namespace = "source",
        description = "File Source",
        parameters = {
                @Parameter(name = "enclosing.element",
                        description =
                                "Used to specify the enclosing element in case of sending multiple events in same "
                                        + "JSON message. WSO2 DAS will treat the child element of given enclosing "
                                        + "element as events"
                                        + " and execute json path expressions on child elements. If enclosing.element "
                                        + "is not provided "
                                        + "multiple event scenario is disregarded and json path will be evaluated "
                                        + "with respect to "
                                        + "root element.",
                        type = {DataType.STRING}),
                @Parameter(name = "fail.on.missing.attribute",
                        description = "This can either have value true or false. By default it will be true. This "
                                + "attribute allows user to handle unknown attributes. By default if an json "
                                + "execution "
                                + "fails or returns null DAS will drop that message. However setting this property"
                                + " to "
                                + "false will prompt DAS to send and event with null value to Siddhi where user "
                                + "can handle"
                                + " it accordingly(ie. Assign a default value)",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description =  "Above configuration will do a default JSON input mapping. Expected "
                                + "input will look like below."
                                + "{\n"
                                + "    \"event\":{\n"
                                + "        \"symbol\":\"WSO2\",\n"
                                + "        \"price\":55.6,\n"
                                + "        \"volume\":100\n"
                                + "    }\n"
                                + "}\n"),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json', "
                                + "enclosing.element=\"$.portfolio\", "
                                + "@attributes(symbol = \"company.symbol\", price = \"price\", volume = \"volume\")))",
                        description =  "Above configuration will perform a custom JSON mapping. Expected input will "
                                + "look like below."
                                + "{"
                                + " \"portfolio\":{\n"
                                + "     \"stock\":{"
                                + "        \"volume\":100,\n"
                                + "        \"company\":{\n"
                                + "           \"symbol\":\"WSO2\"\n"
                                + "       },\n"
                                + "        \"price\":55.6\n"
                                + "    }\n"
                                + "}")
        }
)
public class FileSource extends Source{
    private static final Logger log = Logger.getLogger(FileSource.class);

    private SourceEventListener sourceEventListener;
    private FileSourceConfiguration fileSourceConfiguration;
    private FileSystemServerConnectorProvider fileSystemServerConnectorProvider;
    private final static String FILE_SYSTEM_SERVER_CONNECTOR_ID = "siddhi.io.file";
    private FileServerConnector fileServerConnector = null;
    private ServerConnector serverConnector;
    private FileSystemMessageProcessor fileSystemMessageProcessor = null;
    private final String POLLING_INTERVAL = "1000";
    private Map<String,Object> currentState;
    private long filePointer = 0;

    private boolean isDirectory = false;

    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        fileSystemServerConnectorProvider = new FileSystemServerConnectorProvider();
        fileSourceConfiguration = new FileSourceConfiguration();
        fileSourceConfiguration.setUri(optionHolder.validateAndGetStaticValue(Constants.URI, null));
        fileSourceConfiguration.setMode(optionHolder.validateAndGetStaticValue(Constants.MODE, null));
        fileSourceConfiguration.setActionAfterProcess(optionHolder.validateAndGetStaticValue(
                Constants.ACTION_AFTER_PROCESS.toUpperCase(), null));
        fileSourceConfiguration.setMoveAfterProcessUri(optionHolder.validateAndGetStaticValue(
                Constants.MOVE_AFTER_PROCESS, null));
        String isTailingEnabled = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        fileSourceConfiguration.setTailingEnabled(Constants.TRUE.equalsIgnoreCase(isTailingEnabled));
        fileSourceConfiguration.setBeginRegex(optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null));
        fileSourceConfiguration.setEndRegex(optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null));
    }

    public void connect() throws ConnectionUnavailableException {
        Map<String, String> parameters = new HashMap<String,String>();

        parameters.put(Constants.TRANSPORT_FILE_DIR_URI, fileSourceConfiguration.getUri());
        parameters.put(Constants.POLLING_INTERVAL, POLLING_INTERVAL);
        if(Constants.BINARY_FULL.equalsIgnoreCase(fileSourceConfiguration.getMode()) ||
                Constants.TEXT_FULL.equalsIgnoreCase(fileSourceConfiguration.getMode())){
            parameters.put(Constants.READ_FILE_FROM_BEGINNING,Constants.TRUE);
        } else{
            parameters.put(Constants.READ_FILE_FROM_BEGINNING,Constants.FALSE);
        }
        parameters.put(Constants.ACTION_AFTER_PROCESS_KEY, "NONE");

        serverConnector = fileSystemServerConnectorProvider.createConnector(FILE_SYSTEM_SERVER_CONNECTOR_ID,
                parameters);
        fileSystemMessageProcessor = new FileSystemMessageProcessor(sourceEventListener, fileSourceConfiguration);
        serverConnector.setMessageProcessor(fileSystemMessageProcessor);

        try{
            serverConnector.start();
            fileSystemMessageProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            log.error("Exception in establishing a connection with file server for stream: "
                    + sourceEventListener.getStreamDefinition().getId(), e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {

    }

    public void destroy() {

    }

    public void pause() {

    }

    public void resume() {

    }

    public Map<String, Object> currentState() {
        currentState.put(Constants.FILE_POINTER, fileSystemMessageProcessor);
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        filePointer = (long) map.get(Constants.FILE_POINTER);
        fileSourceConfiguration.setFilePointer(filePointer);
    }

    public void tmpFileServerRun(){
        Map<String, String> properties = new HashMap<>();
        properties.put(Constants.PATH, "/home/minudika/Projects/WSO2/siddhi-io-file/testDir/test2.txt");
        properties.put(Constants.START_POSITION, fileSourceConfiguration.getFilePointer());
        properties.put(Constants.READ_FILE_FROM_BEGINNING, Constants.TRUE);
        properties.put(Constants.ACTION, Constants.READ);
        properties.put(Constants.POLLING_INTERVAL, "1000");

        FileProcessor fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration);
        FileServerConnectorProvider fileServerConnectorProvider;
        fileServerConnectorProvider = new FileServerConnectorProvider();
        ServerConnector fileServerConnector = fileServerConnectorProvider.createConnector("siddhi-io-line",
                properties);
        fileServerConnector.setMessageProcessor(fileProcessor);
        try {
            fileServerConnector.start();
            fileProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
