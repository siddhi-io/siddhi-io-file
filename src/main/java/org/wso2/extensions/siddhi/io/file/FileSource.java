package org.wso2.extensions.siddhi.io.file;

import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
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
    private static String URI_IDENTIFIER = "uri";
    private static String FILE_SERVER_CONNECTOR_ID = "siddhi.io.file";
    private String fileURI = null;
    private FileServerConnector fileServerConnector = null;
    private FileMessageProcessor fileMessageProcessor = null;
    private String mode = null;
    private String moveAfterAction = null;
    private String actionAfterProcess = null;
    private Map<String,Object> currentState;
    private long filePointer = 0;

    private boolean isDirectory = false;

    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        fileSourceConfiguration = new FileSourceConfiguration();
        fileSourceConfiguration.setUri(optionHolder.validateAndGetStaticValue(Constants.URI, null));
        fileSourceConfiguration.setMode(optionHolder.validateAndGetStaticValue(Constants.MODE, null));
        fileSourceConfiguration.setActionAfterProcess(optionHolder.validateAndGetStaticValue(
                Constants.ACTION_AFTER_PROCESS, null));
        fileSourceConfiguration.setMoveAfterProcessUri(optionHolder.validateAndGetStaticValue(
                Constants.MOVE_AFTER_PROCESS, null));
        String isTailingEnabled = optionHolder.validateAndGetStaticValue(Constants.TAILING, Constants.TRUE);
        fileSourceConfiguration.setTailingEnabled(Constants.TRUE.equalsIgnoreCase(isTailingEnabled));
        fileSourceConfiguration.setBeginRegex(optionHolder.validateAndGetStaticValue(Constants.BEGIN_REGEX, null));
        fileSourceConfiguration.setEndRegex(optionHolder.validateAndGetStaticValue(Constants.END_REGEX, null));
    }

    public void connect() throws ConnectionUnavailableException {
        Map<String, String> parameters = new HashMap<String,String>();
        parameters.put(Constants.TRANSPORT_FILE_FILE_URI, fileSourceConfiguration.getUri());
        parameters.put(Constants.POLLING_INTERVAL, "1000");
        parameters.put(Constants.FILE_POINTER, Long.toString(filePointer));
        parameters.put(Constants.READ_FILE_FROM_BEGINNING,Constants.TRUE);
        fileServerConnector = new FileServerConnector(FILE_SERVER_CONNECTOR_ID,parameters);
        fileMessageProcessor = new FileMessageProcessor(sourceEventListener, fileSourceConfiguration);
        fileMessageProcessor = new FileMessageProcessor(sourceEventListener, fileSourceConfiguration);
        fileServerConnector.setMessageProcessor(fileMessageProcessor);
        try{
            fileServerConnector.start();
            fileMessageProcessor.waitTillDone();
        } catch (ServerConnectorException e) {
            log.error("Exception in establishing a connection with file server for stream: "
                    + sourceEventListener.getStreamDefinition().getId(), e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        fileMessageProcessor.getFileContent();
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
        currentState.put(Constants.FILE_POINTER, fileMessageProcessor);
        return currentState;
    }

    public void restoreState(Map<String, Object> map) {
        filePointer = (long) map.get(Constants.FILE_POINTER);
        fileMessageProcessor.setFilePointer(filePointer);
    }
}
