package org.wso2.extensions.siddhi.io.file;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.exceptions.ServerConnectorException;
import org.wso2.carbon.transport.file.connector.server.FileServerConnector;
import org.wso2.carbon.transport.file.connector.server.util.Constants;
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
        name = "file2",
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
public class FileSource2 extends Source{
    private static final Logger log = Logger.getLogger(FileSource2.class);

    private SourceEventListener sourceEventListener;
    private static String URI_IDENTIFIER = "uri";
    private static String TAILING_ENABLED_INDENTIFIER = "tail.file";
    private String fileURI = null;
    private Boolean isFileTailingEnabled = false;
    private FileServerConnector fileServerConnector = null;
    private FileMessageProcessor fileMessageProcessor = null;
    private FileSystemManager fileSystemManager = null;
    private FileObject fileObject = null;
    private RandomAccessContent randomAccessContent = null;
    private SiddhiAppContext siddhiAppContext = null;
    private Map<String,FileProcessor> fileProcessorMap;

    public void init(SourceEventListener sourceEventListener, OptionHolder optionHolder, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.sourceEventListener = sourceEventListener;
        this.siddhiAppContext = siddhiAppContext;
        this.fileProcessorMap = new HashMap<String,FileProcessor>();
        fileURI = optionHolder.validateAndGetStaticValue(URI_IDENTIFIER,null);
        isFileTailingEnabled = Boolean.parseBoolean(
                optionHolder.validateAndGetStaticValue(TAILING_ENABLED_INDENTIFIER,"false"));
    }

    public void connect() throws ConnectionUnavailableException {
        try {
            fileSystemManager = VFS.getManager();

            if (fileSystemManager != null) {
                fileObject = fileSystemManager.resolveFile(fileURI);
            }

            if (fileObject != null && isFileTailingEnabled) {
                try {
                    randomAccessContent = fileObject.getContent().getRandomAccessContent(RandomAccessMode.READ);
                } catch (FileSystemException e) {
                    e.printStackTrace();
                }
            }

            FileType fileType = fileObject.getType();
            if(fileType == FileType.FILE){
                FileProcessor fileProcessor = new FileProcessor(
                        siddhiAppContext,sourceEventListener, fileObject, isFileTailingEnabled);
                fileProcessorMap.put(fileURI,fileProcessor);
                fileProcessor.run();
            }else if(fileType == FileType.FOLDER){
                FileObject []fileObjects = fileObject.getChildren();
                for(FileObject file : fileObjects){
                    FileProcessor fileProcessor = new FileProcessor(
                            siddhiAppContext,sourceEventListener, fileObject, isFileTailingEnabled);
                    fileProcessorMap.put(fileURI,fileProcessor);
                    fileProcessor.run();
                }
            }

        } catch (FileSystemException e) {
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
        return null;
    }

    public void restoreState(Map<String, Object> map) {

    }
}
