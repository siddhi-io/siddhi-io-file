/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.io.file.function;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.siddhi.extension.io.file.util.Constants.WAIT_TILL_DONE;
import static io.siddhi.query.api.definition.Attribute.Type.BOOL;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to delete a file.
 */
@Extension(
        name = "delete",
        namespace = "file",
        description = "This function deletes file/files in a particular path",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The file path of the files to be deleted.",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The success of the file deletion.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "from DeleteFileStream\n" +
                                "select file:delete('/User/wso2/source/test.txt') as deleted\n" +
                                "insert into  ResultStream;",
                        description = "This query deletes a particular file in the given path. " +
                                "The successfulness of the process will be returned as an boolean to the" +
                                "stream named 'RecordStream'."
                )
        }
)
public class FileDeleteExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileDeleteExtension.class);
    Attribute.Type returnType = BOOL;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        int executorsCount = attributeExpressionExecutors.length;
        if (executorsCount != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:move() function, "
                    + "required 1, but found " + executorsCount);
        }
        ExpressionExecutor executor1 = attributeExpressionExecutors[0];
        if (executor1.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the fileDeletePath " +
                    "(first argument) of file:delete() function, required " + STRING.toString() + ", but found "
                    + executor1.getReturnType().toString());

        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null;
    }

    @Override
    protected Object execute(Object data, State state) {
        VFSClientConnector vfsClientConnector = new VFSClientConnector();
        VFSClientConnectorCallback vfsClientConnectorCallback = new VFSClientConnectorCallback();
        String fileDeletePathUri = (String) data;
        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                fileDeletePathUri.getBytes(StandardCharsets.UTF_8)), true);
        Map<String, String> properties = new HashMap<>();
        properties.put(Constants.URI, fileDeletePathUri);
        properties.put(Constants.ACTION, Constants.DELETE);
        try {
            vfsClientConnector.send(carbonMessage, vfsClientConnectorCallback, properties);
            vfsClientConnectorCallback.waitTillDone(WAIT_TILL_DONE, fileDeletePathUri);
        } catch (ClientConnectorException e) {
            throw new SiddhiAppRuntimeException("Failure occurred in vfs-client while deleting the file " +
                    fileDeletePathUri, e);
        } catch (InterruptedException e) {
            throw new SiddhiAppRuntimeException("Failed to get callback from vfs-client for file " +
                    fileDeletePathUri, e);
        }
        return true;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
