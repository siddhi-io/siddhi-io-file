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
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.log4j.Logger;

import static io.siddhi.query.api.definition.Attribute.Type.LONG;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to check the size of a given file.
 */
@Extension(
        name = "size",
        namespace = "file",
        description = "This function checks for a given file's size",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The file path to be checked for a directory.",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The size of the given file.",
                        type = DataType.LONG
                )
        },
        examples = {
                @Example(
                        syntax = "from FileSizeStream\n" +
                                "select file:size('/User/wso2/source/test.txt') as fileSize\n" +
                                "insert into  ResultStream;",
                        description = "This query will be used to get the size of a file in a given path. The size " +
                                "of the file will be returned as an long value to the stream named 'RecordStream'."
                )
        }
)
public class FileSizeExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileSizeExtension.class);
    private Attribute.Type returnType = LONG;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        int executorsCount = attributeExpressionExecutors.length;
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:size() function, "
                    + "required 1, but found " + executorsCount);
        }
        ExpressionExecutor executor1 = attributeExpressionExecutors[0];
        if (executor1.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the filePath (first argument) of "
                    + "file:size() function, required " + STRING.toString() + ", but found "
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
        String sourceFileUri = (String) data;
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(sourceFileUri, opts);
            return fileObj.getContent().getSize();
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the file size of " + sourceFileUri, e);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
