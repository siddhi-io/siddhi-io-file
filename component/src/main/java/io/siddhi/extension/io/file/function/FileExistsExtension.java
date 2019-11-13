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

import static io.siddhi.query.api.definition.Attribute.Type.BOOL;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to check if a file or a folder exists or not.
 */
@Extension(
        name = "isExist",
        namespace = "file",
        description = "This function checks whether a file or a folder exists in a given path",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The file path to check for existence.",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The value of the return parameter will contain whether the path exists or not.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "from CheckExistsFileStream\n" +
                                "select file:isExist('/User/wso2/source/test.txt') as exists\n" +
                                "insert into  ResultStream;",
                        description = "This query checks for the existence of a file in the given path. " +
                                "Result will be returned as an boolean to the stream named 'RecordStream'."
                )
        }
)
public class FileExistsExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileExistsExtension.class);
    private Attribute.Type returnType = BOOL;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        int executorsCount = attributeExpressionExecutors.length;
        if (executorsCount != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:isExist() function, "
                    + "required 1, but found " + executorsCount);
        }
        ExpressionExecutor executor1 = attributeExpressionExecutors[0];
        if (executor1.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the fileExistPath " +
                    "(first argument) of file:isExist() function, required " + STRING.toString() + ", but found "
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
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        String fileExistPathUri = (String) data;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(fileExistPathUri, opts);
            return fileObj.exists();
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking the existence of  " +
                    fileExistPathUri, e);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
