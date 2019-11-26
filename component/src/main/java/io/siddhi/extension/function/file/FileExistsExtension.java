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
package io.siddhi.extension.function.file;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
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
                        name = "uri",
                        description = "File path to check for existence.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "is.directory",
                        description = "File type of the existence is a directory.\n" +
                                "Note: Existence will be be checked for both file and directory if this option is " +
                                "NOT given.",
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "is.directory"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "Value will be set to true if the path exists. False if otherwise.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "file:isExist('/User/wso2/source/test.txt') as exists",
                        description = "Checks existence of a file in the given path. Result will be returned " +
                                "as an boolean ."
                ),
                @Example(
                        syntax = "file:isExist('/User/wso2/source/') as exists",
                        description = "Checks existence of a folder in the given path. Result will be returned " +
                                "as an boolean ."
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
        if (executorsCount == 1 || executorsCount == 2) {
            ExpressionExecutor executor1 = attributeExpressionExecutors[0];
            if (executor1.getReturnType() != STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found for the uri " +
                        "(first argument) of file:isExist() function, required " + STRING.toString() + ", but found "
                        + executor1.getReturnType().toString());
            }
            if (executorsCount == 2) {
                ExpressionExecutor executor2 = attributeExpressionExecutors[1];
                if (executor2.getReturnType() != BOOL) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for the is.directory " +
                            "(second argument) of file:isExist() function,required " + BOOL.toString() +
                            ", but found " + executor2.getReturnType().toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:isExist() function, "
                    + "required 1 or 2, but found " + executorsCount);
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String fileExistPathUri = (String) data[0];
        boolean isDirectory = (Boolean) data[1];
        try {
            FileObject fileObj = Utils.getFileObject(fileExistPathUri);
            if (isDirectory) {
                return fileObj.isFolder();
            } else {
                return fileObj.isFile();
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when checking the existence of  " +
                    fileExistPathUri, e);
        }
    }

    @Override
    protected Object execute(Object data, State state) {
        String fileExistPathUri = (String) data;
        try {
            return Utils.getFileObject((String) data).exists();
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
