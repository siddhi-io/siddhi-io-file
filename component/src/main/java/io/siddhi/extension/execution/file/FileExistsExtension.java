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
package io.siddhi.extension.execution.file;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;

import static io.siddhi.query.api.definition.Attribute.Type.BOOL;

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
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. " +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "file.system.options"}
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
    private String fileSystemOptions = null;
    private int inputExecutorLength;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 2 &&
                attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue().toString();
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return checkIsExist((String) data[0]);
    }

    @Override
    protected Object execute(Object data, State state) {
        return checkIsExist((String) data);
    }

    private Object checkIsExist(String fileExistPathUri) {
        try {
            return Utils.getFileObject(fileExistPathUri, fileSystemOptions).exists();
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
