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
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
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
                        name = "uri",
                        description = "Absolute path to the file or directory to be checked for the size.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. " +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true",
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
                        description = "Size of the given file or the directory.",
                        type = DataType.LONG
                )
        },
        examples = {
                @Example(
                        syntax = "file:size('/User/wso2/source/test.txt') as fileSize",
                        description = "Size of a file in a given path will be returned."
                )
        }
)
public class FileSizeExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileSizeExtension.class);
    private Attribute.Type returnType = LONG;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        int inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength > 2) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:size() function, "
                    + "required 2, but found " + inputExecutorLength);
        }
        if (inputExecutorLength == 2 &&
                attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue().toString();
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
        return calculateFileSizeRecusrively(Utils.getFileObject((String) data[0], fileSystemOptions));
    }

    @Override
    protected Object execute(Object data, State state) {
        return calculateFileSizeRecusrively(Utils.getFileObject((String) data, fileSystemOptions));
    }

    public static long calculateFileSizeRecusrively(FileObject fileObj) {
        long size = 0;
        try {
            if (fileObj.isFolder()) {
                FileObject[] files = fileObj.getChildren();
                for (FileObject file : files) {
                    size += calculateFileSizeRecusrively(file);
                }
            } else {
                size += fileObj.getContent().getSize();
            }

            return size;
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the children of  " +
                    fileObj.getName().getURI(), e);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
