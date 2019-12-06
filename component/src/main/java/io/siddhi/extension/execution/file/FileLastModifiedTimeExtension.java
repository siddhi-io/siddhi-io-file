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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;

import static io.siddhi.extension.util.Constant.LAST_MODIFIED_DATETIME_FORMAT;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to check the last modified datetime of a file.
 */
@Extension(
        name = "lastModifiedTime",
        namespace = "file",
        description = "Checks for the last modified time for a given file path",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "File path to be checked for te last modified time.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "datetime.format",
                        description = "Format of the last modified datetime to be returned.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "MM/dd/yyyy HH:mm:ss"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "datetime.format"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "Last modified date time of a file in the given format.",
                        type = DataType.STRING
                )
        },
        examples = {
                @Example(
                        syntax = "file:lastModifiedTime(filePath) as lastModifiedTime",
                        description = "Last modified datetime of a file will be returned as an string in " +
                                LAST_MODIFIED_DATETIME_FORMAT + "."
                ),
                @Example(
                        syntax = "file:lastModifiedTime(filePath, dd/MM/yyyy HH:mm:ss) as lastModifiedTime",
                        description = "Last modified datetime of a file will be returned as an string in " +
                                "'dd/MM/yyyy HH:mm:ss' format."
                )
        }
)
public class FileLastModifiedTimeExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileLastModifiedTimeExtension.class);
    private Attribute.Type returnType = STRING;
    private SimpleDateFormat simpleDateFormat = null;
    private int inputExecutorLength;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (attributeExpressionExecutors.length >= 2) {
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                simpleDateFormat = new SimpleDateFormat(((ConstantExpressionExecutor)
                        attributeExpressionExecutors[1]).getValue().toString());
            }
        } else {
            simpleDateFormat = new SimpleDateFormat(LAST_MODIFIED_DATETIME_FORMAT);
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String sourceFileUri = (String) data[0];
        if (inputExecutorLength == 2 && simpleDateFormat == null) {
            String pattern = (String) data[1];
            if (pattern != null && !pattern.isEmpty()) {
                try {
                    simpleDateFormat = new SimpleDateFormat(pattern);
                } catch (IllegalArgumentException e) {
                    throw new SiddhiAppRuntimeException("Illegal pattern: " + pattern +
                            ", given to the datetime pattern ", e);
                }
            } else {
                throw new SiddhiAppRuntimeException("Exception occurred when getting datetime pattern. " +
                        "Pattern is either null or empty " + sourceFileUri);
            }
        }
        try {
            FileObject fileObj = Utils.getFileObject(sourceFileUri);
            return simpleDateFormat.format(fileObj.getContent().getLastModifiedTime());
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the last modified datetime of " +
                    sourceFileUri, e);
        }
    }

    @Override
    protected Object execute(Object data, State state) {
        String sourceFileUri = (String) data;
        try {
            FileObject fileObj = Utils.getFileObject(sourceFileUri);
            SimpleDateFormat sdf;
            sdf = new SimpleDateFormat(LAST_MODIFIED_DATETIME_FORMAT);
            return sdf.format(fileObj.getContent().getLastModifiedTime());
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the last modified datetime of " +
                    sourceFileUri, e);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
