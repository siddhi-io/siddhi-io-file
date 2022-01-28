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
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "datetime.format",
                        description = "Format of the last modified datetime to be returned.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "MM/dd/yyyy HH:mm:ss"
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. \n" +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'\n" +
                                "Note: when IDENTITY is used, use a RSA PRIVATE KEY",
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
                        parameterNames = {"uri", "datetime.format"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "datetime.format", "file.system.options"}
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
    private static final long serialVersionUID = 1L;
    private Attribute.Type returnType = STRING;
    private SimpleDateFormat simpleDateFormat = null;
    private int inputExecutorLength;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 1) {
            simpleDateFormat = new SimpleDateFormat(LAST_MODIFIED_DATETIME_FORMAT);
        }
        if (inputExecutorLength >= 2) {
            if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
                String dataFormatString =
                        ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue().toString();
                if (!dataFormatString.trim().isEmpty()) {
                    simpleDateFormat = new SimpleDateFormat(dataFormatString);
                } else {
                    simpleDateFormat = new SimpleDateFormat(LAST_MODIFIED_DATETIME_FORMAT);
                }
            }
        }
        if (inputExecutorLength == 3 &&
                attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue().toString();
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
            FileObject fileObj = Utils.getFileObject(sourceFileUri, fileSystemOptions);
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
            FileObject fileObj = Utils.getFileObject(sourceFileUri, fileSystemOptions);
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
