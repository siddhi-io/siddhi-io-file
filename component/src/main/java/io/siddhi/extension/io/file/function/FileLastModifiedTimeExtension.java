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

import java.text.SimpleDateFormat;

import static io.siddhi.extension.io.file.util.Constants.LAST_MODIFIED_DATETIME_FORMAT;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to check the last modified datetime of a file.
 */
@Extension(
        name = "lastModifiedTime",
        namespace = "file",
        description = "This function checks for the last modified time for a given file path",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The file path to be checked for te last modified time.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "datetime.format",
                        description = "The file path to be checked for the last modified datetime.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "MM/dd/yyyy HH:mm:ss"
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The last modified date time of a file in the given format.",
                        type = DataType.STRING
                )
        },
        examples = {
                @Example(
                        syntax = "from FileLastModifiedStream\n" +
                                "select file:lastModifiedTime(filePath) as lastModifiedTime\n" +
                                "insert into  ResultStream;",
                        description = "This query checks last modified datetime of a file. " +
                                "Result will be returned as an string in " + LAST_MODIFIED_DATETIME_FORMAT +
                                " format to the stream named 'RecordStream'."
                ),
                @Example(
                        syntax = "from FileLastModifiedStream\n" +
                                "select file:lastModifiedTime(filePath, dd/MM/yyyy HH:mm:ss) as lastModifiedTime\n" +
                                "insert into  ResultStream;",
                        description = "This query checks last modified datetime of a file. " +
                                "Result will be returned as an string in 'dd/MM/yyyy HH:mm:ss' format " +
                                "to the stream named 'RecordStream'."
                )
        }
)
public class FileLastModifiedTimeExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileLastModifiedTimeExtension.class);
    private Attribute.Type returnType = STRING;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        int executorsCount = attributeExpressionExecutors.length;
        if (executorsCount == 1 || executorsCount == 2) {
            ExpressionExecutor executor1 = attributeExpressionExecutors[0];
            if (executor1.getReturnType() != STRING) {
                throw new SiddhiAppValidationException("Invalid parameter type found for the filePath " +
                        "(first argument) of file:lastModifiedTime() function, required " + STRING.toString() +
                        ", but found " + executor1.getReturnType().toString());

            }
            if (executorsCount == 2) {
                ExpressionExecutor executor2 = attributeExpressionExecutors[1];
                if (executor2.getReturnType() != STRING) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for the datetimeFormat " +
                            "(second argument) of file:lastModifiedTime() function,required " + STRING.toString() +
                            ", but found " + executor2.getReturnType().toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:lastModifiedTime() " +
                    "function, required 2, but found " + executorsCount);
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String sourceFileUri = (String) data[0];
        String pattern = (String) data[1];
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(sourceFileUri, opts);
            SimpleDateFormat sdf;
            if (pattern != null && !pattern.isEmpty()) {
                sdf = new SimpleDateFormat(pattern);
            } else {
                sdf = new SimpleDateFormat(LAST_MODIFIED_DATETIME_FORMAT);
            }
            return sdf.format(fileObj.getContent().getLastModifiedTime());
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the last modified datetime of " +
                    sourceFileUri, e);
        } catch (IllegalArgumentException e) {
            throw new SiddhiAppRuntimeException("Illegal pattern: " + pattern + ", given to the datetime pattern ", e);
        }
    }

    @Override
    protected Object execute(Object data, State state) {
        String sourceFileUri = (String) data;
        FileSystemOptions opts = new FileSystemOptions();
        FileSystemManager fsManager;
        try {
            fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(sourceFileUri, opts);
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
