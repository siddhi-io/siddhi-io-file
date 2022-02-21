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
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.util.ArrayList;
import java.util.List;

/**
 * This extension can be used to create a file or a folder.
 */
@Extension(
        name = "create",
        namespace = "file",
        description = "Create a file or a folder in the given location",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path which needs to be created.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "is.directory",
                        description = "This flag is used when creating file path is a directory",
                        type = DataType.BOOL,
                        dynamic = true,
                        optional = true,
                        defaultValue = "false"
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
                        parameterNames = {"uri", "is.directory"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "is.directory", "file.system.options"}
                )
        },
        examples = {
                @Example(
                        syntax = "from CreateFileStream#file:create('/User/wso2/source/test.txt', false)",
                        description = "Creates a file in the given path with the name of 'test.txt'."
                ),
                @Example(
                        syntax = "from CreateFileStream#file:create('/User/wso2/source/', true)",
                        description = "Creates a folder in the given path with the name of 'source'."
                )
        }
)
public class FileCreateExtension extends StreamFunctionProcessor {
    private int inputExecutorLength;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 3 &&
                attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue().toString();
        }
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<>();
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    protected Object[] process(Object[] data) {
        String fileSourcePath = (String) data[0];
        boolean isDirectory;
        if (inputExecutorLength >= 2) {
            isDirectory = (Boolean) data[1];
        } else {
            isDirectory = false;
        }
        FileObject rootFileObject = Utils.getFileObject(fileSourcePath, fileSystemOptions);
        try {
            if (isDirectory) {
                rootFileObject.createFolder();
            } else {
                rootFileObject.createFile();
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Failure occurred when creating the file " + fileSourcePath, e);
        }
        return new Object[0];
    }

    @Override
    protected Object[] process(Object data) {
        return new Object[0];
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
