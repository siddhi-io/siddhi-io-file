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
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.siddhi.extension.io.file.util.Constants.BUFFER_SIZE;
import static io.siddhi.query.api.definition.Attribute.Type.BOOL;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to unarchive a file.
 */
@Extension(
        name = "unarchive",
        namespace = "file",
        description = "This function decompresses a given file",
        parameters = {
                @Parameter(
                        name = "file.path",
                        description = "The path of the file to be decompressed.",
                        type = DataType.STRING
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The success of the file decompress process.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "from UnArchiveFileStream\n" +
                                "select file:unarchive('/User/wso2/source/test.zip', '/User/wso2/destination') " +
                                "as success\n" +
                                "insert into  ResultStream;",
                        description = "This query will be used unarchive a file in a given path. The successfulness " +
                                "of the process will be returned as an boolean to the stream named 'RecordStream'."
                )
        }
)
public class FileUnarchiveExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileUnarchiveExtension.class);
    private Attribute.Type returnType = BOOL;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        int executorsCount = attributeExpressionExecutors.length;
        if (executorsCount != 2) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:move() function, "
                    + "required 2, but found " + executorsCount);
        }
        ExpressionExecutor executor1 = attributeExpressionExecutors[0];
        ExpressionExecutor executor2 = attributeExpressionExecutors[1];
        if (executor1.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the filePath (first argument) of "
                    + "file:unarchive() function, required " + STRING.toString() + ", but found "
                    + executor1.getReturnType().toString());
        }
        if (executor2.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the destinationPath " +
                    "(second argument) of file:unarchive() function, required " + STRING.toString() + ", but found "
                    + executor1.getReturnType().toString());
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String filePathUri = (String) data[0];
        String destinationPathUri = (String) data[1];
        File dir = new File(destinationPathUri);
        // create output directory if it doesn't exist
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new SiddhiAppRuntimeException("Directory cannot be created to unzip the file: " + filePathUri);
            }
        }
        FileInputStream fis;
        //buffer for read and write data to file
        byte[] buffer = new byte[BUFFER_SIZE];
        ZipInputStream zis = null;
        String filePath = null;
        FileOutputStream fos = null;
        try {
            fis = new FileInputStream(filePathUri);
            zis = new ZipInputStream(fis);
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();
                filePath = destinationPathUri + File.separator + fileName;
                File newFile = new File(filePath);
                if (log.isDebugEnabled()) {
                    log.debug("Decompressing: " + newFile.getAbsolutePath());
                }
                //create directories for sub directories in zip
                if (!newFile.getParentFile().exists()) {
                    boolean suDirCreateResult = new File(newFile.getParent()).mkdirs();
                    if (!suDirCreateResult) {
                        throw new SiddhiAppRuntimeException("Subdirectories cannot be created to decompress the " +
                                "file: " + filePathUri);
                    }
                }
                fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                //close this ZipEntry
                zis.closeEntry();
                ze = zis.getNextEntry();
            }
            //close last ZipEntry
            zis.closeEntry();
            zis.close();
            fis.close();
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the decompressing file: " +
                    filePathUri, e);
        } finally {
            if (zis != null) {
                try {
                    zis.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                }
            }
        }
        return true;
    }

    @Override
    protected Object execute(Object data, State state) {
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
