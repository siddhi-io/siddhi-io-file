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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.siddhi.query.api.definition.Attribute.Type.BOOL;
import static io.siddhi.query.api.definition.Attribute.Type.STRING;

/**
 * This extension can be used to archive files.
 */
@Extension(
        name = "archive",
        namespace = "file",
        description = "This function archives files at a given file path (recursively if given).\n",
        parameters = {
                @Parameter(
                        name = "source.path",
                        description = "The source path of the folder or the file to be archived.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "destination.folder.path",
                        description = "The destination path of the archived file.",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = "archive.recursively",
                        description = "This parameter will allow recursive folder archive.",
                        type = DataType.BOOL
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        description = "The success of the file archive.",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "from ArchiveFileStream\n" +
                                "select file:archive('/User/wso2/to_be_archived', " +
                                    "'User/wso2/archive_destination/file.zip', ' + true + ') as isArchived \n" +
                                "insert into  ResultStream;",
                        description = "This query archives a particular folder and sub-folders to a given path. " +
                                "The successfulness of the process will be returned as an boolean to the" +
                                "stream named 'RecordStream'."
                )
        }
)
public class FileArchiveExtension extends FunctionExecutor {
    private static final Logger log = Logger.getLogger(FileArchiveExtension.class);
    private Attribute.Type returnType = BOOL;
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        int executorsCount = attributeExpressionExecutors.length;
        if (executorsCount != 3) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:archive() function, "
                    + "required 2, but found " + executorsCount);
        }
        ExpressionExecutor executor1 = attributeExpressionExecutors[0];
        ExpressionExecutor executor2 = attributeExpressionExecutors[1];
        ExpressionExecutor executor3 = attributeExpressionExecutors[2];
        if (executor1.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the sourceFolder parameter " +
                    "(first argument) of file:archive() function, required " + STRING.toString() + ", but found "
                    + executor1.getReturnType().toString());
        }
        if (executor2.getReturnType() != STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the destinationFolder parameter " +
                    "(second argument) of file:archive() function, required " + STRING.toString() + ", but found "
                    + executor1.getReturnType().toString());
        }
        if (executor3.getReturnType() != BOOL) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the recursive parameter " +
                    "(third argument) of file:archive() function,required " + BOOL.toString() + ", but found "
                    + executor2.getReturnType().toString());
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        String sourceFolderUri = (String) data[0];
        String destinationFolderUri = (String) data[1];
        boolean recursive = (Boolean) data[2];
        File sourceFile = new File(sourceFolderUri);
        List<String> fileList = new ArrayList<>();
        generateFileList(sourceFolderUri, sourceFile, fileList, recursive);
        zip(sourceFolderUri, destinationFolderUri, fileList);
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

    /**
     * Zip it
     *
     * @param zipFile output ZIP file location
     */
    public void zip(String sourceFileUri, String zipFile, List<String> fileList) {
        byte[] buffer = new byte[1024];
        FileInputStream in = null;
        String filePath = null;
        ZipOutputStream zos = null;
        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            zos = new ZipOutputStream(fos);
            log.debug("Output to Zip : " + zipFile + " started.");
            for (String file : fileList) {
                log.debug("File Added : " + file + " to " + zipFile + ".");
                ZipEntry ze = new ZipEntry(file);
                zos.putNextEntry(ze);
                filePath = sourceFileUri + File.separator + file;
                in = new FileInputStream(sourceFileUri + File.separator + file);
                int len;
                while ((len = in.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                in.close();
            }
            zos.closeEntry();
            //remember close it
            zos.close();
            log.debug("Output to Zip : " + zipFile + " is complete.");
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("IOException occurred when archiving  " + sourceFileUri, e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                }
            }
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                }
            }
        }
    }

    /**
     * Traverse a directory and get all files,
     * and add the file into fileList
     *
     * @param node file or directory
     */
    public void generateFileList(String sourceFileUri, File node, List<String> fileList, boolean recursive) {
        //add file only
        if (node.isFile()) {
            fileList.add(generateZipEntry(node.getAbsoluteFile().toString(), sourceFileUri));
        }
        if (node.isDirectory() && recursive) {
            String[] subNote = node.list();
            if (subNote != null) {
                for (String filename : subNote) {
                    generateFileList(sourceFileUri, new File(node, filename), fileList, true);
                }
            }
        }
    }

    /**
     * Format the file path for zip
     *
     * @param file file path
     * @return Formatted file path
     */
    private String generateZipEntry(String file, String sourceFileUri) {
        return file.substring(sourceFileUri.length());
    }
}
