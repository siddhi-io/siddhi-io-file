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
import io.siddhi.extension.util.Constant;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.siddhi.extension.io.file.util.Constants.BUFFER_SIZE;

/**
 * This extension can be used to unarchive a file.
 */
@Extension(
        name = "unarchive",
        namespace = "file",
        description = "This function decompresses a given file",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file to be decompressed in the format of zip or tar.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute path of the destination directory.\n" +
                                "Note: If the folder structure does not exist, it will be created.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "exclude.root.dir",
                        description = "This flag excludes parent folder when extracting the content.",
                        type = DataType.BOOL,
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
                        parameterNames = {"uri", "destination.dir.uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "exclude.root.dir"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "exclude.root.dir", "file.system.options"}
                )
        },
        examples = {
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.zip', '/User/wso2/destination')",
                        description = "Unarchive a zip file in a given path to a given destination."
                ),
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination')",
                        description = "Unarchive a tar file in a given path to a given destination."
                ),
                @Example(
                        syntax = "file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination', true)",
                        description = "Unarchive a tar file in a given path to a given destination excluding " +
                                "the root folder."
                )
        }
)
public class FileUnarchiveExtension extends StreamFunctionProcessor {
    private static final Logger log = LogManager.getLogger(FileUnarchiveExtension.class);
    private int inputExecutorLength;
    private String fileSystemOptions = null;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (inputExecutorLength == 4 &&
                attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue().toString();
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
        String filePathUri = (String) data[0];
        String destinationDirUri = (String) data[1];
        boolean excludeRootFolder = false;
        if (inputExecutorLength == 3) {
            excludeRootFolder = (Boolean) data[2];
        }
        FileObject sourceFileObject = Utils.getFileObject(filePathUri, fileSystemOptions);
        String sourceFileExtension = sourceFileObject.getName().getExtension();
        if (!excludeRootFolder) {
            if (destinationDirUri.endsWith(File.separator)) {
                destinationDirUri = destinationDirUri.substring(0, destinationDirUri.length() - 1);
            }
            String fileName = sourceFileObject.getName().getBaseName();
            destinationDirUri =
                    destinationDirUri.concat(File.separator +
                            fileName.substring(0, fileName.lastIndexOf(sourceFileExtension) - 1));
        }
        FileObject destinationDirFile = Utils.getFileObject(destinationDirUri, fileSystemOptions);
        // create output directory if it doesn't exist
        try {
            if (!destinationDirFile.exists() || !destinationDirFile.isFolder()) {
                destinationDirFile.createFolder();
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException("Directory cannot be created to unzip the file: " +
                    filePathUri);
        }
        FileInputStream fis = null;
        //buffer for read and write data to file
        byte[] buffer = new byte[BUFFER_SIZE];
        ZipInputStream zis = null;
        String filePath = null;
        FileOutputStream fos = null;
        try {
            if (sourceFileExtension.compareToIgnoreCase(Constant.ZIP_FILE_EXTENSION) == 0) {
                fis = new FileInputStream(filePathUri);
                zis = new ZipInputStream(fis);
                ZipEntry ze = zis.getNextEntry();
                while (ze != null) {
                    String fileName = ze.getName();
                    if (!fileName.isEmpty()) {
                        filePath = destinationDirUri + File.separator + fileName;
                        File newFile = new File(filePath);
                        if (log.isDebugEnabled()) {
                            log.debug("Decompressing: " + newFile.getAbsolutePath());
                        }
                        createParentDirectory(newFile, filePathUri);
                        fos = new FileOutputStream(newFile);
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                        fos.close();
                    }
                    //Closes the current ZIP entry and positions the stream for reading the next entry
                    zis.closeEntry();
                    ze = zis.getNextEntry();
                }
            } else if (sourceFileExtension.compareToIgnoreCase(Constant.TAR_FILE_EXTENSION) == 0) {
                try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(filePathUri))) {
                    TarArchiveEntry entry;
                    while ((entry = fin.getNextTarEntry()) != null) {
                        if (entry.isDirectory()) {
                            continue;
                        }
                        File curfile = new File(destinationDirFile.getName().getPath(), entry.getName());
                        createParentDirectory(curfile, filePathUri);
                        IOUtils.copy(fin, new FileOutputStream(curfile));
                    }
                }
            } else {
                throw new SiddhiAppRuntimeException("Unsupported extension found for file: " + filePathUri +
                        ". Function only supports zip and tar files.");
            }
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Exception occurred when getting the decompressing file: " +
                    filePathUri, e);
        } finally {
            if (zis != null) {
                try {
                    zis.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing zip input stream for file path: " + filePath);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file output stream for file path: " + filePath);
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file input stream for file path: " + filePath);
                }
            }
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

    private void createParentDirectory(File file, String filePathUri) {
        if (!file.getParentFile().exists()) {
            boolean suDirCreateResult = new File(file.getParent()).mkdirs();
            if (!suDirCreateResult) {
                throw new SiddhiAppRuntimeException("Subdirectories cannot be created to decompress the " +
                        "file: " + filePathUri);
            }
        }
    }
}
