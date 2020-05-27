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
import io.siddhi.extension.io.file.metrics.FileArchiveMetrics;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.siddhi.extension.util.Constant.TAR_FILE_EXTENSION;
import static io.siddhi.extension.util.Constant.ZIP_FILE_EXTENSION;

/**
 * This extension can be used to archive files.
 */
@Extension(
        name = "archive",
        namespace = "file",
        description = "Archives files and folders as a zip or in tar format that are available in the given file " +
                "uri.\n",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file or the directory",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "destination.dir.uri",
                        description = "Absolute directory path of the the archived file.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "archive.type",
                        description = "Archive type can be zip or tar",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "zip"
                ),
                @Parameter(
                        name = "include.by.regexp",
                        description = "Only the files matching the patterns will be archived.\n" +
                                "Note: Add an empty string to match all files",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                ),
                @Parameter(
                        name = "exclude.subdirectories",
                        description = "This flag is used to exclude the subdirectories and its files without " +
                                "archiving.",
                        type = DataType.BOOL,
                        defaultValue = "false",
                        optional = true
                ),
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type", "include.by.regexp"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "destination.dir.uri", "archive.type", "include.by.regexp",
                                "exclude.subdirectories"}
                )
        },
        examples = {
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file.zip')",
                        description = "Archives to_be_archived folder in zip format and stores archive_destination " +
                                "folder as file.zip."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', 'tar')",
                        description = "Archives to_be_archived folder in tar format and stores in " +
                                "archive_destination folder as file.tar."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', 'tar', '.*test3.txt$')",
                        description = "Archives files which adheres to '.*test3.txt$' regex in to_be_archived " +
                                "folder in tar format and stores in archive_destination folder as file.tar."
                ),
                @Example(
                        syntax = "InputStream#file:archive('/User/wso2/to_be_archived', " +
                                "'/User/wso2/archive_destination/file', '', '', 'false')",
                        description = "Archives to_be_archived folder excluding the sub-folders in zip format and " +
                                "stores in archive_destination folder as file.tar."
                )
        }
)
public class FileArchiveExtension extends StreamFunctionProcessor {
    private static final Logger log = Logger.getLogger(FileArchiveExtension.class);
    private Pattern pattern = null;
    private int inputExecutorLength;
    private String siddhiAppName;
    private FileArchiveMetrics fileArchiveMetrics;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;
        if (attributeExpressionExecutors.length >= 4 &&
                attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            pattern = Pattern.compile(
                    ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue().toString());
        }
        siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                    "prometheus")) {
                fileArchiveMetrics = new FileArchiveMetrics(siddhiAppName);
            }
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
        String uri = (String) data[0];
        String destinationDirUri = (String) data[1];
        if (destinationDirUri.lastIndexOf(File.separator) != destinationDirUri.length() - 1) {
            destinationDirUri = destinationDirUri + File.separator;
        }
        boolean excludeSubdirectories = false;
        String regex = "";
        String archiveType = ZIP_FILE_EXTENSION;
        if (inputExecutorLength >= 3) {
            archiveType = (String) data[2];
        }
        if (inputExecutorLength >= 4) {
            regex = (String) data[3];
        }
        if (inputExecutorLength == 5) {
            excludeSubdirectories = (Boolean) data[4];
        }
        if (pattern == null) {
            pattern = Pattern.compile(regex);
        }
        FileObject destinationDirUriObject = Utils.getFileObject(destinationDirUri);
        try {
            if (!destinationDirUriObject.exists() || !destinationDirUriObject.isFolder()) {
                destinationDirUriObject.createFolder();
            }
        } catch (FileSystemException e) {
            throw new SiddhiAppRuntimeException(
                    "Exception occurred when creating the subdirectories for the destination directory  " +
                            destinationDirUriObject.getName().getPath(), e);
        }
        if (fileArchiveMetrics !=  null) {
            fileArchiveMetrics.setSource(Utils.getShortFilePath(uri));
            fileArchiveMetrics.setDestination(Utils.getShortFilePath(destinationDirUri));
            fileArchiveMetrics.setType(Utils.getShortFilePath(archiveType));
            fileArchiveMetrics.setTime(System.currentTimeMillis());
        }
        File sourceFile = new File(uri);
        String destinationFile = destinationDirUri + sourceFile.getName();

        if (archiveType.compareToIgnoreCase(ZIP_FILE_EXTENSION) == 0) {
            List<String> fileList = new ArrayList<>();
            generateFileList(uri, sourceFile, fileList, excludeSubdirectories);
            try {
                zip(uri, destinationFile, fileList);

            } catch (IOException e) {
                if (fileArchiveMetrics !=  null) {
                    fileArchiveMetrics.getArchiveMetric(0);
                }
                throw new SiddhiAppRuntimeException("IOException occurred when archiving  " + uri, e);
            }
        } else {
            try {
                if (archiveType.compareToIgnoreCase(TAR_FILE_EXTENSION) == 0) {
                    addToTarArchiveCompression(
                            getTarArchiveOutputStream(destinationFile), sourceFile, uri);

                } else {
                    throw new SiddhiAppRuntimeException("Unsupported archive type: " + archiveType);
                }
            } catch (IOException e) {
                if (fileArchiveMetrics !=  null) {
                    fileArchiveMetrics.getArchiveMetric(0);
                }
                throw new SiddhiAppRuntimeException("Exception occurred when archiving " + uri, e);
            }
        }
        if (fileArchiveMetrics !=  null) {
            fileArchiveMetrics.getArchiveMetric(1);
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

    /**
     * Zip it
     *
     * @param zipFile output ZIP file location
     */
    private void zip(String sourceFileUri, String zipFile, List<String> fileList) throws IOException {
        byte[] buffer = new byte[1024];
        FileInputStream in = null;
        ZipOutputStream zos = null;
        FileOutputStream fos = null;
        try {
            if (!zipFile.endsWith(ZIP_FILE_EXTENSION)) {
                zipFile = zipFile.concat("." + ZIP_FILE_EXTENSION);
            }
            fos = new FileOutputStream(zipFile);
            zos = new ZipOutputStream(fos);
            if (log.isDebugEnabled()) {
                log.debug("Output to Zip : " + zipFile + " started for folder/ file: " + sourceFileUri);
            }
            for (String file : fileList) {
                if (log.isDebugEnabled()) {
                    log.debug("File Adding : " + file + " to " + zipFile + ".");
                }
                ZipEntry ze = new ZipEntry(file);
                zos.putNextEntry(ze);
                in = new FileInputStream(sourceFileUri + File.separator + file);
                int len;
                while ((len = in.read(buffer)) > 0) {
                    zos.write(buffer, 0, len);
                }
                in.close();
            }
            zos.closeEntry();
            if (log.isDebugEnabled()) {
                log.debug("Output to Zip : " + zipFile + " is complete for folder/ file: " + sourceFileUri);
            }
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing zip input stream for file path: " + sourceFileUri);
                }
            }
            if (zos != null) {
                try {
                    zos.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing zip output stream for file path: " + sourceFileUri);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    log.error("IO exception occurred when closing file output stream for file path: " + sourceFileUri);
                }
            }
        }
    }

    private void addToTarArchiveCompression(TarArchiveOutputStream out, File file, String sourceFileUri)
            throws IOException {
        if (file.isFile()) {
            String entryName = generateZipEntry(file.getAbsoluteFile().toString(), sourceFileUri);
            TarArchiveEntry entry = new TarArchiveEntry(file, entryName);
            out.putArchiveEntry(entry);
            FileInputStream fin = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fin);
            IOUtils.copy(bis, out);
            out.closeArchiveEntry();
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    addToTarArchiveCompression(out, child, sourceFileUri);
                }
            }
        } else {
            log.error(file.getName() + " is not supported for archiving. Archiving process continues..");
        }
    }

    private static TarArchiveOutputStream getTarArchiveOutputStream(String name) throws IOException {
        if (!name.endsWith(TAR_FILE_EXTENSION)) {
            name = name.concat("." + TAR_FILE_EXTENSION);
        }
        TarArchiveOutputStream taos = new TarArchiveOutputStream(new FileOutputStream(name));
        // TAR has an 8 gig file limit by default, this gets around that
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
        // TAR originally didn't support long file names, so enable the support for it
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        taos.setAddPaxHeadersForNonAsciiNames(true);
        return taos;
    }

    /**
     * Traverse a directory and get all files,
     * and add the file into fileList
     *
     * @param node file or directory
     */
    private void generateFileList(String sourceFileUri, File node, List<String> fileList,
                                  boolean excludeSubdirectories) {
        //add file only
        if (node.isFile() && pattern.matcher(node.getName()).lookingAt()) {
            fileList.add(generateZipEntry(node.getAbsoluteFile().toString(), sourceFileUri));
        }
        if (node.isDirectory() && !excludeSubdirectories) {
            String[] subNote = node.list();
            if (subNote != null) {
                for (String filename : subNote) {
                    generateFileList(sourceFileUri, new File(node, filename), fileList, false);
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
