/*
 * Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.io.file.metrics.FileRenameMetrics;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.ArrayList;
import java.util.List;

/**
 *
 **/
@Extension(
        name = "rename",
        namespace = "file",
        description = "This method can be used to rename a file/folder in a particular path, move a file from to a " +
                "different path. \n" +
                "Ex- \n" +
                " file:rename('/User/wso2/source', 'User/wso2/destination') \n" +
                " file:rename('/User/wso2/source/file.csv', 'User/wso2/source/newFile.csv') \n " +
                " file:rename('/User/wso2/source/file.csv', 'User/wso2/destination/file.csv')",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file or the directory to be rename.",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "new.destination.name",
                        description = "Absolute path of the new file/folder",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "file.system.options",
                        description = "The file options in key:value pairs separated by commas. \n" +
                                "eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true," +
                                "IDENTITY:<Realative path from '<Product_Home>/wso2/server/' directory>," +
                                "IDENTITY_PASS_PHRASE:wso2carbon'\n" +
                                "Note: when IDENTITY is used, use a RSA PRIVATE KEY",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri", "new.destination.name"}
                ),
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "isSuccess",
                        description = "Status of the file rename operation (true if success)",
                        type = DataType.BOOL
                )
        },
        examples = {
                @Example(
                        syntax = "InputStream#" +
                                "file:rename('/User/wso2/source/', 'User/wso2/destination/')",
                        description = "Rename the file resides in 'source' folder to 'destination' folder."
                ),
                @Example(
                        syntax = "InputStream#" +
                                "file:rename('/User/wso2/folder/old.csv', 'User/wso2/folder/new.txt')",
                        description = "Rename 'old.csv' file resides in folder to 'new.txt'"
                )
        }
)
public class FileRenameExtension extends StreamFunctionProcessor {
    private static final Logger log = LogManager.getLogger(FileRenameExtension.class);
    private String fileSystemOptions = null;

    private FileRenameMetrics fileRenameMetrics;

    @Override
    protected Object[] process(Object[] objects) {
        return renameFileOrFolder((String) objects[0], (String) objects[1]);
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }

    @Override
    protected StateFactory init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length == 3) {
            fileSystemOptions = ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue().toString();
        }
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        Constants.PROMETHEUS_REPORTER_NAME)) {
                    String siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
                    fileRenameMetrics = new FileRenameMetrics(siddhiAppName);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence file metrics will not be initialized.");
            }
        }
        return null;

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("isSuccess", Attribute.Type.BOOL));
        return attributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    private Object[] renameFileOrFolder(String oldFileOrFolderName, String newFileOrFolderName) {

        if (fileRenameMetrics != null) {
            fileRenameMetrics.setSource(oldFileOrFolderName);
            fileRenameMetrics.setTime(System.currentTimeMillis());
        }
        FileObject oldFileObject = Utils.getFileObject(oldFileOrFolderName, fileSystemOptions);
        FileObject newFileObject = Utils.getFileObject(newFileOrFolderName, fileSystemOptions);

        if (oldFileObject.canRenameTo(newFileObject)) {
            try {
                newFileObject.copyFrom(oldFileObject, Selectors.SELECT_ALL);
            } catch (FileSystemException e) {
                log.error("Error while copying the content from " + oldFileOrFolderName + " to "
                        + newFileOrFolderName + ": " + e.getMessage());
                if (fileRenameMetrics != null) {
                    fileRenameMetrics.getRenameMetric(0);
                }
                return new Object[]{false};
            }

            try {
                oldFileObject.delete(Selectors.SELECT_ALL);
            } catch (FileSystemException e) {
                log.error("Error while deleting the file " + oldFileOrFolderName + " after renaming ",
                        e);
                if (fileRenameMetrics != null) {
                    fileRenameMetrics.getRenameMetric(0);
                }
            }
        } else {
            log.error("Cannot rename the given file " + oldFileOrFolderName + " to " + newFileOrFolderName);
            return new Object[]{false};
        }
        if (fileRenameMetrics != null) {
            fileRenameMetrics.getRenameMetric(1);
        }
        return new Object[]{true};
    }
}
