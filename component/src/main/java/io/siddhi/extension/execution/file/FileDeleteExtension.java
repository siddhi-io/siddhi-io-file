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
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.io.file.metrics.FileDeleteMetrics;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.Selectors;
import org.apache.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;

import java.util.ArrayList;
import java.util.List;

/**
 * This extension can be used to delete a file or a folder.
 */
@Extension(
        name = "delete",
        namespace = "file",
        description = "Deletes file/files in a particular path",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute path of the file or the directory to be deleted.",
                        type = DataType.STRING,
                        dynamic = true
                )
        },
        examples = {
                @Example(
                        syntax = "from DeleteFileStream#file:delete('/User/wso2/source/test.txt')",
                        description = "Deletes the file in the given path. "
                ),
                @Example(
                        syntax = "from DeleteFileStream#file:delete('/User/wso2/source/')",
                        description = "Deletes the folder in the given path. "
                )
        }
)
public class FileDeleteExtension extends StreamFunctionProcessor {
    private static final Logger log = Logger.getLogger(FileDeleteExtension.class);
    private FileDeleteMetrics fileDeleteMetrics;

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        if (MetricsDataHolder.getInstance().getMetricService() != null &&
                MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService().isReporterRunning(
                        Constants.PROMETHEUS_REPORTER_NAME)) {
                    String siddhiAppName = siddhiQueryContext.getSiddhiAppContext().getName();
                    fileDeleteMetrics = new FileDeleteMetrics(siddhiAppName);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence file metrics will not be initialized.");
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
        return new Object[0];
    }

    @Override
    protected Object[] process(Object data) {
        String fileDeletePathUri = (String) data;
        if (fileDeleteMetrics != null) {
            fileDeleteMetrics.setSource(fileDeletePathUri);
            fileDeleteMetrics.setTime(System.currentTimeMillis());
        }
        try {
            FileObject rootFileObject = Utils.getFileObject(fileDeletePathUri);
            rootFileObject.delete(Selectors.SELECT_ALL);
            if (fileDeleteMetrics != null) {
                fileDeleteMetrics.getDeleteMetric(1);
            }
        } catch (FileSystemException e) {
            if (fileDeleteMetrics != null) {
                fileDeleteMetrics.getDeleteMetric(0);
            }
            throw new SiddhiAppRuntimeException("Failure occurred when deleting the file " + fileDeletePathUri, e);
        }
        return new Object[0];
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
}
