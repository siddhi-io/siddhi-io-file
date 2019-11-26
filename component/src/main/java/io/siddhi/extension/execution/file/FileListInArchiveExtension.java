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
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.util.Constant;
import io.siddhi.extension.util.Utils;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This class provides implementation to list files in a compressed file.
 */
@Extension(
        name = "listFilesInArchive",
        namespace = "file",
        description = "This.",
        parameters = {
                @Parameter(
                        name = "uri",
                        description = "Absolute file path of the zip or tar file.",
                        type = {DataType.STRING},
                        dynamic = true
                ),
                @Parameter(
                        name = "regexp",
                        description = "Regex pattern to be matched with file base name before listing the file.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "<Empty_String>"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"uri"}
                ),
                @ParameterOverload(
                        parameterNames = {"uri", "regexp"}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "fileName",
                        description = "The file names in the archived file.",
                        type = {DataType.STRING})},
        examples = {
                @Example(
                        syntax = "ListArchivedFileStream#file:listFilesInArchive(filePath)",
                        description = "Lists the files inside the compressed file in the given path."
                ),
                @Example(
                        syntax = "ListArchivedFileStream#file:listFilesInArchive(filePath, '.*test3.txt$')",
                        description = "Filters file names adheres to the given regex and lists the files inside the " +
                                "compressed file in the given path."
                )
        }
)
public class FileListInArchiveExtension extends StreamProcessor<State> {
    private static final Logger log = Logger.getLogger(FileListInArchiveExtension.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            String zipFilePathUri = (String) attributeExpressionExecutors[0].execute(streamEvent);
            String regex = "";
            if (attributeExpressionExecutors.length == 2) {
                regex = (String) attributeExpressionExecutors[0].execute(streamEvent);
            }
            Pattern pattern = Pattern.compile(regex);
            ZipInputStream zip = null;
            TarArchiveInputStream tarInput = null;
            try {
                FileObject fileObj = Utils.getFileObject(zipFilePathUri);
                if (fileObj.isFile()) {
                    if (zipFilePathUri.endsWith(Constant.ZIP_FILE_EXTENSION)) {
                        InputStream input = fileObj.getContent().getInputStream();
                        zip = new ZipInputStream(input);
                        ZipEntry zipEntry;
                        // iterates over entries in the zip file
                        while ((zipEntry = zip.getNextEntry()) != null) {
                            if (!zipEntry.isDirectory()) {
                                //add the entries
                                if (pattern.matcher(zipEntry.getName()).lookingAt()) {
                                    Object[] data = {zipEntry.getName()};
                                    sendEvents(streamEvent, data, streamEventChunk);
                                }
                            }
                        }
                    } else if (zipFilePathUri.endsWith(Constant.TAR_FILE_EXTENSION)) {
                        tarInput = new TarArchiveInputStream(new FileInputStream(zipFilePathUri));
                        TarArchiveEntry tarEntry;
                        while ((tarEntry = tarInput.getNextTarEntry()) != null) {
                            if (!tarEntry.isDirectory()) {
                                if (pattern.matcher(tarEntry.getName()).lookingAt()) {
                                    Object[] data = {tarEntry.getName()};
                                    sendEvents(streamEvent, data, streamEventChunk);
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new SiddhiAppRuntimeException("Error while processing file: " + zipFilePathUri + ". " +
                        e.getMessage(), e);
            } finally {
                if (zip != null) {
                    try {
                        zip.close();
                    } catch (IOException e) {
                        log.error("IO exception occurred when closing zip input stream for file path:" +
                                zipFilePathUri, e);
                    }
                }
                if (tarInput != null) {
                    try {
                        tarInput.close();
                    } catch (IOException e) {
                        log.error("IO exception occurred when closing tar input stream for file path:" +
                                zipFilePathUri, e);
                    }
                }
            }
        }
    }

    /**
     * The initialization method for {@link StreamProcessor}, which will be called before other methods and validate
     * the all configuration and getting the initial values.
     *
     * @param metaStreamEvent            the  stream event meta
     * @param abstractDefinition         the incoming stream definition
     * @param expressionExecutors        the executors for the function parameters
     * @param configReader               this hold the Stream Processor configuration reader.
     * @param streamEventClonerHolder    streamEventCloner Holder
     * @param outputExpectsExpiredEvents whether output can be expired events
     * @param findToBeExecuted           find will be executed
     * @param siddhiQueryContext         current siddhi query context
     */
    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition abstractDefinition,
                                       ExpressionExecutor[] expressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length == 1 || attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] == null) {
                throw new SiddhiAppValidationException("Invalid input given to sourceFileUri (first argument) " +
                        "file:listFilesInArchive() function. Argument cannot be null");
            }
            Attribute.Type firstAttributeType = attributeExpressionExecutors[0].getReturnType();
            if (!(firstAttributeType == Attribute.Type.STRING)) {
                throw new SiddhiAppValidationException("Invalid parameter type found for sourceFileUri " +
                        "(first argument) of file:listFilesInArchive() function, required " + Attribute.Type.STRING +
                        " but found " + firstAttributeType.toString());
            }
            if (attributeExpressionExecutors.length == 2) {
                if (attributeExpressionExecutors[1] == null) {
                    throw new SiddhiAppValidationException("Invalid input given to regex (second argument) " +
                            "file:listFilesInArchive() function. Argument cannot be null");
                }
                Attribute.Type secondAttributeType = attributeExpressionExecutors[1].getReturnType();
                if (!(secondAttributeType == Attribute.Type.STRING)) {
                    throw new SiddhiAppValidationException("Invalid parameter type found for regex " +
                            "(second argument) of file:listFilesInArchive() function, required " +
                            Attribute.Type.STRING + " but found " + secondAttributeType.toString());
                }
            }
        } else {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to file:listFilesInArchive() " +
                    "function, required 1, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(new Attribute("fileName", Attribute.Type.STRING));
        return attributes;
    }

    private void sendEvents(StreamEvent streamEvent, Object[] data, ComplexEventChunk<StreamEvent> streamEventChunk) {
        complexEventPopulater.populateComplexEvent(streamEvent, data);
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
