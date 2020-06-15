/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.io.file.listeners;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.file.processors.FileProcessor;
import io.siddhi.extension.io.file.util.Constants;
import io.siddhi.extension.io.file.util.FileSourceConfiguration;
import io.siddhi.extension.io.file.util.VFSClientConnectorCallback;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.wso2.carbon.messaging.BinaryCarbonMessage;
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.siddhi.extension.io.file.util.Util.constructPath;
import static io.siddhi.extension.io.file.util.Util.generateProperties;
import static io.siddhi.extension.io.file.util.Util.getFileName;
import static io.siddhi.extension.io.file.util.Util.reProcessFileGenerateProperties;

/**
 * FileCronExecutor is executed when the cron expression is given. If the current time satisfied by the cron
 * expression then the file processing will be executed.
 */
public class FileCronExecutor implements Job {
    private static final Logger log = Logger.getLogger(FileCronExecutor.class);

    public FileCronExecutor() {
    }

    /**
     * To initialize the cron job to execute at given cron expression
     */
    public static void scheduleJob(FileSourceConfiguration fileSourceConfiguration,
                                   SourceEventListener sourceEventListener, SiddhiAppContext siddhiAppContext) {
        try {
            JobKey jobKey = new JobKey(Constants.JOB_NAME, Constants.JOB_GROUP);

            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            fileSourceConfiguration.setScheduler(scheduler);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
            scheduler.start();
            // JobDataMap used to access the object in the job class
            JobDataMap dataMap = new JobDataMap();
            dataMap.put(Constants.FILE_SOURCE_CONFIGURATION, fileSourceConfiguration);
            dataMap.put(Constants.SOURCE_EVENT_LISTENER, sourceEventListener);

            // Define instances of Jobs
            JobDetail cron = JobBuilder.newJob(FileCronExecutor.class)
                    .usingJobData(dataMap)
                    .withIdentity(jobKey)
                    .build();
            //Trigger the job to at given cron expression
            Trigger trigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(Constants.TRIGGER_NAME, Constants.TRIGGER_GROUP)
                    .withSchedule(
                            CronScheduleBuilder.cronSchedule(fileSourceConfiguration.getCronExpression())).build();
            // Tell quartz to schedule the job using our trigger
            scheduler.scheduleJob(cron, trigger);
        } catch (SchedulerException e) {
            log.error("The error occurs at scheduler start in SiddhiApp " + siddhiAppContext.getName() + " : " + e);
        }
    }

    /**
     * Method gets called when the cron Expression satisfies the system time
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        FileSourceConfiguration fileSourceConfiguration = (FileSourceConfiguration) dataMap.get(
                Constants.FILE_SOURCE_CONFIGURATION);
        SourceEventListener sourceEventListener = (SourceEventListener) dataMap.get(
                Constants.SOURCE_EVENT_LISTENER);
        File listeningFileObject = new File(fileSourceConfiguration.getUri());
        if (listeningFileObject.isDirectory()) {
            File[] listOfFiles = listeningFileObject.listFiles();
            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        processFile(file.toURI().toString(), jobExecutionContext, sourceEventListener);
                    }
                }
            }
        } else {
            processFile(listeningFileObject.toURI().toString(), jobExecutionContext, sourceEventListener);
        }
    }

    /**
     * Action taken while processing a file
     */
    public void processFile(String fileURI, JobExecutionContext jobExecutionContext,
                            SourceEventListener sourceEventListener) {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        FileSourceConfiguration fileSourceConfiguration = (FileSourceConfiguration) dataMap.get(
                Constants.FILE_SOURCE_CONFIGURATION);
        FileProcessor fileProcessor = new FileProcessor(sourceEventListener, fileSourceConfiguration, null);
        VFSClientConnector vfsClientConnector = new VFSClientConnector();
        vfsClientConnector.setMessageProcessor(fileProcessor);
        Map<String, String> properties = generateProperties(fileSourceConfiguration, fileURI);
        VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
        initialProcessFile(vfsClientConnector, carbonCallback, properties, fileURI,
                fileSourceConfiguration, fileProcessor);
    }

    public void initialProcessFile(VFSClientConnector vfsClientConnector, VFSClientConnectorCallback carbonCallback,
                                   Map<String, String> properties, String fileURI,
                                   FileSourceConfiguration fileSourceConfiguration, FileProcessor fileProcessor) {
        vfsClientConnector.setMessageProcessor(fileProcessor);
        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                fileURI.getBytes(StandardCharsets.UTF_8)), true);
        try {
            vfsClientConnector.send(carbonMessage, carbonCallback, properties);
            try {
                carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
            } catch (InterruptedException e) {
                log.error(String.format("Failed to get callback from vfs-client  for file '%s'.",
                        fileURI), e);
            }
            reProcessFile(vfsClientConnector, carbonCallback, properties, fileURI, fileSourceConfiguration);
        } catch (ClientConnectorException e) {
            log.error(String.format("Failed to provide file '%s' for consuming.", fileURI), e);
        }
    }

    /**
     * Method use to move file from one path to another if action.after.process is 'move'
     */
    public void reProcessFile(VFSClientConnector vfsClientConnector,
                              VFSClientConnectorCallback vfsClientConnectorCallback,
                              Map<String, String> properties, String fileUri,
                              FileSourceConfiguration fileSourceConfiguration) {
        BinaryCarbonMessage carbonMessage = new BinaryCarbonMessage(ByteBuffer.wrap(
                fileUri.getBytes(StandardCharsets.UTF_8)), true);
        String moveAfterProcess = fileSourceConfiguration.getMoveAfterProcess();
        Map<String, String> reGeneratedProperties = reProcessFileGenerateProperties(fileSourceConfiguration, fileUri,
                properties);
        try {
            File file = new File(fileSourceConfiguration.getUri());
            if (file.isFile()) {
                reGeneratedProperties.put(Constants.DESTINATION, moveAfterProcess);
            } else {
                String destination = constructPath(moveAfterProcess, getFileName(fileUri,
                        fileSourceConfiguration.getProtocolForMoveAfterProcess()));
                if (destination != null) {
                    reGeneratedProperties.put(Constants.DESTINATION, destination);
                }
            }
            vfsClientConnector.send(carbonMessage, vfsClientConnectorCallback, reGeneratedProperties);
            vfsClientConnectorCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileUri);
        } catch (ClientConnectorException e) {
            log.error(String.format("Failure occurred in vfs-client while reading the file '%s '.", fileUri), e);
        } catch (InterruptedException e) {
            log.error(String.format("Failed to get callback from vfs-client for file '%s '.", fileUri), e);
        }
    }
}
