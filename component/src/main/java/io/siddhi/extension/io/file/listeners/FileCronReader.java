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
import org.wso2.carbon.messaging.exceptions.ClientConnectorException;
import org.wso2.transport.file.connector.sender.VFSClientConnector;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * FileCron Listener is executted when the cron expression is given. If the current time satisfied by the cron
 * expression then the file processing will be executed.
 */
public class FileCronReader implements Job {
    private static final Logger log = Logger.getLogger(FileSystemListener.class);
    SourceEventListener sourceEventListener;

    public FileCronReader() {
    }

    public static void scheduleJob(FileSourceConfiguration fileSourceConfiguration, FileProcessor fileProcessor,
                                   VFSClientConnector vfsClientConnector) {
        try {
            JobKey jobKey = new JobKey(Constants.JOB_NAME, Constants.JOB_GROUP);

            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            fileSourceConfiguration.setScheduler(scheduler);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
            scheduler.start();
            JobDataMap dataMap = new JobDataMap();
            dataMap.put(Constants.FILE_SOURCE_CONFIGURATION, fileSourceConfiguration);
            dataMap.put(Constants.FILE_PROCESSOR, fileProcessor);
            dataMap.put(Constants.VFS_CLIENT_CONNECTOR, vfsClientConnector);
            JobDetail cron = JobBuilder.newJob(FileCronReader.class)
                    .usingJobData(dataMap)
                    .withIdentity(jobKey)
                    .build();
            Trigger trigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(Constants.TRIGGER_NAME, Constants.TRIGGER_GROUP)
                    .withSchedule(
                            CronScheduleBuilder.cronSchedule(fileSourceConfiguration.getCronExpression())).build();
            scheduler.scheduleJob(cron, trigger);
        } catch (SchedulerException e) {
            log.error("The error occurs at scheduler start : " + e);
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        FileSourceConfiguration fileSourceConfiguration = (FileSourceConfiguration) dataMap.get(
                Constants.FILE_SOURCE_CONFIGURATION);
        FileProcessor fileProcessor = (FileProcessor) dataMap.get(Constants.FILE_PROCESSOR);
        File listeningFileObject = new File(fileSourceConfiguration.getUri());
        if (listeningFileObject.isDirectory()) {
            File[] listOfFiles = listeningFileObject.listFiles();
            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        String fileURI = file.toURI().toString();
                        Map<String, String> properties = new HashMap<>();
                        properties.put(Constants.URI, fileURI);
                        properties.put(Constants.ACTION, Constants.READ);
                        properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
                        VFSClientConnector vfsClientConnector =
                                (VFSClientConnector) dataMap.get(Constants.VFS_CLIENT_CONNECTOR);
                        vfsClientConnector.setMessageProcessor(fileProcessor);
                        VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                        FileSystemListener fileSystemListener = new FileSystemListener
                                (sourceEventListener, fileSourceConfiguration);
                        fileSystemListener.initialProcessFile(vfsClientConnector, carbonCallback, properties, fileURI,
                                fileSourceConfiguration, fileProcessor);
                    }
                }
            }
        } else {
            String fileURI = listeningFileObject.toURI().toString();
            try {
                Map<String, String> properties = new HashMap<>();
                properties.put(Constants.URI, fileURI);
                properties.put(Constants.ACTION, Constants.READ);
                properties.put(Constants.CRON_EXPRESSION, fileSourceConfiguration.getCronExpression());
                VFSClientConnector vfsClientConnector = (VFSClientConnector)
                        dataMap.get(Constants.VFS_CLIENT_CONNECTOR);
                vfsClientConnector.setMessageProcessor(fileProcessor);
                VFSClientConnectorCallback carbonCallback = new VFSClientConnectorCallback();
                vfsClientConnector.send(null, carbonCallback, properties);
                carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
                if (fileSourceConfiguration.getActionAfterProcess() != null) {
                    properties.put(Constants.URI, fileURI);
                    properties.put(Constants.ACTION, fileSourceConfiguration.getActionAfterProcess());
                    if (fileSourceConfiguration.getMoveAfterProcess() != null) {
                        properties.put(Constants.DESTINATION, fileSourceConfiguration.getMoveAfterProcess());
                    }
                    vfsClientConnector.send(null, carbonCallback, properties);
                    carbonCallback.waitTillDone(fileSourceConfiguration.getTimeout(), fileURI);
                }
            } catch (ClientConnectorException e) {
                log.error(String.format("Failure occurred in vfs-client while reading the file " +
                        fileURI + " through siddhi app.", e));
            } catch (InterruptedException e) {
                log.error(String.format("Failed to get callback from vfs-client for file " +
                        fileURI + " through siddhi app.", e));
            }
        }
    }
}
