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

package io.siddhi.extension.io.file;

import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.extension.io.file.util.Status;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.siddhi.extension.io.file.util.Util.getFileHandlerEvent;

/**
 * Implementation of  FileAlterationImpl  implements FileAlterationListener
 *  This class throw events if any file or Directory gets created, modified or deleted.
 */
public class FileAlterationImpl implements FileAlterationListener {
    private static final Logger log = LogManager.getLogger(FileAlterationImpl.class);
    private Map<String, Long> fileObjectMap = new ConcurrentHashMap<>();
    private SourceEventListener sourceEventListener;
    private List<String> fileObjectList;
    private Status enumStatus;

    public FileAlterationImpl(SourceEventListener sourceEventListener, List<String> fileObjectList) {
        this.sourceEventListener = sourceEventListener;
        this.fileObjectList = fileObjectList;
    }

    @Override
    public void onStart(final FileAlterationObserver observer) {
    }

    @Override
    public void onDirectoryCreate(final File directory) {
        log.debug(directory.getAbsolutePath() + " was created.");
        fileObjectMap.put(directory.getAbsolutePath(), directory.length());
        sourceEventListener.onEvent(getFileHandlerEvent(directory, fileObjectList, Status.STATUS_NEW), null);
    }

    @Override
    public void onDirectoryChange(final File directory) {
        log.debug(directory.getAbsolutePath() + " was modified.");
        fileObjectMap.put(directory.getAbsolutePath(), directory.lastModified());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error("Error occurred in Thread.sleep() " , e);
        }
        long recentModifiedTimestamp = fileObjectMap.get(directory.getAbsolutePath());
        if (recentModifiedTimestamp < directory.lastModified()) {
            enumStatus = Status.STATUS_PROCESS;
            fileObjectMap.put(directory.getAbsolutePath(), directory.lastModified());
        } else {
            enumStatus = Status.STATUS_DONE;
        }
        sourceEventListener.onEvent(getFileHandlerEvent(directory, fileObjectList, enumStatus), null);
    }

    @Override
    public void onDirectoryDelete(final File directory) {
        log.debug(directory.getAbsolutePath() + " was deleted.");
        fileObjectMap.remove(directory.getAbsolutePath());
        sourceEventListener.onEvent(getFileHandlerEvent(directory, fileObjectList, Status.STATUS_REMOVE), null);
    }

    @Override
    public void onFileCreate(final File file) {
        log.debug(file.getAbsolutePath() + " was created.");
        fileObjectMap.put(file.getAbsolutePath(), file.length());
        sourceEventListener.onEvent(getFileHandlerEvent(file, fileObjectList, Status.STATUS_NEW), null);
    }

    @Override
    public void onFileChange(final File file) {
        log.debug(file.getAbsolutePath() + " was modified.");
        fileObjectMap.put(file.getAbsolutePath(), file.lastModified());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error("Error occurred in Thread.sleep() " , e);
        }
        long recentModifiedTimestamp = fileObjectMap.get(file.getAbsolutePath());
        if (recentModifiedTimestamp < file.lastModified()) {
            enumStatus = Status.STATUS_PROCESS;
            fileObjectMap.put(file.getAbsolutePath(), file.lastModified());
        } else {
            enumStatus = Status.STATUS_DONE;
        }
        sourceEventListener.onEvent(getFileHandlerEvent(file, fileObjectList, enumStatus), null);
    }

    @Override
    public void onFileDelete(final File file) {
        log.debug(file.getAbsolutePath() + " was deleted.");
        fileObjectMap.remove(file.getAbsolutePath());
        sourceEventListener.onEvent(getFileHandlerEvent(file, fileObjectList, Status.STATUS_REMOVE), null);
    }

    @Override
    public void onStop(final FileAlterationObserver observer) {

    }
}
