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

package io.siddhi.extension.io.file.util;

import io.siddhi.core.event.Event;

import java.io.File;
import java.util.List;

/**
 * Util Class.
 * This method used to get the fileHandlerEvent
 */
public class Util {
    public static Event getFileHandlerEvent(final File file, List<String> fileObjectList, Status enumStatus) {
        boolean listenerEventsURLValidated = false;
        String status;
        switch (enumStatus) {
            case STATUS_NEW: status = "created"; break;
            case STATUS_PROCESS: status = "modifying"; break;
            case STATUS_DONE: status = "modifyingCompleted"; break;
            case STATUS_REMOVE: status = "removed"; break;
            default: throw new IllegalStateException("Unexpected value: " + enumStatus);
        }
        if (fileObjectList.contains(file.getAbsolutePath())) {
            listenerEventsURLValidated = true;
        }
        //If the fileObjectList contains this file
        for (String fileObjectPath : fileObjectList) {
            File fileObject = new File(fileObjectPath);
            if (fileObject.isDirectory()) {
                //If a fileObject is a folder then the events for the files in the folder should thrown.
                if (file.getAbsolutePath().startsWith(fileObjectPath)) {
                    listenerEventsURLValidated = true;
                }
            }
        }
        if (listenerEventsURLValidated) {
            Object[] obj = {file.getAbsolutePath(), file.getName(), status};
            return new Event(System.currentTimeMillis(), obj);
        }
        return null;
    }
}
