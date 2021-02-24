/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

/**
 * Constants used in siddhi-io-file extension.
 */
public class Constants {

    /* configuration parameters*/
    public static final String URI = "uri";
    public static final String PATH = "path";
    public static final String MODE = "mode";
    public static final String ACTION_AFTER_PROCESS = "action.after.process";
    public static final String ACTION_AFTER_FAILURE = "action.after.failure";
    public static final String MOVE_AFTER_PROCESS = "move.after.process";
    public static final String MOVE_AFTER_FAILURE = "move.after.failure";
    public static final String APPEND = "append";
    public static final String WRITE = "write";
    public static final String READ = "read";
    public static final String TAILING = "tailing";
    public static final String BEGIN_REGEX = "begin.regex";
    public static final String END_REGEX = "end.regex";
    public static final String DIR_URI = "dir.uri";
    public static final String FILE_URI = "file.uri";
    public static final String FILE_NAME_LIST = "file.name.list";
    public static final String DIRECTORY_POLLING_INTERVAL = "dir.polling.interval";
    public static final String FILE_POLLING_INTERVAL = "file.polling.interval";
    public static final String MONITORING_INTERVAL = "monitoring.interval";
    public static final String TIMEOUT = "timeout";
    public static final String ADD_EVENT_SEPARATOR = "add.line.separator";
    public static final String FILE_READ_WAIT_TIMEOUT = "file.read.wait.timeout";
    public static final int WAIT_TILL_DONE = 5000;
    public static final String HEADER_PRESENT = "header.present";
    public static final String READ_ONLY_HEADER = "read.only.header";
    public static final String CRON_EXPRESSION = "cron.expression";
    public static final String FILE_NAME_PATTERN = "file.name.pattern";
    public static final String FILE_NAME_PATTERN_PROPERTY_NAME = "fileNamePattern";
    public static final String FILE_SYSTEM_OPTIONS = "file.system.options";

    /* configuration param values*/
    public static final String MOVE = "move";
    public static final String DELETE = "delete";
    public static final String KEEP = "keep";
    public static final String TEXT_FULL = "text.full";
    public static final String BINARY_FULL = "binary.full";
    public static final String BINARY_CHUNKED = "binary.chunked";
    public static final String REGEX = "regex";
    public static final String LINE = "line";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String NONE = "none";
    public static final int BUFFER_SIZE = 4096;
    public static final String BUFFER_SIZE_IN_BINARY_CHUNKED = "buffer.size";

    /*property keys*/
    public static final String ACTION = "action";
    public static final String FILE_POINTER = "filePointer";
    public static final String FILE_POINTER_MAP = "filePointerMap";
    public static final String TRANSPORT_FILE_FILE_URI = "fileURI";
    public static final String TRANSPORT_FILE_DIR_URI = "dirURI";
    public static final String TRANSPORT_FILE_URI = "uri";
    public static final String POLLING_INTERVAL = "pollingInterval";
    public static final String READ_FILE_FROM_BEGINNING = "readFromBeginning";
    public static final String ACTION_AFTER_PROCESS_KEY = "actionAfterProcess";
    public static final String ACTION_AFTER_FAILURE_KEY = "actionAfterFailure";
    public static final String MOVE_AFTER_PROCESS_KEY = "moveAfterProcess";
    public static final String MOVE_AFTER_FAILURE_KEY = "moveAfterFailure";
    public static final String START_POSITION = "startPosition";
    public static final String MAX_LINES_PER_POLL = "maxLinesPerPoll";
    public static final String FILE_SORT_ATTRIBUTE = "fileSortAttribute";
    public static final String FILE_SORT_ASCENDING = "fileSortAscending";
    public static final String CREATE_MOVE_DIR = "createMoveDir";
    public static final String ACK_TIME_OUT = "ackTimeOut";
    public static final String DESTINATION = "destination";
    public static final String FILE_READ_WAIT_TIMEOUT_KEY = "fileReadWaitTimeout";
    public static final String CURRENT_POSITION = "currentPosition";
    public static final String STREAM_DEFINITION_SOURCE_ANNOTATION_NAME = "source";
    public static final String STREAM_DEFINITION_MAP_ANNOTATION_NAME = "map";
    public static final String ANNOTATION_TYPE_ELEMENT_NAME = "type";
    public static final String MAP_ANNOTATION_BINARY_TYPE = "binary";
    public static final String SOURCE_ANNOTATION_FILE_TYPE_NAME = "file";
    public static final String SOURCE_EVENT_LISTENER = "SourceEventListener";
    public static final String FILE_SOURCE_CONFIGURATION = "FileSourceConfiguration";
    public static final String JOB_GROUP = "JobGroup";
    public static final String JOB_NAME = "JobName";
    public static final String TRIGGER_NAME = "TriggerName";
    public static final String TRIGGER_GROUP = "TriggerGroup";


    /*source property keys*/
    public static final String TAILED_FILE = "tailedFile";
    public static final String TAILING_REGEX_STRING_BUILDER = "regexStringBuilder";

    /*property values*/
    public static final String ACTION_AFTER_PROCESS_DELETE = "DELETE";
    public static final String ACTION_AFTER_PROCESS_MOVE = "MOVE";
    public static final String NAME = "name";
    public static final String SIZE = "size";
    public static final String LAST_MODIFIED_TIMESTAMP = "lastModifiedTimestamp";

    public static final String UTF_8 = "UTF-8";

    public static final String PROCESSED_FILE_LIST = "processedFileList";

    /*prometheus reporte values*/
    public static final String PROMETHEUS_REPORTER_NAME = "prometheus";
}
