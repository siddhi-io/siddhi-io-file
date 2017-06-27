package org.wso2.extensions.siddhi.io.file.utils;

/**
 * Created by minudika on 19/6/17.
 */
public class Constants {
    /* configuration parameters*/
    public final static String URI = "uri";
    public final static String MODE = "mode";
    public final static String ACTION_AFTER_PROCESS = "action.after.process";
    public final static String MOVE_AFTER_PROCESS = "move.after.process";
    public final static String APPEND = "append";
    public final static String WRITE = "write";
    public final static String TAILING = "tailing";
    public final static String BEGIN_REGEX = "begin.regex";
    public final static String END_REGEX = "end.regex";

    /* configuration param values*/
    public final static String MOVE = "move";
    public final static String DELETE = "delete";
    public final static String TEXT_FULL = "text.full";
    public final static String BINARY_FULL = "binary.full";
    public final static String REGEX = "regex";
    public final static String LINE = "line";
    public final static String TRUE = "true";
    public final static String FALSE = "false";

    /*property keys*/
    public final static String ACTION = "action";
    public final static String FILE_POINTER = "filePointer";
    public static final String TRANSPORT_FILE_FILE_URI = "fileURI";
    public static final String POLLING_INTERVAL = "pollingInterval";
    public static final String READ_FILE_FROM_BEGINNING = "readFromBeginning";



    /*property values*/

    private static enum MODE {
        TEXT_FULL,
        BINARY_FULL,
        REGEX,
        LINE
    };

}
