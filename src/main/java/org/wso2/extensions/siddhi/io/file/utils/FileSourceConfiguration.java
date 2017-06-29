package org.wso2.extensions.siddhi.io.file.utils;

/**
 * Created by minudika on 20/6/17.
 */
public class FileSourceConfiguration {
    public String getBeginRegex() {
        return beginRegex;
    }

    public void setBeginRegex(String beginRegex) {
        this.beginRegex = beginRegex;
    }

    public String getEndRegex() {
        return endRegex;
    }

    public void setEndRegex(String endRegex) {
        this.endRegex = endRegex;
    }

    public long getFilePointer() {
        return filePointer;
    }

    public void setFilePointer(long filePointer) {
        this.filePointer = filePointer;
    }

    private enum MODE {
        TEXT_FULL,
        BINARY_FULL,
        REGEX,
        LINE
    };
    private String actionAfterProcess;
    private String moveAfterProcessUri;
    private boolean isTailingEnabled;
    private String uri;
    private String mode;
    private String beginRegex = null;
    private String endRegex = null;
    private long filePointer = 0;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getActionAfterProcess() {
        return actionAfterProcess;
    }

    public void setActionAfterProcess(String actionAfterProcess) {
        this.actionAfterProcess = actionAfterProcess;
    }

    public String getMoveAfterProcessUri() {
        return moveAfterProcessUri;
    }

    public void setMoveAfterProcessUri(String moveAfterProcessUri) {
        this.moveAfterProcessUri = moveAfterProcessUri;
    }

    public boolean isTailingEnabled() {
        return isTailingEnabled;
    }

    public void setTailingEnabled(boolean tailingEnabled) {
        isTailingEnabled = tailingEnabled;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
