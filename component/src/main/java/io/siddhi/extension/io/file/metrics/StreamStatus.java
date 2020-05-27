package io.siddhi.extension.io.file.metrics;

/**
 * Enum that defines stream status.
 */
public enum StreamStatus {
    CONNECTING,
    PROCESSING,
    COMPLETED,
    IDLE,
    RETRY,
    ERROR,
}
