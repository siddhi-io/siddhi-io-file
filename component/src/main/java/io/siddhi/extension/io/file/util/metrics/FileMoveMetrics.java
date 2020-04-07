package io.siddhi.extension.io.file.util.metrics;

import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsManagement;

public class FileMoveMetrics extends Metrics {

    private String _source;
    private String destination;
    private long time;

    public FileMoveMetrics(String siddhiAppName) {
        super(siddhiAppName);
    }

    public void getMoveMetric(int status) {
        MetricsManagement.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Operations.Move.%s.%s.%s",
                        siddhiAppName, time, destination, _source), Level.INFO,() -> status);
    }

    public void set_source(String _source) {
        this._source = _source;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
