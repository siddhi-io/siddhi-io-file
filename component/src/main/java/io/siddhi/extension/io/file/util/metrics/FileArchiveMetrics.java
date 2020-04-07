package io.siddhi.extension.io.file.util.metrics;

import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsManagement;

public class FileArchiveMetrics extends Metrics {

    private String destination;
    private String type;
    protected String _source;
    protected Long time;

    public FileArchiveMetrics(String siddhiAppName) {
        super(siddhiAppName);
    }

    public void getArchiveMetric(int status) {
        MetricsManagement.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Operations.Archived.%s.%s.%s.%s",
                        siddhiAppName, type, time, _source, destination), Level.INFO,() -> status);
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void set_source(String _source) {
        this._source = _source;
    }

    public void setTime(Long time) {
        this.time = time;
    }
}
