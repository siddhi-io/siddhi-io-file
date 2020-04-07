package io.siddhi.extension.io.file.util.metrics;

import org.wso2.carbon.metrics.core.Level;
import org.wso2.carbon.si.metrics.core.internal.MetricsManagement;

public class FileDeleteMetrics extends Metrics {

    private String _source;
    private long time;

    public FileDeleteMetrics(String siddhiAppName) {
        super(siddhiAppName);
    }

    public void getDeleteMetric(int status) {
        MetricsManagement.getInstance().getMetricService()
                .gauge(String.format("io.siddhi.SiddhiApps.%s.Siddhi.File.Operations.Delete.%s.%s",
                        siddhiAppName, time, _source), Level.INFO,() -> status);
    }

    public void set_source(String _source) {
        this._source = _source;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
